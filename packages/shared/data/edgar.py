"""SEC EDGAR filing connector.

Fetches 8-K, 10-Q, 10-K, S-1, and SC 13D/13G filings from the SEC EDGAR
system for portfolio tickers and stores them as events in the PostgreSQL
database.  The EDGAR full-text search API is free and requires no API key
— only a compliant ``User-Agent`` header.

Rate-limit contract: max 10 req/s to SEC servers (we stay well under with
a 120 ms sleep between requests).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from ..db.engine import get_shared_engine

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_FORM_TYPES: list[str] = [
    "8-K",
    "10-K",
    "10-Q",
    "S-1",
    "SC 13D",
    "SC 13G",
]

_SEVERITY_BY_FORM: dict[str, int] = {
    "8-K": 75,
    "10-K": 60,
    "10-Q": 50,
    "S-1": 70,
    "SC 13D": 80,
    "SC 13G": 80,
}

_REASON_CODE_BY_FORM: dict[str, str] = {
    "8-K": "8k_material_event",
    "10-K": "10k_annual_report",
    "10-Q": "10q_quarterly_report",
    "S-1": "s1_offering",
    "SC 13D": "sc13d_activist_holder",
    "SC 13G": "sc13g_large_holder",
}

_DEFAULT_SEVERITY = 40
_DEFAULT_REASON_CODE = "sec_filing"

# ---------------------------------------------------------------------------
# Module-level CIK cache
# ---------------------------------------------------------------------------

_cik_cache: dict[str, str] | None = None
_cik_cache_ts: float = 0.0
_CIK_CACHE_TTL_SECONDS: float = 86_400.0  # 24 hours

# ---------------------------------------------------------------------------
# SEC request helpers
# ---------------------------------------------------------------------------


def _get_sec_headers() -> dict[str, str]:
    """Return HTTP headers compliant with SEC EDGAR requirements.

    The SEC mandates a ``User-Agent`` that identifies the requester with a
    company name and administrative contact email.
    """
    return {
        "User-Agent": "TradingWorkstation admin@localhost",
        "Accept-Encoding": "gzip, deflate",
        "Accept": "application/json",
    }


# ---------------------------------------------------------------------------
# CIK mapping
# ---------------------------------------------------------------------------


async def _load_cik_mapping() -> dict[str, str]:
    """Fetch and cache the SEC ticker → CIK mapping.

    The mapping is refreshed once every 24 hours.  Each CIK is returned as a
    zero-padded 10-digit string suitable for use in EDGAR submission URLs.

    Returns:
        Dictionary mapping uppercase ticker symbols to padded CIK strings.
    """
    global _cik_cache, _cik_cache_ts

    now = time.monotonic()
    if _cik_cache is not None and (now - _cik_cache_ts) < _CIK_CACHE_TTL_SECONDS:
        return _cik_cache

    url = "https://www.sec.gov/files/company_tickers.json"
    logger.info("edgar_loading_cik_mapping", url=url)

    try:
        async with httpx.AsyncClient(
            headers=_get_sec_headers(), timeout=30.0
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()

        mapping: dict[str, str] = {}
        for _idx, entry in data.items():
            ticker = str(entry.get("ticker", "")).upper().strip()
            cik_int = entry.get("cik_str")
            if ticker and cik_int is not None:
                mapping[ticker] = str(cik_int).zfill(10)

        _cik_cache = mapping
        _cik_cache_ts = now

        logger.info("edgar_cik_mapping_loaded", tickers_count=len(mapping))
        return mapping

    except Exception:
        logger.error("edgar_cik_mapping_error", exc_info=True)
        # Return stale cache if available, otherwise empty dict
        if _cik_cache is not None:
            logger.warning("edgar_using_stale_cik_cache")
            return _cik_cache
        return {}


# ---------------------------------------------------------------------------
# Fetch recent filings for a CIK
# ---------------------------------------------------------------------------


async def _fetch_recent_filings(
    cik: str,
    form_types: list[str] | None = None,
) -> list[dict]:
    """Fetch recent filings for a CIK from the EDGAR submissions endpoint.

    Args:
        cik: 10-digit zero-padded CIK string.
        form_types: Filing types to include.  Defaults to the standard set
            (8-K, 10-K, 10-Q, S-1, SC 13D, SC 13G).

    Returns:
        List of filing dicts, each with keys: ``accession_number``,
        ``form_type``, ``filing_date``, ``report_date``,
        ``primary_document``, ``description``.
    """
    if form_types is None:
        form_types = list(_DEFAULT_FORM_TYPES)

    form_types_upper = [ft.upper() for ft in form_types]
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"

    logger.debug("edgar_fetch_submissions", cik=cik, url=url)

    try:
        async with httpx.AsyncClient(
            headers=_get_sec_headers(), timeout=30.0
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        logger.error(
            "edgar_submissions_http_error",
            cik=cik,
            status_code=exc.response.status_code,
        )
        return []
    except Exception:
        logger.error("edgar_submissions_fetch_error", cik=cik, exc_info=True)
        return []

    # SEC returns recent filings under data["filings"]["recent"]
    recent = data.get("filings", {}).get("recent", {})
    if not recent:
        logger.debug("edgar_no_recent_filings", cik=cik)
        return []

    # All arrays are parallel — index-aligned
    accession_numbers: list[str] = recent.get("accessionNumber", [])
    forms: list[str] = recent.get("form", [])
    filing_dates: list[str] = recent.get("filingDate", [])
    report_dates: list[str] = recent.get("reportDate", [])
    primary_docs: list[str] = recent.get("primaryDocument", [])
    descriptions: list[str] = recent.get("primaryDocDescription", [])

    n = len(accession_numbers)
    results: list[dict] = []

    for i in range(n):
        form_type = forms[i] if i < len(forms) else ""
        if form_type.upper() not in form_types_upper:
            continue

        results.append(
            {
                "accession_number": accession_numbers[i] if i < len(accession_numbers) else "",
                "form_type": form_type,
                "filing_date": filing_dates[i] if i < len(filing_dates) else "",
                "report_date": report_dates[i] if i < len(report_dates) else "",
                "primary_document": primary_docs[i] if i < len(primary_docs) else "",
                "description": descriptions[i] if i < len(descriptions) else "",
            }
        )

    logger.debug(
        "edgar_filings_parsed",
        cik=cik,
        total_recent=n,
        matched=len(results),
    )

    # Rate-limit: keep under 10 req/s to SEC servers
    await asyncio.sleep(0.12)

    return results


# ---------------------------------------------------------------------------
# Filing → event conversion
# ---------------------------------------------------------------------------


def _filing_to_event(ticker: str, cik: str, filing: dict) -> dict:
    """Convert a filing dict to an events-table row dict.

    The returned dict has keys matching the ``events`` table columns and is
    ready for direct SQL insertion.
    """
    accession = filing.get("accession_number", "")
    form_type = filing.get("form_type", "")
    filing_date_str = filing.get("filing_date", "")
    report_date_str = filing.get("report_date", "")
    primary_doc = filing.get("primary_document", "")
    description = (filing.get("description", "") or "")[:200]

    # Stable dedup id
    hash_input = f"edgar:{cik}:{accession}"
    event_id = hashlib.sha256(hash_input.encode()).hexdigest()

    # Parse filing date
    try:
        ts_utc = datetime.strptime(filing_date_str, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
    except (ValueError, TypeError):
        ts_utc = datetime.now(timezone.utc)

    # Build EDGAR archive URL
    accession_dashed = accession.replace("-", "")
    cik_stripped = cik.lstrip("0") or "0"
    source_url = (
        f"https://www.sec.gov/Archives/edgar/data/"
        f"{cik_stripped}/{accession_dashed}/{primary_doc}"
    )

    # Severity
    severity_score = _SEVERITY_BY_FORM.get(form_type.upper(), _DEFAULT_SEVERITY)

    # Reason codes
    reason_code = _REASON_CODE_BY_FORM.get(form_type.upper(), _DEFAULT_REASON_CODE)
    reason_codes = json.dumps([reason_code, "portfolio_holding"])

    # Title
    title = f"{ticker} \u2014 {form_type}: {description}" if description else f"{ticker} \u2014 {form_type}"

    # Metadata
    metadata = {
        "cik": cik,
        "form_type": form_type,
        "accession_number": accession,
        "filing_date": filing_date_str,
        "report_date": report_date_str,
    }

    now = datetime.now(timezone.utc)

    return {
        "id": event_id,
        "ts_utc": ts_utc,
        "type": "SEC_FILING",
        "tickers": json.dumps([ticker.upper()]),
        "title": title,
        "source_name": "SEC/EDGAR",
        "source_url": source_url,
        "raw_text_snippet": None,
        "severity_score": severity_score,
        "reason_codes": reason_codes,
        "llm_summary": None,
        "status": "NEW",
        "metadata_json": json.dumps(metadata),
        "created_at_utc": now,
        "updated_at_utc": now,
    }


# ---------------------------------------------------------------------------
# Main entry point: fetch + store filings
# ---------------------------------------------------------------------------


async def fetch_edgar_filings(
    tickers: list[str],
    lookback_days: int = 30,
    engine: AsyncEngine | None = None,
) -> dict[str, Any]:
    """Fetch SEC filings for a list of tickers and upsert them into the
    ``events`` table.

    Args:
        tickers: Ticker symbols to check for filings.
        lookback_days: Only include filings from the last N days.
        engine: SQLAlchemy async engine.  Falls back to the shared singleton.

    Returns:
        Stats dict with ``tickers_checked``, ``filings_found``,
        ``events_inserted``, and ``errors``.
    """
    if engine is None:
        engine = get_shared_engine()

    cik_mapping = await _load_cik_mapping()
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)

    stats: dict[str, Any] = {
        "tickers_checked": 0,
        "filings_found": 0,
        "events_inserted": 0,
        "errors": [],
    }

    for ticker in tickers:
        ticker_upper = ticker.upper().strip()
        stats["tickers_checked"] += 1

        cik = cik_mapping.get(ticker_upper)
        if not cik:
            logger.warning("edgar_cik_not_found", ticker=ticker_upper)
            stats["errors"].append(f"{ticker_upper}: CIK not found")
            continue

        try:
            filings = await _fetch_recent_filings(cik)

            # Filter by lookback window
            filtered: list[dict] = []
            for f in filings:
                try:
                    fdate = datetime.strptime(
                        f.get("filing_date", ""), "%Y-%m-%d"
                    ).replace(tzinfo=timezone.utc)
                    if fdate >= cutoff_date:
                        filtered.append(f)
                except (ValueError, TypeError):
                    # Can't parse date — include it to be safe
                    filtered.append(f)

            stats["filings_found"] += len(filtered)

            if not filtered:
                logger.debug(
                    "edgar_no_recent_filings_for_ticker",
                    ticker=ticker_upper,
                    lookback_days=lookback_days,
                )
                # Still update sync status even if no filings
                await _update_sync_status(
                    engine=engine,
                    ticker=ticker_upper,
                    items_fetched=0,
                )
                continue

            # Convert filings to event rows
            events = [_filing_to_event(ticker_upper, cik, f) for f in filtered]

            # Bulk upsert into events table
            inserted = await _upsert_events(engine, events)
            stats["events_inserted"] += inserted

            # Update sync status
            latest_ts = max(
                (e["ts_utc"] for e in events), default=None
            )
            await _update_sync_status(
                engine=engine,
                ticker=ticker_upper,
                items_fetched=len(filtered),
                last_item_ts=latest_ts,
            )

            logger.info(
                "edgar_ticker_done",
                ticker=ticker_upper,
                filings=len(filtered),
                inserted=inserted,
            )

        except Exception as exc:
            error_msg = f"{ticker_upper}: {exc}"
            stats["errors"].append(error_msg)
            logger.error(
                "edgar_ticker_error",
                ticker=ticker_upper,
                error=str(exc),
                exc_info=True,
            )
            # Update sync status with error
            await _update_sync_status(
                engine=engine,
                ticker=ticker_upper,
                items_fetched=0,
                error=str(exc),
            )

    logger.info(
        "edgar_fetch_complete",
        tickers_checked=stats["tickers_checked"],
        filings_found=stats["filings_found"],
        events_inserted=stats["events_inserted"],
        error_count=len(stats["errors"]),
    )
    return stats


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


async def _upsert_events(engine: AsyncEngine, events: list[dict]) -> int:
    """Bulk upsert event rows into the ``events`` table.

    Uses ``ON CONFLICT (id) DO NOTHING`` so duplicate filings are silently
    skipped.

    Returns:
        Number of newly inserted rows.
    """
    if not events:
        return 0

    stmt = text("""
        INSERT INTO events (
            id, ts_utc, type, tickers, title, source_name, source_url,
            raw_text_snippet, severity_score, reason_codes, llm_summary,
            status, metadata_json, created_at_utc, updated_at_utc
        ) VALUES (
            :id, :ts_utc, :type, :tickers, :title, :source_name, :source_url,
            :raw_text_snippet, :severity_score, :reason_codes, :llm_summary,
            :status, :metadata_json, :created_at_utc, :updated_at_utc
        )
        ON CONFLICT (id) DO NOTHING
    """)

    inserted = 0
    async with engine.begin() as conn:
        for event in events:
            result = await conn.execute(stmt, event)
            if result.rowcount > 0:
                inserted += result.rowcount

    logger.debug("edgar_events_upserted", attempted=len(events), inserted=inserted)
    return inserted


async def _update_sync_status(
    engine: AsyncEngine,
    ticker: str,
    items_fetched: int,
    last_item_ts: datetime | None = None,
    error: str | None = None,
) -> None:
    """Upsert a row in ``event_sync_status`` for the EDGAR connector."""
    now = datetime.now(timezone.utc)

    if error:
        stmt = text("""
            INSERT INTO event_sync_status
                (connector, sync_key, last_sync_at, last_item_ts,
                 items_fetched, error_count, last_error)
            VALUES
                ('edgar', :sync_key, :last_sync_at, :last_item_ts,
                 :items_fetched, 1, :last_error)
            ON CONFLICT (connector, sync_key) DO UPDATE SET
                last_sync_at  = EXCLUDED.last_sync_at,
                error_count   = event_sync_status.error_count + 1,
                last_error    = EXCLUDED.last_error
        """)
        params: dict[str, Any] = {
            "sync_key": ticker,
            "last_sync_at": now,
            "last_item_ts": last_item_ts,
            "items_fetched": items_fetched,
            "last_error": error,
        }
    else:
        stmt = text("""
            INSERT INTO event_sync_status
                (connector, sync_key, last_sync_at, last_item_ts,
                 items_fetched, error_count, last_error)
            VALUES
                ('edgar', :sync_key, :last_sync_at, :last_item_ts,
                 :items_fetched, 0, NULL)
            ON CONFLICT (connector, sync_key) DO UPDATE SET
                last_sync_at  = EXCLUDED.last_sync_at,
                last_item_ts  = COALESCE(EXCLUDED.last_item_ts,
                                         event_sync_status.last_item_ts),
                items_fetched = EXCLUDED.items_fetched,
                error_count   = 0,
                last_error    = NULL
        """)
        params = {
            "sync_key": ticker,
            "last_sync_at": now,
            "last_item_ts": last_item_ts,
            "items_fetched": items_fetched,
        }

    async with engine.begin() as conn:
        await conn.execute(stmt, params)


# ---------------------------------------------------------------------------
# Portfolio ticker discovery
# ---------------------------------------------------------------------------


async def get_portfolio_tickers(
    engine: AsyncEngine | None = None,
) -> list[str]:
    """Query the ``positions_current`` table for distinct ticker symbols.

    Returns:
        Sorted list of uppercase ticker strings currently held.
    """
    if engine is None:
        engine = get_shared_engine()

    stmt = text("""
        SELECT DISTINCT symbol
        FROM positions_current
        WHERE symbol IS NOT NULL
        ORDER BY symbol
    """)

    try:
        async with engine.connect() as conn:
            result = await conn.execute(stmt)
            rows = result.fetchall()
            tickers = [str(row[0]).upper().strip() for row in rows if row[0]]
            logger.info("edgar_portfolio_tickers", count=len(tickers))
            return tickers
    except Exception:
        logger.error("edgar_portfolio_tickers_error", exc_info=True)
        return []


# ---------------------------------------------------------------------------
# High-level orchestrator
# ---------------------------------------------------------------------------


async def sync_edgar_events(
    engine: AsyncEngine | None = None,
) -> dict[str, Any]:
    """End-to-end EDGAR sync: discover portfolio tickers, fetch filings,
    and store them as events.

    This is the function the scheduler should call on a regular cadence
    (e.g., every 6 hours).

    Returns:
        Combined stats from :func:`fetch_edgar_filings`.
    """
    if engine is None:
        engine = get_shared_engine()

    logger.info("edgar_sync_starting")

    tickers = await get_portfolio_tickers(engine)
    if not tickers:
        logger.warning("edgar_sync_no_tickers")
        return {
            "tickers_checked": 0,
            "filings_found": 0,
            "events_inserted": 0,
            "errors": ["No portfolio tickers found"],
        }

    stats = await fetch_edgar_filings(tickers, engine=engine)

    logger.info(
        "edgar_sync_complete",
        tickers_checked=stats["tickers_checked"],
        filings_found=stats["filings_found"],
        events_inserted=stats["events_inserted"],
        error_count=len(stats["errors"]),
    )
    return stats
