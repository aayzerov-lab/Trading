"""Macro economic schedule scraper for the Trading Workstation.

Fetches upcoming macro economic release dates (Fed FOMC, BLS CPI/NFP,
BEA GDP, PCE, ISM, jobless claims) and stores them as events in the
PostgreSQL database.

Uses a hybrid approach: hardcoded known dates for reliability, with a
date-estimation algorithm for recurring releases beyond the known range.
Optionally scrapes the Federal Reserve FOMC calendar as a live source.
"""

from __future__ import annotations

import hashlib
import json
import re
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from shared.db.engine import get_shared_engine

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Timezone constants
# ---------------------------------------------------------------------------

ET = ZoneInfo("America/New_York")
UTC = timezone.utc

# ---------------------------------------------------------------------------
# Source URLs
# ---------------------------------------------------------------------------

SOURCE_URLS: dict[str, str] = {
    "Federal Reserve": "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
    "BLS": "https://www.bls.gov/schedule/",
    "BEA": "https://www.bea.gov/news/schedule",
    "ISM": "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/",
    "DOL": "https://www.dol.gov/ui/data.pdf",
}

# ---------------------------------------------------------------------------
# Release metadata
# ---------------------------------------------------------------------------

MACRO_RELEASES: list[dict[str, Any]] = [
    {
        "name": "FOMC Rate Decision",
        "source": "Federal Reserve",
        "severity": 90,
        "reason_codes": ["fomc", "rate_decision", "high_impact"],
    },
    {
        "name": "Nonfarm Payrolls",
        "source": "BLS",
        "severity": 85,
        "reason_codes": ["nfp", "employment", "high_impact"],
    },
    {
        "name": "CPI Release",
        "source": "BLS",
        "severity": 85,
        "reason_codes": ["cpi", "inflation", "high_impact"],
    },
    {
        "name": "GDP Release",
        "source": "BEA",
        "severity": 80,
        "reason_codes": ["gdp", "growth", "high_impact"],
    },
    {
        "name": "PCE Price Index",
        "source": "BEA",
        "severity": 75,
        "reason_codes": ["pce", "inflation", "medium_impact"],
    },
    {
        "name": "ISM Manufacturing PMI",
        "source": "ISM",
        "severity": 70,
        "reason_codes": ["ism", "manufacturing", "medium_impact"],
    },
    {
        "name": "Initial Jobless Claims",
        "source": "DOL",
        "severity": 60,
        "reason_codes": ["claims", "employment", "weekly"],
    },
]

# ---------------------------------------------------------------------------
# Hardcoded known FOMC dates (2024-2026)
# ---------------------------------------------------------------------------

KNOWN_FOMC_DATES: list[date] = [
    # 2024
    date(2024, 1, 31),
    date(2024, 3, 20),
    date(2024, 5, 1),
    date(2024, 6, 12),
    date(2024, 7, 31),
    date(2024, 9, 18),
    date(2024, 11, 7),
    date(2024, 12, 18),
    # 2025
    date(2025, 1, 29),
    date(2025, 3, 19),
    date(2025, 5, 7),
    date(2025, 6, 18),
    date(2025, 7, 30),
    date(2025, 9, 17),
    date(2025, 10, 29),
    date(2025, 12, 17),
    # 2026
    date(2026, 1, 28),
    date(2026, 3, 18),
    date(2026, 4, 29),
    date(2026, 6, 17),
    date(2026, 7, 29),
    date(2026, 9, 16),
    date(2026, 10, 28),
    date(2026, 12, 16),
]

# ---------------------------------------------------------------------------
# Release time overrides  (hour, minute) in ET
# FOMC announcements are at 14:00 ET; most others at 08:30 ET.
# ---------------------------------------------------------------------------

RELEASE_TIMES_ET: dict[str, tuple[int, int]] = {
    "FOMC Rate Decision": (14, 0),
    "Nonfarm Payrolls": (8, 30),
    "CPI Release": (8, 30),
    "GDP Release": (8, 30),
    "PCE Price Index": (8, 30),
    "ISM Manufacturing PMI": (10, 0),
    "Initial Jobless Claims": (8, 30),
}


# ---------------------------------------------------------------------------
# FOMC calendar scraper
# ---------------------------------------------------------------------------


async def _fetch_fomc_dates() -> list[date]:
    """Try to scrape FOMC meeting dates from the Federal Reserve website.

    Falls back to :data:`KNOWN_FOMC_DATES` if scraping fails for any reason.

    Returns:
        Sorted list of FOMC decision announcement dates.
    """
    url = SOURCE_URLS["Federal Reserve"]
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(15.0),
            follow_redirects=True,
            headers={"User-Agent": "trading-macro-schedule/1.0"},
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            html = resp.text

        # The FOMC calendar page lists dates in several formats.  We look for
        # patterns like "January 28-29" or "March 18-19, 2025" inside the
        # panel-body divs.  The *last* day of a two-day meeting is the
        # announcement day.
        current_year = date.today().year

        # Month mapping
        months = {
            "january": 1, "february": 2, "march": 3, "april": 4,
            "may": 5, "june": 6, "july": 7, "august": 8,
            "september": 9, "october": 10, "november": 11, "december": 12,
        }

        dates: list[date] = []

        # Pattern 1: "January 28-29" (two-day meetings)
        pattern_range = re.compile(
            r"(" + "|".join(months.keys()) + r")\s+(\d{1,2})\s*[-/]\s*(\d{1,2})",
            re.IGNORECASE,
        )
        for match in pattern_range.finditer(html):
            month_name = match.group(1).lower()
            day_end = int(match.group(3))
            month_num = months.get(month_name)
            if month_num is None:
                continue
            # Try to find an explicit year near this match
            year_match = re.search(r"20\d{2}", html[match.end():match.end() + 20])
            year = int(year_match.group()) if year_match else current_year
            try:
                dates.append(date(year, month_num, day_end))
            except ValueError:
                pass

        # Pattern 2: "March 18*" (single-day meetings)
        pattern_single = re.compile(
            r"(" + "|".join(months.keys()) + r")\s+(\d{1,2})\*",
            re.IGNORECASE,
        )
        for match in pattern_single.finditer(html):
            month_name = match.group(1).lower()
            day = int(match.group(2))
            month_num = months.get(month_name)
            if month_num is None:
                continue
            year_match = re.search(r"20\d{2}", html[match.end():match.end() + 20])
            year = int(year_match.group()) if year_match else current_year
            try:
                dates.append(date(year, month_num, day))
            except ValueError:
                pass

        if dates:
            unique = sorted(set(dates))
            logger.info(
                "fomc_dates_scraped",
                count=len(unique),
                first=unique[0].isoformat(),
                last=unique[-1].isoformat(),
            )
            return unique

        logger.warning("fomc_scrape_no_dates_found", url=url)

    except httpx.HTTPStatusError as exc:
        logger.warning(
            "fomc_scrape_http_error",
            status=exc.response.status_code,
            url=url,
        )
    except httpx.RequestError as exc:
        logger.warning("fomc_scrape_request_error", error=str(exc), url=url)
    except Exception as exc:
        logger.warning("fomc_scrape_unexpected_error", error=str(exc), url=url)

    # Fallback
    logger.info("fomc_using_hardcoded_dates", count=len(KNOWN_FOMC_DATES))
    return list(KNOWN_FOMC_DATES)


# ---------------------------------------------------------------------------
# Date estimation helpers
# ---------------------------------------------------------------------------


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date:
    """Return the *n*-th occurrence (1-based) of *weekday* (0=Mon) in *month*.

    Raises ``ValueError`` if *n* is larger than the number of occurrences.
    """
    first_day = date(year, month, 1)
    # Days until first target weekday
    offset = (weekday - first_day.weekday()) % 7
    candidate = first_day + timedelta(days=offset)
    candidate += timedelta(weeks=n - 1)
    if candidate.month != month:
        raise ValueError(f"No {n}-th weekday {weekday} in {year}-{month:02d}")
    return candidate


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    """Return the last occurrence of *weekday* (0=Mon) in *month*."""
    _, last_day_num = monthrange(year, month)
    last_day = date(year, month, last_day_num)
    offset = (last_day.weekday() - weekday) % 7
    return last_day - timedelta(days=offset)


def _first_business_day_of_month(year: int, month: int) -> date:
    """Return the first business day (Mon-Fri) of *month*."""
    d = date(year, month, 1)
    while d.weekday() >= 5:  # Sat=5, Sun=6
        d += timedelta(days=1)
    return d


def _every_thursday(start: date, end: date) -> list[date]:
    """Return every Thursday between *start* and *end* inclusive."""
    thursdays: list[date] = []
    # Advance to first Thursday on or after start
    d = start
    offset = (3 - d.weekday()) % 7  # Thursday = 3
    d += timedelta(days=offset)
    while d <= end:
        thursdays.append(d)
        d += timedelta(weeks=1)
    return thursdays


def _iter_months(start: date, end: date):
    """Yield (year, month) tuples covering [start, end]."""
    y, m = start.year, start.month
    while date(y, m, 1) <= end:
        yield y, m
        m += 1
        if m > 12:
            m = 1
            y += 1


def _estimate_release_dates(
    release_name: str,
    start_date: date,
    end_date: date,
) -> list[date]:
    """Generate estimated dates for a recurring release within [start, end].

    Estimation logic per release type:

    * **Nonfarm Payrolls** - First Friday of each month.
    * **CPI Release** - Second Tuesday-to-Thursday (~10th-15th) of each month.
    * **GDP Release** - Last week of month in January, April, July, October.
    * **PCE Price Index** - Last Friday of each month.
    * **ISM Manufacturing PMI** - First business day of each month.
    * **Initial Jobless Claims** - Every Thursday.
    * **FOMC Rate Decision** - Handled separately via scraping/hardcoded list.

    Returns:
        Sorted list of estimated release dates within the window.
    """
    dates: list[date] = []

    if release_name == "Nonfarm Payrolls":
        # First Friday of each month
        for y, m in _iter_months(start_date, end_date):
            try:
                d = _nth_weekday_of_month(y, m, weekday=4, n=1)
                if start_date <= d <= end_date:
                    dates.append(d)
            except ValueError:
                pass

    elif release_name == "CPI Release":
        # Typically released around the 10th-15th; approximate as 2nd
        # Wednesday of each month.
        for y, m in _iter_months(start_date, end_date):
            try:
                d = _nth_weekday_of_month(y, m, weekday=2, n=2)
                if start_date <= d <= end_date:
                    dates.append(d)
            except ValueError:
                pass

    elif release_name == "GDP Release":
        # Advance/second/third estimate: last Thursday of Jan, Apr, Jul, Oct.
        gdp_months = {1, 4, 7, 10}
        for y, m in _iter_months(start_date, end_date):
            if m in gdp_months:
                d = _last_weekday_of_month(y, m, weekday=3)
                if start_date <= d <= end_date:
                    dates.append(d)

    elif release_name == "PCE Price Index":
        # Last Friday of each month
        for y, m in _iter_months(start_date, end_date):
            d = _last_weekday_of_month(y, m, weekday=4)
            if start_date <= d <= end_date:
                dates.append(d)

    elif release_name == "ISM Manufacturing PMI":
        # First business day of each month
        for y, m in _iter_months(start_date, end_date):
            d = _first_business_day_of_month(y, m)
            if start_date <= d <= end_date:
                dates.append(d)

    elif release_name == "Initial Jobless Claims":
        # Every Thursday
        dates = _every_thursday(start_date, end_date)

    else:
        logger.debug(
            "no_estimation_rule",
            release_name=release_name,
        )

    return sorted(dates)


# ---------------------------------------------------------------------------
# Event builder
# ---------------------------------------------------------------------------


def _schedule_to_event(release: dict[str, Any], release_date: date, *, estimated: bool = True) -> dict[str, Any]:
    """Convert a schedule entry and release date into an event dict.

    The event ``id`` is a stable SHA-256 hash derived from
    ``schedule:<release_name>:<release_date_iso>``, ensuring idempotent
    upserts.

    Args:
        release: A dict from :data:`MACRO_RELEASES`.
        release_date: The date of the release.
        estimated: Whether this date is an estimate or a confirmed date.

    Returns:
        A dict matching the ``events`` table columns.
    """
    # Build deterministic id
    id_seed = f"schedule:{release['name']}:{release_date.isoformat()}"
    event_id = hashlib.sha256(id_seed.encode("utf-8")).hexdigest()

    # Determine release time in ET, then convert to UTC
    hour, minute = RELEASE_TIMES_ET.get(release["name"], (8, 30))
    release_dt_et = datetime(
        release_date.year,
        release_date.month,
        release_date.day,
        hour,
        minute,
        tzinfo=ET,
    )
    release_dt_utc = release_dt_et.astimezone(UTC)

    source = release["source"]
    source_url = SOURCE_URLS.get(source, "")

    metadata = {
        "release_name": release["name"],
        "release_date": release_date.isoformat(),
        "source": source,
        "estimated": estimated,
        "release_time_et": f"{hour:02d}:{minute:02d}",
    }

    return {
        "id": event_id,
        "ts_utc": release_dt_utc,
        "type": "MACRO_SCHEDULE",
        "tickers": None,
        "title": f"{release['name']} \u2014 {release_date.strftime('%b %d, %Y')}",
        "source_name": source,
        "source_url": source_url,
        "raw_text_snippet": (
            f"Scheduled macro release: {release['name']} "
            f"on {release_date.strftime('%B %d, %Y')} at {hour:02d}:{minute:02d} ET"
        ),
        "severity_score": release["severity"],
        "reason_codes": json.dumps(release["reason_codes"]),
        "llm_summary": None,
        "status": "NEW",
        "metadata_json": json.dumps(metadata),
    }


# ---------------------------------------------------------------------------
# Main sync function
# ---------------------------------------------------------------------------


async def sync_macro_schedule(
    lookforward_days: int = 90,
    engine: AsyncEngine | None = None,
) -> dict[str, Any]:
    """Synchronise macro economic schedule events into the database.

    1. Scrape (or fall back to hardcoded) FOMC dates.
    2. Estimate recurring release dates for all other series.
    3. Upsert events into the ``events`` table with ``ON CONFLICT DO NOTHING``.
    4. Update ``event_sync_status`` for connector ``macro_schedule``.
    5. Return summary statistics.

    Args:
        lookforward_days: Number of calendar days into the future to generate
            schedule events for. Defaults to 90.
        engine: Optional SQLAlchemy async engine. If ``None``, uses
            :func:`shared.db.engine.get_shared_engine`.

    Returns:
        Dict with keys ``events_generated``, ``events_inserted``,
        ``lookforward_days``, and ``errors``.
    """
    if engine is None:
        engine = get_shared_engine()

    today = date.today()
    end_date = today + timedelta(days=lookforward_days)
    errors: list[str] = []

    logger.info(
        "macro_schedule_sync_started",
        lookforward_days=lookforward_days,
        start=today.isoformat(),
        end=end_date.isoformat(),
    )

    # ------------------------------------------------------------------
    # 1. Gather FOMC dates (scrape with hardcoded fallback)
    # ------------------------------------------------------------------
    try:
        fomc_all_dates = await _fetch_fomc_dates()
    except Exception as exc:
        logger.error("fomc_fetch_failed", error=str(exc))
        fomc_all_dates = list(KNOWN_FOMC_DATES)
        errors.append(f"fomc_fetch: {exc}")

    fomc_dates_in_range = [d for d in fomc_all_dates if today <= d <= end_date]

    # Build a lookup to know which FOMC dates came from scraping vs hardcoded
    hardcoded_set = set(KNOWN_FOMC_DATES)

    # ------------------------------------------------------------------
    # 2. Build all events
    # ------------------------------------------------------------------
    all_events: list[dict[str, Any]] = []

    for release in MACRO_RELEASES:
        name = release["name"]

        if name == "FOMC Rate Decision":
            # Use scraped / hardcoded FOMC dates
            for d in fomc_dates_in_range:
                estimated = d not in hardcoded_set
                ev = _schedule_to_event(release, d, estimated=estimated)
                all_events.append(ev)
        else:
            # Estimate dates for recurring releases
            estimated_dates = _estimate_release_dates(name, today, end_date)
            for d in estimated_dates:
                ev = _schedule_to_event(release, d, estimated=True)
                all_events.append(ev)

    events_generated = len(all_events)
    logger.info("macro_schedule_events_generated", count=events_generated)

    # ------------------------------------------------------------------
    # 3. Upsert into events table
    # ------------------------------------------------------------------
    events_inserted = 0

    try:
        async with engine.begin() as conn:
            for ev in all_events:
                result = await conn.execute(
                    text("""
                        INSERT INTO events (
                            id, ts_utc, type, tickers, title, source_name,
                            source_url, raw_text_snippet, severity_score,
                            reason_codes, llm_summary, status, metadata_json
                        )
                        VALUES (
                            :id, :ts_utc, :type, :tickers, :title, :source_name,
                            :source_url, :raw_text_snippet, :severity_score,
                            :reason_codes, :llm_summary, :status, :metadata_json
                        )
                        ON CONFLICT (id) DO NOTHING
                    """),
                    ev,
                )
                events_inserted += result.rowcount

        logger.info(
            "macro_schedule_events_upserted",
            generated=events_generated,
            inserted=events_inserted,
        )
    except Exception as exc:
        logger.error("macro_schedule_upsert_failed", error=str(exc))
        errors.append(f"upsert: {exc}")

    # ------------------------------------------------------------------
    # 4. Update event_sync_status
    # ------------------------------------------------------------------
    now_utc = datetime.now(UTC)
    last_item_ts = max((ev["ts_utc"] for ev in all_events), default=now_utc)

    try:
        async with engine.begin() as conn:
            await conn.execute(
                text("""
                    INSERT INTO event_sync_status (
                        connector, sync_key, last_sync_at,
                        last_item_ts, items_fetched, error_count, last_error
                    )
                    VALUES (
                        :connector, :sync_key, :last_sync_at,
                        :last_item_ts, :items_fetched, :error_count, :last_error
                    )
                    ON CONFLICT ON CONSTRAINT uq_event_sync_connector_key
                    DO UPDATE SET
                        last_sync_at  = EXCLUDED.last_sync_at,
                        last_item_ts  = EXCLUDED.last_item_ts,
                        items_fetched = EXCLUDED.items_fetched,
                        error_count   = EXCLUDED.error_count,
                        last_error    = EXCLUDED.last_error
                """),
                {
                    "connector": "macro_schedule",
                    "sync_key": "all",
                    "last_sync_at": now_utc,
                    "last_item_ts": last_item_ts,
                    "items_fetched": events_generated,
                    "error_count": len(errors),
                    "last_error": errors[-1] if errors else None,
                },
            )
    except Exception as exc:
        logger.error("macro_schedule_sync_status_update_failed", error=str(exc))
        errors.append(f"sync_status: {exc}")

    # ------------------------------------------------------------------
    # 5. Return summary
    # ------------------------------------------------------------------
    summary = {
        "events_generated": events_generated,
        "events_inserted": events_inserted,
        "lookforward_days": lookforward_days,
        "date_range": {"start": today.isoformat(), "end": end_date.isoformat()},
        "errors": errors,
    }

    logger.info("macro_schedule_sync_completed", **summary)
    return summary
