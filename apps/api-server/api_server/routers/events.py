"""Events and Alerts API endpoints (Phase 2).

Provides REST endpoints for managing ingested events (SEC filings, macro
schedule, RSS news) and user-facing alerts surfaced from event scoring
and risk spikes.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

import structlog
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text

from shared.db.engine import get_shared_engine

logger = structlog.get_logger()

router = APIRouter(prefix="/events", tags=["events"])

# ---------------------------------------------------------------------------
# Pydantic request bodies
# ---------------------------------------------------------------------------


class EventStatusUpdate(BaseModel):
    """Body for PATCH /events/{event_id}/status."""

    status: str


class AlertStatusUpdate(BaseModel):
    """Body for PATCH /alerts/{alert_id}/status."""

    status: str
    snooze_hours: int = 4


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VALID_EVENT_STATUSES = {"NEW", "ACKED", "DISMISSED"}
_VALID_ALERT_STATUSES = {"NEW", "READ", "SNOOZED", "DISMISSED"}


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    """Convert any datetime objects in a row dict to ISO-8601 strings."""
    out: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            out[key] = value.isoformat()
        else:
            out[key] = value
    return out


# Reverse alias map: ticker symbol -> set of lowercase search terms.
# Used by _filter_ticker_relevance to check if an article actually
# mentions a ticker (by symbol or company name).
_TICKER_ALIASES: dict[str, set[str]] = {}


def _build_ticker_aliases() -> dict[str, set[str]]:
    """Build a reverse alias map from the shared rss_feeds module."""
    if _TICKER_ALIASES:
        return _TICKER_ALIASES
    try:
        from shared.data.rss_feeds import _HARDCODED_ALIASES
        for alias, ticker in _HARDCODED_ALIASES.items():
            _TICKER_ALIASES.setdefault(ticker, set()).add(alias.lower())
    except ImportError:
        pass
    return _TICKER_ALIASES


def _filter_ticker_relevance(
    rows: list[dict[str, Any]], symbol: str
) -> list[dict[str, Any]]:
    """Filter RSS_NEWS rows to only those that actually mention *symbol*.

    Articles from curated feeds (non-Google-News) pass through unchanged.
    For Google News articles, we require the ticker symbol or a known
    company name alias to appear in the title or snippet text.
    """
    aliases = _build_ticker_aliases()
    search_terms = {symbol.lower()}
    search_terms.update(aliases.get(symbol, set()))

    filtered: list[dict[str, Any]] = []
    for row in rows:
        source = (row.get("source_name") or "")
        # Curated feeds: always keep
        if not source.startswith("Google News:"):
            filtered.append(row)
            continue
        # Google News: check if ticker actually mentioned
        haystack = " ".join([
            (row.get("title") or ""),
            (row.get("raw_text_snippet") or ""),
        ]).lower()
        if any(term in haystack for term in search_terms):
            filtered.append(row)
    return filtered


# ---------------------------------------------------------------------------
# 1. GET /events – List events with filters
# ---------------------------------------------------------------------------


@router.get("")
async def list_events(
    type: Optional[str] = Query(default=None, description="Filter by event type"),
    ticker: Optional[str] = Query(default=None, description="Filter by ticker (substring match in tickers JSON)"),
    days: int = Query(default=7, ge=1, le=365, description="Lookback window in days"),
    status: Optional[str] = Query(default=None, description="Filter by status (NEW, ACKED, DISMISSED)"),
    limit: int = Query(default=100, ge=1, le=1000, description="Max rows to return"),
    offset: int = Query(default=0, ge=0, description="Pagination offset"),
) -> list[dict[str, Any]]:
    """Return events with optional type, ticker, status, and date filters."""
    try:
        logger.info(
            "list_events_request",
            type=type,
            ticker=ticker,
            days=days,
            status=status,
            limit=limit,
            offset=offset,
        )

        params: dict[str, Any] = {"days": days, "limit": limit, "offset": offset}

        # Build dynamic WHERE clauses
        where_parts = ["ts_utc >= (NOW() - MAKE_INTERVAL(days => :days))"]

        if type is not None:
            where_parts.append("type = :type")
            params["type"] = type

        if ticker is not None:
            where_parts.append("tickers LIKE :ticker")
            params["ticker"] = f"%{ticker}%"

        if status is not None:
            if status not in _VALID_EVENT_STATUSES:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid status: {status}. Must be one of {sorted(_VALID_EVENT_STATUSES)}",
                )
            where_parts.append("status = :status")
            params["status"] = status

        where_clause = " AND ".join(where_parts)
        query = f"""
            SELECT id, ts_utc, scheduled_for_utc, type, tickers, title,
                   source_name, source_url, raw_text_snippet, severity_score,
                   reason_codes, llm_summary, status, metadata_json,
                   created_at_utc, updated_at_utc
            FROM events
            WHERE {where_clause}
            ORDER BY ts_utc DESC
            LIMIT :limit OFFSET :offset
        """

        engine = get_shared_engine()
        async with engine.connect() as conn:
            result = await conn.execute(text(query), params)
            rows = result.mappings().all()
            return [_serialize_row(dict(row)) for row in rows]

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("list_events_failed")
        raise HTTPException(status_code=500, detail=f"Failed to list events: {str(e)}")


# ---------------------------------------------------------------------------
# 2. GET /events/high-priority – Top N high priority events
# ---------------------------------------------------------------------------


@router.get("/high-priority")
async def high_priority_events(
    limit: int = Query(default=20, ge=1, le=100, description="Max high-priority events to return"),
) -> list[dict[str, Any]]:
    """Return top N events with severity_score >= 80 and status NEW."""
    try:
        logger.info("high_priority_events_request", limit=limit)

        query = """
            SELECT id, ts_utc, scheduled_for_utc, type, tickers, title,
                   source_name, source_url, raw_text_snippet, severity_score,
                   reason_codes, llm_summary, status, metadata_json,
                   created_at_utc, updated_at_utc
            FROM events
            WHERE severity_score >= 80 AND status = 'NEW'
            ORDER BY severity_score DESC, ts_utc DESC
            LIMIT :limit
        """

        engine = get_shared_engine()
        async with engine.connect() as conn:
            result = await conn.execute(text(query), {"limit": limit})
            rows = result.mappings().all()
            return [_serialize_row(dict(row)) for row in rows]

    except Exception as e:
        logger.exception("high_priority_events_failed")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch high-priority events: {str(e)}",
        )


# ---------------------------------------------------------------------------
# 3. PATCH /events/{event_id}/status – Update event status
# ---------------------------------------------------------------------------


@router.patch("/{event_id}/status")
async def update_event_status(
    event_id: str,
    body: EventStatusUpdate,
) -> dict[str, Any]:
    """Update the status of an event."""
    if body.status not in _VALID_EVENT_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status: {body.status}. Must be one of {sorted(_VALID_EVENT_STATUSES)}",
        )

    try:
        logger.info("update_event_status", event_id=event_id, status=body.status)

        engine = get_shared_engine()
        async with engine.begin() as conn:
            # Check that the event exists
            check = await conn.execute(
                text("SELECT id FROM events WHERE id = :id"),
                {"id": event_id},
            )
            if check.first() is None:
                raise HTTPException(status_code=404, detail=f"Event not found: {event_id}")

            await conn.execute(
                text("""
                    UPDATE events
                    SET status = :status, updated_at_utc = NOW()
                    WHERE id = :id
                """),
                {"id": event_id, "status": body.status},
            )

        return {"ok": True, "id": event_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("update_event_status_failed", event_id=event_id)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update event status: {str(e)}",
        )


# ---------------------------------------------------------------------------
# 4. GET /events/stats – Event statistics
# ---------------------------------------------------------------------------


@router.get("/stats")
async def event_stats() -> dict[str, Any]:
    """Return aggregate event statistics: counts by type, by status, totals."""
    try:
        logger.info("event_stats_request")

        engine = get_shared_engine()
        async with engine.connect() as conn:
            # Total count
            total_result = await conn.execute(text("SELECT COUNT(*) AS cnt FROM events"))
            total = total_result.scalar() or 0

            # High priority count
            hp_result = await conn.execute(
                text("SELECT COUNT(*) AS cnt FROM events WHERE severity_score >= 80 AND status = 'NEW'")
            )
            high_priority = hp_result.scalar() or 0

            # Counts by type
            by_type_result = await conn.execute(
                text("SELECT type, COUNT(*) AS cnt FROM events GROUP BY type ORDER BY cnt DESC")
            )
            by_type = {row.type: row.cnt for row in by_type_result}

            # Counts by status
            by_status_result = await conn.execute(
                text("SELECT status, COUNT(*) AS cnt FROM events GROUP BY status ORDER BY cnt DESC")
            )
            by_status = {row.status: row.cnt for row in by_status_result}

        return {
            "total": total,
            "high_priority": high_priority,
            "by_type": by_type,
            "by_status": by_status,
        }

    except Exception as e:
        logger.exception("event_stats_failed")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compute event stats: {str(e)}",
        )


# ---------------------------------------------------------------------------
# 5. GET /alerts – List alerts (mounted under /events prefix, full path: /events/alerts)
# ---------------------------------------------------------------------------

_alerts_router = APIRouter(prefix="/alerts", tags=["alerts"])


@_alerts_router.get("")
async def list_alerts(
    status: Optional[str] = Query(default=None, description="Filter by alert status"),
    limit: int = Query(default=50, ge=1, le=500, description="Max alerts to return"),
) -> list[dict[str, Any]]:
    """Return alerts with optional status filter, ordered by newest first."""
    try:
        logger.info("list_alerts_request", status=status, limit=limit)

        params: dict[str, Any] = {"limit": limit}

        if status is not None:
            if status not in _VALID_ALERT_STATUSES:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid status: {status}. Must be one of {sorted(_VALID_ALERT_STATUSES)}",
                )
            where = "WHERE status = :status"
            params["status"] = status
        else:
            where = ""

        query = f"""
            SELECT id, ts_utc, type, message, severity, related_event_id,
                   status, snoozed_until, created_at_utc
            FROM alerts
            {where}
            ORDER BY created_at_utc DESC
            LIMIT :limit
        """

        engine = get_shared_engine()
        async with engine.connect() as conn:
            result = await conn.execute(text(query), params)
            rows = result.mappings().all()
            return [_serialize_row(dict(row)) for row in rows]

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("list_alerts_failed")
        raise HTTPException(status_code=500, detail=f"Failed to list alerts: {str(e)}")


# ---------------------------------------------------------------------------
# 6. GET /alerts/unread-count – Count of unread (NEW) alerts
# ---------------------------------------------------------------------------


@_alerts_router.get("/unread-count")
async def alerts_unread_count() -> dict[str, int]:
    """Return the number of alerts with status NEW."""
    try:
        logger.info("alerts_unread_count_request")

        engine = get_shared_engine()
        async with engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) AS cnt FROM alerts WHERE status = 'NEW'")
            )
            count = result.scalar() or 0

        return {"count": count}

    except Exception as e:
        logger.exception("alerts_unread_count_failed")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to count unread alerts: {str(e)}",
        )


# ---------------------------------------------------------------------------
# 7. PATCH /alerts/{alert_id}/status – Update alert status
# ---------------------------------------------------------------------------


@_alerts_router.patch("/{alert_id}/status")
async def update_alert_status(
    alert_id: int,
    body: AlertStatusUpdate,
) -> dict[str, Any]:
    """Update the status of an alert. If SNOOZED, also set snoozed_until."""
    if body.status not in _VALID_ALERT_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status: {body.status}. Must be one of {sorted(_VALID_ALERT_STATUSES)}",
        )

    try:
        logger.info(
            "update_alert_status",
            alert_id=alert_id,
            status=body.status,
            snooze_hours=body.snooze_hours if body.status == "SNOOZED" else None,
        )

        engine = get_shared_engine()
        async with engine.begin() as conn:
            # Check that the alert exists
            check = await conn.execute(
                text("SELECT id FROM alerts WHERE id = :id"),
                {"id": alert_id},
            )
            if check.first() is None:
                raise HTTPException(status_code=404, detail=f"Alert not found: {alert_id}")

            if body.status == "SNOOZED":
                snoozed_until = datetime.now(timezone.utc) + timedelta(hours=body.snooze_hours)
                await conn.execute(
                    text("""
                        UPDATE alerts
                        SET status = :status, snoozed_until = :snoozed_until
                        WHERE id = :id
                    """),
                    {"id": alert_id, "status": body.status, "snoozed_until": snoozed_until},
                )
            else:
                await conn.execute(
                    text("""
                        UPDATE alerts
                        SET status = :status, snoozed_until = NULL
                        WHERE id = :id
                    """),
                    {"id": alert_id, "status": body.status},
                )

        return {"ok": True, "id": alert_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("update_alert_status_failed", alert_id=alert_id)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update alert status: {str(e)}",
        )


# ---------------------------------------------------------------------------
# 8. POST /events/seed – Insert sample events for development
# ---------------------------------------------------------------------------


@router.post("/seed")
async def seed_events() -> dict[str, Any]:
    """Insert sample events and alerts for development/testing.

    Uses ON CONFLICT DO NOTHING so the endpoint is idempotent.
    """
    try:
        logger.info("seed_events_request")

        now = datetime.now(timezone.utc)

        sample_events = [
            {
                "id": "seed-sec-001",
                "ts_utc": now - timedelta(hours=2),
                "type": "SEC_FILING",
                "tickers": '["AAPL"]',
                "title": "Apple Inc. 10-K Annual Report Filed",
                "source_name": "SEC/EDGAR",
                "source_url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=AAPL",
                "raw_text_snippet": "Apple Inc. filed Form 10-K for fiscal year ending September 2024.",
                "severity_score": 60,
                "reason_codes": '["annual_filing","large_cap"]',
                "llm_summary": "Routine annual filing for AAPL. No material surprises.",
                "status": "NEW",
            },
            {
                "id": "seed-sec-002",
                "ts_utc": now - timedelta(hours=5),
                "type": "SEC_FILING",
                "tickers": '["TSLA"]',
                "title": "Tesla 8-K Current Report: Material Event",
                "source_name": "SEC/EDGAR",
                "source_url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=TSLA",
                "raw_text_snippet": "Tesla Inc. filed Form 8-K reporting a material definitive agreement.",
                "severity_score": 85,
                "reason_codes": '["8k_material","high_vol_name"]',
                "llm_summary": "Tesla disclosed a material definitive agreement. High impact potential.",
                "status": "NEW",
            },
            {
                "id": "seed-macro-001",
                "ts_utc": now - timedelta(hours=12),
                "type": "MACRO_SCHEDULE",
                "tickers": None,
                "title": "FOMC Rate Decision Announcement",
                "source_name": "FederalReserve",
                "source_url": "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
                "raw_text_snippet": "Federal Open Market Committee rate decision scheduled.",
                "severity_score": 95,
                "reason_codes": '["fomc","rate_decision","macro_critical"]',
                "llm_summary": "FOMC rate decision upcoming. Markets pricing 25bp cut.",
                "status": "NEW",
            },
            {
                "id": "seed-rss-001",
                "ts_utc": now - timedelta(hours=1),
                "type": "RSS_NEWS",
                "tickers": '["NVDA","AMD"]',
                "title": "Semiconductor stocks surge on AI demand forecast",
                "source_name": "Reuters",
                "source_url": "https://www.reuters.com/technology/",
                "raw_text_snippet": "NVDA and AMD rallied after analysts raised demand estimates for AI chips.",
                "severity_score": 70,
                "reason_codes": '["sector_move","ai_theme"]',
                "llm_summary": "Bullish analyst notes driving semis higher. NVDA +4%, AMD +3%.",
                "status": "NEW",
            },
            {
                "id": "seed-rss-002",
                "ts_utc": now - timedelta(days=1),
                "type": "RSS_NEWS",
                "tickers": '["SPY","QQQ"]',
                "title": "Market volatility spikes on geopolitical tensions",
                "source_name": "Bloomberg",
                "source_url": "https://www.bloomberg.com/markets",
                "raw_text_snippet": "VIX surged above 25 as geopolitical tensions escalated in the Middle East.",
                "severity_score": 80,
                "reason_codes": '["vol_spike","geopolitical","broad_market"]',
                "llm_summary": "VIX spike driven by geopolitical risk. Broad market impact expected.",
                "status": "ACKED",
            },
        ]

        sample_alerts = [
            {
                "ts_utc": now - timedelta(hours=1),
                "type": "HIGH_SEVERITY_EVENT",
                "message": "High-severity event: FOMC Rate Decision Announcement (score: 95)",
                "severity": 95,
                "related_event_id": "seed-macro-001",
                "status": "NEW",
            },
            {
                "ts_utc": now - timedelta(hours=3),
                "type": "HIGH_SEVERITY_EVENT",
                "message": "High-severity event: Tesla 8-K Material Event (score: 85)",
                "severity": 85,
                "related_event_id": "seed-sec-002",
                "status": "NEW",
            },
            {
                "ts_utc": now - timedelta(hours=6),
                "type": "RISK_SPIKE",
                "message": "Portfolio VaR increased by 15% in the last 24 hours",
                "severity": 75,
                "related_event_id": None,
                "status": "NEW",
            },
        ]

        events_inserted = 0
        alerts_inserted = 0

        engine = get_shared_engine()
        async with engine.begin() as conn:
            # Insert sample events
            for ev in sample_events:
                result = await conn.execute(
                    text("""
                        INSERT INTO events (id, ts_utc, type, tickers, title, source_name,
                                            source_url, raw_text_snippet, severity_score,
                                            reason_codes, llm_summary, status)
                        VALUES (:id, :ts_utc, :type, :tickers, :title, :source_name,
                                :source_url, :raw_text_snippet, :severity_score,
                                :reason_codes, :llm_summary, :status)
                        ON CONFLICT (id) DO NOTHING
                    """),
                    ev,
                )
                events_inserted += result.rowcount

            # Insert sample alerts
            for alert in sample_alerts:
                result = await conn.execute(
                    text("""
                        INSERT INTO alerts (ts_utc, type, message, severity,
                                            related_event_id, status)
                        VALUES (:ts_utc, :type, :message, :severity,
                                :related_event_id, :status)
                    """),
                    alert,
                )
                alerts_inserted += result.rowcount

        logger.info(
            "seed_events_completed",
            events_inserted=events_inserted,
            alerts_inserted=alerts_inserted,
        )

        return {
            "seeded": True,
            "events": events_inserted,
            "alerts": alerts_inserted,
        }

    except Exception as e:
        logger.exception("seed_events_failed")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to seed events: {str(e)}",
        )


# ---------------------------------------------------------------------------
# Connector sync trigger endpoints (Phase 2.1-2.4)
# ---------------------------------------------------------------------------


@router.post("/sync")
async def trigger_event_sync() -> dict[str, Any]:
    """Manually trigger the full event sync pipeline.

    Runs all connectors (EDGAR, schedules, RSS), scoring, optional
    summariser, and alert rules.  Returns combined stats.
    """
    try:
        from shared.data.scheduler import run_event_sync

        engine = get_shared_engine()
        stats = await run_event_sync(engine)
        return stats
    except Exception as e:
        logger.exception("event_sync_trigger_failed")
        raise HTTPException(
            status_code=500,
            detail=f"Event sync failed: {str(e)}",
        )


@router.post("/sync/edgar")
async def trigger_edgar_sync() -> dict[str, Any]:
    """Manually trigger EDGAR SEC filing sync for portfolio tickers."""
    try:
        from shared.data.edgar import sync_edgar_events

        engine = get_shared_engine()
        stats = await sync_edgar_events(engine=engine)
        return stats
    except Exception as e:
        logger.exception("edgar_sync_trigger_failed")
        raise HTTPException(
            status_code=500,
            detail=f"EDGAR sync failed: {str(e)}",
        )


@router.post("/sync/schedules")
async def trigger_schedule_sync() -> dict[str, Any]:
    """Manually trigger macro economic schedule sync."""
    try:
        from shared.data.schedules import sync_macro_schedule

        engine = get_shared_engine()
        stats = await sync_macro_schedule(engine=engine)
        return stats
    except Exception as e:
        logger.exception("schedule_sync_trigger_failed")
        raise HTTPException(
            status_code=500,
            detail=f"Schedule sync failed: {str(e)}",
        )


@router.post("/sync/rss")
async def trigger_rss_sync() -> dict[str, Any]:
    """Manually trigger RSS feed sync."""
    try:
        from shared.data.rss_feeds import sync_rss_feeds

        engine = get_shared_engine()
        stats = await sync_rss_feeds(engine=engine)
        return stats
    except Exception as e:
        logger.exception("rss_sync_trigger_failed")
        raise HTTPException(
            status_code=500,
            detail=f"RSS sync failed: {str(e)}",
        )


@router.post("/sync/score")
async def trigger_scoring() -> dict[str, Any]:
    """Manually trigger portfolio-aware materiality scoring."""
    try:
        from shared.data.scoring import score_new_events

        engine = get_shared_engine()
        stats = await score_new_events(engine=engine)
        return stats
    except Exception as e:
        logger.exception("scoring_trigger_failed")
        raise HTTPException(
            status_code=500,
            detail=f"Scoring failed: {str(e)}",
        )


@router.post("/sync/alerts")
async def trigger_alert_rules() -> dict[str, Any]:
    """Manually trigger alert rule evaluation."""
    try:
        from shared.data.alert_rules import run_alert_rules

        engine = get_shared_engine()
        stats = await run_alert_rules(engine=engine)
        return stats
    except Exception as e:
        logger.exception("alert_rules_trigger_failed")
        raise HTTPException(
            status_code=500,
            detail=f"Alert rules failed: {str(e)}",
        )


# ---------------------------------------------------------------------------
# 9. GET /events/portfolio-tickers – Distinct tickers from positions
# ---------------------------------------------------------------------------


@router.get("/portfolio-tickers")
async def portfolio_tickers():
    """Return the list of distinct tickers from positions_current where position != 0."""
    engine = get_shared_engine()
    async with engine.connect() as conn:
        result = await conn.execute(text(
            "SELECT DISTINCT UPPER(symbol) as symbol FROM positions_current "
            "WHERE position != 0 AND symbol IS NOT NULL ORDER BY symbol"
        ))
        return [row.symbol for row in result]


# ---------------------------------------------------------------------------
# 10. GET /events/today – Live news tape for today (ET timezone)
# ---------------------------------------------------------------------------


@router.get("/today")
async def today_events(
    scope: str = Query(default="my", regex="^(my|all)$"),
    min_severity: int = Query(default=0, ge=0, le=100),
    types: str = Query(default="RSS_NEWS,SEC_FILING"),
    limit: int = Query(default=100, ge=1, le=500),
    cursor: Optional[str] = Query(default=None),
):
    """Live news tape — events from today (America/New_York timezone), newest first."""
    ET = ZoneInfo("America/New_York")

    now_et = datetime.now(ET)
    today_start_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow_start_et = today_start_et + timedelta(days=1)
    today_start_utc = today_start_et.astimezone(timezone.utc)
    tomorrow_start_utc = tomorrow_start_et.astimezone(timezone.utc)

    type_list = [t.strip() for t in types.split(",") if t.strip()]

    params: dict[str, Any] = {
        "today_start": today_start_utc,
        "tomorrow_start": tomorrow_start_utc,
        "min_sev": min_severity,
        "limit": limit,
    }

    where_parts = [
        "ts_utc >= :today_start",
        "ts_utc < :tomorrow_start",
        "severity_score >= :min_sev",
    ]

    if type_list:
        # Build type IN clause
        type_placeholders = []
        for i, t in enumerate(type_list):
            key = f"type_{i}"
            type_placeholders.append(f":{key}")
            params[key] = t
        where_parts.append(f"type IN ({', '.join(type_placeholders)})")

    if cursor:
        try:
            cursor_dt = datetime.fromisoformat(cursor)
            params["cursor_ts"] = cursor_dt
            where_parts.append("ts_utc < :cursor_ts")
        except ValueError:
            pass

    # For scope=my, build portfolio filter
    portfolio_filter = ""
    if scope == "my":
        engine = get_shared_engine()
        async with engine.connect() as conn:
            ptickers = await conn.execute(text(
                "SELECT DISTINCT UPPER(symbol) as symbol FROM positions_current "
                "WHERE position != 0 AND symbol IS NOT NULL"
            ))
            tickers = [r.symbol for r in ptickers]

        if tickers:
            ticker_conditions = []
            for i, tk in enumerate(tickers):
                key = f"ptk_{i}"
                ticker_conditions.append(f"tickers LIKE :{key}")
                params[key] = f"%{tk}%"
            ticker_or = " OR ".join(ticker_conditions)
            # Include if has portfolio ticker OR is high-severity macro
            portfolio_filter = f"AND (({ticker_or}) OR severity_score >= 70)"
        else:
            portfolio_filter = "AND severity_score >= 70"

    where_clause = " AND ".join(where_parts)

    query = f"""
        SELECT id, ts_utc, scheduled_for_utc, type, tickers, title,
               source_name, source_url, raw_text_snippet, severity_score,
               reason_codes, llm_summary, status, metadata_json,
               created_at_utc, updated_at_utc
        FROM events
        WHERE {where_clause} {portfolio_filter}
        ORDER BY ts_utc DESC
        LIMIT :limit
    """

    engine = get_shared_engine()
    async with engine.connect() as conn:
        result = await conn.execute(text(query), params)
        rows = result.mappings().all()
        return [_serialize_row(dict(row)) for row in rows]


# ---------------------------------------------------------------------------
# 11. GET /events/calendar – Upcoming scheduled events
# ---------------------------------------------------------------------------


@router.get("/calendar")
async def calendar_events(
    days: int = Query(default=30, ge=1, le=365),
    scope: str = Query(default="my", regex="^(my|all)$"),
):
    """Upcoming scheduled events sorted ascending by scheduled_for_utc."""
    params: dict[str, Any] = {"days": days}

    portfolio_filter = ""
    if scope == "my":
        engine = get_shared_engine()
        async with engine.connect() as conn:
            ptickers = await conn.execute(text(
                "SELECT DISTINCT UPPER(symbol) as symbol FROM positions_current "
                "WHERE position != 0 AND symbol IS NOT NULL"
            ))
            tickers = [r.symbol for r in ptickers]

        if tickers:
            ticker_conditions = []
            for i, tk in enumerate(tickers):
                key = f"ctk_{i}"
                ticker_conditions.append(f"tickers LIKE :{key}")
                params[key] = f"%{tk}%"
            ticker_or = " OR ".join(ticker_conditions)
            portfolio_filter = f"AND (type = 'MACRO_SCHEDULE' OR ({ticker_or}))"
        else:
            portfolio_filter = "AND type = 'MACRO_SCHEDULE'"

    query = f"""
        SELECT id, ts_utc, scheduled_for_utc, type, tickers, title,
               source_name, source_url, raw_text_snippet, severity_score,
               reason_codes, llm_summary, status, metadata_json,
               created_at_utc, updated_at_utc
        FROM events
        WHERE scheduled_for_utc IS NOT NULL
          AND scheduled_for_utc > NOW()
          AND scheduled_for_utc < NOW() + MAKE_INTERVAL(days => :days)
          {portfolio_filter}
        ORDER BY scheduled_for_utc ASC
    """

    engine = get_shared_engine()
    async with engine.connect() as conn:
        result = await conn.execute(text(query), params)
        rows = result.mappings().all()

        now_utc = datetime.now(timezone.utc)
        return {
            "items": [_serialize_row(dict(row)) for row in rows],
            "range": {
                "start": now_utc.isoformat(),
                "end": (now_utc + timedelta(days=days)).isoformat(),
            },
            "now_utc": now_utc.isoformat(),
        }


# ---------------------------------------------------------------------------
# 12. GET /events/since – Polling fallback for live updates
# ---------------------------------------------------------------------------


@router.get("/since")
async def events_since(
    since_ts: str = Query(description="ISO timestamp — return events newer than this"),
    scope: str = Query(default="my", regex="^(my|all)$"),
    min_severity: int = Query(default=0, ge=0, le=100),
    types: Optional[str] = Query(default=None, description="Comma-separated event types to include"),
):
    """Polling fallback — returns events newer than the given timestamp."""
    try:
        since_dt = datetime.fromisoformat(since_ts)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid since_ts format")

    params: dict[str, Any] = {"since_ts": since_dt, "min_sev": min_severity}

    # Type filter
    type_filter = ""
    if types:
        type_list = [t.strip() for t in types.split(",") if t.strip()]
        if type_list:
            type_placeholders = []
            for i, t in enumerate(type_list):
                key = f"type_{i}"
                type_placeholders.append(f":{key}")
                params[key] = t
            type_filter = f"AND type IN ({', '.join(type_placeholders)})"

    portfolio_filter = ""
    if scope == "my":
        engine = get_shared_engine()
        async with engine.connect() as conn:
            ptickers = await conn.execute(text(
                "SELECT DISTINCT UPPER(symbol) as symbol FROM positions_current "
                "WHERE position != 0 AND symbol IS NOT NULL"
            ))
            tickers = [r.symbol for r in ptickers]

        if tickers:
            ticker_conditions = []
            for i, tk in enumerate(tickers):
                key = f"stk_{i}"
                ticker_conditions.append(f"tickers LIKE :{key}")
                params[key] = f"%{tk}%"
            ticker_or = " OR ".join(ticker_conditions)
            portfolio_filter = f"AND (({ticker_or}) OR severity_score >= 70)"
        else:
            portfolio_filter = "AND severity_score >= 70"

    query = f"""
        SELECT id, ts_utc, scheduled_for_utc, type, tickers, title,
               source_name, source_url, raw_text_snippet, severity_score,
               reason_codes, llm_summary, status, metadata_json,
               created_at_utc, updated_at_utc
        FROM events
        WHERE ts_utc > :since_ts AND severity_score >= :min_sev
              {type_filter} {portfolio_filter}
        ORDER BY ts_utc DESC
        LIMIT 100
    """

    engine = get_shared_engine()
    async with engine.connect() as conn:
        result = await conn.execute(text(query), params)
        rows = result.mappings().all()
        return [_serialize_row(dict(row)) for row in rows]


# ---------------------------------------------------------------------------
# 13. GET /events/ticker/{symbol}/overview – Ticker desk
# ---------------------------------------------------------------------------


@router.get("/ticker/{symbol}/overview")
async def ticker_overview(
    symbol: str,
    days: int = Query(default=7, ge=1, le=90),
):
    """Ticker desk — returns position context + events for a specific ticker."""
    symbol = symbol.upper()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    engine = get_shared_engine()
    async with engine.connect() as conn:
        # 1. Position context
        pos_result = await conn.execute(text(
            "SELECT symbol, position, avg_cost, market_price, market_value, "
            "unrealized_pnl, sector, ib_category "
            "FROM positions_current WHERE UPPER(symbol) = :symbol LIMIT 1"
        ), {"symbol": symbol})
        pos_row = pos_result.mappings().first()

        position_context = None
        if pos_row:
            pos_dict = dict(pos_row)
            # Compute weight (needs total MV)
            total_mv = await conn.execute(text(
                "SELECT SUM(ABS(market_value)) as tmv FROM positions_current "
                "WHERE position != 0"
            ))
            tmv = total_mv.scalar() or 1
            pos_dict["weight_pct"] = round(abs(pos_dict.get("market_value", 0)) / tmv * 100, 2)
            position_context = pos_dict

        # 2. Recent events for this ticker — fetch per-type so filings
        #    don't get crowded out by high-volume RSS news.
        _event_cols = (
            "id, ts_utc, scheduled_for_utc, type, tickers, title, "
            "source_name, source_url, raw_text_snippet, severity_score, "
            "reason_codes, llm_summary, status, metadata_json, "
            "created_at_utc, updated_at_utc"
        )
        recent_events: list[dict] = []
        for _etype, _limit in [("SEC_FILING", 20), ("RSS_NEWS", 100), ("MACRO_SCHEDULE", 10), ("OTHER", 10)]:
            etype_result = await conn.execute(text(
                f"SELECT {_event_cols} FROM events "
                "WHERE tickers LIKE :ticker_pattern AND ts_utc >= :cutoff AND type = :etype "
                "ORDER BY ts_utc DESC LIMIT :lim"
            ), {"ticker_pattern": f"%{symbol}%", "cutoff": cutoff, "etype": _etype, "lim": _limit})
            rows = [_serialize_row(dict(r)) for r in etype_result.mappings().all()]

            # For RSS_NEWS, post-filter Google News articles to only keep
            # those that actually mention the ticker in their text.  Google
            # News search returns many tangentially-related articles that
            # get force-tagged with the search ticker during ingestion.
            if _etype == "RSS_NEWS":
                rows = _filter_ticker_relevance(rows, symbol)[:30]

            recent_events.extend(rows)
        # Sort combined results by ts_utc descending
        recent_events.sort(key=lambda e: e.get("ts_utc", ""), reverse=True)

        # 3. Upcoming scheduled events for this ticker
        upcoming_result = await conn.execute(text(
            "SELECT id, ts_utc, scheduled_for_utc, type, tickers, title, "
            "source_name, source_url, severity_score, reason_codes, "
            "status, metadata_json "
            "FROM events WHERE tickers LIKE :ticker_pattern "
            "AND scheduled_for_utc IS NOT NULL AND scheduled_for_utc > NOW() "
            "ORDER BY scheduled_for_utc ASC LIMIT 20"
        ), {"ticker_pattern": f"%{symbol}%"})
        upcoming = [_serialize_row(dict(r)) for r in upcoming_result.mappings().all()]

    return {
        "symbol": symbol,
        "position": position_context,
        "events": recent_events,
        "upcoming": upcoming,
    }


# ---------------------------------------------------------------------------
# 14. Keyword watchlist CRUD
# ---------------------------------------------------------------------------

_keywords_router = APIRouter(prefix="/keywords", tags=["keywords"])


class KeywordCreate(BaseModel):
    """Body for POST /events/keywords."""
    keyword: str


@_keywords_router.get("")
async def list_keywords() -> list[dict[str, Any]]:
    """Return all keyword watchlist entries."""
    try:
        engine = get_shared_engine()
        async with engine.connect() as conn:
            result = await conn.execute(text(
                "SELECT id, keyword, enabled, created_at_utc "
                "FROM keyword_watchlist ORDER BY keyword ASC"
            ))
            return [_serialize_row(dict(r)) for r in result.mappings().all()]
    except Exception as e:
        logger.exception("list_keywords_failed")
        raise HTTPException(status_code=500, detail=str(e))


@_keywords_router.post("")
async def add_keyword(body: KeywordCreate) -> dict[str, Any]:
    """Add a keyword to the watchlist."""
    kw = body.keyword.strip()
    if not kw:
        raise HTTPException(status_code=400, detail="Keyword cannot be empty")

    try:
        engine = get_shared_engine()
        async with engine.begin() as conn:
            result = await conn.execute(text(
                "INSERT INTO keyword_watchlist (keyword) VALUES (:kw) "
                "ON CONFLICT (keyword) DO NOTHING RETURNING id"
            ), {"kw": kw.lower()})
            row = result.first()
            if row is None:
                return {"ok": True, "message": "Keyword already exists"}
            return {"ok": True, "id": row[0], "keyword": kw.lower()}
    except Exception as e:
        logger.exception("add_keyword_failed")
        raise HTTPException(status_code=500, detail=str(e))


@_keywords_router.delete("/{keyword_id}")
async def delete_keyword(keyword_id: int) -> dict[str, Any]:
    """Remove a keyword from the watchlist."""
    try:
        engine = get_shared_engine()
        async with engine.begin() as conn:
            result = await conn.execute(text(
                "DELETE FROM keyword_watchlist WHERE id = :id"
            ), {"id": keyword_id})
            if result.rowcount == 0:
                raise HTTPException(status_code=404, detail="Keyword not found")
        return {"ok": True, "id": keyword_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("delete_keyword_failed")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Mount sub-routers
# ---------------------------------------------------------------------------

router.include_router(_alerts_router)
router.include_router(_keywords_router)
