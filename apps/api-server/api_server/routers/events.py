"""Events and Alerts API endpoints (Phase 2).

Provides REST endpoints for managing ingested events (SEC filings, macro
schedule, RSS news) and user-facing alerts surfaced from event scoring
and risk spikes.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

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
            SELECT id, ts_utc, type, tickers, title, source_name, source_url,
                   raw_text_snippet, severity_score, reason_codes, llm_summary,
                   status, metadata_json, created_at_utc, updated_at_utc
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
            SELECT id, ts_utc, type, tickers, title, source_name, source_url,
                   raw_text_snippet, severity_score, reason_codes, llm_summary,
                   status, metadata_json, created_at_utc, updated_at_utc
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
# Mount the alerts sub-router
# ---------------------------------------------------------------------------

router.include_router(_alerts_router)
