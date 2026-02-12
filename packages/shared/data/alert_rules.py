"""Alert rules engine for the Trading Workstation notification system.

Evaluates portfolio conditions and event triggers to generate alerts that
appear in the Notification Center.  Each rule implements deduplication logic
to avoid spamming the user with repeated notifications.

Rules
-----
1. KEYWORD_MATCH        -- events whose title/snippet matches a user keyword
2. MACRO_UPCOMING       -- macro schedule events within the next 24 hours
3. VAR_SPIKE            -- portfolio 1d 95% VaR exceeds threshold
4. CONCENTRATION_WARNING -- single position > weight threshold
5. DATA_STALE           -- price data older than N calendar days
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from ..db.engine import get_shared_engine

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    """Return the current UTC datetime, timezone-aware."""
    return datetime.now(timezone.utc)


def _today_utc() -> date:
    """Return today's date in UTC."""
    return _utcnow().date()


def _make_alert(
    alert_type: str,
    message: str,
    severity: int,
    related_event_id: str | None = None,
) -> dict[str, Any]:
    """Build an alert dict ready for insertion into the ``alerts`` table."""
    return {
        "ts_utc": _utcnow(),
        "type": alert_type,
        "message": message[:300],
        "severity": max(0, min(severity, 100)),
        "related_event_id": related_event_id,
        "status": "NEW",
        "snoozed_until": None,
    }


async def _table_exists(conn, table_name: str) -> bool:
    """Check whether a table exists in the current database (Postgres)."""
    result = await conn.execute(
        text(
            "SELECT EXISTS ("
            "  SELECT 1 FROM information_schema.tables"
            "  WHERE table_name = :tbl"
            ")"
        ),
        {"tbl": table_name},
    )
    row = result.scalar()
    return bool(row)


# ---------------------------------------------------------------------------
# Rule 1: Keyword match alerts
# ---------------------------------------------------------------------------


async def _check_keyword_matches(engine: AsyncEngine) -> list[dict]:
    """Create alerts for events whose title or snippet matches a user keyword.

    Reads enabled keywords from the ``keyword_watchlist`` table and scans
    recent NEW events for case-insensitive substring matches.

    Deduplication: skip events that already have a matching KEYWORD_MATCH alert
    (``related_event_id``).
    """
    alerts: list[dict] = []

    try:
        async with engine.connect() as conn:
            for tbl in ("events", "alerts", "keyword_watchlist"):
                if not await _table_exists(conn, tbl):
                    logger.debug("alert_rules_skip_keyword", reason=f"{tbl} table missing")
                    return alerts

            # Fetch enabled keywords
            kw_result = await conn.execute(
                text("SELECT keyword FROM keyword_watchlist WHERE enabled = 1")
            )
            keywords = [str(r[0]).strip().lower() for r in kw_result if r[0]]

            if not keywords:
                return alerts

            # Fetch recent NEW events (last 24 hours)
            cutoff = _utcnow() - timedelta(hours=24)
            stmt = text("""
                SELECT e.id, e.title, e.raw_text_snippet, e.severity_score
                FROM events e
                WHERE e.status = 'NEW'
                  AND e.ts_utc >= :cutoff
                  AND NOT EXISTS (
                      SELECT 1 FROM alerts a
                      WHERE a.related_event_id = e.id
                        AND a.type = 'KEYWORD_MATCH'
                  )
                ORDER BY e.ts_utc DESC
            """)
            result = await conn.execute(stmt, {"cutoff": cutoff})
            rows = result.mappings().all()

            for row in rows:
                title = str(row["title"] or "").lower()
                snippet = str(row["raw_text_snippet"] or "").lower()
                search_text = f"{title} {snippet}"

                matched = [kw for kw in keywords if kw in search_text]
                if matched:
                    display_title = str(row["title"] or "Unknown event")
                    kw_display = ", ".join(matched[:3])
                    severity = max(int(row["severity_score"] or 50), 70)
                    alerts.append(
                        _make_alert(
                            alert_type="KEYWORD_MATCH",
                            message=f"Keyword [{kw_display}]: {display_title}",
                            severity=severity,
                            related_event_id=str(row["id"]),
                        )
                    )

    except Exception:
        logger.error("alert_rules_keyword_match_error", exc_info=True)

    return alerts


# ---------------------------------------------------------------------------
# Rule 2: Upcoming macro releases (within 24 h)
# ---------------------------------------------------------------------------


async def _check_upcoming_macro(engine: AsyncEngine) -> list[dict]:
    """Create alerts for MACRO_SCHEDULE events happening in the next 24 hours.

    Deduplication: one alert per schedule event (by ``related_event_id``).
    """
    alerts: list[dict] = []

    try:
        async with engine.connect() as conn:
            if not await _table_exists(conn, "events"):
                logger.debug("alert_rules_skip_macro_upcoming", reason="events table missing")
                return alerts

            if not await _table_exists(conn, "alerts"):
                logger.debug("alert_rules_skip_macro_upcoming", reason="alerts table missing")
                return alerts

            now = _utcnow()
            window_end = now + timedelta(hours=24)

            stmt = text("""
                SELECT e.id, e.title, e.severity_score, e.ts_utc
                FROM events e
                WHERE e.type = 'MACRO_SCHEDULE'
                  AND e.ts_utc >= :now
                  AND e.ts_utc <= :window_end
                  AND NOT EXISTS (
                      SELECT 1 FROM alerts a
                      WHERE a.related_event_id = e.id
                        AND a.type = 'MACRO_UPCOMING'
                  )
                ORDER BY e.ts_utc ASC
            """)
            result = await conn.execute(stmt, {"now": now, "window_end": window_end})
            rows = result.mappings().all()

            for row in rows:
                title = str(row["title"] or "Macro release")
                severity = int(row["severity_score"] or 50)
                alerts.append(
                    _make_alert(
                        alert_type="MACRO_UPCOMING",
                        message=f"Upcoming: {title}",
                        severity=severity,
                        related_event_id=str(row["id"]),
                    )
                )

    except Exception:
        logger.error("alert_rules_macro_upcoming_error", exc_info=True)

    return alerts


# ---------------------------------------------------------------------------
# Rule 3: VaR spike
# ---------------------------------------------------------------------------


async def _check_var_spike(
    engine: AsyncEngine,
    threshold_pct: float = 3.0,
) -> list[dict]:
    """Create an alert when the latest 1d 95% VaR exceeds *threshold_pct*.

    Deduplication: at most one ``VAR_SPIKE`` alert per calendar day.
    """
    alerts: list[dict] = []

    try:
        async with engine.connect() as conn:
            if not await _table_exists(conn, "risk_results"):
                logger.debug("alert_rules_skip_var_spike", reason="risk_results table missing")
                return alerts

            if not await _table_exists(conn, "alerts"):
                logger.debug("alert_rules_skip_var_spike", reason="alerts table missing")
                return alerts

            # Fetch the latest portfolio-level risk snapshot
            stmt = text("""
                SELECT result_json
                FROM risk_results
                WHERE result_type = 'portfolio_summary'
                ORDER BY created_at DESC
                LIMIT 1
            """)
            result = await conn.execute(stmt)
            row = result.first()

            if row is None:
                return alerts

            try:
                risk_data = json.loads(row[0])
            except (json.JSONDecodeError, TypeError):
                logger.warning("alert_rules_var_spike_bad_json")
                return alerts

            var_pct = risk_data.get("var_95_1d_pct")
            if var_pct is None:
                return alerts

            var_pct = float(var_pct)
            if var_pct <= threshold_pct:
                return alerts

            # Dedupe: already fired today?
            today = _today_utc()
            dedup_stmt = text("""
                SELECT 1 FROM alerts
                WHERE type = 'VAR_SPIKE'
                  AND DATE(ts_utc AT TIME ZONE 'UTC') = :today
                LIMIT 1
            """)
            existing = await conn.execute(dedup_stmt, {"today": today})
            if existing.first() is not None:
                return alerts

            alerts.append(
                _make_alert(
                    alert_type="VAR_SPIKE",
                    message=(
                        f"Portfolio VaR spike: {var_pct:.1f}% "
                        f"(1d 95% VaR exceeds {threshold_pct}%)"
                    ),
                    severity=85,
                )
            )

    except Exception:
        logger.error("alert_rules_var_spike_error", exc_info=True)

    return alerts


# ---------------------------------------------------------------------------
# Rule 4: Position concentration warning
# ---------------------------------------------------------------------------


async def _check_concentration(
    engine: AsyncEngine,
    threshold_pct: float = 15.0,
) -> list[dict]:
    """Create alerts when any single position exceeds *threshold_pct* of
    total portfolio value.

    Deduplication: one alert per symbol per calendar day.
    """
    alerts: list[dict] = []

    try:
        async with engine.connect() as conn:
            if not await _table_exists(conn, "positions_current"):
                logger.debug(
                    "alert_rules_skip_concentration",
                    reason="positions_current table missing",
                )
                return alerts

            if not await _table_exists(conn, "alerts"):
                logger.debug(
                    "alert_rules_skip_concentration",
                    reason="alerts table missing",
                )
                return alerts

            stmt = text("""
                SELECT symbol, market_value
                FROM positions_current
                WHERE market_value IS NOT NULL
                  AND market_value != 0
            """)
            result = await conn.execute(stmt)
            rows = result.mappings().all()

            if not rows:
                return alerts

            total_value = sum(abs(float(r["market_value"])) for r in rows)
            if total_value == 0:
                return alerts

            today = _today_utc()

            for row in rows:
                symbol = str(row["symbol"])
                mv = abs(float(row["market_value"]))
                weight = (mv / total_value) * 100.0

                if weight <= threshold_pct:
                    continue

                # Dedupe: already alerted for this symbol today?
                dedup_stmt = text("""
                    SELECT 1 FROM alerts
                    WHERE type = 'CONCENTRATION_WARNING'
                      AND message LIKE :symbol_pattern
                      AND DATE(ts_utc AT TIME ZONE 'UTC') = :today
                    LIMIT 1
                """)
                existing = await conn.execute(
                    dedup_stmt,
                    {"symbol_pattern": f"%{symbol}%", "today": today},
                )
                if existing.first() is not None:
                    continue

                alerts.append(
                    _make_alert(
                        alert_type="CONCENTRATION_WARNING",
                        message=(
                            f"Position concentration: {symbol} is "
                            f"{weight:.1f}% of portfolio"
                        ),
                        severity=70,
                    )
                )

    except Exception:
        logger.error("alert_rules_concentration_error", exc_info=True)

    return alerts


# ---------------------------------------------------------------------------
# Rule 5: Data staleness
# ---------------------------------------------------------------------------


async def _check_data_staleness(
    engine: AsyncEngine,
    max_stale_days: int = 3,
) -> list[dict]:
    """Create an alert when price data is older than *max_stale_days*
    calendar days.

    Checks ``data_sync_status`` first, then falls back to ``prices_daily``
    for the most recent date.  Degrades gracefully if neither table exists.

    Deduplication: at most one ``DATA_STALE`` alert per calendar day.
    """
    alerts: list[dict] = []

    try:
        async with engine.connect() as conn:
            if not await _table_exists(conn, "alerts"):
                logger.debug(
                    "alert_rules_skip_data_stale",
                    reason="alerts table missing",
                )
                return alerts

            last_date: date | None = None

            # Prefer data_sync_status (more authoritative)
            if await _table_exists(conn, "data_sync_status"):
                stmt = text("""
                    SELECT MAX(last_date) AS latest
                    FROM data_sync_status
                    WHERE source = 'yahoo'
                """)
                result = await conn.execute(stmt)
                row = result.first()
                if row and row[0] is not None:
                    last_date = row[0] if isinstance(row[0], date) else row[0].date()

            # Fallback: prices_daily
            if last_date is None and await _table_exists(conn, "prices_daily"):
                stmt = text("""
                    SELECT MAX(date) AS latest
                    FROM prices_daily
                """)
                result = await conn.execute(stmt)
                row = result.first()
                if row and row[0] is not None:
                    last_date = row[0] if isinstance(row[0], date) else row[0].date()

            if last_date is None:
                logger.debug("alert_rules_data_stale_no_data")
                return alerts

            today = _today_utc()
            days_stale = (today - last_date).days

            if days_stale < max_stale_days:
                return alerts

            # Dedupe: already alerted today?
            dedup_stmt = text("""
                SELECT 1 FROM alerts
                WHERE type = 'DATA_STALE'
                  AND DATE(ts_utc AT TIME ZONE 'UTC') = :today
                LIMIT 1
            """)
            existing = await conn.execute(dedup_stmt, {"today": today})
            if existing.first() is not None:
                return alerts

            alerts.append(
                _make_alert(
                    alert_type="DATA_STALE",
                    message=(
                        f"Price data may be stale -- last update was "
                        f"{days_stale} days ago"
                    ),
                    severity=65,
                )
            )

    except Exception:
        logger.error("alert_rules_data_stale_error", exc_info=True)

    return alerts


# ---------------------------------------------------------------------------
# Bulk insert
# ---------------------------------------------------------------------------


async def _insert_alerts(
    alerts: list[dict],
    engine: AsyncEngine,
) -> int:
    """Bulk-insert new alert rows into the ``alerts`` table.

    Returns:
        Number of rows successfully inserted.
    """
    if not alerts:
        return 0

    stmt = text("""
        INSERT INTO alerts (
            ts_utc, type, message, severity,
            related_event_id, status, snoozed_until
        ) VALUES (
            :ts_utc, :type, :message, :severity,
            :related_event_id, :status, :snoozed_until
        )
    """)

    inserted = 0
    try:
        async with engine.begin() as conn:
            for alert in alerts:
                await conn.execute(stmt, alert)
                inserted += 1
    except Exception:
        logger.error(
            "alert_rules_insert_error",
            attempted=len(alerts),
            inserted=inserted,
            exc_info=True,
        )

    return inserted


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def run_alert_rules(
    engine: AsyncEngine | None = None,
) -> dict[str, Any]:
    """Evaluate all alert rules and insert any new alerts.

    This is the primary entry point.  It runs each rule check, collects the
    resulting alerts, bulk-inserts them, and returns a summary.

    Args:
        engine: SQLAlchemy async engine.  Falls back to the shared singleton.

    Returns:
        Dictionary with ``rules_evaluated``, ``alerts_created``, and
        ``by_type`` counters.
    """
    if engine is None:
        engine = get_shared_engine()

    all_alerts: list[dict] = []
    by_type: dict[str, int] = {}
    rules_evaluated = 0

    # --- Rule 1: Keyword match alerts ---
    try:
        rules_evaluated += 1
        keyword_matches = await _check_keyword_matches(engine)
        all_alerts.extend(keyword_matches)
        if keyword_matches:
            by_type["KEYWORD_MATCH"] = len(keyword_matches)
        logger.debug(
            "alert_rule_evaluated",
            rule="KEYWORD_MATCH",
            new_alerts=len(keyword_matches),
        )
    except Exception:
        logger.error("alert_rule_failed", rule="KEYWORD_MATCH", exc_info=True)

    # --- Rule 2: Upcoming macro releases ---
    try:
        rules_evaluated += 1
        macro_upcoming = await _check_upcoming_macro(engine)
        all_alerts.extend(macro_upcoming)
        if macro_upcoming:
            by_type["MACRO_UPCOMING"] = len(macro_upcoming)
        logger.debug(
            "alert_rule_evaluated",
            rule="MACRO_UPCOMING",
            new_alerts=len(macro_upcoming),
        )
    except Exception:
        logger.error("alert_rule_failed", rule="MACRO_UPCOMING", exc_info=True)

    # --- Rule 3: VaR spike ---
    try:
        rules_evaluated += 1
        var_spike = await _check_var_spike(engine)
        all_alerts.extend(var_spike)
        if var_spike:
            by_type["VAR_SPIKE"] = len(var_spike)
        logger.debug(
            "alert_rule_evaluated",
            rule="VAR_SPIKE",
            new_alerts=len(var_spike),
        )
    except Exception:
        logger.error("alert_rule_failed", rule="VAR_SPIKE", exc_info=True)

    # --- Rule 4: Position concentration ---
    try:
        rules_evaluated += 1
        concentration = await _check_concentration(engine)
        all_alerts.extend(concentration)
        if concentration:
            by_type["CONCENTRATION_WARNING"] = len(concentration)
        logger.debug(
            "alert_rule_evaluated",
            rule="CONCENTRATION_WARNING",
            new_alerts=len(concentration),
        )
    except Exception:
        logger.error("alert_rule_failed", rule="CONCENTRATION_WARNING", exc_info=True)

    # --- Rule 5: Data staleness ---
    try:
        rules_evaluated += 1
        data_stale = await _check_data_staleness(engine)
        all_alerts.extend(data_stale)
        if data_stale:
            by_type["DATA_STALE"] = len(data_stale)
        logger.debug(
            "alert_rule_evaluated",
            rule="DATA_STALE",
            new_alerts=len(data_stale),
        )
    except Exception:
        logger.error("alert_rule_failed", rule="DATA_STALE", exc_info=True)

    # --- Insert all collected alerts ---
    alerts_created = await _insert_alerts(all_alerts, engine)

    summary = {
        "rules_evaluated": rules_evaluated,
        "alerts_created": alerts_created,
        "by_type": by_type,
    }

    logger.info("alert_rules_completed", **summary)
    return summary


# ---------------------------------------------------------------------------
# Snooze maintenance
# ---------------------------------------------------------------------------


async def cleanup_expired_snoozes(
    engine: AsyncEngine | None = None,
) -> int:
    """Un-snooze alerts whose ``snoozed_until`` has passed.

    Sets the status back to ``'NEW'`` so they reappear in the notification
    centre.

    Args:
        engine: SQLAlchemy async engine.  Falls back to the shared singleton.

    Returns:
        Number of alerts that were un-snoozed.
    """
    if engine is None:
        engine = get_shared_engine()

    try:
        async with engine.begin() as conn:
            if not await _table_exists(conn, "alerts"):
                logger.debug(
                    "cleanup_expired_snoozes_skip",
                    reason="alerts table missing",
                )
                return 0

            now = _utcnow()
            stmt = text("""
                UPDATE alerts
                SET status = 'NEW'
                WHERE status = 'SNOOZED'
                  AND snoozed_until IS NOT NULL
                  AND snoozed_until < :now
            """)
            result = await conn.execute(stmt, {"now": now})
            count = result.rowcount

        if count:
            logger.info("expired_snoozes_cleared", count=count)

        return count

    except Exception:
        logger.error("cleanup_expired_snoozes_error", exc_info=True)
        return 0
