"""Portfolio-aware materiality scoring engine.

Takes raw events (from EDGAR, macro schedules, RSS) and applies
portfolio-aware materiality scoring to prioritise what matters most
to the user's current holdings.  The scoring engine UPGRADES the
``severity_score`` already set by connectors and appends reason codes
that explain the boost.

Usage::

    from shared.data.scoring import score_new_events

    stats = await score_new_events()
    # {"events_scored": 12, "boosted": 8, "critical": 2, ...}
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from ..db.engine import get_shared_engine

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Severity tier boundaries
# ---------------------------------------------------------------------------

_TIER_CRITICAL = 85
_TIER_HIGH = 75
_TIER_MEDIUM = 50

# ---------------------------------------------------------------------------
# Booster constants
# ---------------------------------------------------------------------------

_BOOST_DIRECT_HOLDING = 0  # No boost for merely holding — scope=my already filters to portfolio
_BOOST_LARGE_POSITION = 10  # additional if weight > 5%
_BOOST_HIGH_VOL = 5         # annualised vol > 40%
_BOOST_SECTOR_CONCENTRATION = 5  # sector weight > 20%
_BOOST_CORRELATED_CLUSTER = 5
_BOOST_UPCOMING_EXPIRY = 10  # option expires within 7 days (future)

_LARGE_POSITION_THRESHOLD_PCT = 5.0
_HIGH_VOL_THRESHOLD = 0.40  # 40% annualised
_SECTOR_CONCENTRATION_THRESHOLD_PCT = 20.0

_MAX_SCORE = 100

# ---------------------------------------------------------------------------
# Annualised-vol helper (252 trading days)
# ---------------------------------------------------------------------------

_TRADING_DAYS = 252


def _annualised_vol_from_daily(daily_returns: list[float]) -> float:
    """Compute annualised volatility from a list of daily returns.

    Returns 0.0 when there are fewer than two observations.
    """
    if len(daily_returns) < 2:
        return 0.0
    mean = sum(daily_returns) / len(daily_returns)
    variance = sum((r - mean) ** 2 for r in daily_returns) / (len(daily_returns) - 1)
    return math.sqrt(variance) * math.sqrt(_TRADING_DAYS)


# ---------------------------------------------------------------------------
# 1. Portfolio context
# ---------------------------------------------------------------------------


async def _get_portfolio_context(engine: AsyncEngine) -> dict:
    """Gather portfolio context needed for materiality scoring.

    Returns a dict with keys:
        holdings   - {ticker: {"weight_pct": float, "market_value": float}}
        total_value - float
        sectors    - {sector_name: weight_pct}
        tickers    - set[str]
        vol        - {ticker: annualised_vol} (best-effort from prices_daily)
    """
    context: dict[str, Any] = {
        "holdings": {},
        "total_value": 0.0,
        "sectors": {},
        "ticker_sector": {},  # ticker -> sector name
        "tickers": set(),
        "vol": {},
    }

    # ------------------------------------------------------------------
    # 1a. Fetch positions
    # ------------------------------------------------------------------
    try:
        async with engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        """
                        SELECT symbol,
                               COALESCE(market_value, 0) AS market_value,
                               COALESCE(sector, 'Unknown') AS sector
                        FROM positions_current
                        WHERE symbol != 'CASH'
                          AND position != 0
                        ORDER BY symbol
                        """
                    )
                )
            ).mappings().all()
    except Exception:
        logger.error("scoring_portfolio_query_failed", exc_info=True)
        return context

    if not rows:
        logger.warning("scoring_no_positions")
        return context

    # Compute total market value (gross) for weight calculation
    total_value = sum(abs(float(r["market_value"])) for r in rows)
    if total_value == 0:
        logger.warning("scoring_zero_total_value")
        return context

    context["total_value"] = total_value

    sector_values: dict[str, float] = {}

    for row in rows:
        ticker = str(row["symbol"]).upper().strip()
        mv = float(row["market_value"])
        sector = str(row["sector"])
        weight_pct = (abs(mv) / total_value) * 100.0

        context["holdings"][ticker] = {
            "weight_pct": weight_pct,
            "market_value": mv,
        }
        context["tickers"].add(ticker)
        context["ticker_sector"][ticker] = sector

        sector_values.setdefault(sector, 0.0)
        sector_values[sector] += abs(mv)

    # Convert sector values to weight percentages
    for sector, sv in sector_values.items():
        context["sectors"][sector] = (sv / total_value) * 100.0

    # ------------------------------------------------------------------
    # 1b. Best-effort per-ticker annualised vol from prices_daily
    # ------------------------------------------------------------------
    try:
        async with engine.connect() as conn:
            # Fetch the last 90 days of adj_close for portfolio tickers.
            # We compute simple daily returns in Python to avoid
            # database-specific window function differences.
            vol_rows = (
                await conn.execute(
                    text(
                        """
                        SELECT symbol, date, COALESCE(adj_close, close) AS px
                        FROM prices_daily
                        WHERE symbol = ANY(:syms)
                        ORDER BY symbol, date
                        """
                    ),
                    {"syms": list(context["tickers"])},
                )
            ).mappings().all()

        # Group by symbol and compute returns
        prices_by_sym: dict[str, list[float]] = {}
        for vr in vol_rows:
            sym = str(vr["symbol"])
            prices_by_sym.setdefault(sym, []).append(float(vr["px"]))

        for sym, prices in prices_by_sym.items():
            if len(prices) < 10:
                continue
            # Use only the last ~90 prices for a recent-ish window
            recent = prices[-90:]
            daily_rets = [
                (recent[i] - recent[i - 1]) / recent[i - 1]
                for i in range(1, len(recent))
                if recent[i - 1] != 0
            ]
            vol = _annualised_vol_from_daily(daily_rets)
            if vol > 0:
                context["vol"][sym] = vol

    except Exception:
        # Vol enrichment is best-effort; log and move on
        logger.warning("scoring_vol_query_failed", exc_info=True)

    logger.info(
        "scoring_portfolio_context",
        holdings=len(context["holdings"]),
        sectors=len(context["sectors"]),
        tickers_with_vol=len(context["vol"]),
        total_value=context["total_value"],
    )
    return context


# ---------------------------------------------------------------------------
# 2. Compute portfolio boost for a single event
# ---------------------------------------------------------------------------


def _compute_portfolio_boost(
    event: dict,
    portfolio: dict,
) -> tuple[int, list[str]]:
    """Compute the additive boost and reason codes for *event*.

    Args:
        event: dict with at least ``tickers`` (JSON string) and ``type``.
        portfolio: dict returned by :func:`_get_portfolio_context`.

    Returns:
        (total_boost, reason_codes) where *total_boost* is a non-negative
        integer and *reason_codes* is a list of human-readable strings.
    """
    boost = 0
    reasons: list[str] = []

    # Parse event tickers --------------------------------------------------
    event_tickers: list[str] = []
    raw_tickers = event.get("tickers")
    if raw_tickers:
        try:
            parsed = json.loads(raw_tickers)
            if isinstance(parsed, list):
                event_tickers = [str(t).upper().strip() for t in parsed if t]
        except (json.JSONDecodeError, TypeError):
            pass

    portfolio_tickers: set[str] = portfolio.get("tickers", set())
    holdings: dict[str, dict] = portfolio.get("holdings", {})
    sectors: dict[str, float] = portfolio.get("sectors", {})
    ticker_sector: dict[str, str] = portfolio.get("ticker_sector", {})
    vol: dict[str, float] = portfolio.get("vol", {})

    # Rule 1 & 2 & 3: Direct holding / large position / high vol -----------
    for ticker in event_tickers:
        if ticker in portfolio_tickers:
            boost += _BOOST_DIRECT_HOLDING
            reasons.append(f"portfolio_holding:{ticker}")

            weight = holdings.get(ticker, {}).get("weight_pct", 0.0)
            if weight > _LARGE_POSITION_THRESHOLD_PCT:
                boost += _BOOST_LARGE_POSITION
                reasons.append(f"large_position:{ticker}:{weight:.1f}pct")

            ticker_vol = vol.get(ticker, 0.0)
            if ticker_vol > _HIGH_VOL_THRESHOLD:
                boost += _BOOST_HIGH_VOL
                reasons.append(
                    f"high_vol:{ticker}:{ticker_vol * 100:.0f}pct_ann"
                )

    # Rule 4: Sector concentration ------------------------------------------
    # For each event ticker that is a portfolio holding, look up its sector
    # and check whether that sector exceeds the concentration threshold.
    # The boost is applied at most once per scoring call.
    sector_boost_applied = False
    for ticker in event_tickers:
        if sector_boost_applied:
            break
        if ticker not in portfolio_tickers:
            continue
        event_sector = ticker_sector.get(ticker, "Unknown")
        if event_sector == "Unknown":
            continue
        sector_weight = sectors.get(event_sector, 0.0)
        if sector_weight > _SECTOR_CONCENTRATION_THRESHOLD_PCT:
            boost += _BOOST_SECTOR_CONCENTRATION
            reasons.append(
                f"sector_concentration:{event_sector}:{sector_weight:.1f}pct"
            )
            sector_boost_applied = True

    # Rule 5: Correlated cluster (placeholder) ------------------------------
    # Full implementation would query the latest risk_results for cluster
    # labels and check if event tickers belong to the same cluster as a
    # large holding.  For now we leave the hook in place but don't boost.
    # _boost_correlated_cluster(event_tickers, portfolio, boost, reasons)

    return boost, reasons


# ---------------------------------------------------------------------------
# 3. Main scoring entry point
# ---------------------------------------------------------------------------


async def score_events(
    event_ids: list[str] | None = None,
    rescore_all: bool = False,
    engine: AsyncEngine | None = None,
) -> dict[str, Any]:
    """Score events using portfolio-aware materiality boosting.

    Args:
        event_ids: If provided, score only these events.
        rescore_all: If True, re-score all events with ``status='NEW'``.
        engine: SQLAlchemy async engine (falls back to shared singleton).

    Returns:
        Stats dict: ``events_scored``, ``boosted``, ``critical``,
        ``high``, ``medium``, ``low``.
    """
    if engine is None:
        engine = get_shared_engine()

    portfolio = await _get_portfolio_context(engine)

    stats: dict[str, Any] = {
        "events_scored": 0,
        "boosted": 0,
        "critical": 0,
        "high": 0,
        "medium": 0,
        "low": 0,
    }

    # ------------------------------------------------------------------
    # Determine which events to score
    # ------------------------------------------------------------------
    try:
        if event_ids:
            query = text(
                """
                SELECT id, tickers, type, severity_score,
                       reason_codes, metadata_json
                FROM events
                WHERE id = ANY(:ids)
                """
            )
            params: dict[str, Any] = {"ids": event_ids}
        elif rescore_all:
            query = text(
                """
                SELECT id, tickers, type, severity_score,
                       reason_codes, metadata_json
                FROM events
                WHERE status = 'NEW'
                """
            )
            params = {}
        else:
            # Score events that haven't been portfolio-scored yet
            query = text(
                """
                SELECT id, tickers, type, severity_score,
                       reason_codes, metadata_json
                FROM events
                WHERE status = 'NEW'
                  AND (metadata_json NOT LIKE :scored_pattern
                       OR metadata_json IS NULL)
                """
            )
            params = {"scored_pattern": '%"scored_at"%'}

        async with engine.connect() as conn:
            result = await conn.execute(query, params)
            rows = result.mappings().all()

    except Exception:
        logger.error("scoring_event_query_failed", exc_info=True)
        return stats

    if not rows:
        logger.info("scoring_no_events_to_score")
        return stats

    logger.info("scoring_start", events_to_score=len(rows))

    # ------------------------------------------------------------------
    # Score each event
    # ------------------------------------------------------------------
    now_iso = datetime.now(timezone.utc).isoformat()

    updates: list[dict[str, Any]] = []

    for row in rows:
        event = dict(row)
        event_id: str = event["id"]
        base_score: int = int(event.get("severity_score") or 0)

        boost, boost_reasons = _compute_portfolio_boost(event, portfolio)

        new_score = min(base_score + boost, _MAX_SCORE)
        tier = severity_tier(new_score)

        # Merge reason codes
        existing_reasons: list[str] = []
        raw_reasons = event.get("reason_codes")
        if raw_reasons:
            try:
                parsed_reasons = json.loads(raw_reasons)
                if isinstance(parsed_reasons, list):
                    existing_reasons = parsed_reasons
            except (json.JSONDecodeError, TypeError):
                if isinstance(raw_reasons, str) and raw_reasons.strip():
                    existing_reasons = [raw_reasons.strip()]

        # Append boost reasons, avoiding duplicates
        combined_reasons = list(existing_reasons)
        for br in boost_reasons:
            if br not in combined_reasons:
                combined_reasons.append(br)

        # Update metadata_json with scored_at
        metadata: dict[str, Any] = {}
        raw_meta = event.get("metadata_json")
        if raw_meta:
            try:
                metadata = json.loads(raw_meta)
                if not isinstance(metadata, dict):
                    metadata = {}
            except (json.JSONDecodeError, TypeError):
                metadata = {}

        metadata["scored_at"] = now_iso
        metadata["boost"] = boost
        metadata["tier"] = tier

        updates.append(
            {
                "id": event_id,
                "severity_score": new_score,
                "reason_codes": json.dumps(combined_reasons),
                "metadata_json": json.dumps(metadata),
            }
        )

        stats["events_scored"] += 1
        if boost > 0:
            stats["boosted"] += 1

        # Tier counts
        if tier == "critical":
            stats["critical"] += 1
        elif tier == "high":
            stats["high"] += 1
        elif tier == "medium":
            stats["medium"] += 1
        else:
            stats["low"] += 1

    # ------------------------------------------------------------------
    # Batch update
    # ------------------------------------------------------------------
    if updates:
        update_stmt = text(
            """
            UPDATE events
            SET severity_score = :severity_score,
                reason_codes   = :reason_codes,
                metadata_json  = :metadata_json,
                updated_at_utc = now()
            WHERE id = :id
            """
        )
        try:
            async with engine.begin() as conn:
                for upd in updates:
                    await conn.execute(update_stmt, upd)
        except Exception:
            logger.error(
                "scoring_update_failed",
                events_attempted=len(updates),
                exc_info=True,
            )
            # Reset stats on failure so the caller knows nothing persisted
            return {
                "events_scored": 0,
                "boosted": 0,
                "critical": 0,
                "high": 0,
                "medium": 0,
                "low": 0,
                "error": "update_failed",
            }

    logger.info(
        "scoring_complete",
        events_scored=stats["events_scored"],
        boosted=stats["boosted"],
        critical=stats["critical"],
        high=stats["high"],
        medium=stats["medium"],
        low=stats["low"],
    )
    return stats


# ---------------------------------------------------------------------------
# 4. Convenience: score NEW un-scored events
# ---------------------------------------------------------------------------


async def score_new_events(
    engine: AsyncEngine | None = None,
) -> dict[str, Any]:
    """Score only NEW events that have not been portfolio-scored yet.

    This is the function connectors should call immediately after
    ingesting new events to enrich them with portfolio context.

    Returns:
        Stats dict (same shape as :func:`score_events`).
    """
    if engine is None:
        engine = get_shared_engine()

    # Fetch IDs of un-scored NEW events
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT id
                    FROM events
                    WHERE status = 'NEW'
                      AND (metadata_json NOT LIKE :scored_pattern
                           OR metadata_json IS NULL)
                    ORDER BY ts_utc DESC
                    """
                ),
                {"scored_pattern": '%"scored_at"%'},
            )
            ids = [str(r[0]) for r in result.fetchall()]
    except Exception:
        logger.error("scoring_new_events_query_failed", exc_info=True)
        return {
            "events_scored": 0,
            "boosted": 0,
            "critical": 0,
            "high": 0,
            "medium": 0,
            "low": 0,
        }

    if not ids:
        logger.info("scoring_no_new_events")
        return {
            "events_scored": 0,
            "boosted": 0,
            "critical": 0,
            "high": 0,
            "medium": 0,
            "low": 0,
        }

    return await score_events(event_ids=ids, engine=engine)


# ---------------------------------------------------------------------------
# 5. Severity tier helper
# ---------------------------------------------------------------------------


def severity_tier(score: int) -> str:
    """Map a numeric severity score (0-100) to a tier label.

    Returns:
        One of ``"critical"``, ``"high"``, ``"medium"``, or ``"low"``.
    """
    if score >= _TIER_CRITICAL:
        return "critical"
    if score >= _TIER_HIGH:
        return "high"
    if score >= _TIER_MEDIUM:
        return "medium"
    return "low"
