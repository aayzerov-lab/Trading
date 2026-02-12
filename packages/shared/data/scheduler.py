"""Background job scheduler for data updates and risk recomputation.

Coordinates daily data updates from Yahoo Finance and FRED, and triggers
risk recomputation when portfolio composition changes.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from .fred import fetch_fred_series
from .yahoo import fetch_factor_prices, fetch_prices_yahoo

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# Portfolio hashing for cache invalidation
# ---------------------------------------------------------------------------


def compute_portfolio_hash(positions: list[dict[str, Any]]) -> str:
    """Compute a stable hash of current portfolio composition.

    The hash is based on sorted list of (symbol, position_size) tuples to
    detect when portfolio composition has changed and risk needs recomputing.

    Args:
        positions: List of position dicts with 'symbol' and 'position' keys

    Returns:
        Hex string hash of portfolio composition
    """
    # Extract and sort (symbol, position) tuples
    portfolio_items = [
        (pos["symbol"], float(pos.get("position", pos.get("quantity", 0))))
        for pos in positions
        if pos.get("position", pos.get("quantity", 0)) != 0  # Ignore zero positions
    ]
    portfolio_items.sort()

    # Create stable string representation
    portfolio_str = json.dumps(portfolio_items, sort_keys=True)

    # Compute hash
    return hashlib.sha256(portfolio_str.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Position queries
# ---------------------------------------------------------------------------


async def _get_current_positions(engine: AsyncEngine) -> list[dict[str, Any]]:
    """Fetch current positions from positions_current table."""
    stmt = text("""
        SELECT symbol, position, conid
        FROM positions_current
        WHERE position != 0
        ORDER BY symbol
    """)

    async with engine.connect() as conn:
        result = await conn.execute(stmt)
        rows = result.mappings().all()
        return [dict(row) for row in rows]


async def _get_position_symbols(engine: AsyncEngine) -> list[str]:
    """Get list of unique symbols from current positions."""
    positions = await _get_current_positions(engine)
    return sorted({pos["symbol"] for pos in positions if pos.get("symbol")})


# ---------------------------------------------------------------------------
# Risk result queries
# ---------------------------------------------------------------------------


async def _get_latest_risk_portfolio_hash(engine: AsyncEngine) -> str | None:
    """Get the portfolio_hash from the most recent risk_results entry."""
    stmt = text("""
        SELECT portfolio_hash
        FROM risk_results
        ORDER BY created_at DESC
        LIMIT 1
    """)

    async with engine.connect() as conn:
        result = await conn.execute(stmt)
        row = result.first()
        return row[0] if row else None


# ---------------------------------------------------------------------------
# Daily data update pipeline
# ---------------------------------------------------------------------------


async def run_daily_data_update(
    engine: AsyncEngine,
    redis_client: Any = None,
) -> dict[str, Any]:
    """Run the full daily data update pipeline.

    This function orchestrates the daily data refresh:
    1. Fetch factor prices from Yahoo Finance
    2. Fetch position symbols from positions_current
    3. Fetch prices for position symbols from Yahoo
    4. Fetch FRED economic series
    5. Publish 'data_updated' event to Redis if client provided

    Args:
        engine: Database engine
        redis_client: Optional Redis client for event publishing

    Returns:
        Dictionary with update statistics
    """
    logger.info("daily_data_update_started")
    stats: dict[str, Any] = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "factors": 0,
        "positions": 0,
        "fred_series": 0,
        "errors": [],
    }

    try:
        # Step 1: Fetch factor prices
        logger.info("fetching_factor_prices")
        factor_results = await fetch_factor_prices(engine=engine)
        stats["factors"] = len(factor_results)
        logger.info("factor_prices_updated", count=stats["factors"])

    except Exception as e:
        logger.error("factor_fetch_error", error=str(e), exc_info=True)
        stats["errors"].append(f"factor_fetch: {e}")

    try:
        # Step 2: Get position symbols
        logger.info("fetching_position_symbols")
        position_symbols = await _get_position_symbols(engine)
        logger.info("position_symbols_found", count=len(position_symbols))

        # Step 3: Fetch prices for position symbols
        if position_symbols:
            logger.info("fetching_position_prices")
            position_results = await fetch_prices_yahoo(
                symbols=position_symbols,
                engine=engine,
                is_factor=False,
            )
            stats["positions"] = len(position_results)
            logger.info("position_prices_updated", count=stats["positions"])

    except Exception as e:
        logger.error("position_fetch_error", error=str(e), exc_info=True)
        stats["errors"].append(f"position_fetch: {e}")

    # Step 3.5: Fetch FX rates (Phase 1.5)
    try:
        fx_stats = await run_fx_data_update(engine, redis_client)
        stats["fx_pairs"] = fx_stats.get("pairs_fetched", 0)
    except Exception as e:
        logger.error("fx_update_error", error=str(e), exc_info=True)
        stats["errors"].append(f"fx_update: {e}")

    try:
        # Step 4: Fetch FRED series
        logger.info("fetching_fred_series")
        fred_results = await fetch_fred_series(engine=engine)
        stats["fred_series"] = len(fred_results)
        logger.info("fred_series_updated", count=stats["fred_series"])

    except Exception as e:
        logger.error("fred_fetch_error", error=str(e), exc_info=True)
        stats["errors"].append(f"fred_fetch: {e}")

    # Step 5: Publish event to Redis
    if redis_client:
        try:
            event = {
                "event": "data_updated",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "stats": stats,
            }
            await redis_client.publish(
                "trading:events",
                json.dumps(event),
            )
            logger.info("data_updated_event_published")
        except Exception as e:
            logger.error("redis_publish_error", error=str(e), exc_info=True)
            stats["errors"].append(f"redis_publish: {e}")

    stats["completed_at"] = datetime.now(timezone.utc).isoformat()
    logger.info("daily_data_update_completed", stats=stats)
    return stats


# ---------------------------------------------------------------------------
# Phase 2: Event sync pipeline (EDGAR, schedules, RSS, scoring, alerts)
# ---------------------------------------------------------------------------


async def run_event_sync(
    engine: AsyncEngine,
    redis_client: Any = None,
) -> dict[str, Any]:
    """Run the full event sync pipeline.

    Steps:
    1. Sync EDGAR SEC filings for portfolio tickers
    2. Sync macro economic schedule events
    3. Sync curated RSS feeds
    4. Run portfolio-aware materiality scoring
    5. Optionally summarise high-priority events (if OPENAI_API_KEY set)
    6. Evaluate alert rules and generate notifications
    7. Cleanup expired snoozes

    All steps degrade gracefully — a failure in one connector does not
    prevent others from running.
    """
    logger.info("event_sync_started")
    stats: dict[str, Any] = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "edgar": None,
        "schedules": None,
        "rss": None,
        "scoring": None,
        "summarizer": None,
        "alerts": None,
        "snoozes_cleared": 0,
        "errors": [],
    }

    # Step 1: EDGAR SEC filings
    try:
        from .edgar import sync_edgar_events

        edgar_stats = await sync_edgar_events(engine=engine)
        stats["edgar"] = edgar_stats
        logger.info("event_sync_edgar_done", **edgar_stats)
    except Exception as e:
        logger.error("event_sync_edgar_error", error=str(e), exc_info=True)
        stats["errors"].append(f"edgar: {e}")

    # Step 2: Macro schedule
    try:
        from .schedules import sync_macro_schedule

        schedule_stats = await sync_macro_schedule(engine=engine)
        stats["schedules"] = schedule_stats
        logger.info("event_sync_schedules_done", **schedule_stats)
    except Exception as e:
        logger.error("event_sync_schedules_error", error=str(e), exc_info=True)
        stats["errors"].append(f"schedules: {e}")

    # Step 3: RSS feeds (curated)
    try:
        from .rss_feeds import sync_rss_feeds

        rss_stats = await sync_rss_feeds(engine=engine)
        stats["rss"] = rss_stats
        logger.info("event_sync_rss_done", **rss_stats)
    except Exception as e:
        logger.error("event_sync_rss_error", error=str(e), exc_info=True)
        stats["errors"].append(f"rss: {e}")

    # Step 3b: Per-ticker Google News RSS — skipped here because the
    # dedicated _run_ticker_news_loop (every 60s) handles this independently.
    # Running it again would waste Google API calls and is redundant.

    # Step 4: Portfolio-aware materiality scoring
    try:
        from .scoring import score_new_events

        scoring_stats = await score_new_events(engine=engine)
        stats["scoring"] = scoring_stats
        logger.info("event_sync_scoring_done", **scoring_stats)
    except Exception as e:
        logger.error("event_sync_scoring_error", error=str(e), exc_info=True)
        stats["errors"].append(f"scoring: {e}")

    # Step 5: Optional LLM summarisation
    try:
        from .summarizer import summarize_events

        summarizer_stats = await summarize_events(engine=engine)
        stats["summarizer"] = summarizer_stats
        logger.info("event_sync_summarizer_done", **summarizer_stats)
    except Exception as e:
        logger.error("event_sync_summarizer_error", error=str(e), exc_info=True)
        stats["errors"].append(f"summarizer: {e}")

    # Step 6: Alert rules
    try:
        from .alert_rules import run_alert_rules

        alert_stats = await run_alert_rules(engine=engine)
        stats["alerts"] = alert_stats
        logger.info("event_sync_alerts_done", **alert_stats)
    except Exception as e:
        logger.error("event_sync_alerts_error", error=str(e), exc_info=True)
        stats["errors"].append(f"alerts: {e}")

    # Step 7: Cleanup expired snoozes
    try:
        from .alert_rules import cleanup_expired_snoozes

        cleared = await cleanup_expired_snoozes(engine=engine)
        stats["snoozes_cleared"] = cleared
    except Exception as e:
        logger.error("event_sync_snooze_cleanup_error", error=str(e), exc_info=True)
        stats["errors"].append(f"snooze_cleanup: {e}")

    # Publish event to Redis
    if redis_client:
        try:
            event = {
                "event": "events_synced",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "stats": {
                    k: v for k, v in stats.items()
                    if k not in ("errors",)
                },
            }
            await redis_client.publish(
                "trading:events",
                json.dumps(event, default=str),
            )
        except Exception as e:
            stats["errors"].append(f"redis_publish: {e}")

    stats["completed_at"] = datetime.now(timezone.utc).isoformat()
    logger.info("event_sync_completed", error_count=len(stats["errors"]))
    return stats


# ---------------------------------------------------------------------------
# Risk recomputation triggers
# ---------------------------------------------------------------------------


async def check_and_trigger_risk_recompute(
    engine: AsyncEngine,
    redis_client: Any = None,
    force: bool = False,
) -> dict[str, Any]:
    """Check if risk needs recomputing and trigger if necessary.

    Compares current portfolio composition hash with the most recent
    risk_results portfolio_hash. If different (or force=True), publishes
    a risk recompute event to Redis.

    Args:
        engine: Database engine
        redis_client: Optional Redis client for event publishing
        force: If True, trigger recompute regardless of hash comparison

    Returns:
        Dictionary with recompute decision and details
    """
    logger.info("checking_risk_recompute_needed", force=force)

    result: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "recompute_needed": False,
        "reason": None,
        "current_hash": None,
        "last_hash": None,
    }

    try:
        # Get current portfolio composition
        positions = await _get_current_positions(engine)
        if not positions:
            logger.info("no_positions_found")
            result["reason"] = "no_positions"
            return result

        current_hash = compute_portfolio_hash(positions)
        result["current_hash"] = current_hash

        # Get last computed risk hash
        last_hash = await _get_latest_risk_portfolio_hash(engine)
        result["last_hash"] = last_hash

        # Determine if recompute needed
        if force:
            result["recompute_needed"] = True
            result["reason"] = "forced"
        elif last_hash is None:
            result["recompute_needed"] = True
            result["reason"] = "no_previous_results"
        elif current_hash != last_hash:
            result["recompute_needed"] = True
            result["reason"] = "portfolio_changed"

        # Publish event to Redis if recompute needed
        if result["recompute_needed"] and redis_client:
            try:
                event = {
                    "event": "risk_recompute",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "portfolio_hash": current_hash,
                    "reason": result["reason"],
                    "positions_count": len(positions),
                }
                await redis_client.publish(
                    "trading:events",
                    json.dumps(event),
                )
                logger.info("risk_recompute_event_published", reason=result["reason"])
            except Exception as e:
                logger.error("redis_publish_error", error=str(e), exc_info=True)

        logger.info(
            "risk_recompute_check_complete",
            recompute_needed=result["recompute_needed"],
            reason=result["reason"],
        )

    except Exception as e:
        logger.error("risk_recompute_check_error", error=str(e), exc_info=True)
        result["error"] = str(e)

    return result


# ---------------------------------------------------------------------------
# Scheduled job entry points
# ---------------------------------------------------------------------------


async def run_daily_jobs(
    engine: AsyncEngine,
    redis_client: Any = None,
) -> dict[str, Any]:
    """Run all daily scheduled jobs in sequence.

    This is the main entry point for daily scheduled tasks:
    1. Update all market data (factors, positions, FRED)
    2. Check and trigger risk recomputation if needed

    Args:
        engine: Database engine
        redis_client: Optional Redis client for event publishing

    Returns:
        Dictionary with results from both jobs
    """
    logger.info("daily_jobs_started")

    results = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "data_update": None,
        "risk_check": None,
        "event_sync": None,
    }

    # Run data update
    try:
        data_update_result = await run_daily_data_update(engine, redis_client)
        results["data_update"] = data_update_result
    except Exception as e:
        logger.error("daily_data_update_failed", error=str(e), exc_info=True)
        results["data_update"] = {"error": str(e)}

    # Check risk recomputation
    try:
        risk_check_result = await check_and_trigger_risk_recompute(
            engine,
            redis_client,
        )
        results["risk_check"] = risk_check_result
    except Exception as e:
        logger.error("risk_recompute_check_failed", error=str(e), exc_info=True)
        results["risk_check"] = {"error": str(e)}

    # Phase 2: Event sync (EDGAR, schedules, RSS, scoring, alerts)
    try:
        event_sync_result = await run_event_sync(engine, redis_client)
        results["event_sync"] = event_sync_result
    except Exception as e:
        logger.error("event_sync_failed", error=str(e), exc_info=True)
        results["event_sync"] = {"error": str(e)}

    results["completed_at"] = datetime.now(timezone.utc).isoformat()
    logger.info("daily_jobs_completed")
    return results


# ---------------------------------------------------------------------------
# Weekly adjustment sweep (Phase 1.5)
# ---------------------------------------------------------------------------


async def run_weekly_adjustment_sweep(
    engine: AsyncEngine,
    redis_client: Any = None,
) -> dict[str, Any]:
    """Re-fetch last 60 trading days for all symbols to capture retroactive
    adj_close corrections (splits, dividends, etc.).

    Should be run once per week (e.g., Saturday morning).

    Args:
        engine: Database engine
        redis_client: Optional Redis client for event publishing

    Returns:
        Dictionary with sweep statistics
    """
    logger.info("weekly_adjustment_sweep_started")
    stats: dict[str, Any] = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "factors": 0,
        "positions": 0,
        "fx_pairs": 0,
        "errors": [],
    }

    # ~60 trading days = ~84 calendar days
    sweep_start = date.today() - timedelta(days=90)

    # Step 1: Re-fetch factor prices
    try:
        from .yahoo import FACTOR_SYMBOLS, fetch_prices_yahoo
        factor_results = await fetch_prices_yahoo(
            symbols=FACTOR_SYMBOLS,
            start_date=sweep_start,
            engine=engine,
            is_factor=True,
        )
        stats["factors"] = len(factor_results)
    except Exception as e:
        logger.error("weekly_sweep_factor_error", error=str(e))
        stats["errors"].append(f"factors: {e}")

    # Step 2: Re-fetch position prices
    try:
        from .yahoo import fetch_prices_yahoo
        position_symbols = await _get_position_symbols(engine)
        if position_symbols:
            pos_results = await fetch_prices_yahoo(
                symbols=position_symbols,
                start_date=sweep_start,
                engine=engine,
                is_factor=False,
            )
            stats["positions"] = len(pos_results)
    except Exception as e:
        logger.error("weekly_sweep_position_error", error=str(e))
        stats["errors"].append(f"positions: {e}")

    # Step 3: Re-fetch FX rates
    try:
        from .fx import fetch_fx_rates, get_required_fx_currencies
        currencies = await get_required_fx_currencies(engine)
        if currencies:
            fx_results = await fetch_fx_rates(currencies, engine=engine)
            stats["fx_pairs"] = len(fx_results)
    except Exception as e:
        logger.error("weekly_sweep_fx_error", error=str(e))
        stats["errors"].append(f"fx: {e}")

    # Publish event
    if redis_client:
        try:
            event = {
                "event": "adjustment_sweep_complete",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "stats": stats,
            }
            await redis_client.publish(
                "trading:events",
                json.dumps(event),
            )
        except Exception as e:
            stats["errors"].append(f"redis: {e}")

    stats["completed_at"] = datetime.now(timezone.utc).isoformat()
    logger.info("weekly_adjustment_sweep_completed", stats=stats)
    return stats


# ---------------------------------------------------------------------------
# FX data update (Phase 1.5)
# ---------------------------------------------------------------------------


async def run_fx_data_update(
    engine: AsyncEngine,
    redis_client: Any = None,
) -> dict[str, Any]:
    """Fetch FX rates for any non-USD positions.

    Args:
        engine: Database engine
        redis_client: Optional Redis client

    Returns:
        Dictionary with FX update stats
    """
    logger.info("fx_data_update_started")
    stats: dict[str, Any] = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "pairs_fetched": 0,
        "errors": [],
    }

    try:
        from .fx import fetch_fx_rates, get_required_fx_currencies
        currencies = await get_required_fx_currencies(engine)
        if currencies:
            results = await fetch_fx_rates(currencies, engine=engine)
            stats["pairs_fetched"] = len(results)
            logger.info("fx_data_updated", pairs=len(results), currencies=currencies)
        else:
            logger.info("fx_no_currencies_needed")
    except Exception as e:
        logger.error("fx_data_update_error", error=str(e))
        stats["errors"].append(str(e))

    stats["completed_at"] = datetime.now(timezone.utc).isoformat()
    return stats
