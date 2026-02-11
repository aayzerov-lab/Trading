"""Data orchestration service.

Provides functions to ensure price data is fresh and to fetch portfolio
weights for risk computation.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import numpy as np
import structlog
from sqlalchemy import text

from shared.data.yahoo import fetch_prices_yahoo
from shared.db.engine import get_shared_engine

logger = structlog.get_logger()


async def ensure_data_fresh(force: bool = False) -> dict[str, Any]:
    """Ensure price data is up to date.

    Checks data_sync_status for each required symbol and triggers
    fetch if data is stale or missing.
    """
    try:
        engine = get_shared_engine()

        # Get list of symbols that need data (from positions_current)
        query_symbols = text(
            """
            SELECT DISTINCT symbol
            FROM positions_current
            WHERE symbol != 'CASH'
            ORDER BY symbol
            """
        )

        async with engine.connect() as conn:
            result = await conn.execute(query_symbols)
            rows = result.mappings().all()
            symbols = [row["symbol"] for row in rows]

        if not symbols:
            logger.info("no_symbols_to_update")
            return {
                "symbols_updated": 0,
                "symbols_checked": 0,
                "errors": [],
            }

        logger.info("checking_data_freshness", num_symbols=len(symbols))

        # Check sync status for each symbol
        today = date.today()
        stale_threshold = today - timedelta(days=3)

        symbols_to_update = []
        symbols_checked = 0

        query_status = text(
            """
            SELECT symbol, last_date, last_fetched_at
            FROM data_sync_status
            WHERE symbol = :symbol AND source = 'yahoo'
            """
        )

        async with engine.connect() as conn:
            for symbol in symbols:
                symbols_checked += 1
                result = await conn.execute(query_status, {"symbol": symbol})
                row = result.mappings().first()

                if force:
                    symbols_to_update.append(symbol)
                elif row is None:
                    symbols_to_update.append(symbol)
                else:
                    last_date = row["last_date"]
                    if last_date is None or last_date < stale_threshold:
                        symbols_to_update.append(symbol)

        # Fetch updates for stale symbols
        errors = []
        symbols_updated = 0

        if symbols_to_update:
            logger.info(
                "updating_price_data",
                num_symbols=len(symbols_to_update),
                force=force,
            )

            # Fetch in batches
            batch_size = 10
            for i in range(0, len(symbols_to_update), batch_size):
                batch = symbols_to_update[i : i + batch_size]
                try:
                    start = today - timedelta(days=500)
                    await fetch_prices_yahoo(
                        symbols=batch,
                        start_date=start,
                        engine=engine,
                    )
                    symbols_updated += len(batch)
                except Exception as e:
                    error_msg = f"Failed to update batch {batch}: {str(e)}"
                    errors.append(error_msg)
                    logger.exception("batch_update_failed", symbols=batch)

        return {
            "symbols_updated": symbols_updated,
            "symbols_checked": symbols_checked,
            "errors": errors,
        }

    except Exception as e:
        logger.exception("ensure_data_fresh_failed")
        return {
            "symbols_updated": 0,
            "symbols_checked": 0,
            "errors": [f"Unexpected error: {str(e)}"],
        }


async def get_position_weights() -> tuple[np.ndarray, list[str], float]:
    """Get current position weights for risk computation."""
    engine = get_shared_engine()

    query = text(
        """
        SELECT symbol, position as quantity, market_value
        FROM positions_current
        WHERE symbol != 'CASH' AND position != 0
        ORDER BY symbol
        """
    )

    async with engine.connect() as conn:
        result = await conn.execute(query)
        rows = result.mappings().all()
        positions = [dict(row) for row in rows]

    if not positions:
        raise ValueError("No positions found in portfolio")

    symbols = [p["symbol"] for p in positions]
    market_values = np.array([float(p["market_value"]) for p in positions])

    gross_exposure = np.sum(np.abs(market_values))
    if gross_exposure == 0:
        raise ValueError("Portfolio has zero gross exposure")

    weights = market_values / gross_exposure

    return weights, symbols, float(gross_exposure)
