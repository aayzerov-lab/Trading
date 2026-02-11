"""Risk computation orchestration service.

Provides high-level functions to compute and cache risk metrics for the
current portfolio. Integrates with shared.risk modules and the Phase 1
database schema.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
import structlog
from sqlalchemy import text

from shared.data.scheduler import compute_portfolio_hash
from shared.data.yahoo import get_prices_from_db, FACTOR_SYMBOLS
from shared.db.engine import get_shared_engine
from shared.risk.covariance import estimate_covariance
from shared.risk.correlation import (
    cluster_exposures,
    correlation_matrix,
    hierarchical_clusters,
    top_correlated_pairs,
)
from shared.risk.metrics import build_risk_contributors, build_risk_summary
from shared.risk.returns import (
    build_price_matrix,
    compute_log_returns,
    trim_to_window,
)
from shared.risk.stress import run_all_stress_tests

logger = structlog.get_logger()


async def get_cached_risk_result(
    result_type: str,
    asof_date: date,
    window: int,
    method: str,
    portfolio_hash: str,
) -> dict[str, Any] | None:
    """Check risk_results table for cached result."""
    try:
        engine = get_shared_engine()

        query = text(
            """
            SELECT result_json, created_at
            FROM risk_results
            WHERE result_type = :result_type
              AND asof_date = :asof_date
              AND "window" = :window
              AND method = :method
              AND portfolio_hash = :portfolio_hash
            ORDER BY created_at DESC
            LIMIT 1
            """
        )

        async with engine.connect() as conn:
            result = await conn.execute(
                query,
                {
                    "result_type": result_type,
                    "asof_date": asof_date,
                    "window": window,
                    "method": method,
                    "portfolio_hash": portfolio_hash,
                },
            )
            row = result.mappings().first()
            if row is None:
                return None

            logger.info(
                "risk_cache_hit",
                result_type=result_type,
                asof_date=asof_date,
                created_at=row["created_at"],
            )
            return json.loads(row["result_json"])

    except Exception:
        logger.exception("risk_cache_lookup_failed")
        return None


async def cache_risk_result(
    result_type: str,
    asof_date: date,
    window: int,
    method: str,
    portfolio_hash: str,
    result: dict[str, Any],
) -> None:
    """Store risk result in risk_results table."""
    try:
        engine = get_shared_engine()

        query = text(
            """
            INSERT INTO risk_results (
                result_type, asof_date, asof_ts, "window", method, portfolio_hash,
                result_json
            )
            VALUES (
                :result_type, :asof_date, :asof_ts, :window, :method, :portfolio_hash,
                :result_json
            )
            ON CONFLICT (asof_date, "window", method, portfolio_hash, result_type)
            DO UPDATE SET
                result_json = EXCLUDED.result_json,
                asof_ts = EXCLUDED.asof_ts
            """
        )

        async with engine.begin() as conn:
            await conn.execute(
                query,
                {
                    "result_type": result_type,
                    "asof_date": asof_date,
                    "asof_ts": datetime.now(timezone.utc),
                    "window": window,
                    "method": method,
                    "portfolio_hash": portfolio_hash,
                    "result_json": json.dumps(result, default=str),
                },
            )

        logger.info(
            "risk_result_cached",
            result_type=result_type,
            asof_date=asof_date,
            window=window,
            method=method,
        )

    except Exception:
        logger.exception("risk_cache_write_failed")


async def _get_positions_from_db() -> list[dict[str, Any]]:
    """Fetch current positions from positions_current table."""
    engine = get_shared_engine()

    query = text(
        """
        SELECT symbol, position as quantity, market_price, market_value,
               avg_cost as cost_basis, sector, country
        FROM positions_current
        WHERE symbol != 'CASH' AND position != 0
        ORDER BY symbol
        """
    )

    async with engine.connect() as conn:
        result = await conn.execute(query)
        rows = result.mappings().all()
        return [dict(row) for row in rows]


async def compute_risk_pack(
    window: int = 252,
    method: str = "lw",
    force: bool = False,
) -> dict[str, Any]:
    """Orchestrate full risk computation.

    Steps:
    1. Fetch current positions
    2. Compute portfolio weights and hash
    3. Check cache
    4. Fetch prices from DB for positions + factors
    5. Build aligned returns matrix
    6. Estimate covariance
    7. Compute all risk metrics
    8. Cache and return results
    """
    try:
        # 1. Fetch positions
        positions = await _get_positions_from_db()
        if not positions:
            logger.warning("no_positions_for_risk_computation")
            return _empty_result(window, method, "No positions found")

        # 2. Compute weights and hash
        symbols = [p["symbol"] for p in positions]
        market_values = np.array([float(p["market_value"] or 0) for p in positions])
        gross_exposure = np.sum(np.abs(market_values))

        if gross_exposure == 0:
            return _empty_result(window, method, "Zero gross exposure")

        # Weights preserve sign, normalized by gross exposure
        weights = market_values / gross_exposure
        portfolio_value = float(gross_exposure)

        portfolio_hash = compute_portfolio_hash(positions)
        asof = date.today()

        # 3. Check cache
        if not force:
            cached = await get_cached_risk_result(
                "risk_pack", asof, window, method, portfolio_hash
            )
            if cached is not None:
                return cached

        logger.info(
            "computing_risk_pack",
            num_positions=len(positions),
            window=window,
            method=method,
        )

        # 4. Fetch price data from DB
        # Need ~1.5x calendar days vs trading days (weekends + holidays)
        start_date = date.today() - timedelta(days=int(window * 1.6) + 50)
        position_prices = await get_prices_from_db(
            symbols=symbols,
            start_date=start_date,
            table="prices_daily",
        )
        factor_prices = await get_prices_from_db(
            symbols=FACTOR_SYMBOLS,
            start_date=start_date,
            table="factor_prices_daily",
        )

        if not position_prices:
            return _empty_result(window, method, "No price data available")

        # 5. Build returns matrix
        # Two-pass approach: first try the full window. If that excludes a
        # significant share of the portfolio (>10%), retry with a lower bar
        # so newer IPOs are included at the cost of a shorter estimation window.
        price_matrix = build_price_matrix(
            position_prices,
            price_col="adj_close",
            min_history=window + 1,
        )

        if not price_matrix.empty:
            # Check coverage: what % of portfolio is included?
            included = set(price_matrix.columns)
            included_weight = sum(
                abs(weights[i]) for i, s in enumerate(symbols) if s in included
            )
            if included_weight < 0.90:
                # Too much portfolio excluded — retry with lower min_history
                logger.warning(
                    "low_coverage_retrying_with_lower_min_history",
                    included_weight=float(included_weight),
                    included_symbols=len(included),
                    total_symbols=len(symbols),
                )
                MIN_HISTORY = 60
                price_matrix = build_price_matrix(
                    position_prices,
                    price_col="adj_close",
                    min_history=MIN_HISTORY + 1,
                )
        else:
            # First pass returned empty — try with lower bar
            MIN_HISTORY = 60
            price_matrix = build_price_matrix(
                position_prices,
                price_col="adj_close",
                min_history=MIN_HISTORY + 1,
            )

        if price_matrix.empty:
            return _empty_result(window, method, "Empty price matrix after alignment")

        returns = compute_log_returns(price_matrix)
        valid_symbols = list(returns.columns)

        # Determine effective window (use requested, or what's available)
        effective_window = min(window, len(returns))
        if effective_window < window:
            logger.info(
                "using_reduced_window",
                requested=window,
                effective=effective_window,
                available_returns=len(returns),
            )

        if effective_window >= window:
            trimmed_returns = trim_to_window(returns, window)
        else:
            trimmed_returns = returns

        # Align weights with valid symbols
        aligned_weights = _align_weights(symbols, weights, valid_symbols)
        if aligned_weights is None:
            return _empty_result(window, method, "No positions with price data")

        # 6. Estimate covariance
        cov_matrix = estimate_covariance(trimmed_returns, method=method)

        # 7. Compute all risk metrics
        summary = build_risk_summary(
            weights=aligned_weights,
            cov=cov_matrix,
            symbols=valid_symbols,
            portfolio_value=portfolio_value,
        )

        contributors = build_risk_contributors(
            weights=aligned_weights,
            cov=cov_matrix,
            symbols=valid_symbols,
            portfolio_value=portfolio_value,
        )

        # Correlation analysis
        corr = correlation_matrix(trimmed_returns)
        pairs = top_correlated_pairs(corr, n=20)

        # Cluster analysis
        cluster_result = hierarchical_clusters(corr, max_clusters=8)
        cluster_exp = cluster_exposures(
            cluster_labels=cluster_result["labels"],
            weights=aligned_weights,
            symbols=valid_symbols,
        )

        # Merge cluster info with exposure data
        clusters_with_exposure = _merge_clusters_and_exposures(
            cluster_result["clusters"], cluster_exp
        )

        # Stress tests
        sectors = {
            p["symbol"]: (p.get("sector") or "Unknown")
            for p in positions
            if p["symbol"] in valid_symbols
        }

        factor_returns_df = _build_factor_returns(factor_prices, window)

        stress_results = run_all_stress_tests(
            position_returns=trimmed_returns,
            factor_returns=factor_returns_df,
            weights=aligned_weights,
            symbols=valid_symbols,
            portfolio_value=portfolio_value,
            all_prices=position_prices,
            sectors=sectors,
        )

        # Identify excluded positions for user transparency
        excluded = [s for s in symbols if s not in valid_symbols]

        # 8. Build result pack
        result = {
            "summary": summary,
            "contributors": contributors,
            "correlation_pairs": pairs,
            "clusters": clusters_with_exposure,
            "stress": stress_results,
            "metadata": {
                "window": window,
                "effective_window": effective_window,
                "method": method,
                "asof_date": asof.isoformat(),
                "portfolio_hash": portfolio_hash,
                "num_positions": len(positions),
                "num_valid_symbols": len(valid_symbols),
                "portfolio_value": portfolio_value,
                "excluded_symbols": excluded,
            },
        }

        await cache_risk_result("risk_pack", asof, window, method, portfolio_hash, result)

        logger.info(
            "risk_pack_computed",
            window=window,
            method=method,
            num_positions=len(positions),
            num_valid=len(valid_symbols),
        )

        return result

    except Exception:
        logger.exception("risk_pack_computation_failed")
        raise


def _empty_result(window: int, method: str, error: str) -> dict[str, Any]:
    """Return an empty risk result structure."""
    return {
        "summary": {},
        "contributors": [],
        "correlation_pairs": [],
        "clusters": [],
        "stress": {"historical": {}, "factor": {}, "computed_at": datetime.now(timezone.utc).isoformat() + "Z"},
        "metadata": {
            "window": window,
            "method": method,
            "asof_date": date.today().isoformat(),
            "error": error,
        },
    }


def _align_weights(
    original_symbols: list[str],
    original_weights: np.ndarray,
    valid_symbols: list[str],
) -> np.ndarray | None:
    """Align portfolio weights to symbols that have valid price data."""
    aligned = []
    for sym in valid_symbols:
        if sym in original_symbols:
            idx = original_symbols.index(sym)
            aligned.append(original_weights[idx])
        else:
            aligned.append(0.0)

    aligned_arr = np.array(aligned)

    weight_sum = np.sum(np.abs(aligned_arr))
    if weight_sum == 0:
        return None

    return aligned_arr / weight_sum


def _build_factor_returns(
    factor_prices: dict[str, pd.DataFrame],
    window: int,
) -> pd.DataFrame:
    """Build factor returns DataFrame for stress testing."""
    if not factor_prices:
        return pd.DataFrame()

    factor_matrix = build_price_matrix(
        factor_prices,
        price_col="adj_close",
        min_history=min(window, 60),
    )

    if factor_matrix.empty:
        return pd.DataFrame()

    return compute_log_returns(factor_matrix)


def _merge_clusters_and_exposures(
    clusters: list[dict],
    exposures: list[dict],
) -> list[dict]:
    """Merge cluster definitions with their exposure data."""
    exposure_map = {e["cluster_id"]: e for e in exposures}
    merged = []

    for cluster in clusters:
        cid = cluster["cluster_id"]
        exp = exposure_map.get(cid, {})
        merged.append({
            "cluster_id": cid,
            "members": cluster["members"],
            "size": cluster["size"],
            "avg_intra_corr": cluster["avg_intra_corr"],
            "gross_exposure_pct": exp.get("gross_exposure_pct", 0.0),
            "net_exposure_pct": exp.get("net_exposure_pct", 0.0),
        })

    return merged
