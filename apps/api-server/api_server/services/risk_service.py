"""Risk computation orchestration service.

Provides high-level functions to compute and cache risk metrics for the
current portfolio. Integrates with shared.risk modules and the Phase 1
database schema.  Phase 1.5 adds FX-aware returns, data quality pack,
regression diagnostics, and enhanced metadata.
"""

from __future__ import annotations

import hashlib
import json
import platform
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
import structlog
from sqlalchemy import text

from shared.data.fx import get_fx_rates_from_db, get_security_fx_info
from shared.data.scheduler import compute_portfolio_hash
from shared.data.yahoo import fetch_prices_yahoo, get_prices_from_db, FACTOR_SYMBOLS
from shared.db.engine import get_shared_engine
from shared.risk.covariance import estimate_covariance, pairwise_cov
from shared.risk.correlation import (
    cluster_exposures,
    correlation_matrix,
    hierarchical_clusters,
    top_correlated_pairs,
)
from shared.risk.data_quality import build_data_quality_pack, compute_beta_quality_summary
from shared.risk.metrics import build_risk_contributors, build_risk_summary
from shared.risk.returns import (
    build_fx_aware_returns,
    build_per_symbol_returns,
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

        # Auto-fetch prices for any new symbols missing from the DB
        missing = [s for s in symbols if s not in position_prices]
        if missing:
            logger.info("fetching_missing_prices", symbols=missing)
            try:
                engine = get_shared_engine()
                fetched = await fetch_prices_yahoo(
                    symbols=missing, engine=engine, is_factor=False,
                )
                if fetched:
                    # Re-read from DB so format matches
                    extra = await get_prices_from_db(
                        symbols=list(fetched.keys()),
                        start_date=start_date,
                        table="prices_daily",
                    )
                    position_prices.update(extra)
            except Exception:
                logger.warning("auto_fetch_prices_failed", symbols=missing, exc_info=True)

        factor_prices = await get_prices_from_db(
            symbols=FACTOR_SYMBOLS,
            start_date=start_date,
            table="factor_prices_daily",
        )

        if not position_prices:
            return _empty_result(window, method, "No price data available")

        # 4b. Fetch FX data and security info (Phase 1.5)
        engine = get_shared_engine()
        security_info = await get_security_fx_info(symbols, engine=engine)

        # Collect FX pairs needed from security_info
        fx_pairs_needed = list({
            info["fx_pair"]
            for info in security_info.values()
            if info.get("fx_pair")
        })
        fx_rates: dict[str, pd.DataFrame] = {}
        if fx_pairs_needed:
            fx_rates = await get_fx_rates_from_db(
                pairs=fx_pairs_needed, start_date=start_date, engine=engine
            )

        # 5. Build per-symbol return series with FX adjustment (Phase 1.5).
        #    Each symbol keeps its own date index, trimmed to `window`.
        MIN_HISTORY = 60
        returns_dict, fx_flags = build_fx_aware_returns(
            prices=position_prices,
            fx_rates=fx_rates,
            security_info=security_info,
            price_col="adj_close",
            window=window,
            min_history=MIN_HISTORY,
        )

        if not returns_dict:
            return _empty_result(window, method, "No symbols with sufficient history")

        valid_symbols = [s for s in symbols if s in returns_dict]
        if not valid_symbols:
            return _empty_result(window, method, "No positions with price data")

        # Align weights with valid symbols
        aligned_weights = _align_weights(symbols, weights, valid_symbols)
        if aligned_weights is None:
            return _empty_result(window, method, "No positions with price data")

        # 6. Estimate covariance via pairwise overlapping returns.
        #    Each pair uses their common dates (up to `window`), so AAPL
        #    with 252 days and CRCL with 170 days both contribute fully.
        use_fallback = False
        try:
            cov_matrix = pairwise_cov(
                returns_dict,
                valid_symbols,
                window=window,
                min_overlap=max(30, MIN_HISTORY // 2),
            )
            # Quality check: pairwise assembly + PSD eigenvalue clamping can
            # destroy correlation structure. Always verify the result.
            if len(valid_symbols) > 3:
                diag_check = np.sqrt(np.diag(cov_matrix))
                diag_check[diag_check == 0] = 1.0
                corr_check = cov_matrix / np.outer(diag_check, diag_check)
                np.fill_diagonal(corr_check, 0)
                avg_abs_corr = float(np.abs(corr_check).mean())
                if avg_abs_corr < 0.02:
                    n_assets = len(valid_symbols)
                    min_obs = min(len(returns_dict[s]) for s in valid_symbols)
                    logger.warning(
                        "pairwise_cov: degenerate correlations, falling back to LW",
                        avg_abs_corr=avg_abs_corr,
                        n_assets=n_assets,
                        min_obs=min_obs,
                    )
                    use_fallback = True
        except ValueError as e:
            logger.warning("pairwise_cov_failed_fallback", error=str(e))
            use_fallback = True

        if use_fallback:
            # Fallback: build aligned matrix for symbols that DO overlap
            price_matrix = build_price_matrix(
                position_prices,
                price_col="adj_close",
                min_history=MIN_HISTORY + 1,
            )
            if price_matrix.empty:
                return _empty_result(window, method, "Empty price matrix after alignment")
            returns_aligned = compute_log_returns(price_matrix)
            eff = min(window, len(returns_aligned))
            trimmed_returns = returns_aligned.iloc[-eff:]
            valid_symbols = list(trimmed_returns.columns)
            aligned_weights = _align_weights(symbols, weights, valid_symbols)
            if aligned_weights is None:
                return _empty_result(window, method, "No positions with price data")
            cov_matrix = estimate_covariance(trimmed_returns, method=method)

        # Standalone annualized vol per symbol from its own full history
        standalone_vols: dict[str, float] = {}
        for sym in valid_symbols:
            s = returns_dict[sym]
            daily_vol = float(s.std())
            standalone_vols[sym] = daily_vol * np.sqrt(252) * 100  # ann %

        effective_window = min(
            window,
            min(len(returns_dict[s]) for s in valid_symbols),
        )

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
            standalone_vols=standalone_vols,
        )

        # Correlation from pairwise cov: corr_ij = cov_ij / (sig_i * sig_j)
        diag = np.sqrt(np.diag(cov_matrix))
        diag[diag == 0] = 1.0  # avoid division by zero
        corr_values = cov_matrix / np.outer(diag, diag)
        np.fill_diagonal(corr_values, 1.0)
        corr = pd.DataFrame(corr_values, index=valid_symbols, columns=valid_symbols)
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

        # Build an aligned returns DataFrame for stress tests (inner-join
        # is fine here — stress tests don't need per-symbol precision).
        aligned_returns_df = pd.DataFrame(
            {sym: returns_dict[sym] for sym in valid_symbols}
        ).dropna()
        if len(aligned_returns_df) > window:
            aligned_returns_df = aligned_returns_df.iloc[-window:]

        # Stress tests
        sectors = {
            p["symbol"]: (p.get("sector") or "Unknown")
            for p in positions
            if p["symbol"] in valid_symbols
        }

        factor_returns_df = _build_factor_returns(factor_prices, window)

        stress_results = run_all_stress_tests(
            position_returns=aligned_returns_df,
            factor_returns=factor_returns_df,
            weights=aligned_weights,
            symbols=valid_symbols,
            portfolio_value=portfolio_value,
            all_prices=position_prices,
            sectors=sectors,
        )

        # Identify excluded positions for user transparency
        excluded = [s for s in symbols if s not in valid_symbols]

        # 7b. Build data quality pack (Phase 1.5)
        # Compute valid_symbols for both windows for coverage metrics
        valid_symbols_60 = [s for s in symbols if s in returns_dict and len(returns_dict[s]) >= 60]
        valid_symbols_252 = [s for s in symbols if s in returns_dict and len(returns_dict[s]) >= 252]

        # Fetch timestamps for data quality panel
        timestamps = await _get_data_timestamps()

        data_quality = build_data_quality_pack(
            positions=positions,
            prices=position_prices,
            returns_dict=returns_dict,
            symbols=symbols,
            valid_symbols_60=valid_symbols_60,
            valid_symbols_252=valid_symbols_252,
            security_info=security_info,
            fx_flags=fx_flags,
            stress_results=stress_results,
            timestamps=timestamps,
        )

        # 7c. Negative portfolio variance guard (Phase 1.5)
        port_var = float(aligned_weights @ cov_matrix @ aligned_weights)
        if port_var < 0:
            logger.error(
                "risk_pack: NEGATIVE portfolio variance detected!",
                port_var=port_var,
                method=method,
                window=window,
            )

        # 8. Build universe hash for cache identity
        universe_hash = hashlib.sha256(
            ",".join(sorted(valid_symbols)).encode()
        ).hexdigest()[:12]

        # 8b. Enhanced metadata (Phase 1.5)
        fx_adjusted_count = sum(
            1 for s in valid_symbols
            if security_info.get(s, {}).get("fx_pair") is not None
            and not security_info.get(s, {}).get("is_usd_listed", True)
        )

        metadata = {
            "window": window,
            "effective_window": effective_window,
            "method": method,
            "asof_date": asof.isoformat(),
            "computed_at": datetime.now(timezone.utc).isoformat() + "Z",
            "portfolio_hash": portfolio_hash,
            "universe_hash": universe_hash,
            "num_positions": len(positions),
            "num_valid_symbols": len(valid_symbols),
            "num_excluded": len(excluded),
            "portfolio_value": portfolio_value,
            "excluded_symbols": excluded,
            "fx_adjusted_count": fx_adjusted_count,
            "fx_flags": fx_flags,
            "lib_versions": {
                "numpy": np.__version__,
                "pandas": pd.__version__,
                "python": platform.python_version(),
            },
        }

        result = {
            "summary": summary,
            "contributors": contributors,
            "correlation_pairs": pairs,
            "clusters": clusters_with_exposure,
            "stress": stress_results,
            "data_quality": data_quality,
            "metadata": metadata,
        }

        await cache_risk_result("risk_pack", asof, window, method, portfolio_hash, result)

        logger.info(
            "risk_pack_computed",
            window=window,
            method=method,
            num_positions=len(positions),
            num_valid=len(valid_symbols),
            fx_adjusted=fx_adjusted_count,
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
        "data_quality": {"coverage": {}, "integrity": {}, "warnings": [], "computed_at": datetime.now(timezone.utc).isoformat() + "Z"},
        "metadata": {
            "window": window,
            "method": method,
            "asof_date": date.today().isoformat(),
            "error": error,
        },
    }


async def _get_data_timestamps() -> dict[str, Any]:
    """Fetch latest data sync timestamps for the data quality panel."""
    timestamps: dict[str, Any] = {
        "last_positions_update": None,
        "last_prices_update": None,
        "last_fx_update": None,
        "last_risk_compute": None,
    }
    try:
        engine = get_shared_engine()
        async with engine.connect() as conn:
            # Latest position sync
            r = await conn.execute(text(
                "SELECT MAX(updated_at) FROM positions_current"
            ))
            row = r.first()
            if row and row[0]:
                timestamps["last_positions_update"] = row[0].isoformat()

            # Latest price sync
            r = await conn.execute(text(
                "SELECT MAX(last_sync) FROM data_sync_status WHERE source = 'yahoo'"
            ))
            row = r.first()
            if row and row[0]:
                timestamps["last_prices_update"] = row[0].isoformat()

            # Latest FX sync
            r = await conn.execute(text(
                "SELECT MAX(updated_at) FROM fx_daily"
            ))
            row = r.first()
            if row and row[0]:
                timestamps["last_fx_update"] = row[0].isoformat()

            # Latest risk compute
            r = await conn.execute(text(
                "SELECT MAX(created_at) FROM risk_results"
            ))
            row = r.first()
            if row and row[0]:
                timestamps["last_risk_compute"] = row[0].isoformat()
    except Exception:
        logger.debug("_get_data_timestamps: query failed (some tables may not exist yet)")

    return timestamps


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
