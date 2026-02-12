"""
Stress Testing Module

Historical replay and factor-based stress testing for portfolio risk analysis.
Implements both actual historical scenarios and parametric factor shocks.
"""

import numpy as np
import pandas as pd
import structlog
from typing import Dict, List, Optional
from datetime import datetime

logger = structlog.get_logger(__name__)


# Historical scenario windows (inclusive date ranges)
HISTORICAL_SCENARIOS = {
    'gfc_2008': {
        'name': 'GFC (Oct 2007 - Mar 2009)',
        'start': '2007-10-09',
        'end': '2009-03-09',
    },
    'covid_crash_2020': {
        'name': 'COVID Crash (Feb-Mar 2020)',
        'start': '2020-02-19',
        'end': '2020-03-23',
    },
    'rates_shock_2022': {
        'name': '2022 Rates Shock (Jan-Jun 2022)',
        'start': '2022-01-03',
        'end': '2022-06-16',
    },
    'q4_2018_selloff': {
        'name': 'Q4 2018 Selloff (Oct-Dec 2018)',
        'start': '2018-10-03',
        'end': '2018-12-24',
    },
}

# Factor shock scenarios: factor_symbol -> shocked return
FACTOR_SHOCKS = {
    'equity_crash': {
        'name': 'Equity Crash',
        'shocks': {
            'SPY': -0.10,
        },
    },
    'rates_up': {
        'name': 'Rates Spike',
        'shocks': {
            'TLT': -0.05,
            'HYG': -0.05,
        },
    },
    'usd_rally': {
        'name': 'USD Rally',
        'shocks': {
            'UUP': 0.03,
        },
    },
    'commodity_spike': {
        'name': 'Commodity Spike',
        'shocks': {
            'USO': 0.15,
            'DBC': 0.15,
        },
    },
    'crypto_crash': {
        'name': 'Crypto Crash',
        'shocks': {
            'BTC-USD': -0.15,
        },
    },
    'combined_stress': {
        'name': 'Combined Stress',
        'shocks': {
            'SPY': -0.10,
            'TLT': 0.05,
            'HYG': -0.05,
            'UUP': 0.03,
            'USO': -0.15,
            'DBC': -0.15,
            'BTC-USD': -0.15,
        },
    },
}

# ---------------------------------------------------------------------------
# Regression quality thresholds (Phase 1.5)
# ---------------------------------------------------------------------------

BETA_WINDOW = 60             # Rolling window for beta estimation (trading days)
MIN_OVERLAP_BETA = 40        # Minimum overlap for valid beta
WARN_OVERLAP_BETA = 50       # Overlap threshold for "Good" quality
MIN_R2_GOOD = 0.20          # R\u00b2 threshold for "Good" quality

def _orthogonalize_factors(
    factor_df: pd.DataFrame,
    factors: List[str],
) -> Dict[str, np.ndarray]:
    """Orthogonalize each factor against all others.

    Returns residual series for each factor to reduce multicollinearity.
    Falls back to raw factor returns if regression fails.
    """
    residuals: Dict[str, np.ndarray] = {}
    if len(factors) <= 1:
        for f in factors:
            residuals[f] = factor_df[f].values
        return residuals

    for f in factors:
        y = factor_df[f].values
        others = [o for o in factors if o != f]
        X = factor_df[others].values
        if X.size == 0:
            residuals[f] = y
            continue

        # Add intercept to avoid bias from non-zero means.
        X_design = np.column_stack([np.ones(len(y)), X])
        try:
            beta, _, _, _ = np.linalg.lstsq(X_design, y, rcond=None)
            y_hat = X_design @ beta
            residuals[f] = y - y_hat
        except Exception:
            residuals[f] = y

    return residuals


def historical_stress_test(
    position_returns: pd.DataFrame,
    weights: np.ndarray,
    symbols: List[str],
    portfolio_value: float,
    scenario_key: str,
    all_prices: Dict[str, pd.DataFrame],
    sectors: Optional[Dict[str, str]] = None,
) -> Optional[Dict]:
    """Run historical replay stress test.

    Replays actual returns during a historical crisis period.

    Args:
        position_returns: DataFrame of position returns (for reference, not used directly)
        weights: Position weights (as decimals or dollar amounts)
        symbols: List of position symbols
        portfolio_value: Total portfolio value in USD
        scenario_key: Key for scenario in HISTORICAL_SCENARIOS
        all_prices: Dict of symbol -> price DataFrame
        sectors: Optional dict mapping symbol -> sector name

    Returns:
        Dict with scenario results, or None if insufficient data
    """
    if scenario_key not in HISTORICAL_SCENARIOS:
        raise ValueError(f"Unknown scenario: {scenario_key}")

    scenario = HISTORICAL_SCENARIOS[scenario_key]
    scenario_name = scenario['name']
    start_date = pd.to_datetime(scenario['start'])
    end_date = pd.to_datetime(scenario['end'])

    weights = np.asarray(weights).flatten()

    if len(weights) != len(symbols):
        raise ValueError(
            f"Weights length {len(weights)} doesn't match symbols length {len(symbols)}"
        )

    logger.info(
        "historical_stress_test: starting",
        scenario=scenario_name,
        start=scenario['start'],
        end=scenario['end']
    )

    # Compute returns for each position during the scenario period
    position_cumulative_returns = {}
    positions_with_data = []

    for i, symbol in enumerate(symbols):
        if symbol not in all_prices:
            logger.warning(
                "historical_stress_test: symbol not in price data",
                symbol=symbol,
                scenario=scenario_name
            )
            continue

        price_df = all_prices[symbol]
        if price_df is None or price_df.empty:
            logger.warning(
                "historical_stress_test: empty price data",
                symbol=symbol,
                scenario=scenario_name
            )
            continue

        # Ensure date column is datetime
        if 'date' not in price_df.columns:
            logger.warning(
                "historical_stress_test: missing date column",
                symbol=symbol
            )
            continue

        price_df = price_df.copy()
        if not pd.api.types.is_datetime64_any_dtype(price_df['date']):
            price_df['date'] = pd.to_datetime(price_df['date'])

        # Filter to scenario period
        scenario_prices = price_df[
            (price_df['date'] >= start_date) &
            (price_df['date'] <= end_date)
        ].copy()

        if len(scenario_prices) < 2:
            logger.warning(
                "historical_stress_test: insufficient data in scenario period",
                symbol=symbol,
                scenario=scenario_name,
                data_points=len(scenario_prices)
            )
            continue

        # Get first and last price
        scenario_prices = scenario_prices.sort_values('date')
        first_price = scenario_prices.iloc[0]['adj_close']
        last_price = scenario_prices.iloc[-1]['adj_close']

        if pd.isna(first_price) or pd.isna(last_price) or first_price <= 0:
            logger.warning(
                "historical_stress_test: invalid prices",
                symbol=symbol,
                first_price=first_price,
                last_price=last_price
            )
            continue

        # Cumulative return
        cumulative_return = (last_price - first_price) / first_price
        position_cumulative_returns[symbol] = cumulative_return
        positions_with_data.append(i)

    if not position_cumulative_returns:
        logger.warning(
            "historical_stress_test: no positions with data",
            scenario=scenario_name
        )
        return None

    # Compute portfolio return (weighted sum of position returns)
    portfolio_return = 0.0
    total_weight = 0.0

    for symbol, cum_return in position_cumulative_returns.items():
        idx = symbols.index(symbol)
        weight = weights[idx]
        portfolio_return += weight * cum_return
        total_weight += weight

    # Renormalize if we only have partial position coverage
    total_abs_weight = float(np.sum(np.abs(weights)))
    if total_weight > 0 and abs(abs(total_weight) - total_abs_weight) > 1e-6:
        coverage_ratio = abs(total_weight) / total_abs_weight if total_abs_weight > 0 else 1.0
        logger.info(
            "historical_stress_test: partial position coverage, renormalizing",
            scenario=scenario_name,
            covered_weight=float(total_weight),
            total_weight=total_abs_weight,
            coverage_ratio=coverage_ratio,
        )
        if coverage_ratio > 0:
            portfolio_return = portfolio_return / coverage_ratio

    portfolio_pnl = portfolio_return * portfolio_value

    # Build top contributors (by PnL contribution)
    contributors = []
    for symbol, cum_return in position_cumulative_returns.items():
        idx = symbols.index(symbol)
        weight = weights[idx]
        pnl_contribution = weight * cum_return * portfolio_value
        weight_pct = float(weight * 100)

        contributors.append({
            'symbol': symbol,
            'return_pct': float(cum_return * 100),
            'pnl_contribution': float(pnl_contribution),
            'weight_pct': float(weight_pct)
        })

    # Sort by absolute PnL contribution
    contributors.sort(key=lambda x: abs(x['pnl_contribution']), reverse=True)
    top_contributors = contributors[:10]

    # Compute by-sector if sectors provided
    by_sector = []
    if sectors:
        sector_pnl = {}
        for symbol, cum_return in position_cumulative_returns.items():
            sector = sectors.get(symbol, 'Unknown')
            idx = symbols.index(symbol)
            weight = weights[idx]
            pnl = weight * cum_return * portfolio_value

            if sector not in sector_pnl:
                sector_pnl[sector] = 0.0
            sector_pnl[sector] += pnl

        for sector, pnl in sector_pnl.items():
            pct = (pnl / portfolio_pnl * 100) if portfolio_pnl != 0 else 0.0
            by_sector.append({
                'sector': sector,
                'pnl': float(pnl),
                'pct': float(pct)
            })

        by_sector.sort(key=lambda x: abs(x['pnl']), reverse=True)

    result = {
        'scenario': scenario_name,
        'period': f"{scenario['start']} to {scenario['end']}",
        'portfolio_return_pct': float(portfolio_return * 100),
        'portfolio_pnl': float(portfolio_pnl),
        'top_contributors': top_contributors,
        'by_sector': by_sector,
    }

    logger.info(
        "historical_stress_test: complete",
        scenario=scenario_name,
        portfolio_return_pct=result['portfolio_return_pct'],
        portfolio_pnl=result['portfolio_pnl'],
        positions_tested=len(position_cumulative_returns)
    )

    return result


def compute_regression_diagnostics(
    position_ret: np.ndarray,
    factor_ret: np.ndarray,
    overlap: int,
) -> Dict:
    """Compute regression diagnostics for a position-factor pair.

    Returns:
        Dict with beta, r2, stderr_beta, t_stat, overlap, quality label
    """
    if overlap < 2:
        return {
            "beta": 0.0,
            "r2": 0.0,
            "stderr_beta": float("inf"),
            "t_stat": 0.0,
            "overlap": overlap,
            "quality": "invalid",
        }

    factor_var = np.var(factor_ret, ddof=1)
    if factor_var == 0 or np.isnan(factor_var):
        return {
            "beta": 0.0,
            "r2": 0.0,
            "stderr_beta": float("inf"),
            "t_stat": 0.0,
            "overlap": overlap,
            "quality": "invalid",
        }

    covariance = np.cov(position_ret, factor_ret, ddof=1)[0, 1]
    beta = covariance / factor_var

    # R\u00b2 = correlation\u00b2 = (cov / (std_x * std_y))\u00b2
    pos_std = np.std(position_ret, ddof=1)
    fac_std = np.sqrt(factor_var)
    if pos_std == 0 or fac_std == 0:
        r2 = 0.0
    else:
        corr = covariance / (pos_std * fac_std)
        r2 = float(corr ** 2)

    # Standard error of beta: SE = sqrt(SSR / ((n-2) * SSX))
    residuals = position_ret - beta * factor_ret
    ssr = float(np.sum(residuals ** 2))
    ssx = float(np.sum((factor_ret - np.mean(factor_ret)) ** 2))

    if overlap > 2 and ssx > 0:
        stderr_beta = float(np.sqrt(ssr / ((overlap - 2) * ssx)))
    else:
        stderr_beta = float("inf")

    # t-statistic
    t_stat = float(beta / stderr_beta) if stderr_beta > 0 and stderr_beta != float("inf") else 0.0

    # Quality label
    if overlap < MIN_OVERLAP_BETA:
        quality = "invalid"
    elif overlap >= WARN_OVERLAP_BETA and r2 >= MIN_R2_GOOD:
        quality = "good"
    else:
        quality = "weak"

    return {
        "beta": float(beta),
        "r2": float(r2),
        "stderr_beta": float(stderr_beta),
        "t_stat": float(t_stat),
        "overlap": int(overlap),
        "quality": quality,
    }


def factor_stress_test(
    position_returns: pd.DataFrame,
    factor_returns: pd.DataFrame,
    weights: np.ndarray,
    symbols: List[str],
    portfolio_value: float,
    scenario_key: str,
    sectors: Optional[Dict[str, str]] = None,
) -> Optional[Dict]:
    """Run factor-based parameter shock stress test.

    Estimates position betas to factors, then applies parametric shocks.

    Args:
        position_returns: DataFrame of position returns (T x N_positions)
        factor_returns: DataFrame of factor returns (T x N_factors)
        weights: Position weights (as decimals or dollar amounts)
        symbols: List of position symbols
        portfolio_value: Total portfolio value in USD
        scenario_key: Key for scenario in FACTOR_SHOCKS
        sectors: Optional dict mapping symbol -> sector name

    Returns:
        Dict with scenario results, or None if insufficient data
    """
    if scenario_key not in FACTOR_SHOCKS:
        raise ValueError(f"Unknown factor scenario: {scenario_key}")

    scenario = FACTOR_SHOCKS[scenario_key]
    scenario_name = scenario['name']
    shocks = scenario['shocks']

    weights = np.asarray(weights).flatten()

    if len(weights) != len(symbols):
        raise ValueError(
            f"Weights length {len(weights)} doesn't match symbols length {len(symbols)}"
        )

    if position_returns.empty or factor_returns.empty:
        logger.warning(
            "factor_stress_test: empty returns data",
            scenario=scenario_name
        )
        return None

    logger.info(
        "factor_stress_test: starting",
        scenario=scenario_name,
        shocks=shocks
    )

    # Align position and factor returns
    common_dates = position_returns.index.intersection(factor_returns.index)

    if len(common_dates) < 10:
        logger.warning(
            "factor_stress_test: insufficient overlapping data",
            scenario=scenario_name,
            common_dates=len(common_dates)
        )
        return None

    # Use rolling window: take last BETA_WINDOW days for reactive beta estimation
    position_returns_aligned = position_returns.loc[common_dates].iloc[-BETA_WINDOW:]
    factor_returns_aligned = factor_returns.loc[common_dates].iloc[-BETA_WINDOW:]

    shocked_factors = [f for f in shocks.keys() if f in factor_returns_aligned.columns]
    if not shocked_factors:
        logger.warning(
            "factor_stress_test: no shocked factors in returns data",
            scenario=scenario_name
        )
        return None

    factor_window = factor_returns_aligned[shocked_factors].dropna()
    if factor_window.empty:
        logger.warning(
            "factor_stress_test: no usable factor data after dropna",
            scenario=scenario_name
        )
        return None

    position_returns_window = position_returns_aligned.loc[factor_window.index]
    orthogonal_factors = _orthogonalize_factors(factor_window, shocked_factors)
    orthogonalized = len(shocked_factors) > 1

    # Compute betas for each position against each shocked factor (Phase 1.5: with diagnostics)
    position_impacts = {}
    regression_diagnostics = {}  # Phase 1.5: store diagnostics per position

    for symbol in symbols:
        if symbol not in position_returns_window.columns:
            logger.warning(
                "factor_stress_test: symbol not in position returns",
                symbol=symbol,
                scenario=scenario_name
            )
            continue

        position_ret = position_returns_window[symbol].values
        total_impact = 0.0
        symbol_diags = {}

        for factor_symbol, shock in shocks.items():
            if factor_symbol not in orthogonal_factors:
                continue

            factor_ret = orthogonal_factors[factor_symbol]
            if len(position_ret) != len(factor_ret):
                min_len = min(len(position_ret), len(factor_ret))
                position_ret_use = position_ret[-min_len:]
                factor_ret_use = factor_ret[-min_len:]
                overlap = min_len
            else:
                position_ret_use = position_ret
                factor_ret_use = factor_ret
                overlap = len(factor_ret)

            diag = compute_regression_diagnostics(position_ret_use, factor_ret_use, overlap)
            diag["orthogonalized"] = orthogonalized
            if orthogonalized:
                diag["orthogonalized_against"] = [
                    other for other in shocked_factors if other != factor_symbol
                ]
            symbol_diags[factor_symbol] = diag

            # Exclude only invalid betas (allow weak)
            if diag["quality"] == "invalid":
                logger.warning(
                    "factor_stress_test: invalid beta excluded",
                    symbol=symbol,
                    factor=factor_symbol,
                    overlap=diag["overlap"],
                    r2=diag["r2"],
                )
                continue

            # Cap beta to avoid extreme leverage from noisy factors
            beta_raw = diag["beta"]
            beta_used = float(np.clip(beta_raw, -5.0, 5.0))
            if beta_used != beta_raw:
                diag["beta_capped"] = True
            diag["beta_used"] = beta_used

            impact = beta_used * shock
            total_impact += impact

        position_impacts[symbol] = total_impact
        regression_diagnostics[symbol] = symbol_diags


    if not position_impacts:
        logger.warning(
            "factor_stress_test: no position impacts computed",
            scenario=scenario_name
        )
        return None

    # Compute portfolio impact (weighted sum)
    portfolio_return = 0.0

    for symbol, impact in position_impacts.items():
        idx = symbols.index(symbol)
        weight = weights[idx]
        portfolio_return += weight * impact

    portfolio_pnl = portfolio_return * portfolio_value

    # Build top contributors
    contributors = []
    for symbol, impact in position_impacts.items():
        idx = symbols.index(symbol)
        weight = weights[idx]
        pnl_contribution = weight * impact * portfolio_value
        weight_pct = float(weight * 100)

        contributors.append({
            'symbol': symbol,
            'return_pct': float(impact * 100),
            'pnl_contribution': float(pnl_contribution),
            'weight_pct': float(weight_pct)
        })

    contributors.sort(key=lambda x: abs(x['pnl_contribution']), reverse=True)
    top_contributors = contributors[:10]

    # Compute by-sector if sectors provided
    by_sector = []
    if sectors:
        sector_pnl = {}
        for symbol, impact in position_impacts.items():
            sector = sectors.get(symbol, 'Unknown')
            idx = symbols.index(symbol)
            weight = weights[idx]
            pnl = weight * impact * portfolio_value

            if sector not in sector_pnl:
                sector_pnl[sector] = 0.0
            sector_pnl[sector] += pnl

        for sector, pnl in sector_pnl.items():
            pct = (pnl / portfolio_pnl * 100) if portfolio_pnl != 0 else 0.0
            by_sector.append({
                'sector': sector,
                'pnl': float(pnl),
                'pct': float(pct)
            })

        by_sector.sort(key=lambda x: abs(x['pnl']), reverse=True)

    # Compute portfolio-level effective factor exposures
    factor_exposures = {}
    for factor_symbol in shocks:
        portfolio_beta = 0.0
        for symbol in position_impacts:
            idx = symbols.index(symbol)
            weight = weights[idx]
            diag = regression_diagnostics.get(symbol, {}).get(factor_symbol, {})
            if diag.get("quality") != "invalid":
                portfolio_beta += weight * diag.get("beta_used", diag.get("beta", 0.0))
        factor_exposures[f"portfolio_beta_{factor_symbol}"] = float(portfolio_beta)

    result = {
        'scenario': scenario_name,
        'period': 'Parameter Shock',
        'portfolio_return_pct': float(portfolio_return * 100),
        'portfolio_pnl': float(portfolio_pnl),
        'top_contributors': top_contributors,
        'by_sector': by_sector,
        'factor_exposures': factor_exposures,
        'regression_diagnostics': regression_diagnostics,
        'factor_orthogonalization': {
            'enabled': orthogonalized,
            'factors': shocked_factors,
            'window': int(len(factor_window)),
        },
    }

    logger.info(
        "factor_stress_test: complete",
        scenario=scenario_name,
        portfolio_return_pct=result['portfolio_return_pct'],
        portfolio_pnl=result['portfolio_pnl'],
        positions_tested=len(position_impacts)
    )

    return result


def run_all_stress_tests(
    position_returns: pd.DataFrame,
    factor_returns: pd.DataFrame,
    weights: np.ndarray,
    symbols: List[str],
    portfolio_value: float,
    all_prices: Dict[str, pd.DataFrame],
    sectors: Optional[Dict[str, str]] = None,
) -> Dict:
    """Run all stress test scenarios.

    Executes both historical replay and factor-based stress tests.

    Args:
        position_returns: DataFrame of position returns (T x N_positions)
        factor_returns: DataFrame of factor returns (T x N_factors)
        weights: Position weights (as decimals or dollar amounts)
        symbols: List of position symbols
        portfolio_value: Total portfolio value in USD
        all_prices: Dict of symbol -> price DataFrame (for historical tests)
        sectors: Optional dict mapping symbol -> sector name

    Returns:
        Dict with:
            - historical: {scenario_key: result_dict, ...}
            - factor: {scenario_key: result_dict, ...}
            - computed_at: ISO timestamp
    """
    logger.info("run_all_stress_tests: starting all scenarios")

    # Run historical scenarios
    historical_results = {}
    for scenario_key in HISTORICAL_SCENARIOS.keys():
        try:
            result = historical_stress_test(
                position_returns=position_returns,
                weights=weights,
                symbols=symbols,
                portfolio_value=portfolio_value,
                scenario_key=scenario_key,
                all_prices=all_prices,
                sectors=sectors,
            )
            if result:
                historical_results[scenario_key] = result
        except Exception as e:
            logger.error(
                "run_all_stress_tests: historical scenario failed",
                scenario=scenario_key,
                error=str(e)
            )

    # Run factor scenarios
    factor_results = {}
    for scenario_key in FACTOR_SHOCKS.keys():
        try:
            result = factor_stress_test(
                position_returns=position_returns,
                factor_returns=factor_returns,
                weights=weights,
                symbols=symbols,
                portfolio_value=portfolio_value,
                scenario_key=scenario_key,
                sectors=sectors,
            )
            if result:
                factor_results[scenario_key] = result
        except Exception as e:
            logger.error(
                "run_all_stress_tests: factor scenario failed",
                scenario=scenario_key,
                error=str(e)
            )

    results = {
        'historical': historical_results,
        'factor': factor_results,
        'computed_at': datetime.utcnow().isoformat() + 'Z',
    }

    logger.info(
        "run_all_stress_tests: complete",
        historical_scenarios=len(historical_results),
        factor_scenarios=len(factor_results)
    )

    return results
