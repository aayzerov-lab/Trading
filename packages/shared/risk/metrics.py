"""
Risk Metrics Module

Comprehensive risk analytics including VaR, Expected Shortfall, risk contributions,
and portfolio concentration metrics. Pure computation functions for hedge fund analytics.
"""

import numpy as np
import pandas as pd
import structlog
from scipy import stats
from typing import List, Dict

logger = structlog.get_logger(__name__)


def portfolio_volatility(
    weights: np.ndarray,
    cov: np.ndarray,
    horizon_days: int = 1,
) -> float:
    """Compute portfolio volatility.

    Daily vol = sqrt(w' * Sigma * w)
    Horizon vol = daily_vol * sqrt(horizon_days)

    Args:
        weights: Position weights (N x 1 array or flat array)
        cov: Daily covariance matrix (N x N)
        horizon_days: Time horizon in days (default 1)

    Returns:
        Portfolio volatility scaled to horizon (as decimal, not %)
    """
    weights = np.asarray(weights).flatten()

    if weights.shape[0] != cov.shape[0]:
        raise ValueError(
            f"Weights dimension {weights.shape[0]} doesn't match covariance {cov.shape[0]}"
        )

    if horizon_days < 1:
        raise ValueError(f"Horizon must be >= 1, got {horizon_days}")

    # Portfolio variance
    portfolio_var = weights @ cov @ weights

    if portfolio_var < -1e-10:
        raise ValueError(
            f"Negative portfolio variance ({portfolio_var:.6e}). "
            "Covariance matrix is not positive semi-definite."
        )
    # Clamp tiny negative values from numerical noise to zero
    portfolio_var = max(portfolio_var, 0.0)

    # Daily volatility
    daily_vol = np.sqrt(portfolio_var)

    # Scale to horizon
    horizon_vol = daily_vol * np.sqrt(horizon_days)

    return float(horizon_vol)


def parametric_var(
    weights: np.ndarray,
    cov: np.ndarray,
    confidence: float = 0.95,
    horizon_days: int = 1,
    portfolio_value: float = 1.0,
) -> float:
    """Parametric Value-at-Risk at given confidence level.

    For daily returns with zero mean assumption:
    VaR = portfolio_value * z * sigma * sqrt(horizon_days)

    where z = norm.ppf(confidence), sigma = portfolio daily vol

    Args:
        weights: Position weights (N x 1 or flat)
        cov: Daily covariance matrix (N x N)
        confidence: Confidence level (e.g., 0.95 for 95% VaR)
        horizon_days: Time horizon in days
        portfolio_value: Total portfolio value in USD

    Returns:
        VaR as positive number (loss amount in USD)
    """
    if not 0 < confidence < 1:
        raise ValueError(f"Confidence must be between 0 and 1, got {confidence}")

    if portfolio_value <= 0:
        raise ValueError(f"Portfolio value must be positive, got {portfolio_value}")

    # Get daily volatility
    daily_vol = portfolio_volatility(weights, cov, horizon_days=1)

    # Z-score for confidence level (this is negative for losses)
    z_score = stats.norm.ppf(1 - confidence)  # e.g., -1.645 for 95% confidence

    # VaR as positive loss
    var = portfolio_value * abs(z_score) * daily_vol * np.sqrt(horizon_days)

    return float(var)


def expected_shortfall(
    weights: np.ndarray,
    cov: np.ndarray,
    confidence: float = 0.95,
    horizon_days: int = 1,
    portfolio_value: float = 1.0,
) -> float:
    """Parametric Expected Shortfall (Conditional VaR).

    ES = portfolio_value * sigma * phi(z) / (1 - confidence) * sqrt(horizon_days)

    where phi is the standard normal pdf, z = norm.ppf(confidence)

    Args:
        weights: Position weights (N x 1 or flat)
        cov: Daily covariance matrix (N x N)
        confidence: Confidence level (e.g., 0.95 for 95% ES)
        horizon_days: Time horizon in days
        portfolio_value: Total portfolio value in USD

    Returns:
        ES as positive number (expected loss amount in USD)
    """
    if not 0 < confidence < 1:
        raise ValueError(f"Confidence must be between 0 and 1, got {confidence}")

    if portfolio_value <= 0:
        raise ValueError(f"Portfolio value must be positive, got {portfolio_value}")

    # Get daily volatility
    daily_vol = portfolio_volatility(weights, cov, horizon_days=1)

    # Z-score for confidence level
    z_score = stats.norm.ppf(1 - confidence)

    # Standard normal pdf at z-score
    phi_z = stats.norm.pdf(z_score)

    # Expected Shortfall
    es = portfolio_value * daily_vol * phi_z / (1 - confidence) * np.sqrt(horizon_days)

    return float(es)


def marginal_contribution_to_risk(
    weights: np.ndarray,
    cov: np.ndarray,
) -> np.ndarray:
    """Marginal Contribution to Risk (MCR) per position.

    MCR_i = (Sigma * w)_i / sigma_p

    where sigma_p = sqrt(w' * Sigma * w)

    Represents the change in portfolio volatility from a unit increase in position i.

    Args:
        weights: Position weights (N x 1 or flat)
        cov: Daily covariance matrix (N x N)

    Returns:
        Array of MCR values (length N)
    """
    weights = np.asarray(weights).flatten()

    if weights.shape[0] != cov.shape[0]:
        raise ValueError(
            f"Weights dimension {weights.shape[0]} doesn't match covariance {cov.shape[0]}"
        )

    # Portfolio volatility
    port_vol = portfolio_volatility(weights, cov, horizon_days=1)

    if port_vol == 0:
        logger.warning("marginal_contribution_to_risk: zero portfolio volatility")
        return np.zeros_like(weights)

    # Marginal contribution
    mcr = (cov @ weights) / port_vol

    return mcr


def component_contribution_to_risk(
    weights: np.ndarray,
    cov: np.ndarray,
) -> np.ndarray:
    """Component Contribution to Risk (CCR) per position.

    CCR_i = w_i * MCR_i

    Sum of CCR equals portfolio volatility.

    Args:
        weights: Position weights (N x 1 or flat)
        cov: Daily covariance matrix (N x N)

    Returns:
        Array of CCR values (length N)
    """
    weights = np.asarray(weights).flatten()

    mcr = marginal_contribution_to_risk(weights, cov)
    ccr = weights * mcr

    return ccr


def pct_contribution_to_variance(
    weights: np.ndarray,
    cov: np.ndarray,
) -> np.ndarray:
    """Percentage contribution to portfolio variance per position.

    PCV_i = w_i * (Sigma * w)_i / (w' * Sigma * w)

    Sum of all PCV_i = 100%.

    Args:
        weights: Position weights (N x 1 or flat)
        cov: Daily covariance matrix (N x N)

    Returns:
        Array of percentages (summing to ~100)
    """
    weights = np.asarray(weights).flatten()

    if weights.shape[0] != cov.shape[0]:
        raise ValueError(
            f"Weights dimension {weights.shape[0]} doesn't match covariance {cov.shape[0]}"
        )

    # Portfolio variance
    port_var = weights @ cov @ weights

    if port_var == 0:
        logger.warning("pct_contribution_to_variance: zero portfolio variance")
        return np.zeros_like(weights)

    # Contribution to variance
    contrib = weights * (cov @ weights) / port_var

    # Convert to percentage
    pct_contrib = contrib * 100

    return pct_contrib


def concentration_metrics(
    weights: np.ndarray,
    symbols: List[str],
) -> Dict:
    """Compute concentration metrics.

    Args:
        weights: Position weights (as decimals or dollar amounts)
        symbols: List of symbols corresponding to weights

    Returns:
        Dict with:
            - top_5_pct: Sum of top 5 absolute weights as % of gross
            - hhi: Herfindahl-Hirschman Index
            - top_5_names: List of top 5 symbol names
    """
    weights = np.asarray(weights).flatten()

    if len(weights) != len(symbols):
        raise ValueError(
            f"Weights length {len(weights)} doesn't match symbols length {len(symbols)}"
        )

    # Absolute weights for concentration
    abs_weights = np.abs(weights)
    gross_exposure = np.sum(abs_weights)

    if gross_exposure == 0:
        logger.warning("concentration_metrics: zero gross exposure")
        return {
            'top_5_pct': 0.0,
            'hhi': 0.0,
            'top_5_names': []
        }

    # Normalized weights for HHI
    normalized_weights = abs_weights / gross_exposure

    # HHI: sum of squared normalized weights
    hhi = float(np.sum(normalized_weights ** 2) * 10000)

    # Top 5 concentration
    sorted_indices = np.argsort(abs_weights)[::-1]  # Descending
    top_5_indices = sorted_indices[:min(5, len(weights))]
    top_5_sum = np.sum(abs_weights[top_5_indices])
    top_5_pct = float((top_5_sum / gross_exposure) * 100)

    top_5_names = [symbols[i] for i in top_5_indices]

    return {
        'top_5_pct': top_5_pct,
        'hhi': hhi,
        'top_5_names': top_5_names
    }


def build_risk_summary(
    weights: np.ndarray,
    cov: np.ndarray,
    symbols: List[str],
    portfolio_value: float,
) -> Dict:
    """Build complete risk summary dict.

    Args:
        weights: Position weights (as decimals or dollar amounts)
        cov: Daily covariance matrix (N x N)
        symbols: List of symbols
        portfolio_value: Total portfolio value in USD

    Returns:
        Dict with comprehensive risk metrics
    """
    weights = np.asarray(weights).flatten()

    if len(weights) != len(symbols):
        raise ValueError(
            f"Weights length {len(weights)} doesn't match symbols length {len(symbols)}"
        )

    if weights.shape[0] != cov.shape[0]:
        raise ValueError(
            f"Weights dimension {weights.shape[0]} doesn't match covariance {cov.shape[0]}"
        )

    # Volatility metrics
    vol_1d = portfolio_volatility(weights, cov, horizon_days=1)
    vol_5d = portfolio_volatility(weights, cov, horizon_days=5)

    vol_1d_dollar = vol_1d * portfolio_value
    vol_5d_dollar = vol_5d * portfolio_value

    # VaR and ES metrics
    var_95_1d = parametric_var(weights, cov, confidence=0.95, horizon_days=1, portfolio_value=portfolio_value)
    es_95_1d = expected_shortfall(weights, cov, confidence=0.95, horizon_days=1, portfolio_value=portfolio_value)
    var_95_5d = parametric_var(weights, cov, confidence=0.95, horizon_days=5, portfolio_value=portfolio_value)
    es_95_5d = expected_shortfall(weights, cov, confidence=0.95, horizon_days=5, portfolio_value=portfolio_value)

    # Concentration metrics
    concentration = concentration_metrics(weights, symbols)

    # Count non-zero positions
    num_positions = int(np.sum(np.abs(weights) > 1e-9))

    summary = {
        'vol_1d': float(vol_1d_dollar),
        'vol_1d_pct': float(vol_1d * 100),
        'vol_5d': float(vol_5d_dollar),
        'vol_5d_pct': float(vol_5d * 100),
        'var_95_1d': float(var_95_1d),
        'var_95_1d_pct': float((var_95_1d / portfolio_value) * 100) if portfolio_value > 0 else 0.0,
        'es_95_1d': float(es_95_1d),
        'es_95_1d_pct': float((es_95_1d / portfolio_value) * 100) if portfolio_value > 0 else 0.0,
        'var_95_5d': float(var_95_5d),
        'es_95_5d': float(es_95_5d),
        'top_5_concentration_pct': concentration['top_5_pct'],
        'hhi': concentration['hhi'],
        'top_5_names': concentration['top_5_names'],
        'num_positions': num_positions,
        'portfolio_value': float(portfolio_value),
    }

    logger.info(
        "build_risk_summary: summary built",
        num_positions=num_positions,
        vol_1d_pct=summary['vol_1d_pct'],
        var_95_1d=summary['var_95_1d'],
        top_5_concentration_pct=summary['top_5_concentration_pct']
    )

    return summary


def build_risk_contributors(
    weights: np.ndarray,
    cov: np.ndarray,
    symbols: List[str],
    portfolio_value: float,
    standalone_vols: Dict[str, float] | None = None,
) -> List[Dict]:
    """Build per-position risk contribution table.

    Args:
        weights: Position weights (as decimals or dollar amounts)
        cov: Daily covariance matrix (N x N)
        symbols: List of symbols
        portfolio_value: Total portfolio value in USD
        standalone_vols: Optional {symbol: annualized_vol_%} computed from
            each symbol's own history.  When provided these are used instead
            of the covariance diagonal (which may be distorted by alignment).

    Returns:
        List of dicts, one per position, sorted by |CCR| descending
    """
    weights = np.asarray(weights).flatten()

    if len(weights) != len(symbols):
        raise ValueError(
            f"Weights length {len(weights)} doesn't match symbols length {len(symbols)}"
        )

    if weights.shape[0] != cov.shape[0]:
        raise ValueError(
            f"Weights dimension {weights.shape[0]} doesn't match covariance {cov.shape[0]}"
        )

    # Compute risk contributions
    mcr = marginal_contribution_to_risk(weights, cov)
    ccr = component_contribution_to_risk(weights, cov)

    # Portfolio volatility for percentage calculations
    port_vol = portfolio_volatility(weights, cov, horizon_days=1)

    # Standalone annualized vol: prefer per-symbol vols if provided
    if standalone_vols is None:
        daily_vols = np.sqrt(np.diag(cov))
        ann_vols_arr = daily_vols * np.sqrt(252) * 100  # annualized %
        ann_vols_map = {symbols[i]: float(ann_vols_arr[i]) for i in range(len(symbols))}
    else:
        ann_vols_map = standalone_vols

    # Build list of dicts
    contributors = []
    for i, symbol in enumerate(symbols):
        # Skip zero positions
        if abs(weights[i]) < 1e-9:
            continue

        weight_pct = float(weights[i] * 100)
        ccr_pct = float((ccr[i] / port_vol) * 100) if port_vol != 0 else 0.0

        contributors.append({
            'symbol': symbol,
            'weight_pct': weight_pct,
            'mcr': float(mcr[i]),
            'ccr': float(ccr[i]),
            'ccr_pct': ccr_pct,
            'standalone_vol_ann': ann_vols_map.get(symbol, 0.0),
        })

    # Sort by absolute CCR descending
    contributors.sort(key=lambda x: abs(x['ccr']), reverse=True)

    logger.info(
        "build_risk_contributors: contributors built",
        num_contributors=len(contributors)
    )

    return contributors
