"""
Risk Analytics Engine

Hedge-fund grade risk analytics for portfolio management.
Pure computation modules operating on pandas DataFrames and numpy arrays.

Modules:
- returns: Price matrix construction and return calculations
- covariance: Covariance estimation (Ledoit-Wolf, EWMA)
- metrics: VaR, ES, risk contributions, concentration
- correlation: Correlation analysis and hierarchical clustering
- stress: Historical and factor-based stress testing
"""

# Returns module
from .returns import (
    build_price_matrix,
    compute_log_returns,
    compute_simple_returns,
    trim_to_window,
    get_aligned_position_returns,
)

# Covariance module
from .covariance import (
    ledoit_wolf_cov,
    ewma_cov,
    estimate_covariance,
    annualize_cov,
)

# Metrics module
from .metrics import (
    portfolio_volatility,
    parametric_var,
    expected_shortfall,
    marginal_contribution_to_risk,
    component_contribution_to_risk,
    pct_contribution_to_variance,
    concentration_metrics,
    build_risk_summary,
    build_risk_contributors,
)

# Correlation module
from .correlation import (
    correlation_matrix,
    top_correlated_pairs,
    hierarchical_clusters,
    cluster_exposures,
)

# Stress testing module
from .stress import (
    historical_stress_test,
    factor_stress_test,
    run_all_stress_tests,
    HISTORICAL_SCENARIOS,
    FACTOR_SHOCKS,
)

__all__ = [
    # Returns
    'build_price_matrix',
    'compute_log_returns',
    'compute_simple_returns',
    'trim_to_window',
    'get_aligned_position_returns',
    # Covariance
    'ledoit_wolf_cov',
    'ewma_cov',
    'estimate_covariance',
    'annualize_cov',
    # Metrics
    'portfolio_volatility',
    'parametric_var',
    'expected_shortfall',
    'marginal_contribution_to_risk',
    'component_contribution_to_risk',
    'pct_contribution_to_variance',
    'concentration_metrics',
    'build_risk_summary',
    'build_risk_contributors',
    # Correlation
    'correlation_matrix',
    'top_correlated_pairs',
    'hierarchical_clusters',
    'cluster_exposures',
    # Stress testing
    'historical_stress_test',
    'factor_stress_test',
    'run_all_stress_tests',
    'HISTORICAL_SCENARIOS',
    'FACTOR_SHOCKS',
]
