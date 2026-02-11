"""Data ingestion pipelines for Yahoo Finance and FRED economic data."""

from shared.data.fred import (
    FRED_SERIES,
    compute_macro_overview,
    fetch_fred_series,
    get_fred_from_db,
)
from shared.data.scheduler import (
    check_and_trigger_risk_recompute,
    compute_portfolio_hash,
    run_daily_data_update,
    run_daily_jobs,
)
from shared.data.yahoo import (
    FACTOR_SYMBOLS,
    fetch_factor_prices,
    fetch_prices_yahoo,
    get_prices_from_db,
)

__all__ = [
    # Yahoo Finance
    "FACTOR_SYMBOLS",
    "fetch_factor_prices",
    "fetch_prices_yahoo",
    "get_prices_from_db",
    # FRED
    "FRED_SERIES",
    "compute_macro_overview",
    "fetch_fred_series",
    "get_fred_from_db",
    # Scheduler
    "check_and_trigger_risk_recompute",
    "compute_portfolio_hash",
    "run_daily_data_update",
    "run_daily_jobs",
]
