"""Data ingestion pipelines for Yahoo Finance and FRED economic data."""

try:
    from shared.data.fred import (
        FRED_SERIES,
        compute_macro_overview,
        fetch_fred_series,
        get_fred_from_db,
    )
except ModuleNotFoundError:  # Optional dependency during local tests
    FRED_SERIES = {}

    def _fred_missing(*_args, **_kwargs):  # type: ignore[return-value]
        raise ModuleNotFoundError("fredapi is required for FRED data access")

    compute_macro_overview = _fred_missing  # type: ignore[assignment]
    fetch_fred_series = _fred_missing  # type: ignore[assignment]
    get_fred_from_db = _fred_missing  # type: ignore[assignment]
try:
    from shared.data.scheduler import (
        check_and_trigger_risk_recompute,
        compute_portfolio_hash,
        run_daily_data_update,
        run_daily_jobs,
    )
except ModuleNotFoundError:
    def _scheduler_missing(*_args, **_kwargs):  # type: ignore[return-value]
        raise ModuleNotFoundError("scheduler dependencies are missing")

    check_and_trigger_risk_recompute = _scheduler_missing  # type: ignore[assignment]
    compute_portfolio_hash = _scheduler_missing  # type: ignore[assignment]
    run_daily_data_update = _scheduler_missing  # type: ignore[assignment]
    run_daily_jobs = _scheduler_missing  # type: ignore[assignment]
try:
    from shared.data.yahoo import (
        FACTOR_SYMBOLS,
        fetch_factor_prices,
        fetch_prices_yahoo,
        get_prices_from_db,
    )
except ModuleNotFoundError:
    FACTOR_SYMBOLS = []

    def _yahoo_missing(*_args, **_kwargs):  # type: ignore[return-value]
        raise ModuleNotFoundError("yfinance is required for Yahoo data access")

    fetch_factor_prices = _yahoo_missing  # type: ignore[assignment]
    fetch_prices_yahoo = _yahoo_missing  # type: ignore[assignment]
    get_prices_from_db = _yahoo_missing  # type: ignore[assignment]

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
