"""
Return Construction Module

Pure functions for building price matrices and computing returns from market data.
All functions operate on pandas DataFrames and return aligned, clean data structures.
"""

import pandas as pd
import numpy as np
import structlog
from typing import Dict, List, Tuple

logger = structlog.get_logger(__name__)


def build_price_matrix(
    prices: Dict[str, pd.DataFrame],
    price_col: str = 'adj_close',
    min_history: int = 60,
) -> pd.DataFrame:
    """Build aligned price matrix from dict of symbol -> DataFrame.

    Each DataFrame has columns: date, close, adj_close
    Aligns all series to a common date index (intersection of trading days).
    Drops symbols with fewer than min_history observations.

    Args:
        prices: Dictionary mapping symbol to DataFrame with 'date' and price columns
        price_col: Column name to use for prices ('adj_close' or 'close')
        min_history: Minimum number of observations required per symbol

    Returns:
        DataFrame with DatetimeIndex and symbol columns containing aligned prices

    Note:
        Does NOT forward-fill prices as this creates false returns.
        Only uses intersection of available trading days across all symbols.
    """
    if not prices:
        logger.warning("build_price_matrix: empty prices dict provided")
        return pd.DataFrame()

    # Extract price series for each symbol
    series_dict = {}
    dropped_symbols = []

    for symbol, df in prices.items():
        if df is None or df.empty:
            dropped_symbols.append((symbol, "empty_dataframe"))
            continue

        if price_col not in df.columns:
            dropped_symbols.append((symbol, f"missing_{price_col}_column"))
            continue

        if 'date' not in df.columns:
            dropped_symbols.append((symbol, "missing_date_column"))
            continue

        # Ensure date is datetime
        df = df.copy()
        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            df['date'] = pd.to_datetime(df['date'])

        # Set date as index and extract price series
        df = df.set_index('date')
        price_series = df[price_col].copy()

        # Drop NaN prices (don't forward fill)
        price_series = price_series.dropna()

        if len(price_series) < min_history:
            dropped_symbols.append((symbol, f"insufficient_history_{len(price_series)}_lt_{min_history}"))
            continue

        series_dict[symbol] = price_series

    if dropped_symbols:
        logger.info(
            "build_price_matrix: dropped symbols",
            dropped_count=len(dropped_symbols),
            dropped=dropped_symbols[:10]  # Log first 10
        )

    if not series_dict:
        logger.warning("build_price_matrix: no valid symbols remain after filtering")
        return pd.DataFrame()

    # Concatenate all series and use inner join (intersection)
    price_matrix = pd.DataFrame(series_dict)

    # Drop any rows with NaN (ensures complete data across all symbols)
    original_rows = len(price_matrix)
    price_matrix = price_matrix.dropna()

    if len(price_matrix) < original_rows:
        logger.info(
            "build_price_matrix: dropped rows with missing data",
            original_rows=original_rows,
            final_rows=len(price_matrix),
            dropped_rows=original_rows - len(price_matrix)
        )

    # Final validation: check for any symbols that now have insufficient history
    symbols_to_drop = []
    for col in price_matrix.columns:
        if len(price_matrix[col]) < min_history:
            symbols_to_drop.append(col)

    if symbols_to_drop:
        logger.info(
            "build_price_matrix: dropping symbols after alignment",
            symbols=symbols_to_drop
        )
        price_matrix = price_matrix.drop(columns=symbols_to_drop)

    logger.info(
        "build_price_matrix: matrix built",
        num_symbols=len(price_matrix.columns),
        num_dates=len(price_matrix),
        date_range=f"{price_matrix.index.min()} to {price_matrix.index.max()}" if len(price_matrix) > 0 else "empty"
    )

    return price_matrix


def compute_log_returns(price_matrix: pd.DataFrame) -> pd.DataFrame:
    """Compute log returns from price matrix.

    log_return = ln(P_t / P_{t-1})

    Args:
        price_matrix: DataFrame with DatetimeIndex and symbol columns

    Returns:
        DataFrame with log returns (first row dropped due to NaN)

    Raises:
        ValueError: If infinite values detected (from zero prices)
    """
    if price_matrix.empty:
        logger.warning("compute_log_returns: empty price matrix provided")
        return pd.DataFrame()

    # Check for zero or negative prices
    if (price_matrix <= 0).any().any():
        zero_prices = (price_matrix <= 0).sum()
        logger.error(
            "compute_log_returns: zero or negative prices detected",
            affected_symbols=zero_prices[zero_prices > 0].to_dict()
        )
        raise ValueError("Zero or negative prices detected in price matrix")

    # Compute log returns
    log_returns = np.log(price_matrix / price_matrix.shift(1))

    # Drop first row (NaN)
    log_returns = log_returns.iloc[1:]

    # Verify no infinities
    if np.isinf(log_returns.values).any():
        inf_counts = np.isinf(log_returns.values).sum(axis=0)
        affected_symbols = [
            price_matrix.columns[i]
            for i, count in enumerate(inf_counts)
            if count > 0
        ]
        logger.error(
            "compute_log_returns: infinite values detected",
            affected_symbols=affected_symbols
        )
        raise ValueError(f"Infinite values detected in returns for symbols: {affected_symbols}")

    logger.info(
        "compute_log_returns: returns computed",
        num_symbols=len(log_returns.columns),
        num_periods=len(log_returns)
    )

    return log_returns


def compute_simple_returns(price_matrix: pd.DataFrame) -> pd.DataFrame:
    """Compute simple returns: (P_t - P_{t-1}) / P_{t-1}

    Args:
        price_matrix: DataFrame with DatetimeIndex and symbol columns

    Returns:
        DataFrame with simple returns (first row dropped due to NaN)
    """
    if price_matrix.empty:
        logger.warning("compute_simple_returns: empty price matrix provided")
        return pd.DataFrame()

    # Check for zero prices
    if (price_matrix == 0).any().any():
        zero_prices = (price_matrix == 0).sum()
        logger.error(
            "compute_simple_returns: zero prices detected",
            affected_symbols=zero_prices[zero_prices > 0].to_dict()
        )
        raise ValueError("Zero prices detected in price matrix")

    # Compute simple returns
    simple_returns = price_matrix.pct_change()

    # Drop first row (NaN)
    simple_returns = simple_returns.iloc[1:]

    logger.info(
        "compute_simple_returns: returns computed",
        num_symbols=len(simple_returns.columns),
        num_periods=len(simple_returns)
    )

    return simple_returns


def trim_to_window(returns: pd.DataFrame, window: int) -> pd.DataFrame:
    """Take the last `window` rows of returns.

    Args:
        returns: DataFrame of returns
        window: Number of periods to keep

    Returns:
        DataFrame with last `window` rows

    Raises:
        ValueError: If insufficient data for the requested window
    """
    if returns.empty:
        raise ValueError("Cannot trim empty returns DataFrame")

    if len(returns) < window:
        raise ValueError(
            f"Insufficient data for window: have {len(returns)} periods, need {window}"
        )

    trimmed = returns.iloc[-window:]

    logger.info(
        "trim_to_window: returns trimmed",
        original_length=len(returns),
        window=window,
        date_range=f"{trimmed.index.min()} to {trimmed.index.max()}"
    )

    return trimmed


def get_aligned_position_returns(
    position_symbols: List[str],
    all_prices: Dict[str, pd.DataFrame],
    window: int = 252,
    price_col: str = 'adj_close',
) -> Tuple[pd.DataFrame, List[str]]:
    """Get aligned returns matrix for portfolio positions.

    Convenience function that:
    1. Filters prices to position symbols
    2. Builds aligned price matrix
    3. Computes log returns
    4. Trims to requested window

    Args:
        position_symbols: List of symbols in the portfolio
        all_prices: Dictionary of all available price data
        window: Number of periods to use for calculations (default 252 = 1 year)
        price_col: Price column to use

    Returns:
        Tuple of (returns_df, missing_symbols):
            - returns_df: DataFrame of aligned log returns, trimmed to window
            - missing_symbols: List of symbols that were dropped
    """
    if not position_symbols:
        logger.warning("get_aligned_position_returns: empty position_symbols list")
        return pd.DataFrame(), []

    # Filter to position symbols
    position_prices = {
        symbol: all_prices.get(symbol)
        for symbol in position_symbols
        if symbol in all_prices
    }

    symbols_not_in_data = [s for s in position_symbols if s not in all_prices]
    if symbols_not_in_data:
        logger.warning(
            "get_aligned_position_returns: symbols not found in price data",
            missing=symbols_not_in_data
        )

    # Build price matrix (already handles min_history and alignment)
    # Use min_history = window + 1 to ensure we have enough data after computing returns
    price_matrix = build_price_matrix(
        position_prices,
        price_col=price_col,
        min_history=window + 1
    )

    if price_matrix.empty:
        logger.error("get_aligned_position_returns: no valid price data after alignment")
        return pd.DataFrame(), position_symbols

    # Compute returns
    returns = compute_log_returns(price_matrix)

    # Trim to window
    try:
        returns = trim_to_window(returns, window)
    except ValueError as e:
        logger.error(
            "get_aligned_position_returns: insufficient data for window",
            error=str(e),
            available=len(returns),
            requested=window
        )
        raise

    # Determine missing symbols
    symbols_in_returns = set(returns.columns)
    missing_symbols = [s for s in position_symbols if s not in symbols_in_returns]

    if missing_symbols:
        logger.info(
            "get_aligned_position_returns: some positions excluded",
            missing_count=len(missing_symbols),
            missing_symbols=missing_symbols
        )

    logger.info(
        "get_aligned_position_returns: returns prepared",
        requested_symbols=len(position_symbols),
        returned_symbols=len(returns.columns),
        window=window,
        date_range=f"{returns.index.min()} to {returns.index.max()}"
    )

    return returns, missing_symbols
