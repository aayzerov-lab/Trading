"""
Shared test fixtures for the risk analytics test suite.

Provides consistent test data across all test modules:
- Sample price DataFrames
- Sample returns DataFrames with correlation structure
- Sample portfolio weights and covariance matrices
- Factor proxy prices for stress testing
"""

import pytest
import numpy as np
import pandas as pd


@pytest.fixture
def sample_prices():
    """Create sample price DataFrames for 5 symbols over 300 trading days.

    Returns:
        Dict[str, pd.DataFrame]: Dictionary mapping symbol to price DataFrame
            Each DataFrame has columns: date, close, adj_close
    """
    np.random.seed(42)
    dates = pd.bdate_range('2023-01-01', periods=300)
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']
    prices = {}

    for i, sym in enumerate(symbols):
        # Generate random walk prices starting at different levels
        base_price = 100 + i * 50
        returns = np.random.normal(0.0005, 0.02, len(dates))
        price_series = base_price * np.exp(np.cumsum(returns))

        df = pd.DataFrame({
            'date': dates,
            'close': price_series,
            'adj_close': price_series * (1 - 0.001 * i),  # slight difference for testing
        })
        prices[sym] = df

    return prices


@pytest.fixture
def sample_returns():
    """Create sample returns DataFrame with correlation structure.

    Returns:
        pd.DataFrame: Returns matrix (252 x 5) with DatetimeIndex
            Includes correlation structure: GOOGL correlated with AAPL, MSFT correlated with AAPL
    """
    np.random.seed(42)
    dates = pd.bdate_range('2023-01-01', periods=252)
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']

    # Generate base returns
    data = np.random.normal(0, 0.02, (len(dates), len(symbols)))

    # Add correlation structure
    data[:, 1] = 0.7 * data[:, 0] + 0.3 * data[:, 1]  # GOOGL correlated with AAPL
    data[:, 2] = 0.5 * data[:, 0] + 0.5 * data[:, 2]  # MSFT correlated with AAPL

    return pd.DataFrame(data, index=dates, columns=symbols)


@pytest.fixture
def sample_weights():
    """Sample portfolio weights (long-only, sum to ~1).

    Returns:
        np.ndarray: Array of 5 weights summing to 1.0
    """
    w = np.array([0.30, 0.25, 0.20, 0.15, 0.10])
    return w


@pytest.fixture
def sample_cov(sample_returns):
    """Sample covariance matrix from returns.

    Args:
        sample_returns: Fixture providing returns DataFrame

    Returns:
        np.ndarray: 5x5 covariance matrix
    """
    return sample_returns.cov().values


@pytest.fixture
def sample_factor_prices():
    """Factor proxy prices for stress testing.

    Returns:
        Dict[str, pd.DataFrame]: Dictionary mapping factor symbol to price DataFrame
            Covers ~5 years including COVID crash (2020) and rate shock (2022)
    """
    np.random.seed(123)
    dates = pd.bdate_range('2019-01-01', periods=1200)  # ~5 years

    factors = {
        'SPY': 300,     # S&P 500
        'QQQ': 200,     # Nasdaq
        'TLT': 130,     # 20Y Treasury
        'HYG': 85,      # High Yield
        'UUP': 25,      # Dollar index
        'USO': 40,      # Oil
        'BTC-USD': 10000,  # Bitcoin
    }

    prices = {}
    for sym, base in factors.items():
        returns = np.random.normal(0.0003, 0.015, len(dates))
        price_series = base * np.exp(np.cumsum(returns))

        prices[sym] = pd.DataFrame({
            'date': dates,
            'close': price_series,
            'adj_close': price_series,
        })

    return prices


@pytest.fixture
def sample_symbols():
    """Standard list of symbols used across tests.

    Returns:
        List[str]: List of 5 stock symbols
    """
    return ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']
