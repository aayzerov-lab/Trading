"""
Unit tests for returns.py - Return Construction Module

Tests cover:
- Price matrix construction and alignment
- Log and simple return computation
- Return trimming to window
- Position return alignment
"""

import pytest
import numpy as np
import pandas as pd
from numpy.testing import assert_allclose

from shared.risk.returns import (
    build_price_matrix,
    compute_log_returns,
    compute_simple_returns,
    trim_to_window,
    get_aligned_position_returns,
)


class TestBuildPriceMatrix:
    """Tests for build_price_matrix function."""

    def test_build_price_matrix_alignment(self, sample_prices):
        """Verify price matrix has common date index (intersection of trading days)."""
        price_matrix = build_price_matrix(sample_prices, price_col='adj_close', min_history=60)

        assert not price_matrix.empty
        assert len(price_matrix.columns) == 5  # All symbols should be included
        assert price_matrix.index.is_monotonic_increasing
        assert price_matrix.isna().sum().sum() == 0  # No NaN values after intersection

    def test_build_price_matrix_drops_short_history(self, sample_prices):
        """Symbols with fewer than min_history observations should be dropped."""
        # Create a price dict with one symbol having insufficient history
        short_prices = sample_prices.copy()
        short_prices['SHORT'] = pd.DataFrame({
            'date': pd.bdate_range('2023-01-01', periods=30),
            'adj_close': np.random.rand(30) * 100,
        })

        price_matrix = build_price_matrix(short_prices, price_col='adj_close', min_history=60)

        assert 'SHORT' not in price_matrix.columns  # Should be dropped
        assert len(price_matrix.columns) == 5  # Only original symbols remain

    def test_build_price_matrix_no_forward_fill(self, sample_prices):
        """Verify NaN values are not forward filled (should be dropped instead)."""
        # Add symbol with gaps (NaN values)
        gapped_prices = sample_prices.copy()
        dates = pd.bdate_range('2023-01-01', periods=100)
        prices = np.random.rand(100) * 100
        prices[50:60] = np.nan  # Create a gap

        gapped_prices['GAPPED'] = pd.DataFrame({
            'date': dates,
            'adj_close': prices,
        })

        price_matrix = build_price_matrix(gapped_prices, price_col='adj_close', min_history=60)

        # The matrix should drop rows with NaN rather than forward filling
        assert price_matrix.isna().sum().sum() == 0

    def test_build_price_matrix_missing_column(self, sample_prices):
        """Test handling of missing price column."""
        bad_prices = sample_prices.copy()
        bad_prices['BAD'] = pd.DataFrame({
            'date': pd.bdate_range('2023-01-01', periods=100),
            'close': np.random.rand(100) * 100,
            # Missing adj_close
        })

        price_matrix = build_price_matrix(bad_prices, price_col='adj_close', min_history=60)

        assert 'BAD' not in price_matrix.columns

    def test_build_price_matrix_empty_input(self):
        """Test handling of empty price dictionary."""
        price_matrix = build_price_matrix({}, price_col='adj_close', min_history=60)
        assert price_matrix.empty


class TestComputeLogReturns:
    """Tests for compute_log_returns function."""

    def test_compute_log_returns_shape(self, sample_prices):
        """Output shape should be (T-1, N) since first row is dropped."""
        price_matrix = build_price_matrix(sample_prices, price_col='adj_close', min_history=60)
        log_returns = compute_log_returns(price_matrix)

        assert len(log_returns) == len(price_matrix) - 1
        assert len(log_returns.columns) == len(price_matrix.columns)

    def test_compute_log_returns_values(self, sample_prices):
        """Spot check: ln(P1/P0) should match expected calculation."""
        price_matrix = build_price_matrix(sample_prices, price_col='adj_close', min_history=60)
        log_returns = compute_log_returns(price_matrix)

        # Manually calculate first return for first symbol
        symbol = price_matrix.columns[0]
        expected_return = np.log(price_matrix[symbol].iloc[1] / price_matrix[symbol].iloc[0])
        actual_return = log_returns[symbol].iloc[0]

        assert_allclose(actual_return, expected_return, rtol=1e-10)

    def test_compute_log_returns_no_inf(self, sample_prices):
        """Verify no infinities in output (which would indicate zero prices)."""
        price_matrix = build_price_matrix(sample_prices, price_col='adj_close', min_history=60)
        log_returns = compute_log_returns(price_matrix)

        assert not np.isinf(log_returns.values).any()

    def test_compute_log_returns_zero_price_raises(self):
        """Zero prices should raise ValueError."""
        # Create price matrix with zero price
        dates = pd.bdate_range('2023-01-01', periods=10)
        price_matrix = pd.DataFrame({
            'A': [100, 110, 0, 120, 130, 140, 150, 160, 170, 180],  # Zero price
        }, index=dates)

        with pytest.raises(ValueError, match="Zero or negative prices"):
            compute_log_returns(price_matrix)

    def test_compute_log_returns_empty_input(self):
        """Empty price matrix should return empty DataFrame."""
        empty_df = pd.DataFrame()
        result = compute_log_returns(empty_df)
        assert result.empty


class TestComputeSimpleReturns:
    """Tests for compute_simple_returns function."""

    def test_compute_simple_returns_shape(self, sample_prices):
        """Output shape should be (T-1, N)."""
        price_matrix = build_price_matrix(sample_prices, price_col='adj_close', min_history=60)
        simple_returns = compute_simple_returns(price_matrix)

        assert len(simple_returns) == len(price_matrix) - 1
        assert len(simple_returns.columns) == len(price_matrix.columns)

    def test_compute_simple_returns_values(self, sample_prices):
        """Spot check: (P1 - P0) / P0 should match expected."""
        price_matrix = build_price_matrix(sample_prices, price_col='adj_close', min_history=60)
        simple_returns = compute_simple_returns(price_matrix)

        # Manually calculate first return for first symbol
        symbol = price_matrix.columns[0]
        expected_return = (price_matrix[symbol].iloc[1] - price_matrix[symbol].iloc[0]) / price_matrix[symbol].iloc[0]
        actual_return = simple_returns[symbol].iloc[0]

        assert_allclose(actual_return, expected_return, rtol=1e-10)

    def test_compute_simple_returns_zero_price_raises(self):
        """Zero prices should raise ValueError."""
        dates = pd.bdate_range('2023-01-01', periods=10)
        price_matrix = pd.DataFrame({
            'A': [100, 110, 0, 120, 130, 140, 150, 160, 170, 180],
        }, index=dates)

        with pytest.raises(ValueError, match="Zero prices"):
            compute_simple_returns(price_matrix)


class TestTrimToWindow:
    """Tests for trim_to_window function."""

    def test_trim_to_window_correct_length(self, sample_returns):
        """Should return exactly window rows."""
        window = 100
        trimmed = trim_to_window(sample_returns, window)

        assert len(trimmed) == window
        assert list(trimmed.columns) == list(sample_returns.columns)

    def test_trim_to_window_takes_last_rows(self, sample_returns):
        """Should take the last N rows."""
        window = 100
        trimmed = trim_to_window(sample_returns, window)

        # Check that the last date matches
        assert trimmed.index[-1] == sample_returns.index[-1]
        assert trimmed.index[0] == sample_returns.index[-window]

    def test_trim_to_window_insufficient_data(self, sample_returns):
        """Should raise ValueError if insufficient data."""
        window = 300  # More than available (252)

        with pytest.raises(ValueError, match="Insufficient data"):
            trim_to_window(sample_returns, window)

    def test_trim_to_window_empty_raises(self):
        """Empty returns should raise ValueError."""
        empty_df = pd.DataFrame()

        with pytest.raises(ValueError, match="Cannot trim empty"):
            trim_to_window(empty_df, 10)


class TestGetAlignedPositionReturns:
    """Tests for get_aligned_position_returns function."""

    def test_get_aligned_position_returns_structure(self, sample_prices):
        """Should return tuple of (returns_df, missing_symbols)."""
        symbols = ['AAPL', 'GOOGL', 'MSFT']
        returns, missing = get_aligned_position_returns(
            symbols, sample_prices, window=100, price_col='adj_close'
        )

        assert isinstance(returns, pd.DataFrame)
        assert isinstance(missing, list)
        assert len(returns) == 100  # Trimmed to window
        assert set(returns.columns).issubset(set(symbols))

    def test_get_aligned_position_returns_missing_symbols(self, sample_prices):
        """Should report symbols not found in price data."""
        symbols = ['AAPL', 'GOOGL', 'NOTFOUND']
        returns, missing = get_aligned_position_returns(
            symbols, sample_prices, window=100, price_col='adj_close'
        )

        assert 'NOTFOUND' in missing
        assert 'NOTFOUND' not in returns.columns

    def test_get_aligned_position_returns_all_valid(self, sample_prices):
        """When all symbols are valid, missing list should be empty."""
        symbols = ['AAPL', 'GOOGL', 'MSFT']
        returns, missing = get_aligned_position_returns(
            symbols, sample_prices, window=100, price_col='adj_close'
        )

        assert len(missing) == 0
        assert len(returns.columns) == 3

    def test_get_aligned_position_returns_empty_positions(self, sample_prices):
        """Empty position list should return empty DataFrame and empty list."""
        returns, missing = get_aligned_position_returns(
            [], sample_prices, window=100, price_col='adj_close'
        )

        assert returns.empty
        assert len(missing) == 0

    def test_get_aligned_position_returns_insufficient_window(self, sample_prices):
        """Should return empty DataFrame when window exceeds available data."""
        symbols = ['AAPL', 'GOOGL']

        returns, missing = get_aligned_position_returns(
            symbols, sample_prices, window=500, price_col='adj_close'
        )

        # With insufficient data, all symbols are dropped and returns is empty
        assert returns.empty
        assert set(missing) == set(symbols)
