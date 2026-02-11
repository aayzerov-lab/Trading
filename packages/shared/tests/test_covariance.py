"""
Unit tests for covariance.py - Covariance Estimation Module

Tests cover:
- Ledoit-Wolf shrinkage estimation
- EWMA (RiskMetrics) covariance
- Unified estimation interface
- Covariance annualization
"""

import pytest
import numpy as np
import pandas as pd
from numpy.testing import assert_allclose

from shared.risk.covariance import (
    ledoit_wolf_cov,
    ewma_cov,
    estimate_covariance,
    annualize_cov,
)


class TestLedoitWolfCov:
    """Tests for ledoit_wolf_cov function."""

    def test_ledoit_wolf_shape(self, sample_returns):
        """Output should be (N, N) symmetric matrix."""
        cov = ledoit_wolf_cov(sample_returns)

        n_assets = len(sample_returns.columns)
        assert cov.shape == (n_assets, n_assets)

    def test_ledoit_wolf_symmetric(self, sample_returns):
        """Matrix should be symmetric."""
        cov = ledoit_wolf_cov(sample_returns)

        assert_allclose(cov, cov.T, rtol=1e-10)

    def test_ledoit_wolf_positive_diagonal(self, sample_returns):
        """All diagonal elements should be positive (variances > 0)."""
        cov = ledoit_wolf_cov(sample_returns)

        diagonal = np.diag(cov)
        assert np.all(diagonal > 0)

    def test_ledoit_wolf_positive_semidefinite(self, sample_returns):
        """Matrix should be positive semi-definite (all eigenvalues >= 0)."""
        cov = ledoit_wolf_cov(sample_returns)

        eigenvalues = np.linalg.eigvals(cov)
        # Allow small numerical errors
        assert np.all(eigenvalues.real > -1e-8)

    def test_ledoit_wolf_empty_raises(self):
        """Empty returns should raise ValueError."""
        empty_df = pd.DataFrame()

        with pytest.raises(ValueError, match="empty returns"):
            ledoit_wolf_cov(empty_df)

    def test_ledoit_wolf_insufficient_data_raises(self):
        """Less than 2 observations should raise ValueError."""
        single_row = pd.DataFrame([[0.01, 0.02, 0.03]], columns=['A', 'B', 'C'])

        with pytest.raises(ValueError, match="at least 2 observations"):
            ledoit_wolf_cov(single_row)

    def test_ledoit_wolf_nan_raises(self):
        """NaN values in returns should raise ValueError."""
        dates = pd.bdate_range('2023-01-01', periods=100)
        data = np.random.normal(0, 0.02, (100, 3))
        data[50, 1] = np.nan  # Insert NaN

        returns = pd.DataFrame(data, index=dates, columns=['A', 'B', 'C'])

        with pytest.raises(ValueError, match="NaN values"):
            ledoit_wolf_cov(returns)


class TestEwmaCov:
    """Tests for ewma_cov function."""

    def test_ewma_shape(self, sample_returns):
        """Output should be (N, N) matrix."""
        cov = ewma_cov(sample_returns, lambd=0.94)

        n_assets = len(sample_returns.columns)
        assert cov.shape == (n_assets, n_assets)

    def test_ewma_symmetric(self, sample_returns):
        """Matrix should be symmetric."""
        cov = ewma_cov(sample_returns, lambd=0.94)

        assert_allclose(cov, cov.T, rtol=1e-10)

    def test_ewma_positive_diagonal(self, sample_returns):
        """All diagonal elements should be positive."""
        cov = ewma_cov(sample_returns, lambd=0.94)

        diagonal = np.diag(cov)
        assert np.all(diagonal > 0)

    def test_ewma_positive_semidefinite(self, sample_returns):
        """Matrix should be positive semi-definite."""
        cov = ewma_cov(sample_returns, lambd=0.94)

        eigenvalues = np.linalg.eigvals(cov)
        assert np.all(eigenvalues.real > -1e-8)

    def test_ewma_lambda_effect(self, sample_returns):
        """Higher lambda should give more weight to history (less reactive to recent data)."""
        # Create returns with a shock in the last observation
        returns_with_shock = sample_returns.copy()
        returns_with_shock.iloc[-1] = 0.10  # Large positive return

        # Lower lambda (more reactive)
        cov_low = ewma_cov(returns_with_shock, lambd=0.80)

        # Higher lambda (less reactive)
        cov_high = ewma_cov(returns_with_shock, lambd=0.98)

        # The lower lambda should show higher variance (more reactive to shock)
        var_low = np.diag(cov_low).mean()
        var_high = np.diag(cov_high).mean()

        # Lower lambda should result in higher variance due to recent shock
        assert var_low > var_high

    def test_ewma_invalid_lambda_raises(self, sample_returns):
        """Lambda outside (0, 1) should raise ValueError."""
        with pytest.raises(ValueError, match="Lambda must be between 0 and 1"):
            ewma_cov(sample_returns, lambd=1.5)

        with pytest.raises(ValueError, match="Lambda must be between 0 and 1"):
            ewma_cov(sample_returns, lambd=-0.1)

    def test_ewma_empty_raises(self):
        """Empty returns should raise ValueError."""
        empty_df = pd.DataFrame()

        with pytest.raises(ValueError, match="empty returns"):
            ewma_cov(empty_df)

    def test_ewma_single_asset(self):
        """Should handle single asset case."""
        dates = pd.bdate_range('2023-01-01', periods=100)
        returns = pd.DataFrame(
            np.random.normal(0, 0.02, 100),
            index=dates,
            columns=['A']
        )

        cov = ewma_cov(returns, lambd=0.94)

        assert cov.shape == (1, 1)
        assert cov[0, 0] > 0


class TestEstimateCovariance:
    """Tests for unified estimate_covariance interface."""

    def test_estimate_covariance_lw_method(self, sample_returns):
        """Method='lw' should call Ledoit-Wolf estimator."""
        cov = estimate_covariance(sample_returns, method='lw')

        assert cov.shape == (len(sample_returns.columns), len(sample_returns.columns))
        assert_allclose(cov, cov.T, rtol=1e-10)  # Symmetric

    def test_estimate_covariance_ewma_method(self, sample_returns):
        """Method='ewma' should call EWMA estimator."""
        cov = estimate_covariance(sample_returns, method='ewma', ewma_lambda=0.94)

        assert cov.shape == (len(sample_returns.columns), len(sample_returns.columns))
        assert_allclose(cov, cov.T, rtol=1e-10)  # Symmetric

    def test_estimate_covariance_case_insensitive(self, sample_returns):
        """Method names should be case-insensitive."""
        cov_lower = estimate_covariance(sample_returns, method='lw')
        cov_upper = estimate_covariance(sample_returns, method='LW')

        assert_allclose(cov_lower, cov_upper)

    def test_estimate_covariance_invalid_method_raises(self, sample_returns):
        """Invalid method name should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown covariance estimation method"):
            estimate_covariance(sample_returns, method='invalid')

    def test_estimate_covariance_both_methods_positive_definite(self, sample_returns):
        """Both methods should produce positive semi-definite matrices."""
        cov_lw = estimate_covariance(sample_returns, method='lw')
        cov_ewma = estimate_covariance(sample_returns, method='ewma')

        # Check eigenvalues
        eig_lw = np.linalg.eigvals(cov_lw)
        eig_ewma = np.linalg.eigvals(cov_ewma)

        assert np.all(eig_lw.real > -1e-8)
        assert np.all(eig_ewma.real > -1e-8)


class TestAnnualizeCov:
    """Tests for annualize_cov function."""

    def test_annualize_cov_multiplies_by_trading_days(self, sample_cov):
        """Annualized cov should equal daily cov * trading_days."""
        trading_days = 252
        annualized = annualize_cov(sample_cov, trading_days=trading_days)

        expected = sample_cov * trading_days
        assert_allclose(annualized, expected)

    def test_annualize_cov_preserves_shape(self, sample_cov):
        """Annualization should preserve matrix shape."""
        annualized = annualize_cov(sample_cov, trading_days=252)

        assert annualized.shape == sample_cov.shape

    def test_annualize_cov_preserves_symmetry(self, sample_cov):
        """Annualization should preserve symmetry."""
        annualized = annualize_cov(sample_cov, trading_days=252)

        assert_allclose(annualized, annualized.T, rtol=1e-10)

    def test_annualize_cov_custom_trading_days(self, sample_cov):
        """Should work with custom trading days."""
        annualized_365 = annualize_cov(sample_cov, trading_days=365)
        annualized_252 = annualize_cov(sample_cov, trading_days=252)

        # 365 day should be larger than 252 day
        assert annualized_365[0, 0] > annualized_252[0, 0]

    def test_annualize_cov_empty_raises(self):
        """Empty covariance matrix should raise ValueError."""
        empty = np.array([])

        with pytest.raises(ValueError, match="Cannot annualize empty"):
            annualize_cov(empty)

    def test_annualize_cov_non_square_raises(self):
        """Non-square matrix should raise ValueError."""
        non_square = np.array([[1, 2, 3], [4, 5, 6]])

        with pytest.raises(ValueError, match="must be square"):
            annualize_cov(non_square)

    def test_annualize_cov_negative_trading_days_raises(self):
        """Negative trading days should raise ValueError."""
        cov = np.eye(3)

        with pytest.raises(ValueError, match="must be positive"):
            annualize_cov(cov, trading_days=-1)
