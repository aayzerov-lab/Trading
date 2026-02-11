"""
Unit tests for metrics.py - Risk Metrics Module

Tests cover:
- Portfolio volatility calculation
- VaR and Expected Shortfall
- Risk contributions (MCR, CCR, percentage)
- Concentration metrics
- Risk summary and contributor builders
"""

import pytest
import numpy as np
import pandas as pd
from numpy.testing import assert_allclose

from shared.risk.metrics import (
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


class TestPortfolioVolatility:
    """Tests for portfolio_volatility function."""

    def test_portfolio_vol_positive(self, sample_weights, sample_cov):
        """Volatility should always be positive."""
        vol = portfolio_volatility(sample_weights, sample_cov, horizon_days=1)

        assert vol > 0

    def test_portfolio_vol_single_asset(self):
        """For single asset portfolio (w=[1]), vol should equal asset vol."""
        # Single asset
        cov = np.array([[0.0004]])  # Variance = 0.0004, vol = 0.02
        weights = np.array([1.0])

        vol = portfolio_volatility(weights, cov, horizon_days=1)

        expected_vol = np.sqrt(0.0004)
        assert_allclose(vol, expected_vol, rtol=1e-10)

    def test_portfolio_vol_horizon_scaling(self, sample_weights, sample_cov):
        """5-day vol should equal 1-day vol * sqrt(5)."""
        vol_1d = portfolio_volatility(sample_weights, sample_cov, horizon_days=1)
        vol_5d = portfolio_volatility(sample_weights, sample_cov, horizon_days=5)

        expected_5d = vol_1d * np.sqrt(5)
        assert_allclose(vol_5d, expected_5d, rtol=1e-10)

    def test_portfolio_vol_dimension_mismatch_raises(self):
        """Mismatched weights and covariance dimensions should raise ValueError."""
        weights = np.array([0.5, 0.5])
        cov = np.eye(3)  # 3x3 matrix

        with pytest.raises(ValueError, match="doesn't match covariance"):
            portfolio_volatility(weights, cov)

    def test_portfolio_vol_zero_horizon_raises(self, sample_weights, sample_cov):
        """Horizon < 1 should raise ValueError."""
        with pytest.raises(ValueError, match="Horizon must be"):
            portfolio_volatility(sample_weights, sample_cov, horizon_days=0)


class TestParametricVar:
    """Tests for parametric_var function."""

    def test_var_positive(self, sample_weights, sample_cov):
        """VaR should be positive (represents a loss amount)."""
        var = parametric_var(
            sample_weights, sample_cov,
            confidence=0.95, horizon_days=1, portfolio_value=100000
        )

        assert var > 0

    def test_var_greater_than_zero_for_high_confidence(self, sample_weights, sample_cov):
        """VaR should be positive for typical confidence levels."""
        var_95 = parametric_var(
            sample_weights, sample_cov,
            confidence=0.95, horizon_days=1, portfolio_value=100000
        )
        var_99 = parametric_var(
            sample_weights, sample_cov,
            confidence=0.99, horizon_days=1, portfolio_value=100000
        )

        assert var_95 > 0
        assert var_99 > var_95  # Higher confidence = larger VaR

    def test_var_scales_with_portfolio_value(self, sample_weights, sample_cov):
        """VaR should scale linearly with portfolio value."""
        var_100k = parametric_var(
            sample_weights, sample_cov,
            confidence=0.95, horizon_days=1, portfolio_value=100000
        )
        var_200k = parametric_var(
            sample_weights, sample_cov,
            confidence=0.95, horizon_days=1, portfolio_value=200000
        )

        assert_allclose(var_200k, 2 * var_100k, rtol=1e-10)

    def test_var_scales_with_horizon(self, sample_weights, sample_cov):
        """VaR should scale with sqrt(horizon)."""
        var_1d = parametric_var(
            sample_weights, sample_cov,
            confidence=0.95, horizon_days=1, portfolio_value=100000
        )
        var_5d = parametric_var(
            sample_weights, sample_cov,
            confidence=0.95, horizon_days=5, portfolio_value=100000
        )

        expected_5d = var_1d * np.sqrt(5)
        assert_allclose(var_5d, expected_5d, rtol=1e-10)

    def test_var_invalid_confidence_raises(self, sample_weights, sample_cov):
        """Confidence outside (0, 1) should raise ValueError."""
        with pytest.raises(ValueError, match="Confidence must be between"):
            parametric_var(sample_weights, sample_cov, confidence=1.5, portfolio_value=100000)

    def test_var_zero_portfolio_value_raises(self, sample_weights, sample_cov):
        """Zero or negative portfolio value should raise ValueError."""
        with pytest.raises(ValueError, match="Portfolio value must be positive"):
            parametric_var(sample_weights, sample_cov, confidence=0.95, portfolio_value=0)


class TestExpectedShortfall:
    """Tests for expected_shortfall function."""

    def test_es_positive(self, sample_weights, sample_cov):
        """ES should be positive (represents a loss amount)."""
        es = expected_shortfall(
            sample_weights, sample_cov,
            confidence=0.95, horizon_days=1, portfolio_value=100000
        )

        assert es > 0

    def test_es_greater_than_var(self, sample_weights, sample_cov):
        """ES should always be >= VaR (it's the conditional expectation beyond VaR)."""
        var = parametric_var(
            sample_weights, sample_cov,
            confidence=0.95, horizon_days=1, portfolio_value=100000
        )
        es = expected_shortfall(
            sample_weights, sample_cov,
            confidence=0.95, horizon_days=1, portfolio_value=100000
        )

        assert es >= var

    def test_es_scales_with_portfolio_value(self, sample_weights, sample_cov):
        """ES should scale linearly with portfolio value."""
        es_100k = expected_shortfall(
            sample_weights, sample_cov,
            confidence=0.95, horizon_days=1, portfolio_value=100000
        )
        es_200k = expected_shortfall(
            sample_weights, sample_cov,
            confidence=0.95, horizon_days=1, portfolio_value=200000
        )

        assert_allclose(es_200k, 2 * es_100k, rtol=1e-10)

    def test_es_invalid_confidence_raises(self, sample_weights, sample_cov):
        """Invalid confidence should raise ValueError."""
        with pytest.raises(ValueError, match="Confidence must be between"):
            expected_shortfall(sample_weights, sample_cov, confidence=2.0, portfolio_value=100000)


class TestMarginalContributionToRisk:
    """Tests for marginal_contribution_to_risk function."""

    def test_mcr_shape(self, sample_weights, sample_cov):
        """MCR should have length N."""
        mcr = marginal_contribution_to_risk(sample_weights, sample_cov)

        assert len(mcr) == len(sample_weights)

    def test_mcr_dimension_mismatch_raises(self):
        """Mismatched dimensions should raise ValueError."""
        weights = np.array([0.5, 0.5])
        cov = np.eye(3)

        with pytest.raises(ValueError, match="doesn't match covariance"):
            marginal_contribution_to_risk(weights, cov)

    def test_mcr_zero_vol_returns_zeros(self):
        """Zero portfolio vol should return zero MCR array."""
        # Create weights that result in zero vol (e.g., zero covariance)
        weights = np.array([0.5, 0.5])
        cov = np.zeros((2, 2))

        mcr = marginal_contribution_to_risk(weights, cov)

        assert np.all(mcr == 0)


class TestComponentContributionToRisk:
    """Tests for component_contribution_to_risk function."""

    def test_ccr_sum_equals_portfolio_vol(self, sample_weights, sample_cov):
        """Sum of CCR should equal total portfolio volatility."""
        ccr = component_contribution_to_risk(sample_weights, sample_cov)
        port_vol = portfolio_volatility(sample_weights, sample_cov, horizon_days=1)

        assert_allclose(np.sum(ccr), port_vol, rtol=1e-8)

    def test_ccr_shape(self, sample_weights, sample_cov):
        """CCR should have length N."""
        ccr = component_contribution_to_risk(sample_weights, sample_cov)

        assert len(ccr) == len(sample_weights)


class TestPctContributionToVariance:
    """Tests for pct_contribution_to_variance function."""

    def test_pct_contribution_sums_to_100(self, sample_weights, sample_cov):
        """Sum of percentage contributions should equal 100%."""
        pct_var = pct_contribution_to_variance(sample_weights, sample_cov)

        assert_allclose(np.sum(pct_var), 100.0, rtol=1e-8)

    def test_pct_contribution_shape(self, sample_weights, sample_cov):
        """Output should have length N."""
        pct_var = pct_contribution_to_variance(sample_weights, sample_cov)

        assert len(pct_var) == len(sample_weights)

    def test_pct_contribution_zero_var_returns_zeros(self):
        """Zero portfolio variance should return zeros."""
        weights = np.array([0.5, 0.5])
        cov = np.zeros((2, 2))

        pct_var = pct_contribution_to_variance(weights, cov)

        assert np.all(pct_var == 0)


class TestConcentrationMetrics:
    """Tests for concentration_metrics function."""

    def test_concentration_top5(self):
        """Should correctly identify top 5 positions."""
        weights = np.array([0.30, 0.25, 0.20, 0.15, 0.10])
        symbols = ['A', 'B', 'C', 'D', 'E']

        metrics = concentration_metrics(weights, symbols)

        assert metrics['top_5_names'] == ['A', 'B', 'C', 'D', 'E']
        assert_allclose(metrics['top_5_pct'], 100.0)  # All 5 are the top 5

    def test_concentration_top5_with_more_positions(self):
        """Should select top 5 from larger portfolio."""
        weights = np.array([0.05, 0.10, 0.15, 0.20, 0.25, 0.05, 0.10, 0.10])
        symbols = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']

        metrics = concentration_metrics(weights, symbols)

        # Top 5 should be E, D, C, B, G/H (two with 0.10)
        assert len(metrics['top_5_names']) == 5
        assert 'E' in metrics['top_5_names']  # Largest
        assert 'D' in metrics['top_5_names']

    def test_hhi_bounds(self):
        """HHI should be between 0 and 10000 (conventional scale)."""
        weights = np.array([0.30, 0.25, 0.20, 0.15, 0.10])
        symbols = ['A', 'B', 'C', 'D', 'E']

        metrics = concentration_metrics(weights, symbols)

        assert 0 <= metrics['hhi'] <= 10000

    def test_hhi_single_position(self):
        """Single position should have HHI = 10000."""
        weights = np.array([1.0])
        symbols = ['A']

        metrics = concentration_metrics(weights, symbols)

        assert_allclose(metrics['hhi'], 10000.0)

    def test_hhi_equal_weights(self):
        """Equal weights should have HHI = 10000/N."""
        n = 10
        weights = np.ones(n) / n
        symbols = [f'S{i}' for i in range(n)]

        metrics = concentration_metrics(weights, symbols)

        expected_hhi = 10000.0 / n
        assert_allclose(metrics['hhi'], expected_hhi, rtol=1e-8)

    def test_concentration_uses_absolute_weights(self):
        """Should use absolute values (long and short positions)."""
        weights = np.array([0.30, -0.25, 0.20, -0.15, 0.10])  # Mixed long/short
        symbols = ['A', 'B', 'C', 'D', 'E']

        metrics = concentration_metrics(weights, symbols)

        # Top position by absolute value is A (0.30)
        assert metrics['top_5_names'][0] == 'A'

    def test_concentration_zero_gross_exposure(self):
        """Zero gross exposure should return zero metrics."""
        weights = np.array([0.0, 0.0, 0.0])
        symbols = ['A', 'B', 'C']

        metrics = concentration_metrics(weights, symbols)

        assert metrics['top_5_pct'] == 0.0
        assert metrics['hhi'] == 0.0
        assert metrics['top_5_names'] == []


class TestBuildRiskSummary:
    """Tests for build_risk_summary function."""

    def test_build_risk_summary_keys(self, sample_weights, sample_cov, sample_symbols):
        """Summary should contain all expected keys."""
        summary = build_risk_summary(
            sample_weights, sample_cov, sample_symbols, portfolio_value=100000
        )

        expected_keys = {
            'vol_1d', 'vol_1d_pct', 'vol_5d', 'vol_5d_pct',
            'var_95_1d', 'var_95_1d_pct', 'es_95_1d', 'es_95_1d_pct',
            'var_95_5d', 'es_95_5d',
            'top_5_concentration_pct', 'hhi', 'top_5_names',
            'num_positions', 'portfolio_value'
        }

        assert set(summary.keys()) == expected_keys

    def test_build_risk_summary_values_reasonable(self, sample_weights, sample_cov, sample_symbols):
        """Summary values should be in reasonable ranges."""
        summary = build_risk_summary(
            sample_weights, sample_cov, sample_symbols, portfolio_value=100000
        )

        # Vol should be positive
        assert summary['vol_1d'] > 0
        assert summary['vol_1d_pct'] > 0

        # VaR and ES should be positive
        assert summary['var_95_1d'] > 0
        assert summary['es_95_1d'] > 0

        # ES should be >= VaR
        assert summary['es_95_1d'] >= summary['var_95_1d']

        # 5-day metrics should be > 1-day metrics
        assert summary['vol_5d'] > summary['vol_1d']
        assert summary['var_95_5d'] > summary['var_95_1d']

        # Concentration should be in [0, 100]
        assert 0 <= summary['top_5_concentration_pct'] <= 100
        assert 0 <= summary['hhi'] <= 10000

        # Position count
        assert summary['num_positions'] == 5

    def test_build_risk_summary_dimension_mismatch_raises(self):
        """Mismatched dimensions should raise ValueError."""
        weights = np.array([0.5, 0.5])
        cov = np.eye(3)
        symbols = ['A', 'B']

        with pytest.raises(ValueError):
            build_risk_summary(weights, cov, symbols, portfolio_value=100000)


class TestBuildRiskContributors:
    """Tests for build_risk_contributors function."""

    def test_build_risk_contributors_sorted(self, sample_weights, sample_cov, sample_symbols):
        """Contributors should be sorted by |CCR| descending."""
        contributors = build_risk_contributors(
            sample_weights, sample_cov, sample_symbols, portfolio_value=100000
        )

        # Check sorted by absolute CCR
        ccr_values = [abs(c['ccr']) for c in contributors]
        assert ccr_values == sorted(ccr_values, reverse=True)

    def test_build_risk_contributors_length(self, sample_weights, sample_cov, sample_symbols):
        """Should return one entry per non-zero position."""
        contributors = build_risk_contributors(
            sample_weights, sample_cov, sample_symbols, portfolio_value=100000
        )

        # All weights are non-zero
        assert len(contributors) == 5

    def test_build_risk_contributors_keys(self, sample_weights, sample_cov, sample_symbols):
        """Each contributor dict should have expected keys."""
        contributors = build_risk_contributors(
            sample_weights, sample_cov, sample_symbols, portfolio_value=100000
        )

        expected_keys = {'symbol', 'weight_pct', 'mcr', 'ccr', 'ccr_pct', 'standalone_vol_ann'}

        for contributor in contributors:
            assert set(contributor.keys()) == expected_keys

    def test_build_risk_contributors_skips_zero_positions(self):
        """Zero positions should be excluded from output."""
        weights = np.array([0.50, 0.30, 0.00, 0.20, 0.00])  # Two zeros
        symbols = ['A', 'B', 'C', 'D', 'E']
        cov = np.eye(5) * 0.0004

        contributors = build_risk_contributors(
            weights, cov, symbols, portfolio_value=100000
        )

        # Should only have 3 contributors (non-zero positions)
        assert len(contributors) == 3
        assert 'C' not in [c['symbol'] for c in contributors]
        assert 'E' not in [c['symbol'] for c in contributors]

    def test_build_risk_contributors_ccr_pct_sums_to_100(self, sample_weights, sample_cov, sample_symbols):
        """Sum of CCR percentages should equal approximately 100%."""
        contributors = build_risk_contributors(
            sample_weights, sample_cov, sample_symbols, portfolio_value=100000
        )

        ccr_pct_sum = sum(c['ccr_pct'] for c in contributors)

        # Should sum to approximately 100% (allowing for rounding)
        assert_allclose(ccr_pct_sum, 100.0, atol=0.1)
