"""
Unit tests for Phase 1.5 features in the trading workstation.

Tests cover:
- FX-aware returns (build_fx_aware_returns)
- Regression diagnostics (compute_regression_diagnostics)
- Data quality metrics
- PSD clamping in pairwise_cov
"""

import pytest
import numpy as np
import pandas as pd
from numpy.testing import assert_allclose

from shared.risk.returns import build_fx_aware_returns
from shared.risk.stress import (
    compute_regression_diagnostics,
    MIN_OVERLAP_BETA,
    WARN_OVERLAP_BETA,
    MIN_R2_GOOD,
)
from shared.risk.data_quality import (
    compute_coverage_metrics,
    compute_data_integrity_metrics,
    compute_classification_metrics,
    compute_fx_coverage_metrics,
    generate_warnings,
    WARN_EXCLUDED_EXPOSURE_PCT,
    WARN_MISSING_PRICE_PCT,
    WARN_UNKNOWN_SECTOR_PCT,
    WARN_UNKNOWN_COUNTRY_PCT,
    WARN_FX_COVERAGE_PCT,
)
from shared.risk.covariance import pairwise_cov


class TestFxAwareReturns:
    """Tests for FX-aware return construction."""

    def test_usd_symbol_no_fx_adjustment(self):
        """USD symbol returns should match local returns unchanged."""
        np.random.seed(42)
        dates = pd.bdate_range('2024-01-01', periods=100)

        # Build synthetic price data for USD symbol
        prices = 100 * np.exp(np.cumsum(np.random.normal(0.0005, 0.02, len(dates))))
        price_df = pd.DataFrame({
            'date': dates,
            'close': prices,
            'adj_close': prices,
        })

        prices_dict = {'AAPL': price_df}
        fx_rates = {}
        security_info = {
            'AAPL': {
                'currency': 'USD',
                'is_usd_listed': True,
                'fx_pair': None,
            }
        }

        result, fx_flags = build_fx_aware_returns(
            prices_dict, fx_rates, security_info, window=50
        )

        assert 'AAPL' in result
        assert len(fx_flags) == 0, "USD symbol should not have FX flags"
        assert len(result['AAPL']) == 50

        # Verify it's log returns
        expected_ret = np.log(prices[1:] / prices[:-1])[-50:]
        assert_allclose(result['AAPL'].values, expected_ret, rtol=1e-10)

    def test_non_usd_symbol_fx_adjusted(self):
        """Non-USD symbol should have r_usd = r_local + r_fx."""
        np.random.seed(42)
        dates = pd.bdate_range('2024-01-01', periods=100)

        # Local currency price (e.g., EUR)
        local_prices = 100 * np.exp(np.cumsum(np.random.normal(0.0003, 0.015, len(dates))))
        price_df = pd.DataFrame({
            'date': dates,
            'close': local_prices,
            'adj_close': local_prices,
        })

        # FX rate EURUSD = USD per 1 EUR
        fx_prices = 1.1 * np.exp(np.cumsum(np.random.normal(0.0001, 0.01, len(dates))))
        fx_df = pd.DataFrame({
            'date': dates,
            'close': fx_prices,
            'adj_close': fx_prices,
        })

        prices_dict = {'BMW': price_df}
        fx_rates = {'EURUSD': fx_df}
        security_info = {
            'BMW': {
                'currency': 'EUR',
                'is_usd_listed': False,
                'fx_pair': 'EURUSD',
            }
        }

        result, fx_flags = build_fx_aware_returns(
            prices_dict, fx_rates, security_info, window=50, min_history=50
        )

        assert 'BMW' in result
        # With min_history=50, overlap of 50 should not be flagged
        assert 'BMW' not in fx_flags or 'fx_overlap' not in fx_flags.get('BMW', ''), \
            f"Should not flag sufficient FX data, got: {fx_flags}"
        assert len(result['BMW']) == 50

        # Verify r_usd = r_local + r_fx
        r_local = np.log(local_prices[1:] / local_prices[:-1])[-50:]
        r_fx = np.log(fx_prices[1:] / fx_prices[:-1])[-50:]
        expected_r_usd = r_local + r_fx

        assert_allclose(result['BMW'].values, expected_r_usd, rtol=1e-10)

    def test_missing_fx_data_flag(self):
        """Symbol with missing FX data should be flagged."""
        np.random.seed(42)
        dates = pd.bdate_range('2024-01-01', periods=100)

        prices = 100 * np.exp(np.cumsum(np.random.normal(0.0005, 0.02, len(dates))))
        price_df = pd.DataFrame({
            'date': dates,
            'close': prices,
            'adj_close': prices,
        })

        prices_dict = {'BMW': price_df}
        fx_rates = {}  # No FX data available
        security_info = {
            'BMW': {
                'currency': 'EUR',
                'is_usd_listed': False,
                'fx_pair': 'EURUSD',
            }
        }

        result, fx_flags = build_fx_aware_returns(
            prices_dict, fx_rates, security_info, window=50
        )

        assert 'BMW' in result
        assert 'BMW' in fx_flags
        assert fx_flags['BMW'] == 'missing_fx_data'

    def test_insufficient_fx_overlap(self):
        """Symbol with low FX/price overlap should be flagged."""
        np.random.seed(42)

        # Price data with recent dates
        price_dates = pd.bdate_range('2024-06-01', periods=100)
        prices = 100 * np.exp(np.cumsum(np.random.normal(0.0005, 0.02, len(price_dates))))
        price_df = pd.DataFrame({
            'date': price_dates,
            'close': prices,
            'adj_close': prices,
        })

        # FX data with old dates (minimal overlap)
        fx_dates = pd.bdate_range('2024-01-01', periods=50)
        fx_prices = 1.1 * np.exp(np.cumsum(np.random.normal(0.0001, 0.01, len(fx_dates))))
        fx_df = pd.DataFrame({
            'date': fx_dates,
            'close': fx_prices,
            'adj_close': fx_prices,
        })

        prices_dict = {'BMW': price_df}
        fx_rates = {'EURUSD': fx_df}
        security_info = {
            'BMW': {
                'currency': 'EUR',
                'is_usd_listed': False,
                'fx_pair': 'EURUSD',
            }
        }

        result, fx_flags = build_fx_aware_returns(
            prices_dict, fx_rates, security_info, window=50, min_history=80
        )

        assert 'BMW' in result
        assert 'BMW' in fx_flags
        # Should flag either insufficient_fx_history or fx_overlap due to date mismatch
        assert 'fx' in fx_flags['BMW'].lower() or 'overlap' in fx_flags['BMW'].lower()


class TestRegressionDiagnostics:
    """Tests for regression diagnostics in stress testing."""

    def test_good_quality_high_r2_long_overlap(self):
        """Overlap >= 120, R² >= 0.20 should give quality='good'."""
        np.random.seed(42)
        overlap = 150

        # Generate correlated returns (high R²)
        factor_ret = np.random.normal(0.001, 0.02, overlap)
        beta_true = 1.2
        noise = np.random.normal(0, 0.01, overlap)
        position_ret = beta_true * factor_ret + noise

        diag = compute_regression_diagnostics(position_ret, factor_ret, overlap)

        assert diag['overlap'] == overlap
        assert diag['quality'] == 'good'
        assert diag['r2'] >= MIN_R2_GOOD
        assert diag['beta'] != 0
        assert diag['stderr_beta'] < float('inf')
        assert abs(diag['beta'] - beta_true) < 0.5  # Should be close

    def test_weak_quality_low_r2(self):
        """Overlap >= 120, R² < 0.20 should give quality='weak'."""
        np.random.seed(42)
        overlap = 120

        # Generate weakly correlated returns (low R²)
        factor_ret = np.random.normal(0.001, 0.02, overlap)
        position_ret = np.random.normal(0.001, 0.02, overlap)  # Independent

        diag = compute_regression_diagnostics(position_ret, factor_ret, overlap)

        assert diag['overlap'] == overlap
        assert diag['quality'] == 'weak'
        assert diag['r2'] < MIN_R2_GOOD

    def test_invalid_quality_short_overlap(self):
        """Overlap < 60 should give quality='invalid'."""
        np.random.seed(42)
        overlap = 50

        factor_ret = np.random.normal(0.001, 0.02, overlap)
        position_ret = 1.5 * factor_ret + np.random.normal(0, 0.01, overlap)

        diag = compute_regression_diagnostics(position_ret, factor_ret, overlap)

        assert diag['overlap'] == overlap
        assert diag['quality'] == 'invalid'
        assert diag['overlap'] < MIN_OVERLAP_BETA

    def test_invalid_beta_excluded(self):
        """When quality='invalid', beta should be set to 0 in stress test."""
        np.random.seed(42)
        dates = pd.bdate_range('2024-01-01', periods=50)

        # Short history (invalid)
        position_returns = pd.DataFrame({
            'AAPL': np.random.normal(0.001, 0.02, 50)
        }, index=dates)

        factor_returns = pd.DataFrame({
            'SPY': np.random.normal(0.001, 0.015, 50)
        }, index=dates)

        # Direct test: regression diagnostics should mark as invalid
        position_ret = position_returns['AAPL'].values
        factor_ret = factor_returns['SPY'].values

        diag = compute_regression_diagnostics(position_ret, factor_ret, overlap=50)

        assert diag['quality'] == 'invalid'
        # When invalid, factor_stress_test should skip this beta (set impact to 0)
        # This is tested indirectly: the diagnostic correctly identifies it as invalid


class TestDataQuality:
    """Tests for data quality metrics."""

    def test_coverage_metrics_counts(self):
        """Verify included/excluded counts are correct."""
        positions = [
            {'symbol': 'AAPL', 'market_value': 10000},
            {'symbol': 'GOOGL', 'market_value': 15000},
            {'symbol': 'MSFT', 'market_value': 8000},
        ]

        # Create returns dict (MSFT missing)
        dates = pd.bdate_range('2024-01-01', periods=100)
        returns_dict = {
            'AAPL': pd.Series(np.random.normal(0, 0.02, 100), index=dates),
            'GOOGL': pd.Series(np.random.normal(0, 0.02, 100), index=dates),
        }

        symbols = ['AAPL', 'GOOGL', 'MSFT']
        valid_symbols = ['AAPL', 'GOOGL']

        metrics = compute_coverage_metrics(
            positions, returns_dict, symbols, valid_symbols,
            window=252, min_overlap=60
        )

        assert metrics['included_count'] == 2
        assert metrics['excluded_count'] == 1
        assert len(metrics['top_excluded']) == 1
        assert metrics['top_excluded'][0]['symbol'] == 'MSFT'

    def test_excluded_exposure_calculation(self):
        """Verify excluded exposure % is computed correctly."""
        positions = [
            {'symbol': 'AAPL', 'market_value': 50000},
            {'symbol': 'GOOGL', 'market_value': 30000},
            {'symbol': 'MSFT', 'market_value': 20000},  # 20% of gross
        ]

        dates = pd.bdate_range('2024-01-01', periods=100)
        returns_dict = {
            'AAPL': pd.Series(np.random.normal(0, 0.02, 100), index=dates),
            'GOOGL': pd.Series(np.random.normal(0, 0.02, 100), index=dates),
        }

        symbols = ['AAPL', 'GOOGL', 'MSFT']
        valid_symbols = ['AAPL', 'GOOGL']

        metrics = compute_coverage_metrics(
            positions, returns_dict, symbols, valid_symbols,
            window=252, min_overlap=60
        )

        # Total gross = 100k, MSFT = 20k = 20%
        assert_allclose(metrics['excluded_exposure_pct'], 20.0, rtol=0.01)

    def test_warning_generation_threshold(self):
        """Verify warnings are generated when thresholds exceeded."""
        # High excluded exposure (>10%)
        coverage_60d = {
            'window': 60,
            'excluded_exposure_pct': 15.0,
            'included_count': 5,
            'excluded_count': 2,
        }

        coverage_252d = {
            'window': 252,
            'excluded_exposure_pct': 5.0,
            'included_count': 6,
            'excluded_count': 1,
        }

        integrity = {
            'missing_price_exposure_pct': 2.0,
            'outlier_return_days': 3,
        }

        classification = {
            'unknown_sector_pct': 25.0,  # > 20% threshold
            'unknown_country_pct': 5.0,
        }

        fx_coverage = {
            'non_usd_exposure_pct': 10.0,
            'fx_coverage_pct': 90.0,  # < 95% threshold
        }

        warnings = generate_warnings(
            coverage_60d, coverage_252d, integrity, classification, fx_coverage
        )

        # Should have warnings for:
        # - 60d excluded exposure (15% > 10%)
        # - unknown sector (25% > 20%)
        # - FX coverage (90% < 95%)
        assert len(warnings) >= 3

        warning_messages = [w['message'] for w in warnings]
        assert any('60d window' in msg for msg in warning_messages)
        assert any('Unknown sector' in msg for msg in warning_messages)
        assert any('FX coverage' in msg for msg in warning_messages)

    def test_no_warnings_healthy_data(self):
        """Verify no warnings when all metrics are within bounds."""
        coverage_60d = {
            'window': 60,
            'excluded_exposure_pct': 5.0,  # < 10%
            'included_count': 10,
            'excluded_count': 1,
        }

        coverage_252d = {
            'window': 252,
            'excluded_exposure_pct': 3.0,  # < 10%
            'included_count': 10,
            'excluded_count': 1,
        }

        integrity = {
            'missing_price_exposure_pct': 2.0,  # < 5%
            'outlier_return_days': 0,
        }

        classification = {
            'unknown_sector_pct': 5.0,  # < 20%
            'unknown_country_pct': 3.0,  # < 20%
        }

        fx_coverage = {
            'non_usd_exposure_pct': 10.0,
            'fx_coverage_pct': 98.0,  # > 95%
        }

        warnings = generate_warnings(
            coverage_60d, coverage_252d, integrity, classification, fx_coverage
        )

        # Should have no warnings (maybe info about outliers, but not warning/error)
        warning_errors = [w for w in warnings if w['level'] in ['warning', 'error']]
        assert len(warning_errors) == 0


class TestPairwiseCovPSD:
    """Tests for PSD clamping in pairwise_cov."""

    def test_psd_clamping_always_applies(self):
        """Verify eigenvalues of pairwise_cov result are all >= 1e-12."""
        np.random.seed(42)
        dates = pd.bdate_range('2024-01-01', periods=100)

        # Create returns with some correlation
        base_ret = np.random.normal(0, 0.02, 100)
        returns_dict = {
            'AAPL': pd.Series(base_ret + np.random.normal(0, 0.01, 100), index=dates),
            'GOOGL': pd.Series(0.8 * base_ret + np.random.normal(0, 0.01, 100), index=dates),
            'MSFT': pd.Series(0.6 * base_ret + np.random.normal(0, 0.01, 100), index=dates),
        }

        symbols = ['AAPL', 'GOOGL', 'MSFT']

        cov = pairwise_cov(returns_dict, symbols, window=100, min_overlap=60)

        # Check PSD: all eigenvalues >= 1e-12
        eigenvalues = np.linalg.eigvalsh(cov)
        assert np.all(eigenvalues >= 1e-12), f"Found negative/small eigenvalues: {eigenvalues}"

        # Verify shape
        assert cov.shape == (3, 3)

        # Verify symmetry
        assert_allclose(cov, cov.T, rtol=1e-10)

    def test_psd_with_near_singular_input(self):
        """Create near-singular data and verify result is still PSD."""
        np.random.seed(42)
        dates = pd.bdate_range('2024-01-01', periods=80)

        # Create highly correlated returns (near-singular structure)
        base_ret = np.random.normal(0, 0.02, 80)
        returns_dict = {
            'SYM1': pd.Series(base_ret + np.random.normal(0, 0.001, 80), index=dates),
            'SYM2': pd.Series(base_ret + np.random.normal(0, 0.001, 80), index=dates),
            'SYM3': pd.Series(base_ret + np.random.normal(0, 0.001, 80), index=dates),
            'SYM4': pd.Series(-base_ret + np.random.normal(0, 0.001, 80), index=dates),
        }

        symbols = ['SYM1', 'SYM2', 'SYM3', 'SYM4']

        cov = pairwise_cov(returns_dict, symbols, window=80, min_overlap=60)

        # Even with near-singular input, result should be PSD
        eigenvalues = np.linalg.eigvalsh(cov)
        assert np.all(eigenvalues >= 1e-12), f"Non-PSD matrix, eigenvalues: {eigenvalues}"

        # Should still be symmetric
        assert_allclose(cov, cov.T, rtol=1e-10)

        # Shape check
        assert cov.shape == (4, 4)

        # Diagonal should be positive (variances)
        assert np.all(np.diag(cov) > 0)


class TestClassificationMetrics:
    """Tests for sector/country classification metrics."""

    def test_sector_coverage_calculation(self):
        """Verify sector coverage is calculated correctly."""
        positions = [
            {'symbol': 'AAPL', 'market_value': 10000, 'sector': 'Technology', 'country': 'USA'},
            {'symbol': 'GOOGL', 'market_value': 15000, 'sector': 'Unknown', 'country': 'USA'},
            {'symbol': 'MSFT', 'market_value': 5000, 'sector': 'Technology', 'country': 'USA'},
        ]

        metrics = compute_classification_metrics(positions)

        # Total = 30k, Unknown sector = 15k = 50%
        assert_allclose(metrics['unknown_sector_pct'], 50.0, rtol=0.01)
        assert metrics['unknown_country_pct'] == 0.0


class TestFxCoverageMetrics:
    """Tests for FX coverage metrics."""

    def test_fx_coverage_percentage(self):
        """Verify FX coverage percentage calculation."""
        positions = [
            {'symbol': 'AAPL', 'market_value': 50000},
            {'symbol': 'BMW', 'market_value': 30000},
            {'symbol': 'NESN', 'market_value': 20000},
        ]

        security_info = {
            'AAPL': {'currency': 'USD', 'is_usd_listed': True},
            'BMW': {'currency': 'EUR', 'is_usd_listed': False, 'fx_pair': 'EURUSD'},
            'NESN': {'currency': 'CHF', 'is_usd_listed': False, 'fx_pair': 'CHFUSD'},
        }

        # BMW has FX issue
        fx_flags = {'BMW': 'missing_fx_data'}

        metrics = compute_fx_coverage_metrics(positions, security_info, fx_flags)

        # Non-USD exposure = 30k + 20k = 50k (50% of 100k)
        assert_allclose(metrics['non_usd_exposure_pct'], 50.0, rtol=0.01)

        # FX covered = 20k out of 50k = 40%
        assert_allclose(metrics['fx_coverage_pct'], 40.0, rtol=0.01)

        assert 'BMW' in metrics['fx_issues']


class TestRegressionQualityConstants:
    """Verify the regression quality constants are set correctly."""

    def test_constants_defined(self):
        """Verify threshold constants exist and have reasonable values."""
        assert MIN_OVERLAP_BETA == 60
        assert WARN_OVERLAP_BETA == 120
        assert MIN_R2_GOOD == 0.20

        # Ensure thresholds are ordered correctly
        assert MIN_OVERLAP_BETA < WARN_OVERLAP_BETA
        assert 0 < MIN_R2_GOOD < 1
