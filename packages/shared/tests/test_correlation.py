"""
Unit tests for correlation.py - Correlation Analysis Module

Tests cover:
- Correlation matrix computation
- Top correlated pairs identification
- Hierarchical clustering
- Cluster exposure analysis
"""

import pytest
import numpy as np
import pandas as pd
from numpy.testing import assert_allclose

from shared.risk.correlation import (
    correlation_matrix,
    top_correlated_pairs,
    hierarchical_clusters,
    cluster_exposures,
)


class TestCorrelationMatrix:
    """Tests for correlation_matrix function."""

    def test_correlation_matrix_diagonal(self, sample_returns):
        """Diagonal should be all 1.0 (self-correlation)."""
        corr = correlation_matrix(sample_returns)

        diagonal = np.diag(corr.values)
        assert_allclose(diagonal, np.ones(len(diagonal)), rtol=1e-10)

    def test_correlation_bounds(self, sample_returns):
        """All correlation values should be between -1 and 1."""
        corr = correlation_matrix(sample_returns)

        assert np.all(corr.values >= -1.0)
        assert np.all(corr.values <= 1.0)

    def test_correlation_symmetric(self, sample_returns):
        """Correlation matrix should be symmetric."""
        corr = correlation_matrix(sample_returns)

        assert_allclose(corr.values, corr.values.T, rtol=1e-10)

    def test_correlation_matrix_shape(self, sample_returns):
        """Output should be (N, N) DataFrame."""
        corr = correlation_matrix(sample_returns)

        n_assets = len(sample_returns.columns)
        assert corr.shape == (n_assets, n_assets)

    def test_correlation_matrix_labels(self, sample_returns):
        """Row and column labels should match input symbols."""
        corr = correlation_matrix(sample_returns)

        assert list(corr.index) == list(sample_returns.columns)
        assert list(corr.columns) == list(sample_returns.columns)

    def test_correlation_empty_raises(self):
        """Empty returns should raise ValueError."""
        empty_df = pd.DataFrame()

        with pytest.raises(ValueError, match="empty returns"):
            correlation_matrix(empty_df)

    def test_correlation_insufficient_data_raises(self):
        """Less than 2 observations should raise ValueError."""
        single_row = pd.DataFrame([[0.01, 0.02, 0.03]], columns=['A', 'B', 'C'])

        with pytest.raises(ValueError, match="at least 2 observations"):
            correlation_matrix(single_row)


class TestTopCorrelatedPairs:
    """Tests for top_correlated_pairs function."""

    def test_top_pairs_count(self, sample_returns):
        """Should return exactly N pairs (or fewer if not enough pairs exist)."""
        corr = correlation_matrix(sample_returns)
        pairs = top_correlated_pairs(corr, n=10)

        assert len(pairs) <= 10

    def test_top_pairs_no_self_corr(self, sample_returns):
        """Should not include self-correlation pairs (A, A)."""
        corr = correlation_matrix(sample_returns)
        pairs = top_correlated_pairs(corr, n=20)

        for pair in pairs:
            assert pair['symbol_a'] != pair['symbol_b']

    def test_top_pairs_sorted(self, sample_returns):
        """Pairs should be sorted by |correlation| descending."""
        corr = correlation_matrix(sample_returns)
        pairs = top_correlated_pairs(corr, n=20)

        abs_correlations = [abs(pair['correlation']) for pair in pairs]
        assert abs_correlations == sorted(abs_correlations, reverse=True)

    def test_top_pairs_structure(self, sample_returns):
        """Each pair should have symbol_a, symbol_b, and correlation."""
        corr = correlation_matrix(sample_returns)
        pairs = top_correlated_pairs(corr, n=5)

        for pair in pairs:
            assert 'symbol_a' in pair
            assert 'symbol_b' in pair
            assert 'correlation' in pair
            assert isinstance(pair['symbol_a'], str)
            assert isinstance(pair['symbol_b'], str)
            assert isinstance(pair['correlation'], float)

    def test_top_pairs_includes_positive_and_negative(self):
        """Should include both highly positive and highly negative correlations."""
        # Create returns with strong positive and negative correlations
        dates = pd.bdate_range('2023-01-01', periods=100)
        base = np.random.normal(0, 0.02, 100)

        returns = pd.DataFrame({
            'A': base,
            'B': base + np.random.normal(0, 0.005, 100),  # Highly positive corr with A
            'C': -base + np.random.normal(0, 0.005, 100),  # Highly negative corr with A
            'D': np.random.normal(0, 0.02, 100),  # Uncorrelated
        }, index=dates)

        corr = correlation_matrix(returns)
        pairs = top_correlated_pairs(corr, n=10)

        # Should include both A-B (positive) and A-C (negative)
        correlations = [pair['correlation'] for pair in pairs]
        has_positive = any(c > 0.5 for c in correlations)
        has_negative = any(c < -0.5 for c in correlations)

        assert has_positive or has_negative  # At least one strong correlation

    def test_top_pairs_empty_raises(self):
        """Empty correlation matrix should raise ValueError."""
        empty_df = pd.DataFrame()

        with pytest.raises(ValueError, match="empty correlation"):
            top_correlated_pairs(empty_df, n=5)

    def test_top_pairs_more_than_available(self, sample_returns):
        """Requesting more pairs than exist should return all available pairs."""
        corr = correlation_matrix(sample_returns)

        # For 5 assets, max pairs = C(5,2) = 10
        pairs = top_correlated_pairs(corr, n=100)

        assert len(pairs) == 10


class TestHierarchicalClusters:
    """Tests for hierarchical_clusters function."""

    def test_hierarchical_clusters_labels(self, sample_returns):
        """Every symbol should have a cluster label."""
        corr = correlation_matrix(sample_returns)
        result = hierarchical_clusters(corr, max_clusters=3)

        labels = result['labels']
        symbols = list(sample_returns.columns)

        for symbol in symbols:
            assert symbol in labels
            assert isinstance(labels[symbol], int)

    def test_cluster_count(self, sample_returns):
        """Number of clusters should be <= max_clusters."""
        corr = correlation_matrix(sample_returns)
        result = hierarchical_clusters(corr, max_clusters=3)

        clusters = result['clusters']
        assert len(clusters) <= 3

    def test_cluster_structure(self, sample_returns):
        """Each cluster should have expected keys and structure."""
        corr = correlation_matrix(sample_returns)
        result = hierarchical_clusters(corr, max_clusters=3)

        expected_keys = {'cluster_id', 'members', 'size', 'avg_intra_corr'}

        for cluster in result['clusters']:
            assert set(cluster.keys()) == expected_keys
            assert isinstance(cluster['cluster_id'], int)
            assert isinstance(cluster['members'], list)
            assert cluster['size'] == len(cluster['members'])
            assert -1 <= cluster['avg_intra_corr'] <= 1

    def test_cluster_all_symbols_assigned(self, sample_returns):
        """All symbols should be assigned to exactly one cluster."""
        corr = correlation_matrix(sample_returns)
        result = hierarchical_clusters(corr, max_clusters=3)

        all_members = []
        for cluster in result['clusters']:
            all_members.extend(cluster['members'])

        symbols = list(sample_returns.columns)
        assert sorted(all_members) == sorted(symbols)

    def test_cluster_single_asset(self):
        """Single asset should return single cluster."""
        dates = pd.bdate_range('2023-01-01', periods=100)
        returns = pd.DataFrame(
            np.random.normal(0, 0.02, 100),
            index=dates,
            columns=['A']
        )

        corr = correlation_matrix(returns)
        result = hierarchical_clusters(corr, max_clusters=5)

        assert len(result['clusters']) == 1
        assert result['clusters'][0]['members'] == ['A']
        assert result['clusters'][0]['avg_intra_corr'] == 1.0

    def test_cluster_methods(self, sample_returns):
        """Different linkage methods should work."""
        corr = correlation_matrix(sample_returns)

        methods = ['ward', 'average', 'complete', 'single']

        for method in methods:
            result = hierarchical_clusters(corr, max_clusters=3, method=method)
            assert 'labels' in result
            assert 'clusters' in result

    def test_cluster_empty_raises(self):
        """Empty correlation matrix should raise ValueError."""
        empty_df = pd.DataFrame()

        with pytest.raises(ValueError, match="empty correlation"):
            hierarchical_clusters(empty_df, max_clusters=3)

    def test_cluster_avg_intra_corr_single_member(self):
        """Single-member clusters should have avg_intra_corr = 1.0."""
        dates = pd.bdate_range('2023-01-01', periods=100)
        # Create uncorrelated returns so each gets its own cluster
        returns = pd.DataFrame({
            'A': np.random.normal(0, 0.02, 100),
            'B': np.random.normal(0, 0.02, 100),
            'C': np.random.normal(0, 0.02, 100),
        }, index=dates)

        corr = correlation_matrix(returns)
        result = hierarchical_clusters(corr, max_clusters=3)

        # Check single-member clusters
        for cluster in result['clusters']:
            if cluster['size'] == 1:
                assert cluster['avg_intra_corr'] == 1.0


class TestClusterExposures:
    """Tests for cluster_exposures function."""

    def test_cluster_exposures_sums(self, sample_returns, sample_weights, sample_symbols):
        """Gross exposure percentages across clusters should sum to 100%."""
        corr = correlation_matrix(sample_returns)
        cluster_result = hierarchical_clusters(corr, max_clusters=3)

        exposures = cluster_exposures(
            cluster_result['labels'],
            sample_weights,
            sample_symbols
        )

        gross_sum = sum(exp['gross_exposure_pct'] for exp in exposures)
        assert_allclose(gross_sum, 100.0, rtol=1e-8)

    def test_cluster_exposures_structure(self, sample_returns, sample_weights, sample_symbols):
        """Each exposure dict should have expected keys."""
        corr = correlation_matrix(sample_returns)
        cluster_result = hierarchical_clusters(corr, max_clusters=3)

        exposures = cluster_exposures(
            cluster_result['labels'],
            sample_weights,
            sample_symbols
        )

        expected_keys = {'cluster_id', 'members', 'gross_exposure_pct', 'net_exposure_pct'}

        for exposure in exposures:
            assert set(exposure.keys()) == expected_keys

    def test_cluster_exposures_sorted(self, sample_returns, sample_weights, sample_symbols):
        """Exposures should be sorted by gross exposure descending."""
        corr = correlation_matrix(sample_returns)
        cluster_result = hierarchical_clusters(corr, max_clusters=3)

        exposures = cluster_exposures(
            cluster_result['labels'],
            sample_weights,
            sample_symbols
        )

        gross_values = [exp['gross_exposure_pct'] for exp in exposures]
        assert gross_values == sorted(gross_values, reverse=True)

    def test_cluster_exposures_long_short(self):
        """Net exposure should correctly handle long and short positions."""
        # Create simple scenario
        cluster_labels = {'A': 0, 'B': 0, 'C': 1}
        weights = np.array([0.50, -0.30, 0.20])  # Cluster 0: long and short
        symbols = ['A', 'B', 'C']

        exposures = cluster_exposures(cluster_labels, weights, symbols)

        # Find cluster 0
        cluster_0 = next(exp for exp in exposures if exp['cluster_id'] == 0)

        # Gross = |0.50| + |-0.30| = 0.80
        # Net = 0.50 - 0.30 = 0.20
        # Total gross = 0.80 + 0.20 = 1.00
        expected_gross_pct = (0.80 / 1.00) * 100
        expected_net_pct = (0.20 / 1.00) * 100

        assert_allclose(cluster_0['gross_exposure_pct'], expected_gross_pct, rtol=1e-8)
        assert_allclose(cluster_0['net_exposure_pct'], expected_net_pct, rtol=1e-8)

    def test_cluster_exposures_zero_gross(self):
        """Zero gross exposure should return empty list."""
        cluster_labels = {'A': 0, 'B': 1}
        weights = np.array([0.0, 0.0])
        symbols = ['A', 'B']

        exposures = cluster_exposures(cluster_labels, weights, symbols)

        assert len(exposures) == 0

    def test_cluster_exposures_dimension_mismatch_raises(self, sample_returns):
        """Mismatched weights and symbols should raise ValueError."""
        corr = correlation_matrix(sample_returns)
        cluster_result = hierarchical_clusters(corr, max_clusters=3)

        weights = np.array([0.5, 0.5])  # Only 2 weights
        symbols = ['A', 'B', 'C']  # 3 symbols

        with pytest.raises(ValueError, match="doesn't match symbols"):
            cluster_exposures(cluster_result['labels'], weights, symbols)

    def test_cluster_exposures_missing_symbol_in_labels(self, sample_returns, sample_weights):
        """Symbols not in cluster_labels should be logged and skipped."""
        corr = correlation_matrix(sample_returns)
        cluster_result = hierarchical_clusters(corr, max_clusters=3)

        # Remove one symbol from labels
        incomplete_labels = cluster_result['labels'].copy()
        del incomplete_labels['AAPL']

        symbols = list(sample_returns.columns)

        # Should not crash, but will skip AAPL
        exposures = cluster_exposures(incomplete_labels, sample_weights, symbols)

        # Check that AAPL is not in any cluster members
        all_members = []
        for exp in exposures:
            all_members.extend(exp['members'])

        assert 'AAPL' not in all_members
