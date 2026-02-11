"""
Correlation Analysis and Clustering Module

Functions for analyzing correlation structure, identifying highly correlated pairs,
and performing hierarchical clustering for portfolio construction and risk analysis.
"""

import numpy as np
import pandas as pd
import structlog
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import squareform
from typing import List, Dict

logger = structlog.get_logger(__name__)


def correlation_matrix(returns: pd.DataFrame) -> pd.DataFrame:
    """Compute correlation matrix from returns DataFrame.

    Args:
        returns: DataFrame of returns (T x N)

    Returns:
        DataFrame with symbol labels on both axes (N x N correlation matrix)
    """
    if returns.empty:
        raise ValueError("Cannot compute correlation from empty returns DataFrame")

    if len(returns) < 2:
        raise ValueError(f"Need at least 2 observations, got {len(returns)}")

    # Compute correlation
    corr = returns.corr()

    # Verify no NaN values
    if corr.isna().any().any():
        nan_symbols = corr.columns[corr.isna().any()].tolist()
        logger.error(
            "correlation_matrix: NaN values in correlation matrix",
            affected_symbols=nan_symbols
        )
        raise ValueError(f"NaN values in correlation matrix for symbols: {nan_symbols}")

    upper_vals = corr.values[np.triu_indices_from(corr.values, k=1)]
    avg_corr = float(upper_vals.mean()) if len(upper_vals) > 0 else 0.0

    logger.info(
        "correlation_matrix: correlation computed",
        num_assets=len(corr),
        avg_correlation=avg_corr,
    )

    return corr


def top_correlated_pairs(
    corr: pd.DataFrame,
    n: int = 20,
) -> List[Dict]:
    """Find top N most correlated pairs (excluding self-correlation).

    Includes both highly positive and highly negative correlations.

    Args:
        corr: Correlation matrix (N x N DataFrame)
        n: Number of top pairs to return

    Returns:
        List of dicts: [{'symbol_a': str, 'symbol_b': str, 'correlation': float}, ...]
        Sorted by |correlation| descending
    """
    if corr.empty:
        raise ValueError("Cannot find pairs from empty correlation matrix")

    # Get upper triangle indices (excluding diagonal)
    rows, cols = np.triu_indices_from(corr.values, k=1)

    # Extract correlations
    pairs = []
    for i, j in zip(rows, cols):
        pairs.append({
            'symbol_a': corr.index[i],
            'symbol_b': corr.columns[j],
            'correlation': float(corr.iloc[i, j])
        })

    # Sort by absolute correlation descending
    pairs.sort(key=lambda x: abs(x['correlation']), reverse=True)

    # Take top N
    top_pairs = pairs[:n]

    logger.info(
        "top_correlated_pairs: pairs identified",
        num_pairs=len(top_pairs),
        max_correlation=top_pairs[0]['correlation'] if top_pairs else 0.0
    )

    return top_pairs


def hierarchical_clusters(
    corr: pd.DataFrame,
    max_clusters: int = 8,
    method: str = 'ward',
) -> Dict:
    """Perform hierarchical clustering on correlation distance matrix.

    Distance metric: d = sqrt(2 * (1 - corr))

    Args:
        corr: Correlation matrix (N x N DataFrame)
        max_clusters: Maximum number of clusters to create
        method: Linkage method ('ward', 'average', 'complete', 'single')

    Returns:
        Dict with:
            - labels: dict[str, int] mapping symbol -> cluster_id
            - clusters: list of cluster info dicts
    """
    if corr.empty:
        raise ValueError("Cannot cluster empty correlation matrix")

    if len(corr) < 2:
        logger.warning("hierarchical_clusters: only one asset, returning single cluster")
        return {
            'labels': {corr.index[0]: 0},
            'clusters': [{
                'cluster_id': 0,
                'members': [corr.index[0]],
                'size': 1,
                'avg_intra_corr': 1.0
            }]
        }

    # Correlation distance: sqrt(2 * (1 - corr))
    # Clamp correlation to [-1, 1] to avoid numerical issues
    corr_values = np.clip(corr.values, -1.0, 1.0)
    distance = np.sqrt(2 * (1 - corr_values))

    # Ensure diagonal is zero
    np.fill_diagonal(distance, 0)

    # Convert to condensed distance matrix (upper triangle)
    condensed_dist = squareform(distance, checks=False)

    # Check for invalid distances
    if np.isnan(condensed_dist).any() or np.isinf(condensed_dist).any():
        logger.error("hierarchical_clusters: invalid distances in matrix")
        raise ValueError("Invalid distances (NaN or Inf) in correlation distance matrix")

    # Perform hierarchical clustering
    try:
        Z = linkage(condensed_dist, method=method)
    except Exception as e:
        logger.error(
            "hierarchical_clusters: linkage failed",
            error=str(e),
            method=method
        )
        raise

    # Cut tree into clusters
    # Use min to avoid more clusters than assets
    num_clusters = min(max_clusters, len(corr))
    cluster_labels_array = fcluster(Z, num_clusters, criterion='maxclust')

    # Build label dict
    labels = {
        symbol: int(cluster_id)
        for symbol, cluster_id in zip(corr.index, cluster_labels_array)
    }

    # Build cluster info
    clusters = []
    unique_clusters = np.unique(cluster_labels_array)

    for cluster_id in unique_clusters:
        # Get members
        member_indices = np.where(cluster_labels_array == cluster_id)[0]
        members = [corr.index[i] for i in member_indices]

        # Compute average intra-cluster correlation
        if len(members) > 1:
            # Get sub-correlation matrix for this cluster
            cluster_corr = corr.loc[members, members].values
            # Upper triangle excluding diagonal
            upper_indices = np.triu_indices_from(cluster_corr, k=1)
            avg_intra_corr = float(cluster_corr[upper_indices].mean())
        else:
            avg_intra_corr = 1.0

        clusters.append({
            'cluster_id': int(cluster_id),
            'members': members,
            'size': len(members),
            'avg_intra_corr': avg_intra_corr
        })

    # Sort by cluster size descending
    clusters.sort(key=lambda x: x['size'], reverse=True)

    logger.info(
        "hierarchical_clusters: clustering complete",
        num_clusters=len(clusters),
        method=method,
        cluster_sizes=[c['size'] for c in clusters]
    )

    return {
        'labels': labels,
        'clusters': clusters
    }


def cluster_exposures(
    cluster_labels: Dict[str, int],
    weights: np.ndarray,
    symbols: List[str],
) -> List[Dict]:
    """Compute gross and net exposure per cluster.

    Args:
        cluster_labels: Dict mapping symbol -> cluster_id
        weights: Position weights (as decimals or dollar amounts)
        symbols: List of symbols corresponding to weights

    Returns:
        List of dicts with cluster exposure info:
        [{
            'cluster_id': int,
            'members': list[str],
            'gross_exposure_pct': float,
            'net_exposure_pct': float,
        }, ...]
    """
    weights = np.asarray(weights).flatten()

    if len(weights) != len(symbols):
        raise ValueError(
            f"Weights length {len(weights)} doesn't match symbols length {len(symbols)}"
        )

    # Calculate total gross exposure
    total_gross = np.sum(np.abs(weights))

    if total_gross == 0:
        logger.warning("cluster_exposures: zero gross exposure")
        return []

    # Group by cluster
    cluster_data = {}
    for symbol, weight in zip(symbols, weights):
        cluster_id = cluster_labels.get(symbol)
        if cluster_id is None:
            logger.warning(
                "cluster_exposures: symbol not in cluster_labels",
                symbol=symbol
            )
            continue

        if cluster_id not in cluster_data:
            cluster_data[cluster_id] = {
                'members': [],
                'weights': []
            }

        cluster_data[cluster_id]['members'].append(symbol)
        cluster_data[cluster_id]['weights'].append(weight)

    # Build exposure list
    exposures = []
    for cluster_id, data in cluster_data.items():
        cluster_weights = np.array(data['weights'])

        gross_exposure = float(np.sum(np.abs(cluster_weights)))
        net_exposure = float(np.sum(cluster_weights))

        gross_exposure_pct = (gross_exposure / total_gross) * 100
        net_exposure_pct = (net_exposure / total_gross) * 100

        exposures.append({
            'cluster_id': cluster_id,
            'members': data['members'],
            'gross_exposure_pct': gross_exposure_pct,
            'net_exposure_pct': net_exposure_pct
        })

    # Sort by gross exposure descending
    exposures.sort(key=lambda x: x['gross_exposure_pct'], reverse=True)

    logger.info(
        "cluster_exposures: exposures computed",
        num_clusters=len(exposures)
    )

    return exposures
