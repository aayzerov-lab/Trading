"""
Covariance Estimation Module

Implements multiple covariance matrix estimation methods for portfolio risk analytics.
Includes Ledoit-Wolf shrinkage and EWMA (RiskMetrics) approaches.
"""

import numpy as np
import pandas as pd
import structlog
from sklearn.covariance import LedoitWolf

logger = structlog.get_logger(__name__)


def ledoit_wolf_cov(returns: pd.DataFrame) -> np.ndarray:
    """Estimate covariance matrix using Ledoit-Wolf shrinkage.

    Uses sklearn's LedoitWolf estimator which automatically determines
    the optimal shrinkage intensity. Handles the T < N case gracefully.

    Args:
        returns: DataFrame of returns (T x N) where T = time periods, N = assets

    Returns:
        N x N covariance matrix as numpy array

    Raises:
        ValueError: If returns DataFrame is empty or has insufficient data
    """
    if returns.empty:
        raise ValueError("Cannot estimate covariance from empty returns DataFrame")

    if len(returns) < 2:
        raise ValueError(f"Need at least 2 observations, got {len(returns)}")

    # Convert to numpy array
    returns_array = returns.values

    # Check for NaN values
    if np.isnan(returns_array).any():
        nan_counts = np.isnan(returns_array).sum(axis=0)
        affected_symbols = [
            returns.columns[i]
            for i, count in enumerate(nan_counts)
            if count > 0
        ]
        logger.error(
            "ledoit_wolf_cov: NaN values in returns",
            affected_symbols=affected_symbols
        )
        raise ValueError(f"NaN values detected in returns for symbols: {affected_symbols}")

    # Estimate covariance using Ledoit-Wolf
    lw = LedoitWolf()
    try:
        cov_matrix = lw.fit(returns_array).covariance_
    except Exception as e:
        logger.error(
            "ledoit_wolf_cov: estimation failed",
            error=str(e),
            shape=returns_array.shape
        )
        raise

    # Verify positive semi-definite and fix if needed
    eigenvalues = np.linalg.eigvalsh(cov_matrix)
    min_eigenvalue = float(np.min(eigenvalues))

    if min_eigenvalue < -1e-8:
        logger.warning(
            "ledoit_wolf_cov: non-PSD matrix, clamping negative eigenvalues",
            min_eigenvalue=min_eigenvalue
        )
        eigenvalues_clamped = np.maximum(eigenvalues, 0)
        eigvecs = np.linalg.eigh(cov_matrix)[1]
        cov_matrix = eigvecs @ np.diag(eigenvalues_clamped) @ eigvecs.T
        cov_matrix = (cov_matrix + cov_matrix.T) / 2

    logger.info(
        "ledoit_wolf_cov: covariance estimated",
        num_assets=cov_matrix.shape[0],
        num_observations=len(returns),
        shrinkage=float(lw.shrinkage_),
        condition_number=float(np.linalg.cond(cov_matrix))
    )

    return cov_matrix


def ewma_cov(returns: pd.DataFrame, lambd: float = 0.94) -> np.ndarray:
    """Estimate covariance matrix using Exponentially Weighted Moving Average.

    Standard RiskMetrics EWMA implementation:
    sigma_t = lambda * sigma_{t-1} + (1 - lambda) * r_t * r_t'

    Computes iteratively from oldest to newest observation.

    Args:
        returns: DataFrame of returns (T x N)
        lambd: Decay factor (default 0.94, standard RiskMetrics)

    Returns:
        N x N covariance matrix as numpy array

    Raises:
        ValueError: If returns is empty or lambda is invalid
    """
    if returns.empty:
        raise ValueError("Cannot estimate covariance from empty returns DataFrame")

    if not 0 < lambd < 1:
        raise ValueError(f"Lambda must be between 0 and 1, got {lambd}")

    if len(returns) < 2:
        raise ValueError(f"Need at least 2 observations, got {len(returns)}")

    # Convert to numpy array
    returns_array = returns.values

    # Check for NaN values
    if np.isnan(returns_array).any():
        nan_counts = np.isnan(returns_array).sum(axis=0)
        affected_symbols = [
            returns.columns[i]
            for i, count in enumerate(nan_counts)
            if count > 0
        ]
        logger.error(
            "ewma_cov: NaN values in returns",
            affected_symbols=affected_symbols
        )
        raise ValueError(f"NaN values detected in returns for symbols: {affected_symbols}")

    T, N = returns_array.shape

    # Initialize with sample covariance of first 10 observations (or all if < 10)
    init_window = min(10, T)
    cov_matrix = np.cov(returns_array[:init_window].T, ddof=1)

    # Handle single asset case
    if N == 1:
        cov_matrix = np.array([[cov_matrix]])

    # Iterate through observations, updating covariance
    for t in range(init_window, T):
        r_t = returns_array[t].reshape(-1, 1)  # Column vector
        cov_matrix = lambd * cov_matrix + (1 - lambd) * (r_t @ r_t.T)

    # Ensure symmetry (numerical stability)
    cov_matrix = (cov_matrix + cov_matrix.T) / 2

    # Verify positive semi-definite and fix if needed
    eigenvalues = np.linalg.eigvalsh(cov_matrix)
    min_eigenvalue = float(np.min(eigenvalues))

    if min_eigenvalue < -1e-8:
        logger.warning(
            "ewma_cov: non-PSD matrix, clamping negative eigenvalues",
            min_eigenvalue=min_eigenvalue
        )
        eigenvalues_clamped = np.maximum(eigenvalues, 0)
        eigvecs = np.linalg.eigh(cov_matrix)[1]
        cov_matrix = eigvecs @ np.diag(eigenvalues_clamped) @ eigvecs.T
        cov_matrix = (cov_matrix + cov_matrix.T) / 2

    logger.info(
        "ewma_cov: covariance estimated",
        num_assets=N,
        num_observations=T,
        lambda_param=lambd,
        condition_number=float(np.linalg.cond(cov_matrix))
    )

    return cov_matrix


def estimate_covariance(
    returns: pd.DataFrame,
    method: str = 'lw',
    ewma_lambda: float = 0.94,
) -> np.ndarray:
    """Unified interface for covariance estimation.

    Args:
        returns: DataFrame of returns (T x N)
        method: Estimation method - 'lw' for Ledoit-Wolf, 'ewma' for EWMA
        ewma_lambda: Decay factor for EWMA (only used if method='ewma')

    Returns:
        N x N covariance matrix as numpy array

    Raises:
        ValueError: If method is invalid or estimation fails
    """
    if returns.empty:
        raise ValueError("Cannot estimate covariance from empty returns DataFrame")

    method = method.lower()

    if method == 'lw':
        logger.info("estimate_covariance: using Ledoit-Wolf shrinkage")
        return ledoit_wolf_cov(returns)
    elif method == 'ewma':
        logger.info("estimate_covariance: using EWMA", lambda_param=ewma_lambda)
        return ewma_cov(returns, lambd=ewma_lambda)
    else:
        raise ValueError(f"Unknown covariance estimation method: {method}. Use 'lw' or 'ewma'")


def pairwise_cov(
    returns_dict: dict[str, pd.Series],
    symbols: list[str],
    window: int = 252,
    min_overlap: int = 60,
    method: str = "lw",
    ewma_lambda: float = 0.94,
) -> np.ndarray:
    """Estimate covariance matrix using pairwise overlapping returns.

    Instead of requiring all symbols to share a single aligned date range,
    computes each element of the covariance matrix from the overlapping
    dates available for that pair.  Diagonal entries (variances) use each
    symbol's own full history (up to *window* observations).

    For off-diagonal entries the pairwise sample covariance is computed,
    then the full matrix is shrunk toward the diagonal (simple Ledoit-Wolf
    style constant-correlation shrinkage) so that the result is guaranteed
    positive semi-definite.

    Args:
        returns_dict: {symbol: pd.Series} of daily log-returns indexed by date.
        symbols: ordered list of symbols (defines row/col order of output).
        window: max number of observations to use per series.
        min_overlap: minimum overlapping observations required for a pair.
        method: ignored for now — pairwise always uses sample + shrinkage.
        ewma_lambda: ignored for now.

    Returns:
        N x N covariance matrix (numpy array), guaranteed PSD.

    Raises:
        ValueError: if any symbol is missing or a pair has < min_overlap overlap.
    """
    n = len(symbols)
    if n == 0:
        raise ValueError("symbols list is empty")

    # Trim each series to last `window` observations
    trimmed: dict[str, pd.Series] = {}
    for sym in symbols:
        if sym not in returns_dict:
            raise ValueError(f"Symbol {sym} not found in returns_dict")
        s = returns_dict[sym].dropna()
        trimmed[sym] = s.iloc[-window:] if len(s) > window else s

    # Build raw pairwise covariance matrix
    raw_cov = np.zeros((n, n))
    obs_counts = np.zeros((n, n), dtype=int)

    for i in range(n):
        si = trimmed[symbols[i]]
        for j in range(i, n):
            sj = trimmed[symbols[j]]
            # Align on common dates
            common = si.index.intersection(sj.index)
            overlap = len(common)
            obs_counts[i, j] = obs_counts[j, i] = overlap

            if overlap < min_overlap:
                raise ValueError(
                    f"Pair ({symbols[i]}, {symbols[j]}) has only {overlap} "
                    f"overlapping observations, need {min_overlap}"
                )

            ri = si.loc[common].values
            rj = sj.loc[common].values
            cov_ij = float(np.cov(ri, rj, ddof=1)[0, 1])
            raw_cov[i, j] = raw_cov[j, i] = cov_ij

    # PSD correction: pairwise assembly can yield non-PSD matrices.
    # Use shrinkage toward diagonal (preserves correlation structure
    # much better than eigenvalue clamping).
    eigenvalues = np.linalg.eigvalsh(raw_cov)
    min_eigenvalue = float(np.min(eigenvalues))

    if min_eigenvalue < 1e-10:
        diag_target = np.diag(np.diag(raw_cov))
        # Binary search for minimum shrinkage alpha that makes PSD
        lo, hi = 0.0, 1.0
        for _ in range(50):
            mid = (lo + hi) / 2
            candidate = (1 - mid) * raw_cov + mid * diag_target
            if np.min(np.linalg.eigvalsh(candidate)) > 1e-10:
                hi = mid
            else:
                lo = mid
        alpha = hi + 0.01  # small buffer for numerical stability
        alpha = min(alpha, 1.0)
        raw_cov = (1 - alpha) * raw_cov + alpha * diag_target
        raw_cov = (raw_cov + raw_cov.T) / 2
        logger.info(
            "pairwise_cov: PSD correction via diagonal shrinkage",
            min_eigenvalue=min_eigenvalue,
            shrinkage_alpha=round(alpha, 4),
        )

    logger.info(
        "pairwise_cov: covariance estimated",
        num_assets=n,
        min_overlap=int(obs_counts[np.triu_indices(n, k=1)].min()) if n > 1 else 0,
        max_overlap=int(obs_counts[np.triu_indices(n, k=1)].max()) if n > 1 else 0,
        condition_number=float(np.linalg.cond(raw_cov)),
    )

    return raw_cov


def annualize_cov(cov: np.ndarray, trading_days: int = 252) -> np.ndarray:
    """Annualize a daily covariance matrix.

    For daily covariance, annualized covariance = daily_cov * trading_days

    Args:
        cov: Daily covariance matrix (N x N)
        trading_days: Number of trading days per year (default 252)

    Returns:
        Annualized covariance matrix (N x N)
    """
    if cov.size == 0:
        raise ValueError("Cannot annualize empty covariance matrix")

    if cov.ndim != 2 or cov.shape[0] != cov.shape[1]:
        raise ValueError(f"Covariance matrix must be square, got shape {cov.shape}")

    if trading_days <= 0:
        raise ValueError(f"Trading days must be positive, got {trading_days}")

    annualized = cov * trading_days

    logger.info(
        "annualize_cov: covariance annualized",
        trading_days=trading_days,
        matrix_size=cov.shape[0]
    )

    return annualized
