"""Risk analytics API endpoints.

Provides REST endpoints for portfolio risk metrics, including summary statistics,
risk contributors, correlation analysis, clustering, and stress testing.
"""

from __future__ import annotations

import asyncio
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException, Query

from api_server.services.risk_service import compute_risk_pack

logger = structlog.get_logger()

router = APIRouter(prefix="/risk", tags=["risk"])


def _validate_params(window: int, method: str | None = None) -> None:
    """Validate common risk query parameters."""
    if window not in [60, 252]:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid window: {window}. Must be 60 or 252.",
        )
    if method is not None and method not in ["lw", "ewma"]:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid method: {method}. Must be 'lw' or 'ewma'.",
        )


@router.get("/summary")
async def risk_summary(
    window: int = Query(default=252, description="Lookback window in days"),
    method: str = Query(default="lw", description="Covariance estimation method"),
) -> dict[str, Any]:
    """Return portfolio risk summary metrics."""
    _validate_params(window, method)

    try:
        logger.info("risk_summary_request", window=window, method=method)
        result = await compute_risk_pack(window=window, method=method)

        return {
            **result["summary"],
            "window": window,
            "method": method,
            "asof_date": result["metadata"].get("asof_date", ""),
        }

    except Exception as e:
        logger.exception("risk_summary_failed", window=window, method=method)
        raise HTTPException(
            status_code=500,
            detail=f"Risk computation failed: {str(e)}",
        )


@router.get("/contributors")
async def risk_contributors(
    window: int = Query(default=252, description="Lookback window in days"),
    method: str = Query(default="lw", description="Covariance estimation method"),
) -> list[dict[str, Any]]:
    """Return per-position risk contributions.

    Returns flat list of contributor dicts for the frontend table.
    """
    _validate_params(window, method)

    try:
        logger.info("risk_contributors_request", window=window, method=method)
        result = await compute_risk_pack(window=window, method=method)
        return result["contributors"]

    except Exception as e:
        logger.exception("risk_contributors_failed", window=window, method=method)
        raise HTTPException(
            status_code=500,
            detail=f"Risk computation failed: {str(e)}",
        )


@router.get("/correlation/pairs")
async def correlation_pairs(
    window: int = Query(default=252, description="Lookback window in days"),
    n: int = Query(default=20, ge=5, le=50, description="Number of top pairs"),
) -> list[dict[str, Any]]:
    """Return top correlated position pairs.

    Returns flat list of pair dicts for the frontend table.
    """
    _validate_params(window)

    try:
        logger.info("correlation_pairs_request", window=window, n=n)
        result = await compute_risk_pack(window=window, method="lw")
        pairs = result["correlation_pairs"][:n]
        return pairs

    except Exception as e:
        logger.exception("correlation_pairs_failed", window=window, n=n)
        raise HTTPException(
            status_code=500,
            detail=f"Correlation computation failed: {str(e)}",
        )


@router.get("/clusters")
async def risk_clusters(
    window: int = Query(default=252, description="Lookback window in days"),
    max_clusters: int = Query(default=8, ge=2, le=20, description="Max clusters"),
) -> list[dict[str, Any]]:
    """Return hierarchical cluster analysis with exposures.

    Returns flat list of cluster dicts for the frontend panel.
    """
    _validate_params(window)

    try:
        logger.info("risk_clusters_request", window=window, max_clusters=max_clusters)
        result = await compute_risk_pack(window=window, method="lw")
        return result["clusters"]

    except Exception as e:
        logger.exception("risk_clusters_failed", window=window)
        raise HTTPException(
            status_code=500,
            detail=f"Cluster computation failed: {str(e)}",
        )


@router.get("/stress")
async def stress_tests() -> dict[str, Any]:
    """Return all stress test results."""
    try:
        logger.info("stress_test_request")
        result = await compute_risk_pack(window=252, method="lw")
        return result["stress"]

    except Exception as e:
        logger.exception("stress_test_failed")
        raise HTTPException(
            status_code=500,
            detail=f"Stress test computation failed: {str(e)}",
        )


@router.post("/recompute")
async def recompute_risk() -> dict[str, str]:
    """Force recomputation of all risk metrics."""
    try:
        logger.info("risk_recompute_request")

        from api_server.main import get_redis

        # Trigger recomputation in background
        asyncio.create_task(_recompute_background())

        # Publish event to Redis
        redis = get_redis()
        if redis is not None:
            await redis.publish("risk_recompute", '{"status": "triggered"}')

        return {"status": "recomputation_triggered"}

    except Exception as e:
        logger.exception("risk_recompute_failed")
        raise HTTPException(
            status_code=500,
            detail=f"Risk recomputation failed: {str(e)}",
        )


async def _recompute_background() -> None:
    """Background task to fetch fresh data and recompute all risk metrics."""
    try:
        from shared.data.scheduler import run_daily_data_update
        from shared.db.engine import get_shared_engine
        from api_server.main import get_redis

        engine = get_shared_engine()
        redis = get_redis()

        # Step 1: Fetch fresh price + FRED data
        logger.info("recompute_fetching_data")
        await run_daily_data_update(engine=engine, redis_client=redis)
        logger.info("recompute_data_fetch_complete")

        # Step 2: Recompute risk metrics
        for window in [60, 252]:
            for method in ["lw", "ewma"]:
                await compute_risk_pack(window=window, method=method, force=True)
                logger.info("risk_recomputed", window=window, method=method)

        if redis is not None:
            await redis.publish("risk_updated", '{"status": "completed"}')

        logger.info("risk_recomputation_completed")

    except Exception:
        logger.exception("background_recomputation_failed")
