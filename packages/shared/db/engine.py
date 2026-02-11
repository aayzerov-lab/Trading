"""Shared async database engine for phase 1 tables.

Provides a singleton engine for market data, factors, and risk results,
with separate connection pooling from the broker-bridge tables.
"""

from __future__ import annotations

import os

import structlog
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .models import phase1_metadata

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Engine singleton
# ---------------------------------------------------------------------------

_shared_engine: AsyncEngine | None = None


def _make_async_url(postgres_url: str) -> str:
    """Ensure the URL uses the asyncpg driver prefix."""
    url = postgres_url
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    elif url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+asyncpg://", 1)
    return url


def get_shared_engine(postgres_url: str | None = None) -> AsyncEngine:
    """Create or return the shared async engine singleton.

    On first call the engine is created with connection pooling.
    Subsequent calls return the cached engine (the *postgres_url* argument
    is ignored after the first call).

    Args:
        postgres_url: PostgreSQL connection string. If None on first call,
            reads from POSTGRES_URL environment variable.

    Returns:
        AsyncEngine instance with asyncpg driver

    Raises:
        RuntimeError: If engine not yet created and no URL available
    """
    global _shared_engine
    if _shared_engine is not None:
        return _shared_engine

    if postgres_url is None:
        postgres_url = os.getenv("POSTGRES_URL", "")

    if not postgres_url:
        raise RuntimeError(
            "Shared engine not initialized and no POSTGRES_URL provided. "
            "Call init_phase1_db() first or set POSTGRES_URL env var."
        )

    url = _make_async_url(postgres_url)
    _shared_engine = create_async_engine(
        url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=3600,
    )
    logger.info("shared_engine_created")
    return _shared_engine


async def close_shared_engine() -> None:
    """Dispose of the shared connection pool."""
    global _shared_engine
    if _shared_engine is not None:
        await _shared_engine.dispose()
        _shared_engine = None
        logger.info("shared_engine_closed")


async def init_phase1_db(postgres_url: str) -> None:
    """Create phase 1 tables if they do not already exist.

    This initializes the market data, factors, FRED series, sync status,
    and risk results tables. Also initializes the shared engine singleton.

    Args:
        postgres_url: PostgreSQL connection string
    """
    engine = get_shared_engine(postgres_url)
    async with engine.begin() as conn:
        await conn.run_sync(phase1_metadata.create_all)
    logger.info("phase1_db_initialized")
