"""Database layer -- async PostgreSQL via SQLAlchemy + asyncpg.

Provides connection management and read-only query helpers for the
positions tables that are owned and written to by the broker-bridge service.
"""

from __future__ import annotations

from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Engine singleton
# ---------------------------------------------------------------------------

_engine: AsyncEngine | None = None


def _make_async_url(postgres_url: str) -> str:
    """Ensure the URL uses the asyncpg driver prefix."""
    url = postgres_url
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    elif url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+asyncpg://", 1)
    return url


async def get_engine(postgres_url: str) -> AsyncEngine:
    """Create or return the async engine singleton.

    On first call the engine is created with connection pooling.
    Subsequent calls return the cached engine (the *postgres_url* argument
    is ignored after the first call).
    """
    global _engine
    if _engine is None:
        url = _make_async_url(postgres_url)
        _engine = create_async_engine(
            url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        logger.info("database_engine_created")
    return _engine


async def close_engine() -> None:
    """Dispose of the connection pool."""
    global _engine
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        logger.info("database_engine_closed")


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

_SELECT_POSITIONS = text(
    "SELECT * FROM positions_current ORDER BY symbol"
)

_SELECT_ACCOUNT_SUMMARY = text(
    "SELECT * FROM account_summary ORDER BY tag"
)


async def get_positions() -> list[dict[str, Any]]:
    """Return every row in positions_current ordered by symbol."""
    if _engine is None:
        raise RuntimeError("Database engine not initialised. Call get_engine() first.")

    async with _engine.connect() as conn:
        result = await conn.execute(_SELECT_POSITIONS)
        rows = result.mappings().all()
        return [dict(row) for row in rows]


async def get_account_summary() -> list[dict[str, Any]]:
    """Return every row in account_summary ordered by tag."""
    if _engine is None:
        raise RuntimeError("Database engine not initialised. Call get_engine() first.")

    async with _engine.connect() as conn:
        result = await conn.execute(_SELECT_ACCOUNT_SUMMARY)
        rows = result.mappings().all()
        return [dict(row) for row in rows]
