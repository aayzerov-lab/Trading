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

_SELECT_POSITIONS_ALL = text(
    "SELECT * FROM positions_current ORDER BY symbol"
)

_SELECT_POSITIONS_BY_ACCOUNT = text(
    "SELECT * FROM positions_current WHERE account = :account ORDER BY symbol"
)

_SELECT_ACCOUNT_SUMMARY_ALL = text(
    "SELECT * FROM account_summary ORDER BY tag"
)

_SELECT_ACCOUNT_SUMMARY_BY_ACCOUNT = text(
    "SELECT * FROM account_summary WHERE account = :account ORDER BY tag"
)


async def get_positions(account: str | None = None) -> list[dict[str, Any]]:
    """Return rows in positions_current ordered by symbol.

    If *account* is provided, filter positions by that account.
    """
    if _engine is None:
        raise RuntimeError("Database engine not initialised. Call get_engine() first.")

    async with _engine.connect() as conn:
        if account:
            result = await conn.execute(_SELECT_POSITIONS_BY_ACCOUNT, {"account": account})
        else:
            result = await conn.execute(_SELECT_POSITIONS_ALL)
        rows = result.mappings().all()
        return [dict(row) for row in rows]


async def get_account_summary(account: str | None = None) -> list[dict[str, Any]]:
    """Return rows in account_summary ordered by tag.

    If *account* is provided, filter rows by that account.
    """
    if _engine is None:
        raise RuntimeError("Database engine not initialised. Call get_engine() first.")

    async with _engine.connect() as conn:
        if account:
            result = await conn.execute(
                _SELECT_ACCOUNT_SUMMARY_BY_ACCOUNT,
                {"account": account},
            )
        else:
            result = await conn.execute(_SELECT_ACCOUNT_SUMMARY_ALL)
        rows = result.mappings().all()
        return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Daily P&L helpers
# ---------------------------------------------------------------------------

_DAILY_PNL_NLV_ALL = text("""
    SELECT
        (SELECT value::float FROM account_summary
         WHERE tag = 'NetLiquidation' AND account != 'All'
         ORDER BY updated_at DESC LIMIT 1) AS nlv_current,
        (SELECT value::float FROM account_summary
         WHERE tag = 'DailyPnL' AND account != 'All'
         ORDER BY updated_at DESC LIMIT 1) AS daily_pnl,
        (SELECT SUM(daily_pnl) FROM positions_current
         WHERE daily_pnl IS NOT NULL) AS daily_pnl_positions
""")

_DAILY_PNL_NLV_BY_ACCOUNT = text("""
    SELECT
        (SELECT value::float FROM account_summary
         WHERE tag = 'NetLiquidation' AND account = :account
         ORDER BY updated_at DESC LIMIT 1) AS nlv_current,
        (SELECT value::float FROM account_summary
         WHERE tag = 'DailyPnL' AND account = :account
         ORDER BY updated_at DESC LIMIT 1) AS daily_pnl,
        (SELECT SUM(daily_pnl) FROM positions_current
         WHERE daily_pnl IS NOT NULL AND account = :account) AS daily_pnl_positions
""")


async def get_daily_pnl(account: str | None = None) -> dict[str, Any]:
    """Return account daily P&L from IBKR's reqPnL subscription.

    If *account* is provided, use that account's values.
    """
    if _engine is None:
        raise RuntimeError("Database engine not initialised. Call get_engine() first.")

    async with _engine.connect() as conn:
        if account:
            result = await conn.execute(_DAILY_PNL_NLV_BY_ACCOUNT, {"account": account})
        else:
            result = await conn.execute(_DAILY_PNL_NLV_ALL)
        row = result.mappings().first()

    if row is None:
        return {"nlv_current": None, "nlv_change": None, "nlv_change_pct": None}

    nlv = row["nlv_current"]
    # Prefer account-level DailyPnL from IB; fall back to sum of position daily_pnl
    change = row["daily_pnl"] if row["daily_pnl"] is not None else row["daily_pnl_positions"]

    if nlv is None or change is None:
        return {"nlv_current": nlv, "nlv_change": change, "nlv_change_pct": None}

    # P&L % = daily_change / (NLV - daily_change) to get change relative to start-of-day
    sod = nlv - change
    pct = (change / abs(sod)) * 100 if sod != 0 else None
    return {
        "nlv_current": nlv,
        "nlv_change": round(change, 2),
        "nlv_change_pct": round(pct, 2) if pct is not None else None,
    }


# ---------------------------------------------------------------------------
# Execution helpers
# ---------------------------------------------------------------------------

_SELECT_EXECUTIONS_TODAY = text(
    "SELECT * FROM executions WHERE exec_time >= CURRENT_DATE ORDER BY exec_time DESC"
)

_SELECT_EXECUTIONS_TODAY_BY_ACCOUNT = text(
    "SELECT * FROM executions WHERE exec_time >= CURRENT_DATE AND account = :account "
    "ORDER BY exec_time DESC"
)


async def get_executions(account: str | None = None) -> list[dict[str, Any]]:
    """Return today's executions, most recent first.

    If *account* is provided, filter rows by that account.
    """
    if _engine is None:
        raise RuntimeError("Database engine not initialised. Call get_engine() first.")

    async with _engine.connect() as conn:
        if account:
            result = await conn.execute(
                _SELECT_EXECUTIONS_TODAY_BY_ACCOUNT,
                {"account": account},
            )
        else:
            result = await conn.execute(_SELECT_EXECUTIONS_TODAY)
        rows = result.mappings().all()
        return [dict(row) for row in rows]


_SELECT_ACCOUNTS = text("""
    SELECT DISTINCT account FROM (
        SELECT account FROM positions_current
        UNION
        SELECT account FROM account_summary WHERE account != 'All'
        UNION
        SELECT account FROM executions
    ) accounts
    ORDER BY account
""")


async def get_accounts() -> list[str]:
    """Return distinct account identifiers present in the database."""
    if _engine is None:
        raise RuntimeError("Database engine not initialised. Call get_engine() first.")

    async with _engine.connect() as conn:
        result = await conn.execute(_SELECT_ACCOUNTS)
        rows = result.scalars().all()
        return [str(row) for row in rows if row]
