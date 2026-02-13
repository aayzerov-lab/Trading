"""Database layer – async PostgreSQL via SQLAlchemy + asyncpg.

Provides table definitions, connection management, and position
upsert / query helpers.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import structlog
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    text,
)
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from broker_bridge.models import ExecutionEvent, PositionEvent

logger = structlog.get_logger()

metadata = MetaData()

# ---------------------------------------------------------------------------
# Table definitions
# ---------------------------------------------------------------------------

positions_current = Table(
    "positions_current",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("account", String, nullable=False),
    Column("conid", Integer, nullable=True),
    Column("symbol", String, nullable=False),
    Column("sec_type", String, nullable=False),
    Column("currency", String, nullable=False),
    Column("exchange", String, nullable=True),
    Column("position", Float, nullable=False),
    Column("avg_cost", Float, nullable=True),
    Column("market_price", Float, nullable=True),
    Column("market_value", Float, nullable=True),
    Column("unrealized_pnl", Float, nullable=True),
    Column("realized_pnl", Float, nullable=True),
    Column("daily_pnl", Float, nullable=True),
    Column("sector", String, nullable=False, server_default=text("'Unknown'")),
    Column("country", String, nullable=False, server_default=text("'Unknown'")),
    Column("ib_industry", String, nullable=True),
    Column("ib_category", String, nullable=True),
    Column("ib_subcategory", String, nullable=True),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

# Partial unique index: when conid IS NOT NULL, unique on (account, conid)
Index(
    "uq_positions_current_conid",
    positions_current.c.account,
    positions_current.c.conid,
    unique=True,
    postgresql_where=positions_current.c.conid.isnot(None),
)

# Partial unique index: when conid IS NULL, unique on the natural key
Index(
    "uq_positions_current_natural",
    positions_current.c.account,
    positions_current.c.symbol,
    positions_current.c.sec_type,
    positions_current.c.currency,
    positions_current.c.exchange,
    unique=True,
    postgresql_where=positions_current.c.conid.is_(None),
)

positions_events = Table(
    "positions_events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ts_utc", DateTime(timezone=True), nullable=False),
    Column("account", String, nullable=False),
    Column("conid", Integer, nullable=True),
    Column("symbol", String, nullable=False),
    Column("sec_type", String, nullable=False),
    Column("currency", String, nullable=False),
    Column("exchange", String, nullable=True),
    Column("position", Float, nullable=False),
    Column("avg_cost", Float, nullable=True),
    Column("market_price", Float, nullable=True),
    Column("market_value", Float, nullable=True),
    Column("unrealized_pnl", Float, nullable=True),
    Column("realized_pnl", Float, nullable=True),
    Column("daily_pnl", Float, nullable=True),
    Column("sector", String, nullable=False, server_default=text("'Unknown'")),
    Column("country", String, nullable=False, server_default=text("'Unknown'")),
    Column("ib_industry", String, nullable=True),
    Column("ib_category", String, nullable=True),
    Column("ib_subcategory", String, nullable=True),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
)

account_summary = Table(
    "account_summary",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("account", String, nullable=False),
    Column("tag", String, nullable=False),
    Column("value", String, nullable=False),
    Column("currency", String, nullable=True),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("account", "tag", name="uq_account_summary_account_tag"),
)

account_summary_events = Table(
    "account_summary_events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("account", String, nullable=False),
    Column("tag", String, nullable=False),
    Column("value", String, nullable=False),
    Column("currency", String, nullable=True),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
)

executions = Table(
    "executions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("exec_id", String, nullable=False),
    Column("account", String, nullable=False),
    Column("conid", Integer, nullable=True),
    Column("symbol", String, nullable=False),
    Column("sec_type", String, nullable=False),
    Column("currency", String, nullable=False),
    Column("exchange", String, nullable=True),
    Column("side", String, nullable=False),
    Column("order_type", String, nullable=False),
    Column("quantity", Float, nullable=False),
    Column("filled_qty", Float, nullable=False, server_default=text("0")),
    Column("avg_fill_price", Float, nullable=True),
    Column("lmt_price", Float, nullable=True),
    Column("commission", Float, nullable=True),
    Column("status", String, nullable=False),
    Column("order_ref", String, nullable=True),
    Column("exec_time", DateTime(timezone=True), nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    UniqueConstraint("exec_id", name="uq_executions_exec_id"),
)

# ---------------------------------------------------------------------------
# Engine singleton
# ---------------------------------------------------------------------------

_engine: AsyncEngine | None = None


def _get_engine(postgres_url: str) -> AsyncEngine:
    """Create or return the async engine singleton."""
    global _engine
    if _engine is None:
        # Ensure the URL uses the asyncpg driver
        url = postgres_url
        if url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        elif url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+asyncpg://", 1)

        kwargs: dict[str, Any] = dict(
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        if os.environ.get("DB_SSL", "").lower() in ("1", "true", "yes"):
            kwargs["connect_args"] = {"ssl": "require"}

        _engine = create_async_engine(url, **kwargs)
    return _engine


# ---------------------------------------------------------------------------
# Migration helpers
# ---------------------------------------------------------------------------

_NEW_POSITION_COLUMNS = [
    ("market_price", "FLOAT"),
    ("market_value", "FLOAT"),
    ("unrealized_pnl", "FLOAT"),
    ("realized_pnl", "FLOAT"),
    ("daily_pnl", "FLOAT"),
    ("ib_industry", "VARCHAR"),
    ("ib_category", "VARCHAR"),
    ("ib_subcategory", "VARCHAR"),
]


async def _run_migrations(engine: AsyncEngine) -> None:
    """Add new columns to existing tables if they don't already exist.

    This handles the case where tables were created by the old schema and
    the new columns are missing.
    """
    async with engine.begin() as conn:
        for table_name in ("positions_current", "positions_events"):
            for col_name, col_type in _NEW_POSITION_COLUMNS:
                stmt = text(
                    f"ALTER TABLE {table_name} "
                    f"ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                )
                await conn.execute(stmt)
    logger.info("migrations_complete")


async def init_db(postgres_url: str) -> None:
    """Create tables if they do not already exist, then run migrations."""
    engine = _get_engine(postgres_url)
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    await _run_migrations(engine)
    logger.info("database_initialised")


async def clear_stale_data() -> None:
    """Truncate position and account data so only fresh IB data is shown."""
    if _engine is None:
        return
    async with _engine.begin() as conn:
        await conn.execute(text("TRUNCATE positions_current, account_summary"))
    logger.info("stale_data_cleared")


async def close_db() -> None:
    """Dispose of the connection pool."""
    global _engine
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        logger.info("database_closed")


# ---------------------------------------------------------------------------
# Upsert helpers
# ---------------------------------------------------------------------------

_UPSERT_WITH_CONID = text("""
    INSERT INTO positions_current
        (account, conid, symbol, sec_type, currency, exchange,
         position, avg_cost, market_price, market_value,
         unrealized_pnl, realized_pnl, daily_pnl,
         sector, country, ib_industry, ib_category, ib_subcategory,
         updated_at)
    VALUES
        (:account, :conid, :symbol, :sec_type, :currency, :exchange,
         :position, :avg_cost, :market_price, :market_value,
         :unrealized_pnl, :realized_pnl, :daily_pnl,
         :sector, :country, :ib_industry, :ib_category, :ib_subcategory,
         :updated_at)
    ON CONFLICT (account, conid) WHERE conid IS NOT NULL
    DO UPDATE SET
        symbol          = EXCLUDED.symbol,
        sec_type        = EXCLUDED.sec_type,
        currency        = EXCLUDED.currency,
        exchange        = EXCLUDED.exchange,
        position        = EXCLUDED.position,
        avg_cost        = EXCLUDED.avg_cost,
        market_price    = COALESCE(EXCLUDED.market_price, positions_current.market_price),
        market_value    = COALESCE(EXCLUDED.market_value, positions_current.market_value),
        unrealized_pnl  = COALESCE(EXCLUDED.unrealized_pnl, positions_current.unrealized_pnl),
        realized_pnl    = COALESCE(EXCLUDED.realized_pnl, positions_current.realized_pnl),
        daily_pnl       = COALESCE(EXCLUDED.daily_pnl, positions_current.daily_pnl),
        sector          = EXCLUDED.sector,
        country         = EXCLUDED.country,
        ib_industry     = COALESCE(EXCLUDED.ib_industry, positions_current.ib_industry),
        ib_category     = COALESCE(EXCLUDED.ib_category, positions_current.ib_category),
        ib_subcategory  = COALESCE(EXCLUDED.ib_subcategory, positions_current.ib_subcategory),
        updated_at      = EXCLUDED.updated_at
""")

_UPSERT_WITHOUT_CONID = text("""
    INSERT INTO positions_current
        (account, conid, symbol, sec_type, currency, exchange,
         position, avg_cost, market_price, market_value,
         unrealized_pnl, realized_pnl, daily_pnl,
         sector, country, ib_industry, ib_category, ib_subcategory,
         updated_at)
    VALUES
        (:account, :conid, :symbol, :sec_type, :currency, :exchange,
         :position, :avg_cost, :market_price, :market_value,
         :unrealized_pnl, :realized_pnl, :daily_pnl,
         :sector, :country, :ib_industry, :ib_category, :ib_subcategory,
         :updated_at)
    ON CONFLICT (account, symbol, sec_type, currency, exchange)
        WHERE conid IS NULL
    DO UPDATE SET
        position        = EXCLUDED.position,
        avg_cost        = EXCLUDED.avg_cost,
        market_price    = COALESCE(EXCLUDED.market_price, positions_current.market_price),
        market_value    = COALESCE(EXCLUDED.market_value, positions_current.market_value),
        unrealized_pnl  = COALESCE(EXCLUDED.unrealized_pnl, positions_current.unrealized_pnl),
        realized_pnl    = COALESCE(EXCLUDED.realized_pnl, positions_current.realized_pnl),
        daily_pnl       = COALESCE(EXCLUDED.daily_pnl, positions_current.daily_pnl),
        sector          = EXCLUDED.sector,
        country         = EXCLUDED.country,
        ib_industry     = COALESCE(EXCLUDED.ib_industry, positions_current.ib_industry),
        ib_category     = COALESCE(EXCLUDED.ib_category, positions_current.ib_category),
        ib_subcategory  = COALESCE(EXCLUDED.ib_subcategory, positions_current.ib_subcategory),
        updated_at      = EXCLUDED.updated_at
""")

_INSERT_EVENT = text("""
    INSERT INTO positions_events
        (ts_utc, account, conid, symbol, sec_type, currency, exchange,
         position, avg_cost, market_price, market_value,
         unrealized_pnl, realized_pnl, daily_pnl,
         sector, country, ib_industry, ib_category, ib_subcategory)
    VALUES
        (:ts_utc, :account, :conid, :symbol, :sec_type, :currency, :exchange,
         :position, :avg_cost, :market_price, :market_value,
         :unrealized_pnl, :realized_pnl, :daily_pnl,
         :sector, :country, :ib_industry, :ib_category, :ib_subcategory)
""")

_UPSERT_ACCOUNT_SUMMARY = text("""
    INSERT INTO account_summary (account, tag, value, currency, updated_at)
    VALUES (:account, :tag, :value, :currency, :updated_at)
    ON CONFLICT (account, tag)
    DO UPDATE SET
        value      = EXCLUDED.value,
        currency   = EXCLUDED.currency,
        updated_at = EXCLUDED.updated_at
""")

_INSERT_ACCOUNT_SUMMARY_EVENT = text("""
    INSERT INTO account_summary_events (account, tag, value, currency)
    VALUES (:account, :tag, :value, :currency)
""")

_UPDATE_MARKET_VALUES = text("""
    UPDATE positions_current
    SET market_price    = :market_price,
        market_value    = :market_value,
        unrealized_pnl  = :unrealized_pnl,
        realized_pnl    = :realized_pnl,
        daily_pnl       = :daily_pnl,
        updated_at      = :updated_at
    WHERE account = :account AND conid = :conid
""")

_UPSERT_EXECUTION = text("""
    INSERT INTO executions
        (exec_id, account, conid, symbol, sec_type, currency, exchange,
         side, order_type, quantity, filled_qty, avg_fill_price,
         lmt_price, commission, status, order_ref, exec_time)
    VALUES
        (:exec_id, :account, :conid, :symbol, :sec_type, :currency, :exchange,
         :side, :order_type, :quantity, :filled_qty, :avg_fill_price,
         :lmt_price, :commission, :status, :order_ref, :exec_time)
    ON CONFLICT (exec_id)
    DO UPDATE SET
        filled_qty      = EXCLUDED.filled_qty,
        avg_fill_price  = EXCLUDED.avg_fill_price,
        commission      = EXCLUDED.commission,
        status          = EXCLUDED.status
""")

_UPDATE_ENRICHMENT = text("""
    UPDATE positions_current
    SET sector     = :sector,
        country    = :country,
        updated_at = :updated_at
    WHERE conid = :conid
""")


async def upsert_position(event: PositionEvent) -> None:
    """Upsert a position into positions_current and append to positions_events."""
    engine = _get_engine("")  # engine already initialised; url ignored
    now = datetime.now(timezone.utc)

    params: dict[str, Any] = {
        "account": event.account,
        "conid": event.conid,
        "symbol": event.symbol,
        "sec_type": event.sec_type,
        "currency": event.currency,
        "exchange": event.exchange,
        "position": event.position,
        "avg_cost": event.avg_cost,
        "market_price": event.market_price,
        "market_value": event.market_value,
        "unrealized_pnl": event.unrealized_pnl,
        "realized_pnl": event.realized_pnl,
        "daily_pnl": event.daily_pnl,
        "sector": event.sector,
        "country": event.country,
        "ib_industry": event.ib_industry,
        "ib_category": event.ib_category,
        "ib_subcategory": event.ib_subcategory,
        "updated_at": now,
        "ts_utc": datetime.fromisoformat(event.ts_utc),
    }

    upsert_stmt = _UPSERT_WITH_CONID if event.conid is not None else _UPSERT_WITHOUT_CONID

    async with engine.begin() as conn:
        await conn.execute(upsert_stmt, params)
        await conn.execute(_INSERT_EVENT, params)

    logger.debug(
        "position_upserted",
        account=event.account,
        symbol=event.symbol,
        conid=event.conid,
    )


async def upsert_account_summary(
    account: str,
    tag: str,
    value: str,
    currency: str | None,
) -> None:
    """Upsert into account_summary and append to account_summary_events."""
    engine = _get_engine("")
    now = datetime.now(timezone.utc)

    params: dict[str, Any] = {
        "account": account,
        "tag": tag,
        "value": value,
        "currency": currency,
        "updated_at": now,
    }

    async with engine.begin() as conn:
        await conn.execute(_UPSERT_ACCOUNT_SUMMARY, params)
        await conn.execute(_INSERT_ACCOUNT_SUMMARY_EVENT, {
            "account": account,
            "tag": tag,
            "value": value,
            "currency": currency,
        })

    logger.debug("account_summary_upserted", account=account, tag=tag)


async def update_market_values(
    account: str,
    conid: int,
    market_price: float | None,
    market_value: float | None,
    unrealized_pnl: float | None,
    realized_pnl: float | None,
    daily_pnl: float | None = None,
) -> None:
    """Update market value fields on an existing positions_current row."""
    engine = _get_engine("")
    now = datetime.now(timezone.utc)

    params: dict[str, Any] = {
        "account": account,
        "conid": conid,
        "market_price": market_price,
        "market_value": market_value,
        "unrealized_pnl": unrealized_pnl,
        "realized_pnl": realized_pnl,
        "daily_pnl": daily_pnl,
        "updated_at": now,
    }

    async with engine.begin() as conn:
        await conn.execute(_UPDATE_MARKET_VALUES, params)

    logger.debug("market_values_updated", account=account, conid=conid)


async def update_enrichment(
    conid: int,
    sector: str,
    country: str,
) -> None:
    """Update sector and country for a position after contract details are fetched."""
    engine = _get_engine("")
    now = datetime.now(timezone.utc)

    async with engine.begin() as conn:
        await conn.execute(_UPDATE_ENRICHMENT, {
            "conid": conid,
            "sector": sector,
            "country": country,
            "updated_at": now,
        })

    logger.debug("enrichment_updated", conid=conid, sector=sector, country=country)


async def upsert_execution(event: ExecutionEvent) -> None:
    """Upsert an execution into the executions table."""
    engine = _get_engine("")
    params: dict[str, Any] = {
        "exec_id": event.exec_id,
        "account": event.account,
        "conid": event.conid,
        "symbol": event.symbol,
        "sec_type": event.sec_type,
        "currency": event.currency,
        "exchange": event.exchange,
        "side": event.side,
        "order_type": event.order_type,
        "quantity": event.quantity,
        "filled_qty": event.filled_qty,
        "avg_fill_price": event.avg_fill_price,
        "lmt_price": event.lmt_price,
        "commission": event.commission,
        "status": event.status,
        "order_ref": event.order_ref,
        "exec_time": datetime.fromisoformat(event.exec_time)
        if isinstance(event.exec_time, str)
        else event.exec_time,
    }
    async with engine.begin() as conn:
        await conn.execute(_UPSERT_EXECUTION, params)
    logger.debug("execution_upserted", exec_id=event.exec_id, symbol=event.symbol)


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


async def get_all_positions() -> list[dict[str, Any]]:
    """Return every row in positions_current as a list of dicts."""
    engine = _get_engine("")
    async with engine.connect() as conn:
        result = await conn.execute(positions_current.select())
        rows = result.mappings().all()
        return [dict(row) for row in rows]


async def get_account_summary(account: str | None = None) -> list[dict[str, Any]]:
    """Return account summary rows, optionally filtered by account."""
    engine = _get_engine("")
    async with engine.connect() as conn:
        if account is not None:
            stmt = account_summary.select().where(
                account_summary.c.account == account
            )
        else:
            stmt = account_summary.select()
        result = await conn.execute(stmt)
        rows = result.mappings().all()
        return [dict(row) for row in rows]
