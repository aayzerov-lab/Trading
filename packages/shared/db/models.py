"""Phase 1 database models for market data, factors, and risk results.

These tables use a separate MetaData instance (phase1_metadata) to avoid
conflicts with the broker-bridge tables that are managed separately.
"""

from __future__ import annotations

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    text,
)

# Separate metadata for phase 1 data tables
phase1_metadata = MetaData()

# ---------------------------------------------------------------------------
# Price data tables
# ---------------------------------------------------------------------------

prices_daily = Table(
    "prices_daily",
    phase1_metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("symbol", String, nullable=False),
    Column("conid", Integer, nullable=True),
    Column("date", Date, nullable=False),
    Column("close", Float, nullable=False),
    Column("adj_close", Float, nullable=True),
    Column("source", String, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("symbol", "date", name="uq_prices_daily_symbol_date"),
)

factor_prices_daily = Table(
    "factor_prices_daily",
    phase1_metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("symbol", String, nullable=False),
    Column("date", Date, nullable=False),
    Column("close", Float, nullable=False),
    Column("adj_close", Float, nullable=True),
    Column("source", String, nullable=False, server_default=text("'yahoo'")),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("symbol", "date", name="uq_factor_prices_daily_symbol_date"),
)

# ---------------------------------------------------------------------------
# FRED economic data
# ---------------------------------------------------------------------------

fred_series_daily = Table(
    "fred_series_daily",
    phase1_metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("series_id", String, nullable=False),
    Column("date", Date, nullable=False),
    Column("value", Float, nullable=False),
    Column("source", String, nullable=False, server_default=text("'fred'")),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("series_id", "date", name="uq_fred_series_daily_series_date"),
)

# ---------------------------------------------------------------------------
# Data synchronization tracking
# ---------------------------------------------------------------------------

data_sync_status = Table(
    "data_sync_status",
    phase1_metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source", String, nullable=False),
    Column("symbol", String, nullable=False),
    Column("last_date", Date, nullable=True),
    Column("last_fetched_at", DateTime(timezone=True), nullable=True),
    UniqueConstraint("source", "symbol", name="uq_data_sync_status_source_symbol"),
)

# ---------------------------------------------------------------------------
# Risk computation results
# ---------------------------------------------------------------------------

risk_results = Table(
    "risk_results",
    phase1_metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("asof_date", Date, nullable=False),
    Column("asof_ts", DateTime(timezone=True), nullable=False),
    Column("window", Integer, nullable=False),
    Column("method", String, nullable=False),
    Column("portfolio_hash", String, nullable=False),
    Column("result_type", String, nullable=False),
    Column("result_json", Text, nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    UniqueConstraint(
        "asof_date",
        "window",
        "method",
        "portfolio_hash",
        "result_type",
        name="uq_risk_results_key",
    ),
)
