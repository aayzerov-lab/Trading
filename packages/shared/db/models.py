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

# ---------------------------------------------------------------------------
# FX rates (Phase 1.5)
# ---------------------------------------------------------------------------

fx_daily = Table(
    "fx_daily",
    phase1_metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("pair", String, nullable=False),
    Column("date", Date, nullable=False),
    Column("close", Float, nullable=False),
    Column("adj_close", Float, nullable=True),
    Column("source", String, nullable=False, server_default=text("'yahoo'")),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("pair", "date", name="uq_fx_daily_pair_date"),
)

# ---------------------------------------------------------------------------
# Security overrides (Phase 1.5) – per-symbol currency & listing info
# ---------------------------------------------------------------------------

security_overrides = Table(
    "security_overrides",
    phase1_metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("symbol", String, nullable=False, unique=True),
    Column("currency", String, nullable=False, server_default=text("'USD'")),
    Column("is_usd_listed", Integer, nullable=False, server_default=text("1")),
    Column("fx_pair", String, nullable=True),
    Column("updated_at", DateTime(timezone=True), nullable=True),
)

# ---------------------------------------------------------------------------
# Events – ingested from SEC/EDGAR, FRED schedule, RSS feeds, etc. (Phase 2)
# ---------------------------------------------------------------------------

events = Table(
    "events",
    phase1_metadata,
    Column("id", String, primary_key=True),
    Column("ts_utc", DateTime(timezone=True), nullable=False),
    Column("scheduled_for_utc", DateTime(timezone=True), nullable=True),
    Column("type", String, nullable=False),
    Column("tickers", Text, nullable=True),
    Column("title", Text, nullable=False),
    Column("source_name", String, nullable=True),
    Column("source_url", Text, nullable=True),
    Column("raw_text_snippet", Text, nullable=True),
    Column("severity_score", Integer, server_default=text("0")),
    Column("reason_codes", Text, nullable=True),
    Column("llm_summary", Text, nullable=True),
    Column("status", String, nullable=False, server_default=text("'NEW'")),
    Column("metadata_json", Text, nullable=True),
    Column("created_at_utc", DateTime(timezone=True), server_default=text("now()")),
    Column("updated_at_utc", DateTime(timezone=True), server_default=text("now()")),
)

# ---------------------------------------------------------------------------
# Event sync tracking – per-connector watermark (Phase 2)
# ---------------------------------------------------------------------------

event_sync_status = Table(
    "event_sync_status",
    phase1_metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("connector", String, nullable=False),
    Column("sync_key", String, nullable=False),
    Column("last_sync_at", DateTime(timezone=True), nullable=True),
    Column("last_item_ts", DateTime(timezone=True), nullable=True),
    Column("items_fetched", Integer, server_default=text("0")),
    Column("error_count", Integer, server_default=text("0")),
    Column("last_error", Text, nullable=True),
    UniqueConstraint("connector", "sync_key", name="uq_event_sync_connector_key"),
)

# ---------------------------------------------------------------------------
# Alerts – surfaced to the UI from event scoring / risk spikes (Phase 2)
# ---------------------------------------------------------------------------

alerts = Table(
    "alerts",
    phase1_metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ts_utc", DateTime(timezone=True), nullable=False),
    Column("type", String, nullable=False),
    Column("message", Text, nullable=False),
    Column("severity", Integer, server_default=text("50")),
    Column("related_event_id", String, nullable=True),
    Column("status", String, nullable=False, server_default=text("'NEW'")),
    Column("snoozed_until", DateTime(timezone=True), nullable=True),
    Column("created_at_utc", DateTime(timezone=True), server_default=text("now()")),
)
