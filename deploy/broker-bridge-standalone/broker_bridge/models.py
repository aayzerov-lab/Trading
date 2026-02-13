"""Pydantic models for broker-bridge domain events."""

from __future__ import annotations

from pydantic import BaseModel


class PositionEvent(BaseModel):
    """Represents a single position snapshot received from Interactive Brokers.

    Fields mirror the IB position callback values augmented with enrichment
    metadata (sector, country).
    """

    ts_utc: str  # ISO 8601 timestamp
    account: str
    conid: int | None = None
    symbol: str
    sec_type: str
    currency: str
    exchange: str | None = None
    position: float
    avg_cost: float | None = None
    market_price: float | None = None
    market_value: float | None = None
    unrealized_pnl: float | None = None
    realized_pnl: float | None = None
    daily_pnl: float | None = None
    sector: str = "Unknown"
    country: str = "Unknown"
    ib_industry: str | None = None
    ib_category: str | None = None
    ib_subcategory: str | None = None


class ExecutionEvent(BaseModel):
    """Represents an order execution from Interactive Brokers."""

    exec_id: str
    account: str
    conid: int | None = None
    symbol: str
    sec_type: str
    currency: str
    exchange: str | None = None
    side: str  # "BUY" or "SELL"
    order_type: str  # "LMT", "MKT", etc.
    quantity: float
    filled_qty: float
    avg_fill_price: float | None = None
    lmt_price: float | None = None
    commission: float | None = None
    status: str  # "Submitted", "Filled", "Cancelled", etc.
    order_ref: str | None = None
    exec_time: str  # ISO 8601 timestamp
