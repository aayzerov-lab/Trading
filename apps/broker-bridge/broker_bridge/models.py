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
    sector: str = "Unknown"
    country: str = "Unknown"
    ib_industry: str | None = None
    ib_category: str | None = None
    ib_subcategory: str | None = None
