"""Tests for broker_bridge.models.PositionEvent."""

from broker_bridge.models import PositionEvent


def test_position_event_all_fields():
    """PositionEvent can be constructed with every field supplied explicitly."""
    event = PositionEvent(
        ts_utc="2025-06-01T12:00:00Z",
        account="U12345",
        conid=265598,
        symbol="AAPL",
        sec_type="STK",
        currency="USD",
        exchange="SMART",
        position=100.0,
        avg_cost=150.25,
        sector="Technology",
        country="US",
    )

    assert event.ts_utc == "2025-06-01T12:00:00Z"
    assert event.account == "U12345"
    assert event.conid == 265598
    assert event.symbol == "AAPL"
    assert event.sec_type == "STK"
    assert event.currency == "USD"
    assert event.exchange == "SMART"
    assert event.position == 100.0
    assert event.avg_cost == 150.25
    assert event.sector == "Technology"
    assert event.country == "US"


def test_position_event_minimal_fields():
    """PositionEvent can be constructed with only the required fields;
    optional fields receive their defaults."""
    event = PositionEvent(
        ts_utc="2025-06-01T12:00:00Z",
        account="U12345",
        symbol="AAPL",
        sec_type="STK",
        currency="USD",
        position=50.0,
    )

    assert event.ts_utc == "2025-06-01T12:00:00Z"
    assert event.account == "U12345"
    assert event.symbol == "AAPL"
    assert event.sec_type == "STK"
    assert event.currency == "USD"
    assert event.position == 50.0

    # Defaults
    assert event.conid is None
    assert event.exchange is None
    assert event.avg_cost is None
    assert event.sector == "Unknown"
    assert event.country == "Unknown"


def test_conid_can_be_none():
    """conid accepts None (the default) to represent positions without a
    contract identifier."""
    event = PositionEvent(
        ts_utc="2025-06-01T12:00:00Z",
        account="U12345",
        symbol="EURUSD",
        sec_type="CASH",
        currency="USD",
        position=100000.0,
        conid=None,
    )
    assert event.conid is None


def test_sector_defaults_to_unknown():
    """When sector is not provided it defaults to 'Unknown'."""
    event = PositionEvent(
        ts_utc="2025-06-01T12:00:00Z",
        account="U12345",
        symbol="XYZ",
        sec_type="STK",
        currency="USD",
        position=10.0,
    )
    assert event.sector == "Unknown"


def test_country_defaults_to_unknown():
    """When country is not provided it defaults to 'Unknown'."""
    event = PositionEvent(
        ts_utc="2025-06-01T12:00:00Z",
        account="U12345",
        symbol="XYZ",
        sec_type="STK",
        currency="USD",
        position=10.0,
    )
    assert event.country == "Unknown"
