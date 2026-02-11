"""Tests for broker_bridge.db upsert logic.

These tests verify the parameter construction and SQL statement selection
logic without requiring a live PostgreSQL connection.
"""

from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from broker_bridge.models import PositionEvent
from broker_bridge import db


def _make_event(**overrides) -> PositionEvent:
    """Helper to build a PositionEvent with sensible defaults."""
    defaults = {
        "ts_utc": "2025-06-01T12:00:00Z",
        "account": "U12345",
        "symbol": "AAPL",
        "sec_type": "STK",
        "currency": "USD",
        "position": 100.0,
        "avg_cost": 150.25,
        "sector": "Technology",
        "country": "US",
        "exchange": "SMART",
    }
    defaults.update(overrides)
    return PositionEvent(**defaults)


def test_upsert_selects_conid_statement_when_conid_present():
    """When conid is not None, the _UPSERT_WITH_CONID SQL should be chosen."""
    event = _make_event(conid=265598)
    stmt = db._UPSERT_WITH_CONID if event.conid is not None else db._UPSERT_WITHOUT_CONID
    assert stmt is db._UPSERT_WITH_CONID


def test_upsert_selects_natural_key_statement_when_conid_none():
    """When conid is None, the _UPSERT_WITHOUT_CONID SQL should be chosen."""
    event = _make_event(conid=None)
    stmt = db._UPSERT_WITH_CONID if event.conid is not None else db._UPSERT_WITHOUT_CONID
    assert stmt is db._UPSERT_WITHOUT_CONID


def test_params_dict_constructed_from_event_with_conid():
    """Verify the params dict built from a PositionEvent with conid contains
    all expected keys and values."""
    event = _make_event(
        conid=265598,
        symbol="AAPL",
        account="U12345",
        sec_type="STK",
        currency="USD",
        exchange="SMART",
        position=100.0,
        avg_cost=150.25,
        sector="Technology",
        country="US",
    )

    params = {
        "account": event.account,
        "conid": event.conid,
        "symbol": event.symbol,
        "sec_type": event.sec_type,
        "currency": event.currency,
        "exchange": event.exchange,
        "position": event.position,
        "avg_cost": event.avg_cost,
        "sector": event.sector,
        "country": event.country,
        "ts_utc": event.ts_utc,
    }

    assert params["account"] == "U12345"
    assert params["conid"] == 265598
    assert params["symbol"] == "AAPL"
    assert params["sec_type"] == "STK"
    assert params["currency"] == "USD"
    assert params["exchange"] == "SMART"
    assert params["position"] == 100.0
    assert params["avg_cost"] == 150.25
    assert params["sector"] == "Technology"
    assert params["country"] == "US"
    assert params["ts_utc"] == "2025-06-01T12:00:00Z"


def test_params_dict_constructed_from_event_without_conid():
    """Verify the params dict built from a PositionEvent without conid has
    conid=None."""
    event = _make_event(conid=None)

    params = {
        "account": event.account,
        "conid": event.conid,
        "symbol": event.symbol,
        "sec_type": event.sec_type,
        "currency": event.currency,
        "exchange": event.exchange,
        "position": event.position,
        "avg_cost": event.avg_cost,
        "sector": event.sector,
        "country": event.country,
        "ts_utc": event.ts_utc,
    }

    assert params["conid"] is None
    assert params["account"] == "U12345"
    assert params["symbol"] == "AAPL"


def test_upsert_with_conid_sql_contains_conflict_clause():
    """The _UPSERT_WITH_CONID statement text should reference the
    (account, conid) conflict target."""
    sql_text = str(db._UPSERT_WITH_CONID.text)
    assert "ON CONFLICT (account, conid)" in sql_text
    assert "WHERE conid IS NOT NULL" in sql_text


def test_upsert_without_conid_sql_contains_natural_key_conflict():
    """The _UPSERT_WITHOUT_CONID statement text should reference the
    natural key conflict target (account, symbol, sec_type, currency, exchange)."""
    sql_text = str(db._UPSERT_WITHOUT_CONID.text)
    assert "ON CONFLICT (account, symbol, sec_type, currency, exchange)" in sql_text
    assert "WHERE conid IS NULL" in sql_text


def test_upsert_position_function_signature():
    """upsert_position should be an async function accepting a PositionEvent."""
    import inspect

    assert inspect.iscoroutinefunction(db.upsert_position)

    sig = inspect.signature(db.upsert_position)
    params = list(sig.parameters.keys())
    assert params == ["event"]


@pytest.mark.asyncio
async def test_upsert_position_calls_engine_with_conid():
    """upsert_position should execute the conid upsert statement when conid
    is present, and also insert into positions_events."""
    event = _make_event(conid=265598)

    mock_conn = AsyncMock()
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_engine.begin.return_value.__aexit__ = AsyncMock(return_value=False)

    with patch.object(db, "_get_engine", return_value=mock_engine):
        await db.upsert_position(event)

    # Should have been called twice: once for upsert, once for event insert
    assert mock_conn.execute.call_count == 2

    # First call should use _UPSERT_WITH_CONID
    first_call_args = mock_conn.execute.call_args_list[0]
    assert first_call_args[0][0] is db._UPSERT_WITH_CONID

    # Second call should use _INSERT_EVENT
    second_call_args = mock_conn.execute.call_args_list[1]
    assert second_call_args[0][0] is db._INSERT_EVENT


@pytest.mark.asyncio
async def test_upsert_position_calls_engine_without_conid():
    """upsert_position should execute the natural key upsert statement when
    conid is None."""
    event = _make_event(conid=None)

    mock_conn = AsyncMock()
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_engine.begin.return_value.__aexit__ = AsyncMock(return_value=False)

    with patch.object(db, "_get_engine", return_value=mock_engine):
        await db.upsert_position(event)

    assert mock_conn.execute.call_count == 2

    # First call should use _UPSERT_WITHOUT_CONID
    first_call_args = mock_conn.execute.call_args_list[0]
    assert first_call_args[0][0] is db._UPSERT_WITHOUT_CONID
