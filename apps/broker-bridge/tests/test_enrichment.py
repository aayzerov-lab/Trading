"""Tests for broker_bridge.enrichment.enrich().

The module-level _mappings dict is patched directly so we never depend on
the security_master.json file on disk.
"""

from unittest.mock import patch

from broker_bridge.models import PositionEvent
from broker_bridge import enrichment


def _make_event(**overrides) -> PositionEvent:
    """Helper to build a PositionEvent with sensible defaults."""
    defaults = {
        "ts_utc": "2025-06-01T12:00:00Z",
        "account": "U12345",
        "symbol": "AAPL",
        "sec_type": "STK",
        "currency": "USD",
        "position": 100.0,
    }
    defaults.update(overrides)
    return PositionEvent(**defaults)


def test_enrich_conid_lookup_hit():
    """When the conid is present in the mappings, sector and country are
    populated from the conid entry."""
    mappings = {
        "265598": {"sector": "Technology", "country": "US"},
    }

    with patch.object(enrichment, "_mappings", mappings):
        event = _make_event(conid=265598)
        enriched = enrichment.enrich(event)

    assert enriched.sector == "Technology"
    assert enriched.country == "US"


def test_enrich_composite_key_lookup_hit():
    """When conid is None, the composite key {symbol}:{sec_type}:{currency}
    is used for lookup."""
    mappings = {
        "MSFT:STK:USD": {"sector": "Technology", "country": "US"},
    }

    with patch.object(enrichment, "_mappings", mappings):
        event = _make_event(symbol="MSFT", conid=None)
        enriched = enrichment.enrich(event)

    assert enriched.sector == "Technology"
    assert enriched.country == "US"


def test_enrich_no_hit_returns_unknown():
    """When neither conid nor composite key match, sector and country
    default to 'Unknown'."""
    mappings = {}  # empty mappings

    with patch.object(enrichment, "_mappings", mappings):
        event = _make_event(conid=999999)
        enriched = enrichment.enrich(event)

    assert enriched.sector == "Unknown"
    assert enriched.country == "Unknown"


def test_conid_lookup_takes_priority_over_composite_key():
    """When both the conid key and composite key exist in mappings,
    the conid entry must win."""
    mappings = {
        "265598": {"sector": "Technology", "country": "US"},
        "AAPL:STK:USD": {"sector": "Consumer Electronics", "country": "IE"},
    }

    with patch.object(enrichment, "_mappings", mappings):
        event = _make_event(conid=265598, symbol="AAPL")
        enriched = enrichment.enrich(event)

    # conid entry should win
    assert enriched.sector == "Technology"
    assert enriched.country == "US"


def test_enrich_composite_key_used_when_conid_not_in_mappings():
    """When conid is provided but not found in mappings, the composite key
    is used as a fallback."""
    mappings = {
        "AAPL:STK:USD": {"sector": "Consumer Electronics", "country": "IE"},
    }

    with patch.object(enrichment, "_mappings", mappings):
        event = _make_event(conid=999999, symbol="AAPL")
        enriched = enrichment.enrich(event)

    assert enriched.sector == "Consumer Electronics"
    assert enriched.country == "IE"


def test_enrich_returns_copy_not_mutation():
    """enrich() should return a new PositionEvent, not mutate the original."""
    mappings = {
        "265598": {"sector": "Technology", "country": "US"},
    }

    with patch.object(enrichment, "_mappings", mappings):
        event = _make_event(conid=265598)
        enriched = enrichment.enrich(event)

    assert enriched is not event
    assert event.sector == "Unknown"  # original unchanged
    assert enriched.sector == "Technology"


def test_enrich_mapping_missing_sector_defaults_to_unknown():
    """If a mapping entry exists but lacks the 'sector' key, it should
    default to 'Unknown'."""
    mappings = {
        "265598": {"country": "US"},  # no 'sector' key
    }

    with patch.object(enrichment, "_mappings", mappings):
        event = _make_event(conid=265598)
        enriched = enrichment.enrich(event)

    assert enriched.sector == "Unknown"
    assert enriched.country == "US"
