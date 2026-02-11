"""Tests for api_server.exposures.compute_exposures()."""

from api_server.exposures import compute_exposures


# ---------------------------------------------------------------------------
# Cost-basis method tests (backwards-compatible with original behavior)
# ---------------------------------------------------------------------------


def test_multiple_sectors_and_countries():
    """Positions spanning different sectors and countries produce separate
    weight entries for each bucket."""
    positions = [
        {"symbol": "AAPL", "position": 10, "avg_cost": 150.0, "sector": "Technology", "country": "US"},
        {"symbol": "JPM", "position": 5, "avg_cost": 200.0, "sector": "Financials", "country": "US"},
        {"symbol": "NVO", "position": 20, "avg_cost": 50.0, "sector": "Healthcare", "country": "DK"},
    ]
    result = compute_exposures(positions, method="cost_basis")

    sector_names = {s["name"] for s in result["by_sector"]}
    country_names = {c["name"] for c in result["by_country"]}

    assert sector_names == {"Technology", "Financials", "Healthcare"}
    assert country_names == {"US", "DK"}


def test_unknown_bucket_included():
    """Positions with missing or 'Unknown' sector/country are grouped under
    the 'Unknown' bucket."""
    positions = [
        {"symbol": "XYZ", "position": 10, "avg_cost": 100.0, "sector": "Unknown", "country": "Unknown"},
        {"symbol": "AAPL", "position": 10, "avg_cost": 100.0, "sector": "Technology", "country": "US"},
    ]
    result = compute_exposures(positions, method="cost_basis")

    sector_names = {s["name"] for s in result["by_sector"]}
    assert "Unknown" in sector_names
    assert "Technology" in sector_names

    country_names = {c["name"] for c in result["by_country"]}
    assert "Unknown" in country_names
    assert "US" in country_names


def test_zero_avg_cost_contributes_zero_notional():
    """Positions with avg_cost=0 (or None) should contribute 0 notional."""
    positions = [
        {"symbol": "AAPL", "position": 10, "avg_cost": 100.0, "sector": "Technology", "country": "US"},
        {"symbol": "FREE", "position": 50, "avg_cost": 0, "sector": "Other", "country": "US"},
        {"symbol": "GIFT", "position": 50, "avg_cost": None, "sector": "Other", "country": "US"},
    ]
    result = compute_exposures(positions, method="cost_basis")

    # Total should only come from AAPL: abs(10 * 100) = 1000
    assert result["total_gross_exposure"] == 1000.0

    # The 'Other' sector should have 0 notional and 0% weight
    other_sector = next(s for s in result["by_sector"] if s["name"] == "Other")
    assert other_sector["notional"] == 0.0
    assert other_sector["weight"] == 0.0


def test_weights_sum_to_100():
    """Sector and country weights should each sum to 100%."""
    positions = [
        {"symbol": "AAPL", "position": 10, "avg_cost": 150.0, "sector": "Technology", "country": "US"},
        {"symbol": "JPM", "position": 5, "avg_cost": 200.0, "sector": "Financials", "country": "US"},
        {"symbol": "NVO", "position": 20, "avg_cost": 50.0, "sector": "Healthcare", "country": "DK"},
        {"symbol": "SAP", "position": 8, "avg_cost": 180.0, "sector": "Technology", "country": "DE"},
    ]
    result = compute_exposures(positions, method="cost_basis")

    sector_weight_sum = sum(s["weight"] for s in result["by_sector"])
    country_weight_sum = sum(c["weight"] for c in result["by_country"])

    assert abs(sector_weight_sum - 100.0) < 0.1
    assert abs(country_weight_sum - 100.0) < 0.1


def test_empty_positions_list():
    """An empty positions list should return zero total with empty
    sector/country lists."""
    result = compute_exposures([], method="cost_basis")

    assert result["total_gross_exposure"] == 0.0
    assert result["by_sector"] == []
    assert result["by_country"] == []
    assert result["weighting_method"] == "cost_basis"


def test_sorting_by_weight_descending():
    """Sectors and countries should be sorted by weight in descending order."""
    positions = [
        {"symbol": "AAPL", "position": 10, "avg_cost": 100.0, "sector": "Technology", "country": "US"},
        {"symbol": "JPM", "position": 30, "avg_cost": 100.0, "sector": "Financials", "country": "US"},
        {"symbol": "NVO", "position": 5, "avg_cost": 100.0, "sector": "Healthcare", "country": "DK"},
    ]
    result = compute_exposures(positions, method="cost_basis")

    # Financials (3000) > Technology (1000) > Healthcare (500)
    sector_weights = [s["weight"] for s in result["by_sector"]]
    assert sector_weights == sorted(sector_weights, reverse=True)

    # US (4000) > DK (500)
    country_weights = [c["weight"] for c in result["by_country"]]
    assert country_weights == sorted(country_weights, reverse=True)


def test_single_position_100_percent_weight():
    """A single position should have 100% weight for its sector and country."""
    positions = [
        {"symbol": "AAPL", "position": 10, "avg_cost": 150.0, "sector": "Technology", "country": "US"},
    ]
    result = compute_exposures(positions, method="cost_basis")

    assert len(result["by_sector"]) == 1
    assert result["by_sector"][0]["name"] == "Technology"
    assert result["by_sector"][0]["weight"] == 100.0

    assert len(result["by_country"]) == 1
    assert result["by_country"][0]["name"] == "US"
    assert result["by_country"][0]["weight"] == 100.0


def test_weighting_method_field_present():
    """The result dict must contain a weighting_method field."""
    result = compute_exposures([], method="cost_basis")
    assert "weighting_method" in result
    assert result["weighting_method"] == "cost_basis"

    result_mv = compute_exposures([], method="market_value")
    assert result_mv["weighting_method"] == "market_value"


def test_notional_uses_absolute_value():
    """Negative positions (short) should contribute positive notional
    via abs(position * avg_cost)."""
    positions = [
        {"symbol": "TSLA", "position": -10, "avg_cost": 200.0, "sector": "Auto", "country": "US"},
    ]
    result = compute_exposures(positions, method="cost_basis")

    assert result["total_gross_exposure"] == 2000.0
    assert result["by_sector"][0]["notional"] == 2000.0
    assert result["by_sector"][0]["weight"] == 100.0


def test_none_sector_and_country_become_unknown():
    """Positions where sector or country is None should be grouped as
    'Unknown'."""
    positions = [
        {"symbol": "XYZ", "position": 10, "avg_cost": 100.0, "sector": None, "country": None},
    ]
    result = compute_exposures(positions, method="cost_basis")

    assert result["by_sector"][0]["name"] == "Unknown"
    assert result["by_country"][0]["name"] == "Unknown"


# ---------------------------------------------------------------------------
# Cash filtering tests
# ---------------------------------------------------------------------------


def test_cash_positions_excluded():
    """Positions with sec_type='CASH' should be entirely excluded from
    sector and country calculations."""
    positions = [
        {"symbol": "AAPL", "position": 10, "avg_cost": 100.0, "sector": "Technology", "country": "US", "sec_type": "STK"},
        {"symbol": "USD", "position": 100000, "avg_cost": 1.0, "sector": "Unknown", "country": "US", "sec_type": "CASH"},
    ]
    result = compute_exposures(positions, method="cost_basis")

    # Only AAPL should contribute
    assert result["total_gross_exposure"] == 1000.0
    sector_names = {s["name"] for s in result["by_sector"]}
    assert "Unknown" not in sector_names
    assert "Technology" in sector_names


def test_cash_positions_excluded_case_insensitive():
    """CASH filtering should be case-insensitive."""
    positions = [
        {"symbol": "AAPL", "position": 10, "avg_cost": 100.0, "sector": "Technology", "country": "US", "sec_type": "STK"},
        {"symbol": "EUR", "position": 5000, "avg_cost": 1.1, "sector": "Unknown", "country": "EU", "sec_type": "cash"},
    ]
    result = compute_exposures(positions, method="cost_basis")

    assert result["total_gross_exposure"] == 1000.0


def test_all_cash_positions_gives_empty():
    """If every position is cash, result should be empty."""
    positions = [
        {"symbol": "USD", "position": 100000, "avg_cost": 1.0, "sector": "Unknown", "country": "US", "sec_type": "CASH"},
    ]
    result = compute_exposures(positions, method="cost_basis")

    assert result["total_gross_exposure"] == 0.0
    assert result["by_sector"] == []
    assert result["by_country"] == []


# ---------------------------------------------------------------------------
# Market-value weighting tests
# ---------------------------------------------------------------------------


def test_market_value_method_uses_market_value():
    """market_value method should prefer the market_value field."""
    positions = [
        {
            "symbol": "AAPL",
            "position": 10,
            "avg_cost": 100.0,
            "market_value": 1500.0,
            "sector": "Technology",
            "country": "US",
        },
    ]
    result = compute_exposures(positions, method="market_value")

    # Should use abs(market_value) = 1500, not abs(10*100) = 1000
    assert result["total_gross_exposure"] == 1500.0
    assert result["weighting_method"] == "market_value"


def test_market_value_method_fallback_when_zero():
    """market_value method falls back to cost basis when market_value is 0."""
    positions = [
        {
            "symbol": "AAPL",
            "position": 10,
            "avg_cost": 100.0,
            "market_value": 0,
            "sector": "Technology",
            "country": "US",
        },
    ]
    result = compute_exposures(positions, method="market_value")

    # Fallback: abs(10 * 100) = 1000
    assert result["total_gross_exposure"] == 1000.0


def test_market_value_method_fallback_when_none():
    """market_value method falls back to cost basis when market_value is None."""
    positions = [
        {
            "symbol": "AAPL",
            "position": 10,
            "avg_cost": 100.0,
            "market_value": None,
            "sector": "Technology",
            "country": "US",
        },
    ]
    result = compute_exposures(positions, method="market_value")

    # Fallback: abs(10 * 100) = 1000
    assert result["total_gross_exposure"] == 1000.0


def test_market_value_method_negative_market_value():
    """market_value method should use abs(market_value) for short positions."""
    positions = [
        {
            "symbol": "TSLA",
            "position": -10,
            "avg_cost": 200.0,
            "market_value": -2500.0,
            "sector": "Auto",
            "country": "US",
        },
    ]
    result = compute_exposures(positions, method="market_value")

    assert result["total_gross_exposure"] == 2500.0


def test_cost_basis_method_ignores_market_value():
    """cost_basis method should always use abs(position * avg_cost),
    even when market_value is present."""
    positions = [
        {
            "symbol": "AAPL",
            "position": 10,
            "avg_cost": 100.0,
            "market_value": 1500.0,
            "sector": "Technology",
            "country": "US",
        },
    ]
    result = compute_exposures(positions, method="cost_basis")

    # Should use abs(10 * 100) = 1000, NOT 1500
    assert result["total_gross_exposure"] == 1000.0
    assert result["weighting_method"] == "cost_basis"


def test_default_method_is_market_value():
    """When no method is specified, market_value should be the default."""
    positions = [
        {
            "symbol": "AAPL",
            "position": 10,
            "avg_cost": 100.0,
            "market_value": 1500.0,
            "sector": "Technology",
            "country": "US",
        },
    ]
    result = compute_exposures(positions)

    assert result["weighting_method"] == "market_value"
    assert result["total_gross_exposure"] == 1500.0
