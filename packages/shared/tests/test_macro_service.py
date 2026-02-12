import json
from datetime import date, datetime, timezone
from pathlib import Path

from shared.data.macro_service import (
    MacroDataService,
    Observation,
    SeriesData,
    TTLCache,
    compute_changes,
    compute_spread_series,
    find_nearest_prior_value,
    parse_fred_observations,
    FredClient,
)

FIXTURES = Path(__file__).parent / "fixtures"


def test_cache_ttl_expiry():
    now = [100.0]

    def now_fn():
        return now[0]

    cache = TTLCache(maxsize=4, now_fn=now_fn)
    cache.set("a", 123, ttl_seconds=10)
    assert cache.get("a") == 123
    now[0] += 11
    assert cache.get("a") is None


def test_fred_url_construction():
    client = FredClient(api_key="demo", base_url="https://api.stlouisfed.org/fred")
    url = client.build_url(
        "series/observations",
        {
            "series_id": "DGS10",
            "units": "lin",
            "frequency": "d",
            "realtime_start": "1776-07-04",
            "realtime_end": "9999-12-31",
        },
    )
    assert "series/observations" in url
    assert "series_id=DGS10" in url
    assert "units=lin" in url
    assert "frequency=d" in url


def test_parse_fred_observations_skips_missing():
    payload = json.loads((FIXTURES / "fred_observations_dgs10.json").read_text())
    obs = parse_fred_observations(payload)
    assert len(obs) == 2
    assert obs[0].value == 4.0
    assert obs[1].value == 4.1


def test_find_nearest_prior_value():
    observations = [
        Observation(obs_date=date(2024, 1, 1), value=1.0),
        Observation(obs_date=date(2024, 1, 10), value=2.0),
        Observation(obs_date=date(2024, 2, 1), value=3.0),
    ]
    assert find_nearest_prior_value(observations, date(2024, 1, 15)) == 2.0
    assert find_nearest_prior_value(observations, date(2023, 12, 31)) is None


def test_compute_spread_series_alignment():
    a = SeriesData(
        series_id="A",
        observations=[
            Observation(obs_date=date(2024, 1, 1), value=3.0),
            Observation(obs_date=date(2024, 1, 2), value=4.0),
        ],
        fetched_at=datetime.now(timezone.utc),
        meta={},
    )
    b = SeriesData(
        series_id="B",
        observations=[
            Observation(obs_date=date(2024, 1, 2), value=1.0),
            Observation(obs_date=date(2024, 1, 3), value=2.0),
        ],
        fetched_at=datetime.now(timezone.utc),
        meta={},
    )
    spread = compute_spread_series(a, b)
    assert len(spread) == 1
    assert spread[0].obs_date == date(2024, 1, 2)
    assert spread[0].value == 3.0


def test_compute_changes_daily_week_window():
    observations = [
        Observation(obs_date=date(2024, 1, 1), value=1.0),
        Observation(obs_date=date(2024, 1, 2), value=2.0),
        Observation(obs_date=date(2024, 1, 3), value=3.0),
        Observation(obs_date=date(2024, 1, 4), value=4.0),
        Observation(obs_date=date(2024, 1, 5), value=5.0),
        Observation(obs_date=date(2024, 1, 8), value=6.0),
    ]
    changes = compute_changes(observations, ["1W"], "Daily")
    # 1W uses 5-day lookback for daily series; nearest prior value is on 2024-01-03
    assert changes["1W"] == 6.0 - 3.0


def test_revision_detection():
    service = MacroDataService(api_key="test")
    rev1 = service.detect_revision("series_x", date(2024, 1, 1), 100.0)
    assert rev1.revised is False
    rev2 = service.detect_revision("series_x", date(2024, 1, 1), 101.0)
    assert rev2.revised is True
    assert rev2.previous_value == 100.0
