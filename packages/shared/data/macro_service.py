"""Macro data service (FRED-only) with caching, revisions, and computed series."""

from __future__ import annotations

import asyncio
import json
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import structlog

from .macro_series import MACRO_CATEGORIES_ORDER, MACRO_SERIES, MacroSeriesConfig

logger = structlog.get_logger(__name__)

FRED_BASE_URL = "https://api.stlouisfed.org/fred"
DEFAULT_REALTIME_START = "1776-07-04"
DEFAULT_REALTIME_END = "9999-12-31"

WINDOW_DAYS = {
    "1W": 7,
    "1M": 30,
    "3M": 90,
    "1Y": 365,
}

REFRESH_TTL_SECONDS = {
    "intraday": 60 * 10,
    "daily": 60 * 60,
    "weekly": 60 * 60 * 6,
    "monthly": 60 * 60 * 12,
    "quarterly": 60 * 60 * 12,
}


@dataclass
class CacheEntry:
    value: Any
    expires_at: float


class TTLCache:
    def __init__(self, maxsize: int = 256, now_fn: Optional[callable] = None) -> None:
        self._data: OrderedDict[str, CacheEntry] = OrderedDict()
        self._maxsize = maxsize
        self._now_fn = now_fn or time.time

    def get(self, key: str) -> Any | None:
        entry = self._data.get(key)
        if entry is None:
            return None
        if entry.expires_at <= self._now_fn():
            self._data.pop(key, None)
            return None
        self._data.move_to_end(key)
        return entry.value

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        expires_at = self._now_fn() + ttl_seconds
        self._data[key] = CacheEntry(value=value, expires_at=expires_at)
        self._data.move_to_end(key)
        while len(self._data) > self._maxsize:
            self._data.popitem(last=False)

    def clear(self) -> None:
        self._data.clear()


@dataclass
class Observation:
    obs_date: date
    value: float
    realtime_start: Optional[str] = None
    realtime_end: Optional[str] = None


@dataclass
class SeriesData:
    series_id: str
    observations: List[Observation]
    fetched_at: datetime
    meta: Dict[str, Any]


@dataclass
class RevisionInfo:
    revised: bool
    previous_value: Optional[float]


class FredClient:
    def __init__(self, api_key: str, base_url: str = FRED_BASE_URL) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    def build_url(self, endpoint: str, params: Dict[str, Any]) -> str:
        base = f"{self.base_url}/{endpoint.lstrip('/')}"
        query = urlencode(params)
        return f"{base}?{query}"

    def _sync_fetch_json(self, url: str) -> Dict[str, Any]:
        req = Request(url, headers={"User-Agent": "trading-macro/1.0"})
        with urlopen(req, timeout=15) as response:
            return json.loads(response.read().decode("utf-8"))

    async def fetch_json(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = self.build_url(endpoint, params)
        backoff = 0.5
        for attempt in range(4):
            try:
                return await asyncio.to_thread(self._sync_fetch_json, url)
            except HTTPError as exc:
                body = ""
                try:
                    body = exc.read().decode("utf-8")
                except Exception:
                    body = ""
                if exc.code in (429, 500, 502, 503, 504) and attempt < 3:
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue
                raise FredHttpError(exc.code, body) from exc
            except URLError:
                if attempt < 3:
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue
                raise
        raise RuntimeError("FRED fetch failed after retries")

    async def get_series_meta(self, series_id: str) -> Dict[str, Any]:
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
        }
        payload = await self.fetch_json("series", params)
        series_list = payload.get("seriess", [])
        return series_list[0] if series_list else {}

    async def get_observations(self, series_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        base_params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
        }
        base_params.update(params)
        try:
            return await self.fetch_json("series/observations", base_params)
        except FredHttpError as exc:
            # FRED limits vintage dates in realtime range; fallback to observation_start.
            if exc.status_code == 400 and "vintage dates" in exc.body:
                fallback_params = dict(base_params)
                fallback_params["realtime_start"] = fallback_params.get("observation_start")
                today_safe = datetime.now(timezone.utc).date() - timedelta(days=1)
                observation_end = datetime.strptime(
                    fallback_params.get("observation_end"), "%Y-%m-%d"
                ).date()
                realtime_end = min(observation_end, today_safe)
                if realtime_end < datetime.strptime(
                    fallback_params.get("observation_start"), "%Y-%m-%d"
                ).date():
                    realtime_end = observation_end
                fallback_params["realtime_end"] = realtime_end.isoformat()
                logger.warning(
                    "fred_realtime_fallback",
                    series_id=series_id,
                    realtime_start=fallback_params.get("realtime_start"),
                    realtime_end=fallback_params.get("realtime_end"),
                )
                return await self.fetch_json("series/observations", fallback_params)
            raise


class FredHttpError(Exception):
    def __init__(self, status_code: int, body: str) -> None:
        super().__init__(f"FRED HTTP {status_code}")
        self.status_code = status_code
        self.body = body


class MacroDataService:
    def __init__(self, api_key: str) -> None:
        self.client = FredClient(api_key)
        self.meta_cache = TTLCache(maxsize=256)
        self.obs_cache = TTLCache(maxsize=512)
        self.revision_cache: Dict[str, Tuple[date, float]] = {}

    def _ttl_for_policy(self, policy: str) -> int:
        return REFRESH_TTL_SECONDS.get(policy, REFRESH_TTL_SECONDS["daily"])

    async def get_series_meta(self, series_id: str) -> Dict[str, Any]:
        cache_key = f"meta:{series_id}"
        cached = self.meta_cache.get(cache_key)
        if cached is not None:
            return cached
        meta = await self.client.get_series_meta(series_id)
        self.meta_cache.set(cache_key, meta, ttl_seconds=60 * 60 * 24)
        return meta

    async def get_observations(self, series_id: str, params: Dict[str, Any], ttl_seconds: int) -> SeriesData:
        cache_key = "obs:" + series_id + ":" + json.dumps(params, sort_keys=True)
        cached = self.obs_cache.get(cache_key)
        if cached is not None:
            return cached
        payload = await self.client.get_observations(series_id, params)
        observations = parse_fred_observations(payload)
        fetched_at = datetime.now(timezone.utc)
        meta = await self.get_series_meta(series_id)
        data = SeriesData(series_id=series_id, observations=observations, fetched_at=fetched_at, meta=meta)
        self.obs_cache.set(cache_key, data, ttl_seconds=ttl_seconds)
        return data

    def detect_revision(self, series_key: str, latest_date: date, latest_value: float) -> RevisionInfo:
        previous = self.revision_cache.get(series_key)
        revised = False
        previous_value = None
        if previous and previous[0] == latest_date and previous[1] != latest_value:
            revised = True
            previous_value = previous[1]
        self.revision_cache[series_key] = (latest_date, latest_value)
        return RevisionInfo(revised=revised, previous_value=previous_value)


macro_service_instance: MacroDataService | None = None


def get_macro_service(api_key: str) -> MacroDataService:
    global macro_service_instance
    if macro_service_instance is None:
        macro_service_instance = MacroDataService(api_key)
    return macro_service_instance


def parse_fred_observations(payload: Dict[str, Any]) -> List[Observation]:
    observations: List[Observation] = []
    for obs in payload.get("observations", []):
        value_str = obs.get("value")
        if value_str is None or value_str == ".":
            continue
        try:
            value = float(value_str)
        except ValueError:
            continue
        obs_date = datetime.strptime(obs.get("date"), "%Y-%m-%d").date()
        observations.append(
            Observation(
                obs_date=obs_date,
                value=value,
                realtime_start=obs.get("realtime_start"),
                realtime_end=obs.get("realtime_end"),
            )
        )
    observations.sort(key=lambda o: o.obs_date)
    return observations


def find_nearest_prior_value(
    observations: List[Observation],
    target_date: date,
) -> Optional[float]:
    if not observations:
        return None
    # observations sorted by date
    lo = 0
    hi = len(observations)
    while lo < hi:
        mid = (lo + hi) // 2
        if observations[mid].obs_date <= target_date:
            lo = mid + 1
        else:
            hi = mid
    idx = lo - 1
    if idx < 0:
        return None
    return observations[idx].value


def compute_changes(
    observations: List[Observation],
    windows: List[str],
    frequency: str,
) -> Dict[str, Optional[float]]:
    if not observations:
        return {w: None for w in windows}
    latest = observations[-1]
    changes: Dict[str, Optional[float]] = {}
    for window in windows:
        days = WINDOW_DAYS.get(window, 30)
        if window == "1W" and "Daily" in frequency:
            days = 5
        target = latest.obs_date - timedelta(days=days)
        prior_value = find_nearest_prior_value(observations, target)
        changes[window] = (latest.value - prior_value) if prior_value is not None else None
    return changes


def compute_spread_series(a: SeriesData, b: SeriesData) -> List[Observation]:
    a_map = {o.obs_date: o.value for o in a.observations}
    b_map = {o.obs_date: o.value for o in b.observations}
    common_dates = sorted(set(a_map.keys()) & set(b_map.keys()))
    return [Observation(obs_date=d, value=a_map[d] - b_map[d]) for d in common_dates]


def format_value(value: Optional[float], fmt: str) -> str:
    if value is None:
        return "—"
    if fmt == "percent":
        return f"{value:.2f}"
    if fmt == "bp":
        return f"{value * 100:.1f}"
    if fmt == "currency":
        return f"${value:,.2f}"
    if fmt == "index":
        return f"{value:.2f}"
    return f"{value:.2f}"


def format_unit(fmt: str) -> str:
    if fmt == "percent":
        return "%"
    if fmt == "bp":
        return "bp"
    if fmt == "currency":
        return ""
    if fmt == "index":
        return "Index"
    return ""


def pick_direction(changes: Dict[str, Optional[float]], windows: List[str]) -> str:
    for window in windows:
        value = changes.get(window)
        if value is None:
            continue
        if value > 0:
            return "up"
        if value < 0:
            return "down"
        return "flat"
    return "flat"


def data_quality_from_frequency(freq: str) -> str:
    if "Daily" in freq or "Business" in freq:
        return "daily"
    return "release"


def _category_order(name: str) -> int:
    try:
        return MACRO_CATEGORIES_ORDER.index(name)
    except ValueError:
        return len(MACRO_CATEGORIES_ORDER)


def _build_series_map(configs: List[MacroSeriesConfig]) -> Dict[str, MacroSeriesConfig]:
    mapping: Dict[str, MacroSeriesConfig] = {}
    for cfg in configs:
        if cfg.fred_series_id:
            mapping[cfg.fred_series_id] = cfg
    return mapping


def _collect_required_series(configs: List[MacroSeriesConfig]) -> List[str]:
    series_ids: set[str] = set()
    for cfg in configs:
        if cfg.fred_series_id:
            series_ids.add(cfg.fred_series_id)
        if cfg.series_ids:
            series_ids.update(cfg.series_ids)
    return sorted(series_ids)


def _observation_params(cfg: MacroSeriesConfig, start_date: date, end_date: date) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "observation_start": start_date.isoformat(),
        "observation_end": end_date.isoformat(),
        "realtime_start": DEFAULT_REALTIME_START,
        "realtime_end": DEFAULT_REALTIME_END,
    }
    if cfg.units_override:
        params["units"] = cfg.units_override
    if cfg.frequency_override:
        params["frequency"] = cfg.frequency_override
    if cfg.aggregation_method:
        params["aggregation_method"] = cfg.aggregation_method
    return params


async def get_macro_summary(api_key: str) -> Dict[str, Any]:
    service = get_macro_service(api_key)
    configs = MACRO_SERIES
    config_by_series = _build_series_map(configs)
    required_series = _collect_required_series(configs)

    # Fetch last 5 years to cover 1Y windows for slower series
    end_date = date.today()
    start_date = end_date - timedelta(days=365 * 5)

    async def fetch_series(series_id: str) -> SeriesData:
        cfg = config_by_series.get(series_id)
        refresh_policy = cfg.refresh_policy if cfg else "daily"
        ttl = service._ttl_for_policy(refresh_policy)
        params = _observation_params(cfg or MacroSeriesConfig(
            id=series_id,
            label=series_id,
            category="",
            format="index",
            recommended_change_windows=["1Y"],
            refresh_policy=refresh_policy,
            description="",
            fred_series_id=series_id,
        ), start_date, end_date)
        return await service.get_observations(series_id, params, ttl_seconds=ttl)

    series_data_list = await asyncio.gather(
        *(fetch_series(series_id) for series_id in required_series)
    )
    series_data_map = {data.series_id: data for data in series_data_list}

    categories: Dict[str, List[Dict[str, Any]]] = {}

    for cfg in configs:
        if not cfg.display:
            continue
        if cfg.computed == "spread":
            if not cfg.series_ids or len(cfg.series_ids) != 2:
                continue
            left = series_data_map.get(cfg.series_ids[0])
            right = series_data_map.get(cfg.series_ids[1])
            if not left or not right:
                continue
            computed_obs = compute_spread_series(left, right)
            if not computed_obs:
                continue
            latest = computed_obs[-1]
            changes = compute_changes(computed_obs, cfg.recommended_change_windows, "Daily")
            direction = pick_direction(changes, cfg.recommended_change_windows)
            revision = service.detect_revision(cfg.id, latest.obs_date, latest.value)
            fetched_at = max(left.fetched_at, right.fetched_at)
            tile = {
                "id": cfg.id,
                "label": cfg.label,
                "format": cfg.format,
                "value": latest.value,
                "valueFormatted": format_value(latest.value, cfg.format),
                "unit": format_unit(cfg.format),
                "obs_date": latest.obs_date.isoformat(),
                "fetched_at": fetched_at.isoformat(),
                "realtime_start": None,
                "realtime_end": None,
                "changes": changes,
                "changeDirection": direction,
                "revised": revision.revised,
                "previousValue": revision.previous_value,
                "description": cfg.description,
                "category": cfg.category,
                "recommendedChangeWindows": cfg.recommended_change_windows,
                "dataQuality": "daily",
            }
        else:
            series_id = cfg.fred_series_id
            if series_id is None:
                continue
            data = series_data_map.get(series_id)
            if not data or not data.observations:
                continue
            latest = data.observations[-1]
            frequency = data.meta.get("frequency", "") if data.meta else ""
            changes = compute_changes(data.observations, cfg.recommended_change_windows, frequency)
            direction = pick_direction(changes, cfg.recommended_change_windows)
            revision = service.detect_revision(cfg.id, latest.obs_date, latest.value)
            tile = {
                "id": cfg.id,
                "label": cfg.label,
                "format": cfg.format,
                "value": latest.value,
                "valueFormatted": format_value(latest.value, cfg.format),
                "unit": format_unit(cfg.format),
                "obs_date": latest.obs_date.isoformat(),
                "fetched_at": data.fetched_at.isoformat(),
                "realtime_start": latest.realtime_start,
                "realtime_end": latest.realtime_end,
                "changes": changes,
                "changeDirection": direction,
                "revised": revision.revised,
                "previousValue": revision.previous_value,
                "description": cfg.description,
                "category": cfg.category,
                "recommendedChangeWindows": cfg.recommended_change_windows,
                "dataQuality": data_quality_from_frequency(frequency),
            }

        categories.setdefault(cfg.category, []).append(tile)

    ordered_categories = [
        {
            "name": name,
            "tiles": categories.get(name, []),
        }
        for name in sorted(categories.keys(), key=_category_order)
    ]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "categories": ordered_categories,
    }
