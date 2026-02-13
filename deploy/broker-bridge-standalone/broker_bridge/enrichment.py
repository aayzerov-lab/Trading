"""Position enrichment via static security-master mappings and IB contract details.

Looks up sector and country by conid first, then falls back to the
composite key ``{symbol}:{sec_type}:{currency}``.  Also integrates GICS
sector classification derived from IB ContractDetails fields.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import structlog

from broker_bridge.models import PositionEvent

logger = structlog.get_logger()

_MAPPINGS_DIR = Path(__file__).resolve().parent.parent / "mappings"
_SECURITY_MASTER_PATH = _MAPPINGS_DIR / "security_master.json"
_GICS_MAPPING_PATH = _MAPPINGS_DIR / "gics_mapping.json"
_CONTRACT_CACHE_PATH = _MAPPINGS_DIR / "contract_cache.json"

_mappings: dict[str, dict[str, Any]] = {}
_gics_mapping: dict[str, Any] = {}

# conid -> {industry, category, subcategory, sector, country}
_contract_details_cache: dict[int, dict[str, str]] = {}


def _load_json(path: Path, label: str) -> dict[str, Any]:
    """Read a JSON file from disk and return the parsed dict."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data: dict[str, Any] = json.load(fh)
        logger.info(f"{label}_loaded", path=str(path), count=len(data))
        return data
    except FileNotFoundError:
        logger.warning(f"{label}_file_not_found", path=str(path))
        return {}
    except json.JSONDecodeError as exc:
        logger.error(f"{label}_parse_error", path=str(path), error=str(exc))
        return {}


def reload_mappings() -> None:
    """Reload the security-master mappings and GICS mapping from disk."""
    global _mappings, _gics_mapping
    _mappings = _load_json(_SECURITY_MASTER_PATH, "security_master")
    _gics_mapping = _load_json(_GICS_MAPPING_PATH, "gics_mapping")


# Eager-load on import so the first enrich() call does not need to wait.
_mappings = _load_json(_SECURITY_MASTER_PATH, "security_master")
_gics_mapping = _load_json(_GICS_MAPPING_PATH, "gics_mapping")


# ---------------------------------------------------------------------------
# Contract details cache
# ---------------------------------------------------------------------------


def cache_contract_details(
    conid: int,
    industry: str | None,
    category: str | None,
    subcategory: str | None,
    exchange: str | None,
) -> None:
    """Map IB industry/category to GICS sector and cache the result.

    Priority for GICS sector mapping:
    1. category_overrides (most specific)
    2. industry_to_gics (broad mapping)
    3. "Unknown" fallback

    Exchange is mapped to country via exchange_to_country.
    """
    category_overrides: dict[str, str] = _gics_mapping.get("category_overrides", {})
    industry_to_gics: dict[str, str] = _gics_mapping.get("industry_to_gics", {})
    exchange_to_country: dict[str, str] = _gics_mapping.get("exchange_to_country", {})

    # Determine GICS sector
    sector = "Unknown"
    if category and category in category_overrides:
        sector = category_overrides[category]
    elif industry and industry in industry_to_gics:
        sector = industry_to_gics[industry]

    # Determine country from exchange
    country = "Global"
    if exchange and exchange in exchange_to_country:
        country = exchange_to_country[exchange]

    _contract_details_cache[conid] = {
        "industry": industry or "",
        "category": category or "",
        "subcategory": subcategory or "",
        "sector": sector,
        "country": country,
    }

    logger.debug(
        "contract_details_cached",
        conid=conid,
        industry=industry,
        category=category,
        sector=sector,
        country=country,
    )


def get_cached_conids() -> set[int]:
    """Return the set of conids already in the contract details cache."""
    return set(_contract_details_cache.keys())


def get_contract_cache() -> dict[int, dict[str, str]]:
    """Return a copy of the full contract details cache."""
    return dict(_contract_details_cache)


def get_manual_override(conid: int) -> dict[str, str] | None:
    """Return the security_master override for a conid, or None."""
    return _mappings.get(str(conid))


def save_contract_cache() -> None:
    """Persist the contract details cache to disk."""
    try:
        # Convert int keys to strings for JSON serialisation
        serialisable = {str(k): v for k, v in _contract_details_cache.items()}
        with open(_CONTRACT_CACHE_PATH, "w", encoding="utf-8") as fh:
            json.dump(serialisable, fh, indent=2)
        logger.info(
            "contract_cache_saved",
            path=str(_CONTRACT_CACHE_PATH),
            count=len(serialisable),
        )
    except Exception:
        logger.exception("contract_cache_save_failed", path=str(_CONTRACT_CACHE_PATH))


def load_contract_cache() -> None:
    """Load the contract details cache from disk."""
    global _contract_details_cache
    try:
        with open(_CONTRACT_CACHE_PATH, "r", encoding="utf-8") as fh:
            data: dict[str, dict[str, str]] = json.load(fh)
        _contract_details_cache = {int(k): v for k, v in data.items()}
        logger.info(
            "contract_cache_loaded",
            path=str(_CONTRACT_CACHE_PATH),
            count=len(_contract_details_cache),
        )
    except FileNotFoundError:
        logger.info("contract_cache_not_found", path=str(_CONTRACT_CACHE_PATH))
    except (json.JSONDecodeError, ValueError) as exc:
        logger.error(
            "contract_cache_load_error",
            path=str(_CONTRACT_CACHE_PATH),
            error=str(exc),
        )


# Load cached contract details on import
load_contract_cache()


# ---------------------------------------------------------------------------
# Enrichment
# ---------------------------------------------------------------------------


def enrich(event: PositionEvent) -> PositionEvent:
    """Return a copy of *event* with sector and country populated from mappings.

    Lookup priority:
    1. Manual override from security_master.json (highest priority)
    2. Contract details cache (GICS mapping from IB ContractDetails)
    3. "Unknown" fallback

    Also sets ib_industry, ib_category, ib_subcategory from the contract
    details cache when available.
    """
    updates: dict[str, Any] = {}

    # --- Priority 1: Manual override from security_master.json ---
    hit: dict[str, Any] | None = None

    if event.conid is not None:
        hit = _mappings.get(str(event.conid))

    if hit is None:
        composite_key = f"{event.symbol}:{event.sec_type}:{event.currency}"
        hit = _mappings.get(composite_key)

    if hit is not None:
        updates["sector"] = hit.get("sector", "Unknown")
        updates["country"] = hit.get("country", "Unknown")
    else:
        # --- Priority 2: Auto-classify by sec_type ---
        if event.sec_type == "CRYPTO":
            updates["sector"] = "Cryptocurrency"
            updates["country"] = "Global"
        # --- Priority 3: Contract details cache ---
        elif event.conid is not None and event.conid in _contract_details_cache:
            cached = _contract_details_cache[event.conid]
            updates["sector"] = cached.get("sector", "Unknown")
            updates["country"] = cached.get("country", "Unknown")
        else:
            # --- Priority 4: fallback ---
            updates["sector"] = "Unknown"
            updates["country"] = "Unknown"
            logger.debug(
                "enrichment_miss",
                symbol=event.symbol,
                conid=event.conid,
            )

    # Normalize remaining "Unknown" country to "Global"
    if updates.get("country") == "Unknown":
        updates["country"] = "Global"

    # Always populate IB classification fields from cache when available
    if event.conid is not None and event.conid in _contract_details_cache:
        cached = _contract_details_cache[event.conid]
        updates["ib_industry"] = cached.get("industry") or None
        updates["ib_category"] = cached.get("category") or None
        updates["ib_subcategory"] = cached.get("subcategory") or None

    return event.model_copy(update=updates)
