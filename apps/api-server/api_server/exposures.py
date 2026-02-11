"""Portfolio exposure computation.

Groups positions by sector and country, computing notional values and
percentage weights for each bucket.  Supports two weighting methods:
``market_value`` and ``cost_basis``.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any


def compute_exposures(
    positions: list[dict[str, Any]],
    method: str = "market_value",
) -> dict[str, Any]:
    """Compute sector and country exposure weights from current positions.

    Parameters
    ----------
    positions:
        List of position dicts (rows from ``positions_current``).
    method:
        ``"market_value"`` -- use ``abs(market_value)`` when available,
        falling back to ``abs(position * avg_cost)``.
        ``"cost_basis"`` -- always use ``abs(position * avg_cost)``.

    Cash positions (``sec_type == "CASH"``) are excluded from the
    sector / country calculations.

    Returns a dict containing ``by_sector``, ``by_country``,
    ``weighting_method``, and ``total_gross_exposure``.
    """
    sector_notionals: dict[str, float] = defaultdict(float)
    country_notionals: dict[str, float] = defaultdict(float)
    total_gross_exposure: float = 0.0

    for pos in positions:
        # Filter out cash positions -- they must not appear in pies
        if (pos.get("sec_type") or "").upper() == "CASH":
            continue

        notional = _compute_notional(pos, method)

        sector = pos.get("sector") or "Unknown"
        country = pos.get("country") or "Unknown"

        sector_notionals[sector] += notional
        country_notionals[country] += notional
        total_gross_exposure += notional

    by_sector = _build_weight_list(sector_notionals, total_gross_exposure)
    by_country = _build_weight_list(country_notionals, total_gross_exposure)

    return {
        "by_sector": by_sector,
        "by_country": by_country,
        "weighting_method": method,
        "total_gross_exposure": round(total_gross_exposure, 2),
    }


def _compute_notional(pos: dict[str, Any], method: str) -> float:
    """Return the notional value for a single position given *method*."""
    position = pos.get("position", 0.0) or 0.0
    avg_cost = pos.get("avg_cost")

    if method == "market_value":
        market_value = pos.get("market_value")
        if market_value is not None and market_value != 0:
            return abs(market_value)
        # Fallback to cost basis
        if avg_cost:
            return abs(position * avg_cost)
        return 0.0

    # cost_basis method
    if avg_cost:
        return abs(position * avg_cost)
    return 0.0


def _build_weight_list(
    bucket_notionals: dict[str, float],
    total_notional: float,
) -> list[dict[str, Any]]:
    """Convert a {name: notional} mapping into a sorted list of weight dicts."""
    items: list[dict[str, Any]] = []
    for name, notional in bucket_notionals.items():
        weight = (notional / total_notional * 100.0) if total_notional > 0 else 0.0
        items.append({
            "name": name,
            "weight": round(weight, 2),
            "notional": round(notional, 2),
        })

    # Sort by weight descending
    items.sort(key=lambda x: x["weight"], reverse=True)
    return items
