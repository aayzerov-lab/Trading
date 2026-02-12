"""Macro economic data API endpoints.

Provides REST endpoints for macro economic indicators from FRED
(Federal Reserve Economic Data).
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException

from api_server.config import get_settings
from shared.data.fred import compute_macro_overview, get_fred_from_db, FRED_SERIES
from shared.data.macro_service import get_macro_summary

logger = structlog.get_logger()

router = APIRouter(prefix="/macro", tags=["macro"])


@router.get("/overview")
async def macro_overview() -> dict[str, Any]:
    """Return macro economic backdrop overview.

    Fetches FRED data from the local database and computes key macro
    indicators with 1-month and 3-month changes.

    Returns a list-based format for the frontend MacroStrip component.
    """
    try:
        logger.info("macro_overview_request")

        # Fetch FRED data from DB (last 6 months for delta calculations)
        start_date = date.today() - timedelta(days=180)
        fred_data = await get_fred_from_db(
            series_ids=list(FRED_SERIES.keys()),
            start_date=start_date,
        )

        if not fred_data:
            logger.warning("no_fred_data_in_db")
            return {
                "indicators": [],
                "computed_at": date.today().isoformat(),
            }

        # compute_macro_overview is synchronous - do not await
        overview = compute_macro_overview(fred_data)

        # Transform dict-based overview into list-based format for frontend
        indicators = []
        for series_id, data in overview.items():
            if series_id == "derived":
                continue
            indicators.append({
                "series_id": series_id,
                "name": data.get("name", series_id),
                "latest_value": data.get("latest_value", 0),
                "latest_date": data.get("latest_date", ""),
                "change_1m": data.get("change_1m"),
                "change_3m": data.get("change_3m"),
                "direction": data.get("direction", "flat"),
                "unit": _get_unit(series_id),
            })

        result = {
            "indicators": indicators,
            "derived": overview.get("derived", {}),
            "computed_at": date.today().isoformat(),
        }

        logger.info(
            "macro_overview_computed",
            num_indicators=len(indicators),
        )

        return result

    except Exception as e:
        logger.exception("macro_overview_failed")
        raise HTTPException(
            status_code=500,
            detail=f"Macro overview computation failed: {str(e)}",
        )


def _get_unit(series_id: str) -> str:
    """Return the unit label for a FRED series."""
    units = {
        "DGS2": "%",
        "DGS10": "%",
        "T10Y2Y": "%",
        "CPIAUCSL": "Index",
        "UNRATE": "%",
        "INDPRO": "Index",
    }
    return units.get(series_id, "")


@router.get("/summary")
async def macro_summary() -> dict[str, Any]:
    """Return macro summary tiles grouped by category from FRED-only data."""
    try:
        logger.info("macro_summary_request")
        settings = get_settings()
        if not settings.FRED_API_KEY:
            logger.warning("macro_summary_missing_api_key")
            return {"generated_at": date.today().isoformat(), "categories": []}
        summary = await get_macro_summary(settings.FRED_API_KEY)
        logger.info(
            "macro_summary_computed",
            categories=len(summary.get("categories", [])),
        )
        return summary
    except Exception as e:
        logger.exception("macro_summary_failed")
        raise HTTPException(
            status_code=500,
            detail=f"Macro summary computation failed: {str(e)}",
        )
