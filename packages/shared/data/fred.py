"""FRED economic data fetcher.

Fetches daily economic time series data from Federal Reserve Economic Data (FRED)
using the fredapi library, with incremental updates tracked via data_sync_status.
"""

from __future__ import annotations

import asyncio
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import structlog
from fredapi import Fred
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from ..db.engine import get_shared_engine

logger = structlog.get_logger()

# FRED series to track for macro backdrop
FRED_SERIES = {
    "DGS2": "2Y Treasury",
    "DGS10": "10Y Treasury",
    "T10Y2Y": "10Y-2Y Spread",
    "CPIAUCSL": "CPI All Urban",
    "UNRATE": "Unemployment Rate",
    "INDPRO": "Industrial Production",
}


# ---------------------------------------------------------------------------
# Sync status helpers
# ---------------------------------------------------------------------------


async def _get_last_sync_date(
    series_id: str,
    engine: AsyncEngine,
) -> date | None:
    """Get the last successfully synced date for a FRED series."""
    stmt = text("""
        SELECT last_date
        FROM data_sync_status
        WHERE source = 'fred' AND symbol = :series_id
    """)
    async with engine.connect() as conn:
        result = await conn.execute(stmt, {"series_id": series_id})
        row = result.first()
        return row[0] if row and row[0] else None


async def _update_sync_status(
    series_id: str,
    last_date: date,
    engine: AsyncEngine,
) -> None:
    """Update the sync status for a FRED series."""
    stmt = text("""
        INSERT INTO data_sync_status (source, symbol, last_date, last_fetched_at)
        VALUES ('fred', :series_id, :last_date, :last_fetched_at)
        ON CONFLICT (source, symbol)
        DO UPDATE SET
            last_date = EXCLUDED.last_date,
            last_fetched_at = EXCLUDED.last_fetched_at
    """)
    now = datetime.now(timezone.utc)
    async with engine.begin() as conn:
        await conn.execute(
            stmt,
            {
                "series_id": series_id,
                "last_date": last_date,
                "last_fetched_at": now,
            },
        )


# ---------------------------------------------------------------------------
# FRED data storage
# ---------------------------------------------------------------------------


async def _store_fred_data(
    series_id: str,
    df: pd.DataFrame,
    engine: AsyncEngine,
) -> None:
    """Store FRED series data in the database using upsert pattern.

    Args:
        series_id: FRED series identifier
        df: DataFrame with columns [date, value]
        engine: Database engine
    """
    if df.empty:
        return

    now = datetime.now(timezone.utc)

    stmt = text("""
        INSERT INTO fred_series_daily (series_id, date, value, source, updated_at)
        VALUES (:series_id, :date, :value, 'fred', :updated_at)
        ON CONFLICT (series_id, date)
        DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = EXCLUDED.updated_at
    """)

    async with engine.begin() as conn:
        for _, row in df.iterrows():
            await conn.execute(
                stmt,
                {
                    "series_id": series_id,
                    "date": row["date"],
                    "value": float(row["value"]),
                    "updated_at": now,
                },
            )

    logger.info(
        "fred_data_stored",
        series_id=series_id,
        rows=len(df),
    )


# ---------------------------------------------------------------------------
# FRED data fetching
# ---------------------------------------------------------------------------


def _fetch_fred_series_sync(
    series_id: str,
    start_date: date,
    fred_client: Fred,
) -> pd.DataFrame | None:
    """Synchronous FRED data fetch (runs in thread pool).

    Returns DataFrame with columns [date, value] or None on error.
    """
    try:
        series = fred_client.get_series(
            series_id,
            observation_start=start_date.isoformat(),
        )

        if series.empty:
            logger.warning("fred_no_data", series_id=series_id)
            return None

        # Convert series to DataFrame
        df = series.reset_index()
        df.columns = ["date", "value"]

        # Convert date to date type
        df["date"] = pd.to_datetime(df["date"]).dt.date

        # Drop NaN values (FRED sometimes has gaps)
        df = df.dropna(subset=["value"])

        logger.debug(
            "fred_fetch_success",
            series_id=series_id,
            rows=len(df),
            start=df["date"].min(),
            end=df["date"].max(),
        )
        return df

    except Exception as e:
        logger.error(
            "fred_fetch_error",
            series_id=series_id,
            error=str(e),
            exc_info=True,
        )
        return None


async def fetch_fred_series(
    series_ids: list[str] | None = None,
    start_date: date | None = None,
    engine: AsyncEngine | None = None,
) -> dict[str, pd.DataFrame]:
    """Fetch FRED time series data.

    Performs incremental updates by checking data_sync_status for each series
    to determine what date range needs to be fetched.

    Args:
        series_ids: List of FRED series IDs to fetch (default: all in FRED_SERIES)
        start_date: Optional start date override (default: 2 years ago or last sync)
        engine: Database engine (will create if not provided)

    Returns:
        Dictionary mapping series_id to DataFrame with [date, value]

    Raises:
        ValueError: If FRED_API_KEY environment variable is not set
    """
    if engine is None:
        engine = get_shared_engine("")

    # Get API key from environment
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise ValueError("FRED_API_KEY environment variable not set")

    # Use all series if none specified
    if series_ids is None:
        series_ids = list(FRED_SERIES.keys())

    # Create FRED client
    fred_client = Fred(api_key=api_key)

    results: dict[str, pd.DataFrame] = {}

    for series_id in series_ids:
        try:
            # Determine start date for this series
            if start_date is None:
                last_sync = await _get_last_sync_date(series_id, engine)
                if last_sync:
                    # Fetch from day after last sync
                    fetch_start = last_sync + timedelta(days=1)
                else:
                    # No sync history, fetch 2 years
                    fetch_start = date.today() - timedelta(days=730)
            else:
                fetch_start = start_date

            # Skip if already up to date
            if fetch_start > date.today():
                logger.debug("fred_skip_uptodate", series_id=series_id)
                continue

            # Fetch data in thread pool (fredapi is synchronous)
            df = await asyncio.to_thread(
                _fetch_fred_series_sync,
                series_id,
                fetch_start,
                fred_client,
            )

            if df is not None and not df.empty:
                # Store in database
                await _store_fred_data(
                    series_id=series_id,
                    df=df,
                    engine=engine,
                )

                # Update sync status
                max_date = df["date"].max()
                await _update_sync_status(series_id, max_date, engine)

                results[series_id] = df

            # Rate limiting
            await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(
                "fred_series_error",
                series_id=series_id,
                error=str(e),
                exc_info=True,
            )
            continue

    logger.info(
        "fred_fetch_complete",
        requested=len(series_ids),
        successful=len(results),
    )
    return results


# ---------------------------------------------------------------------------
# Database query helpers
# ---------------------------------------------------------------------------


async def get_fred_from_db(
    series_ids: list[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    engine: AsyncEngine | None = None,
) -> dict[str, pd.DataFrame]:
    """Read stored FRED data from database.

    Args:
        series_ids: List of FRED series IDs to retrieve (default: all in FRED_SERIES)
        start_date: Optional start date filter
        end_date: Optional end date filter
        engine: Database engine (will create if not provided)

    Returns:
        Dictionary mapping series_id to DataFrame with [date, value]
    """
    if engine is None:
        engine = get_shared_engine("")

    if series_ids is None:
        series_ids = list(FRED_SERIES.keys())

    if end_date is None:
        end_date = date.today()

    results: dict[str, pd.DataFrame] = {}

    # Build query with optional date filters
    date_filter = ""
    if start_date:
        date_filter = "AND date >= :start_date"
    if end_date:
        date_filter += " AND date <= :end_date"

    stmt = text(f"""
        SELECT date, value
        FROM fred_series_daily
        WHERE series_id = :series_id
        {date_filter}
        ORDER BY date
    """)

    async with engine.connect() as conn:
        for series_id in series_ids:
            params: dict[str, Any] = {"series_id": series_id}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date

            result = await conn.execute(stmt, params)
            rows = result.fetchall()

            if rows:
                df = pd.DataFrame(rows, columns=["date", "value"])
                results[series_id] = df
                logger.debug(
                    "fred_loaded_from_db",
                    series_id=series_id,
                    rows=len(df),
                )
            else:
                logger.warning("no_fred_data_in_db", series_id=series_id)

    return results


# ---------------------------------------------------------------------------
# Macro analysis helpers
# ---------------------------------------------------------------------------


def compute_macro_overview(fred_data: dict[str, pd.DataFrame]) -> dict[str, Any]:
    """Compute macro backdrop summary from FRED data.

    Analyzes recent changes in key economic indicators and derives signals
    for yield curve, real rates, and overall economic conditions.

    Args:
        fred_data: Dictionary mapping series_id to DataFrame with [date, value]

    Returns:
        Dictionary with:
        - For each series: latest value, 1m change, 3m change, direction
        - Derived signals: curve_slope, real_rate_proxy
    """
    overview: dict[str, Any] = {}

    for series_id, df in fred_data.items():
        if df.empty:
            continue

        # Sort by date to ensure correct ordering
        df = df.sort_values("date")

        # Get latest value
        latest = df.iloc[-1]
        latest_value = float(latest["value"])
        latest_date = latest["date"]

        # Calculate 1-month and 3-month changes
        one_month_ago = latest_date - timedelta(days=30)
        three_months_ago = latest_date - timedelta(days=90)

        df_1m = df[df["date"] >= one_month_ago]
        df_3m = df[df["date"] >= three_months_ago]

        change_1m = None
        change_3m = None
        direction = "flat"

        if len(df_1m) >= 2:
            old_value_1m = float(df_1m.iloc[0]["value"])
            change_1m = latest_value - old_value_1m
            if abs(change_1m) > 0.01:  # Threshold for meaningful change
                direction = "up" if change_1m > 0 else "down"

        if len(df_3m) >= 2:
            old_value_3m = float(df_3m.iloc[0]["value"])
            change_3m = latest_value - old_value_3m

        overview[series_id] = {
            "name": FRED_SERIES.get(series_id, series_id),
            "latest_value": latest_value,
            "latest_date": latest_date.isoformat(),
            "change_1m": change_1m,
            "change_3m": change_3m,
            "direction": direction,
        }

    # Derive additional signals
    derived: dict[str, Any] = {}

    # Yield curve slope (10Y-2Y spread)
    if "T10Y2Y" in overview:
        slope = overview["T10Y2Y"]["latest_value"]
        derived["curve_slope"] = {
            "value": slope,
            "signal": "inverted" if slope < 0 else "normal",
        }
    elif "DGS10" in overview and "DGS2" in overview:
        slope = overview["DGS10"]["latest_value"] - overview["DGS2"]["latest_value"]
        derived["curve_slope"] = {
            "value": slope,
            "signal": "inverted" if slope < 0 else "normal",
        }

    # Real rate proxy (10Y yield minus CPI YoY)
    if "DGS10" in overview and "CPIAUCSL" in fred_data:
        cpi_df = fred_data["CPIAUCSL"].sort_values("date")
        if len(cpi_df) >= 12:
            # Calculate YoY CPI inflation
            latest_cpi = float(cpi_df.iloc[-1]["value"])
            year_ago_cpi = float(cpi_df.iloc[-12]["value"])
            cpi_yoy = ((latest_cpi - year_ago_cpi) / year_ago_cpi) * 100

            ten_year = overview["DGS10"]["latest_value"]
            real_rate = ten_year - cpi_yoy

            derived["real_rate_proxy"] = {
                "value": real_rate,
                "ten_year": ten_year,
                "cpi_yoy": cpi_yoy,
                "signal": "positive" if real_rate > 0 else "negative",
            }

    overview["derived"] = derived

    logger.info("macro_overview_computed", series_count=len(fred_data))
    return overview
