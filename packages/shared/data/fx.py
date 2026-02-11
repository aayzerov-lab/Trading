"""FX rate ingestion from Yahoo Finance.

Fetches daily FX rates (e.g. EURUSD=X) and stores in fx_daily table.
Supports incremental updates via data_sync_status tracking.

Convention: all FX pairs are stored as CCCUSD (units of USD per 1 unit of
foreign currency). For Yahoo tickers like EURUSD=X the quote IS USD/EUR
so close price = how many USD per 1 EUR.  For JPY=X Yahoo returns USD/JPY
inverted (i.e. JPY per 1 USD), so we invert to get USD per 1 JPY.

Usage in returns: r_usd = r_local + r_fx  where r_fx = log(fx_t / fx_{t-1})
and fx is in "USD per 1 local currency" orientation.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import structlog
import yfinance as yf
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from ..db.engine import get_shared_engine

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Currency -> Yahoo FX ticker mapping
# ---------------------------------------------------------------------------

# Currencies where Yahoo returns USD per 1 unit of foreign currency directly
_DIRECT_PAIRS: dict[str, str] = {
    "EUR": "EURUSD=X",
    "GBP": "GBPUSD=X",
    "AUD": "AUDUSD=X",
    "NZD": "NZDUSD=X",
}

# Currencies where Yahoo returns foreign per 1 USD (need to invert)
_INVERTED_PAIRS: dict[str, str] = {
    "JPY": "JPY=X",
    "CAD": "CAD=X",
    "CHF": "CHF=X",
    "KRW": "KRW=X",
    "CNY": "CNY=X",
    "HKD": "HKD=X",
    "SGD": "SGD=X",
    "SEK": "SEK=X",
    "NOK": "NOK=X",
    "DKK": "DKK=X",
    "INR": "INR=X",
    "BRL": "BRL=X",
    "MXN": "MXN=X",
    "ZAR": "ZAR=X",
    "TWD": "TWD=X",
    "THB": "THB=X",
    "ILS": "ILS=X",
}


def get_yahoo_fx_ticker(currency: str) -> str | None:
    """Return Yahoo Finance ticker for a currency's USD rate."""
    currency = currency.upper()
    if currency == "USD":
        return None
    if currency in _DIRECT_PAIRS:
        return _DIRECT_PAIRS[currency]
    if currency in _INVERTED_PAIRS:
        return _INVERTED_PAIRS[currency]
    # Try generic pattern: CCCUSD=X
    return f"{currency}USD=X"


def needs_inversion(currency: str) -> bool:
    """Return True if Yahoo's quote needs inversion to get USD/foreign."""
    return currency.upper() in _INVERTED_PAIRS


def fx_pair_name(currency: str) -> str:
    """Canonical pair name stored in DB: e.g. 'EURUSD', 'JPYUSD'."""
    return f"{currency.upper()}USD"


# ---------------------------------------------------------------------------
# Fetch + store
# ---------------------------------------------------------------------------


def _fetch_fx_yahoo_sync(
    ticker: str,
    start_date: date,
    end_date: date,
    invert: bool = False,
) -> pd.DataFrame | None:
    """Synchronous Yahoo FX fetch (runs in thread pool)."""
    try:
        t = yf.Ticker(ticker)
        df = t.history(start=start_date, end=end_date, auto_adjust=False)
        if df.empty:
            logger.warning("fx_yahoo_no_data", ticker=ticker)
            return None

        df = df.reset_index()
        col_map = {}
        if "Date" in df.columns:
            col_map["Date"] = "date"
        if "Close" in df.columns:
            col_map["Close"] = "close"
        if "Adj Close" in df.columns:
            col_map["Adj Close"] = "adj_close"
        elif "Adjusted Close" in df.columns:
            col_map["Adjusted Close"] = "adj_close"
        df = df.rename(columns=col_map)

        df["date"] = pd.to_datetime(df["date"]).dt.date

        if "adj_close" not in df.columns:
            df["adj_close"] = df["close"]

        # Replace NaN adj_close with close (no NULLs)
        df["adj_close"] = df["adj_close"].fillna(df["close"])

        df = df[["date", "close", "adj_close"]].dropna(subset=["close"])

        if invert:
            df["close"] = 1.0 / df["close"]
            df["adj_close"] = 1.0 / df["adj_close"]

        return df
    except Exception as e:
        logger.error("fx_yahoo_error", ticker=ticker, error=str(e))
        return None


async def _store_fx_prices(
    pair: str,
    df: pd.DataFrame,
    engine: AsyncEngine,
) -> int:
    """Bulk-upsert FX prices into fx_daily."""
    if df.empty:
        return 0

    now = datetime.now(timezone.utc)
    rows = []
    for _, row in df.iterrows():
        if pd.isna(row["close"]):
            continue
        rows.append({
            "pair": pair,
            "date": row["date"],
            "close": float(row["close"]),
            "adj_close": float(row["adj_close"]) if pd.notna(row["adj_close"]) else float(row["close"]),
            "source": "yahoo",
            "updated_at": now,
        })

    if not rows:
        return 0

    stmt = text("""
        INSERT INTO fx_daily (pair, date, close, adj_close, source, updated_at)
        VALUES (:pair, :date, :close, :adj_close, :source, :updated_at)
        ON CONFLICT (pair, date)
        DO UPDATE SET
            close = EXCLUDED.close,
            adj_close = EXCLUDED.adj_close,
            updated_at = EXCLUDED.updated_at
    """)

    async with engine.begin() as conn:
        await conn.execute(stmt, rows)

    return len(rows)


async def _get_fx_sync_date(pair: str, engine: AsyncEngine) -> date | None:
    """Get last sync date for an FX pair."""
    stmt = text("""
        SELECT last_date FROM data_sync_status
        WHERE source = 'yahoo_fx' AND symbol = :pair
    """)
    async with engine.connect() as conn:
        result = await conn.execute(stmt, {"pair": pair})
        row = result.first()
        return row[0] if row and row[0] else None


async def _update_fx_sync(pair: str, last_date: date, engine: AsyncEngine) -> None:
    """Update sync status for FX pair."""
    stmt = text("""
        INSERT INTO data_sync_status (source, symbol, last_date, last_fetched_at)
        VALUES ('yahoo_fx', :pair, :last_date, :now)
        ON CONFLICT (source, symbol)
        DO UPDATE SET last_date = EXCLUDED.last_date, last_fetched_at = EXCLUDED.last_fetched_at
    """)
    async with engine.begin() as conn:
        await conn.execute(stmt, {
            "pair": pair,
            "last_date": last_date,
            "now": datetime.now(timezone.utc),
        })


async def fetch_fx_rates(
    currencies: list[str],
    engine: AsyncEngine | None = None,
) -> dict[str, pd.DataFrame]:
    """Fetch FX rates for given currencies from Yahoo Finance.

    Uses incremental updates with 10-business-day overlap buffer.

    Args:
        currencies: List of ISO currency codes (e.g. ['EUR', 'KRW'])
        engine: Database engine

    Returns:
        Dict mapping pair name (e.g. 'EURUSD') to DataFrame
    """
    if engine is None:
        engine = get_shared_engine("")

    end_date = date.today() + timedelta(days=1)
    results: dict[str, pd.DataFrame] = {}

    for ccy in currencies:
        ccy = ccy.upper()
        if ccy == "USD":
            continue

        ticker = get_yahoo_fx_ticker(ccy)
        if ticker is None:
            continue

        pair = fx_pair_name(ccy)
        invert = needs_inversion(ccy)

        try:
            last_sync = await _get_fx_sync_date(pair, engine)
            if last_sync:
                # Incremental: overlap buffer of 10 business days
                fetch_start = last_sync - timedelta(days=14)  # ~10 biz days
            else:
                fetch_start = date.today() - timedelta(days=730)

            if fetch_start > end_date:
                continue

            df = await asyncio.to_thread(
                _fetch_fx_yahoo_sync, ticker, fetch_start, end_date, invert
            )

            if df is not None and not df.empty:
                stored = await _store_fx_prices(pair, df, engine)
                max_date = df["date"].max()
                await _update_fx_sync(pair, max_date, engine)
                results[pair] = df
                logger.info("fx_fetched", pair=pair, ticker=ticker, rows=stored)

            await asyncio.sleep(0.3)

        except Exception as e:
            logger.error("fx_fetch_error", currency=ccy, error=str(e))
            continue

    return results


async def get_fx_rates_from_db(
    pairs: list[str],
    start_date: date | None = None,
    end_date: date | None = None,
    engine: AsyncEngine | None = None,
) -> dict[str, pd.DataFrame]:
    """Read FX rates from database.

    Args:
        pairs: List of pair names (e.g. ['EURUSD', 'KRWUSD'])
        start_date: Optional start date
        end_date: Optional end date

    Returns:
        Dict mapping pair name to DataFrame with [date, close, adj_close]
    """
    if engine is None:
        engine = get_shared_engine("")

    if end_date is None:
        end_date = date.today()

    results: dict[str, pd.DataFrame] = {}

    date_filter = ""
    if start_date:
        date_filter = "AND date >= :start_date"
    if end_date:
        date_filter += " AND date <= :end_date"

    stmt = text(f"""
        SELECT date, close, adj_close
        FROM fx_daily
        WHERE pair = :pair {date_filter}
        ORDER BY date
    """)

    async with engine.connect() as conn:
        for pair in pairs:
            params: dict[str, Any] = {"pair": pair}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date

            result = await conn.execute(stmt, params)
            rows = result.fetchall()

            if rows:
                df = pd.DataFrame(rows, columns=["date", "close", "adj_close"])
                results[pair] = df

    return results


async def get_required_fx_currencies(engine: AsyncEngine | None = None) -> list[str]:
    """Get list of non-USD currencies that need FX rates.

    Checks both positions_current.currency and security_overrides.
    """
    if engine is None:
        engine = get_shared_engine("")

    currencies: set[str] = set()

    # From positions_current
    try:
        stmt = text("""
            SELECT DISTINCT currency FROM positions_current
            WHERE currency IS NOT NULL AND currency != 'USD' AND position != 0
        """)
        async with engine.connect() as conn:
            result = await conn.execute(stmt)
            for row in result:
                currencies.add(row[0])
    except Exception:
        pass

    # From security_overrides
    try:
        stmt = text("""
            SELECT DISTINCT currency FROM security_overrides
            WHERE currency != 'USD' AND is_usd_listed = 0
        """)
        async with engine.connect() as conn:
            result = await conn.execute(stmt)
            for row in result:
                currencies.add(row[0])
    except Exception:
        pass

    return sorted(currencies)


async def get_security_fx_info(
    symbols: list[str],
    engine: AsyncEngine | None = None,
) -> dict[str, dict]:
    """Get FX-relevant info for symbols.

    Returns dict like:
    {
        'AAPL': {'currency': 'USD', 'is_usd_listed': True, 'fx_pair': None},
        '005930.KS': {'currency': 'KRW', 'is_usd_listed': False, 'fx_pair': 'KRWUSD'},
    }

    Uses security_overrides table if present, otherwise defaults to USD-listed.
    """
    if engine is None:
        engine = get_shared_engine("")

    result: dict[str, dict] = {}

    # Default all to USD-listed
    for sym in symbols:
        result[sym] = {
            "currency": "USD",
            "is_usd_listed": True,
            "fx_pair": None,
        }

    # Override from security_overrides table
    try:
        stmt = text("""
            SELECT symbol, currency, is_usd_listed, fx_pair
            FROM security_overrides
            WHERE symbol = ANY(:symbols)
        """)
        async with engine.connect() as conn:
            rows = await conn.execute(stmt, {"symbols": symbols})
            for row in rows:
                sym = row[0]
                ccy = row[1] or "USD"
                is_usd = bool(row[2])
                fx_p = row[3] or (fx_pair_name(ccy) if ccy != "USD" and not is_usd else None)
                result[sym] = {
                    "currency": ccy,
                    "is_usd_listed": is_usd,
                    "fx_pair": fx_p,
                }
    except Exception as e:
        logger.debug("security_overrides_query_failed", error=str(e))

    # Also check positions_current for currency info
    try:
        stmt = text("""
            SELECT symbol, currency FROM positions_current
            WHERE symbol = ANY(:symbols) AND currency IS NOT NULL
        """)
        async with engine.connect() as conn:
            rows = await conn.execute(stmt, {"symbols": symbols})
            for row in rows:
                sym = row[0]
                ccy = row[1]
                if sym in result and result[sym]["currency"] == "USD" and ccy != "USD":
                    # positions_current says non-USD but no override exists
                    # Default: assume USD-listed (ADR) unless overridden
                    pass  # Keep default; user must set is_usd_listed=False via overrides

    except Exception:
        pass

    return result
