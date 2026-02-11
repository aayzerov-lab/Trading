"""Yahoo Finance data fetcher for price data.

Fetches daily OHLCV data for stocks and factor proxies using yfinance,
with incremental updates tracked via data_sync_status table.
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

# Factor proxy symbols for market regime and risk analysis
FACTOR_SYMBOLS = [
    "SPY",      # S&P 500
    "QQQ",      # Nasdaq 100
    "IWM",      # Russell 2000
    "TLT",      # 20+ Year Treasury
    "IEF",      # 7-10 Year Treasury
    "HYG",      # High Yield Corporate Bonds
    "UUP",      # US Dollar Index
    "USO",      # Oil
    "DBC",      # Commodities
    "^VIX",     # Volatility Index
    "BTC-USD",  # Bitcoin
]


# ---------------------------------------------------------------------------
# Sync status helpers
# ---------------------------------------------------------------------------


async def _get_last_sync_date(
    symbol: str,
    source: str,
    engine: AsyncEngine,
) -> date | None:
    """Get the last successfully synced date for a symbol."""
    stmt = text("""
        SELECT last_date
        FROM data_sync_status
        WHERE source = :source AND symbol = :symbol
    """)
    async with engine.connect() as conn:
        result = await conn.execute(stmt, {"source": source, "symbol": symbol})
        row = result.first()
        return row[0] if row and row[0] else None


async def _update_sync_status(
    symbol: str,
    source: str,
    last_date: date,
    engine: AsyncEngine,
) -> None:
    """Update the sync status for a symbol."""
    stmt = text("""
        INSERT INTO data_sync_status (source, symbol, last_date, last_fetched_at)
        VALUES (:source, :symbol, :last_date, :last_fetched_at)
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
                "source": source,
                "symbol": symbol,
                "last_date": last_date,
                "last_fetched_at": now,
            },
        )


# ---------------------------------------------------------------------------
# Price data storage
# ---------------------------------------------------------------------------


async def _store_prices(
    symbol: str,
    df: pd.DataFrame,
    table: str,
    source: str,
    conid: int | None,
    engine: AsyncEngine,
) -> None:
    """Store price data in the database using upsert pattern.

    Args:
        symbol: Ticker symbol
        df: DataFrame with columns [date, close, adj_close]
        table: Table name ('prices_daily' or 'factor_prices_daily')
        source: Data source identifier ('yahoo', 'ibkr', etc)
        conid: Interactive Brokers contract ID (optional)
        engine: Database engine
    """
    if df.empty:
        return

    now = datetime.now(timezone.utc)

    # Build upsert statement based on table
    if table == "factor_prices_daily":
        stmt = text("""
            INSERT INTO factor_prices_daily (symbol, date, close, adj_close, source, updated_at)
            VALUES (:symbol, :date, :close, :adj_close, :source, :updated_at)
            ON CONFLICT (symbol, date)
            DO UPDATE SET
                close = EXCLUDED.close,
                adj_close = EXCLUDED.adj_close,
                updated_at = EXCLUDED.updated_at
        """)
    else:
        stmt = text("""
            INSERT INTO prices_daily (symbol, conid, date, close, adj_close, source, updated_at)
            VALUES (:symbol, :conid, :date, :close, :adj_close, :source, :updated_at)
            ON CONFLICT (symbol, date)
            DO UPDATE SET
                close = EXCLUDED.close,
                adj_close = EXCLUDED.adj_close,
                conid = COALESCE(EXCLUDED.conid, prices_daily.conid),
                updated_at = EXCLUDED.updated_at
        """)

    async with engine.begin() as conn:
        stored = 0
        for _, row in df.iterrows():
            # Skip rows with NaN close price (data integrity)
            if pd.isna(row["close"]):
                continue
            params: dict[str, Any] = {
                "symbol": symbol,
                "date": row["date"],
                "close": float(row["close"]),
                "adj_close": float(row["adj_close"]) if pd.notna(row["adj_close"]) else float(row["close"]),
                "source": source,
                "updated_at": now,
            }
            if table == "prices_daily":
                params["conid"] = conid

            await conn.execute(stmt, params)
            stored += 1

    logger.info(
        "prices_stored",
        symbol=symbol,
        table=table,
        rows=stored,
    )


# ---------------------------------------------------------------------------
# Yahoo Finance data fetching
# ---------------------------------------------------------------------------


def _fetch_yahoo_data_sync(
    symbol: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame | None:
    """Synchronous Yahoo Finance data fetch (runs in thread pool).

    Returns DataFrame with columns [date, close, adj_close] or None on error.
    """
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(
            start=start_date,
            end=end_date,
            auto_adjust=False,  # We want both Close and Adj Close
        )

        if df.empty:
            logger.warning("yahoo_no_data", symbol=symbol)
            return None

        # Reset index to get date as column
        df = df.reset_index()

        # Rename columns to match our schema
        col_map = {}
        if "Date" in df.columns:
            col_map["Date"] = "date"
        if "Close" in df.columns:
            col_map["Close"] = "close"
        # Handle different yfinance versions for adjusted close
        if "Adj Close" in df.columns:
            col_map["Adj Close"] = "adj_close"
        elif "Adjusted Close" in df.columns:
            col_map["Adjusted Close"] = "adj_close"

        df = df.rename(columns=col_map)

        # Convert date to date type (remove time component)
        df["date"] = pd.to_datetime(df["date"]).dt.date

        # Ensure adj_close column exists (fall back to close)
        if "adj_close" not in df.columns:
            df["adj_close"] = df["close"]

        # Select only the columns we need
        df = df[["date", "close", "adj_close"]]

        logger.debug(
            "yahoo_fetch_success",
            symbol=symbol,
            rows=len(df),
            start=df["date"].min(),
            end=df["date"].max(),
        )
        return df

    except Exception as e:
        logger.error(
            "yahoo_fetch_error",
            symbol=symbol,
            error=str(e),
            exc_info=True,
        )
        return None


async def fetch_prices_yahoo(
    symbols: list[str],
    start_date: date | None = None,
    end_date: date | None = None,
    engine: AsyncEngine | None = None,
    is_factor: bool = False,
) -> dict[str, pd.DataFrame]:
    """Fetch daily price data from Yahoo Finance for given symbols.

    Performs incremental updates by checking data_sync_status for each symbol
    to determine what date range needs to be fetched.

    Args:
        symbols: List of ticker symbols to fetch
        start_date: Optional start date override (default: 2 years ago or last sync)
        end_date: Optional end date (default: today)
        engine: Database engine (will create if not provided)
        is_factor: If True, store in factor_prices_daily, else prices_daily

    Returns:
        Dictionary mapping symbol to DataFrame with [date, close, adj_close]
    """
    if engine is None:
        engine = get_shared_engine("")

    if end_date is None:
        # yfinance 'end' param is exclusive, so add 1 day to include today
        end_date = date.today() + timedelta(days=1)

    table = "factor_prices_daily" if is_factor else "prices_daily"
    source = "yahoo"
    results: dict[str, pd.DataFrame] = {}

    for symbol in symbols:
        try:
            # Determine start date for this symbol
            if start_date is None:
                last_sync = await _get_last_sync_date(symbol, source, engine)
                if last_sync:
                    # Fetch from day after last sync
                    fetch_start = last_sync + timedelta(days=1)
                else:
                    # No sync history, fetch 2 years
                    fetch_start = date.today() - timedelta(days=730)
            else:
                fetch_start = start_date

            # Skip if already up to date
            if fetch_start > end_date:
                logger.debug("yahoo_skip_uptodate", symbol=symbol)
                continue

            # Fetch data in thread pool (yfinance is synchronous)
            df = await asyncio.to_thread(
                _fetch_yahoo_data_sync,
                symbol,
                fetch_start,
                end_date,
            )

            if df is not None and not df.empty:
                # Store in database
                await _store_prices(
                    symbol=symbol,
                    df=df,
                    table=table,
                    source=source,
                    conid=None,
                    engine=engine,
                )

                # Update sync status
                max_date = df["date"].max()
                await _update_sync_status(symbol, source, max_date, engine)

                results[symbol] = df

            # Rate limiting
            await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(
                "yahoo_symbol_error",
                symbol=symbol,
                error=str(e),
                exc_info=True,
            )
            continue

    logger.info(
        "yahoo_fetch_complete",
        requested=len(symbols),
        successful=len(results),
    )
    return results


async def fetch_factor_prices(
    engine: AsyncEngine | None = None,
) -> dict[str, pd.DataFrame]:
    """Fetch all factor proxy prices from Yahoo Finance.

    Factors include: SPY, QQQ, IWM, TLT, IEF, HYG, UUP, USO, DBC, ^VIX, BTC-USD

    Args:
        engine: Database engine (will create if not provided)

    Returns:
        Dictionary mapping symbol to DataFrame with [date, close, adj_close]
    """
    logger.info("fetching_factor_prices", symbols=FACTOR_SYMBOLS)
    return await fetch_prices_yahoo(
        symbols=FACTOR_SYMBOLS,
        engine=engine,
        is_factor=True,
    )


# ---------------------------------------------------------------------------
# Database query helpers
# ---------------------------------------------------------------------------


async def get_prices_from_db(
    symbols: list[str],
    start_date: date | None = None,
    end_date: date | None = None,
    table: str = "prices_daily",
    engine: AsyncEngine | None = None,
) -> dict[str, pd.DataFrame]:
    """Read stored prices from database.

    Args:
        symbols: List of ticker symbols to retrieve
        start_date: Optional start date filter
        end_date: Optional end date filter
        table: Table name ('prices_daily' or 'factor_prices_daily')
        engine: Database engine (will create if not provided)

    Returns:
        Dictionary mapping symbol to DataFrame with [date, close, adj_close]
    """
    if engine is None:
        engine = get_shared_engine("")

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
        SELECT date, close, adj_close
        FROM {table}
        WHERE symbol = :symbol
        {date_filter}
        ORDER BY date
    """)

    async with engine.connect() as conn:
        for symbol in symbols:
            params: dict[str, Any] = {"symbol": symbol}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date

            result = await conn.execute(stmt, params)
            rows = result.fetchall()

            if rows:
                df = pd.DataFrame(rows, columns=["date", "close", "adj_close"])
                results[symbol] = df
                logger.debug(
                    "prices_loaded_from_db",
                    symbol=symbol,
                    rows=len(df),
                )
            else:
                logger.warning("no_prices_in_db", symbol=symbol)

    return results
