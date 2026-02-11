# Data/ETL Agent - Quick Reference

## Overview
The Data/ETL Agent provides database schema, engine management, and data ingestion pipelines for Yahoo Finance and FRED economic data. All files are in the `packages/shared/` directory.

## Files Created (1410 lines total)

### 1. `/packages/shared/db/models.py` (117 lines)
Phase 1 database tables using separate `phase1_metadata`:

**Tables:**
- `prices_daily` - Daily prices for portfolio positions
- `factor_prices_daily` - Daily prices for factor proxies (SPY, QQQ, etc.)
- `fred_series_daily` - FRED economic series data
- `data_sync_status` - Tracks last sync date per symbol/series
- `risk_results` - Stores computed risk analysis results

### 2. `/packages/shared/db/engine.py` (81 lines)
Shared async database engine management:

**Key Functions:**
- `get_shared_engine(postgres_url)` - Get/create engine singleton
- `close_shared_engine()` - Dispose of connection pool
- `init_phase1_db(postgres_url)` - Create all phase 1 tables

**Connection Pool Settings:**
- pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=3600

### 3. `/packages/shared/data/yahoo.py` (410 lines)
Yahoo Finance data fetcher using `yfinance`:

**Key Functions:**
```python
async def fetch_prices_yahoo(
    symbols: list[str],
    start_date: date | None = None,
    end_date: date | None = None,
    engine: AsyncEngine | None = None,
    is_factor: bool = False,
) -> dict[str, pd.DataFrame]
```
- Fetches daily OHLCV data
- Incremental updates via data_sync_status
- Default backfill: 2 years if no sync history
- Rate limit: 0.5s between symbols
- Stores in prices_daily or factor_prices_daily

```python
async def fetch_factor_prices(
    engine: AsyncEngine | None = None,
) -> dict[str, pd.DataFrame]
```
- Fetches all factor proxies: SPY, QQQ, IWM, TLT, IEF, HYG, UUP, USO, DBC, ^VIX, BTC-USD

```python
async def get_prices_from_db(
    symbols: list[str],
    start_date: date | None = None,
    end_date: date | None = None,
    table: str = 'prices_daily',
    engine: AsyncEngine | None = None,
) -> dict[str, pd.DataFrame]
```
- Read stored prices from database

**Constants:**
- `FACTOR_SYMBOLS` - List of factor proxy tickers

### 4. `/packages/shared/data/fred.py` (459 lines)
FRED economic data fetcher using `fredapi`:

**Key Functions:**
```python
async def fetch_fred_series(
    series_ids: list[str] | None = None,
    start_date: date | None = None,
    engine: AsyncEngine | None = None,
) -> dict[str, pd.DataFrame]
```
- Fetches FRED economic series
- Requires FRED_API_KEY environment variable
- Incremental updates via data_sync_status
- Default: fetch all series in FRED_SERIES
- Rate limit: 0.5s between requests

```python
async def get_fred_from_db(
    series_ids: list[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    engine: AsyncEngine | None = None,
) -> dict[str, pd.DataFrame]
```
- Read stored FRED data from database

```python
def compute_macro_overview(
    fred_data: dict[str, pd.DataFrame]
) -> dict[str, Any]
```
- Compute macro backdrop from FRED data
- Returns: latest values, 1m/3m changes, directions
- Derives: curve_slope (10Y-2Y), real_rate_proxy (10Y - CPI YoY)

**Constants:**
- `FRED_SERIES` - Dict of series_id -> description:
  - DGS2: 2Y Treasury
  - DGS10: 10Y Treasury
  - T10Y2Y: 10Y-2Y Spread
  - CPIAUCSL: CPI All Urban
  - UNRATE: Unemployment Rate
  - INDPRO: Industrial Production

### 5. `/packages/shared/data/scheduler.py` (343 lines)
Background job scheduler for data updates and risk triggers:

**Key Functions:**
```python
async def run_daily_data_update(
    engine: AsyncEngine,
    redis_client: Any = None,
) -> dict[str, Any]
```
- Orchestrates full daily data refresh:
  1. Fetch factor prices from Yahoo
  2. Fetch position symbols from positions_current
  3. Fetch prices for position symbols
  4. Fetch FRED economic series
  5. Publish 'data_updated' event to Redis

```python
async def check_and_trigger_risk_recompute(
    engine: AsyncEngine,
    redis_client: Any = None,
    force: bool = False,
) -> dict[str, Any]
```
- Compare current portfolio_hash with last risk_results
- Trigger recompute if portfolio changed
- Publish 'risk_recompute' event to Redis

```python
def compute_portfolio_hash(
    positions: list[dict[str, Any]]
) -> str
```
- Compute stable hash of portfolio composition
- Based on sorted (symbol, position_size) tuples
- Used for cache invalidation

```python
async def run_daily_jobs(
    engine: AsyncEngine,
    redis_client: Any = None,
) -> dict[str, Any]
```
- Main entry point for daily scheduled tasks
- Runs data update + risk check in sequence

## Usage Examples

### Initialize Database
```python
from shared.db.engine import init_phase1_db

await init_phase1_db("postgresql://user:pass@localhost/trading")
```

### Fetch Factor Prices
```python
from shared.data.yahoo import fetch_factor_prices
from shared.db.engine import get_shared_engine

engine = get_shared_engine("postgresql://user:pass@localhost/trading")
factor_data = await fetch_factor_prices(engine)
# Returns dict: {'SPY': DataFrame, 'QQQ': DataFrame, ...}
```

### Fetch Position Prices
```python
from shared.data.yahoo import fetch_prices_yahoo

symbols = ['AAPL', 'GOOGL', 'MSFT']
price_data = await fetch_prices_yahoo(
    symbols=symbols,
    engine=engine,
    is_factor=False,
)
```

### Fetch FRED Data
```python
import os
from shared.data.fred import fetch_fred_series

os.environ['FRED_API_KEY'] = 'your_api_key'
fred_data = await fetch_fred_series(engine=engine)
# Returns dict: {'DGS2': DataFrame, 'DGS10': DataFrame, ...}
```

### Compute Macro Overview
```python
from shared.data.fred import compute_macro_overview

overview = compute_macro_overview(fred_data)
# Returns dict with latest values, changes, and derived signals
```

### Run Daily Update
```python
from shared.data.scheduler import run_daily_jobs

stats = await run_daily_jobs(engine, redis_client=None)
# Returns dict with update statistics
```

### Check Risk Recompute
```python
from shared.data.scheduler import check_and_trigger_risk_recompute

result = await check_and_trigger_risk_recompute(
    engine,
    redis_client=None,
    force=False,
)
# Returns dict with recompute decision and current/last hashes
```

## Key Design Patterns

1. **Incremental Updates**: All fetchers check `data_sync_status` to only fetch missing dates
2. **Async/Thread Pool**: yfinance and fredapi are sync, so wrapped with `asyncio.to_thread()`
3. **Upsert Pattern**: All DB writes use `ON CONFLICT DO UPDATE` for idempotency
4. **Rate Limiting**: 0.5s sleep between API requests to respect rate limits
5. **Error Handling**: Graceful degradation - log errors and continue with other symbols
6. **Structlog**: Consistent structured logging throughout
7. **Type Hints**: Full type annotations on all functions
8. **Raw SQL**: Uses SQLAlchemy `text()` for consistency with existing codebase

## Environment Variables Required

- `FRED_API_KEY` - Required for FRED data fetching (get from https://fred.stlouisfed.org/docs/api/api_key.html)

## Dependencies

Required Python packages (add to requirements.txt):
- sqlalchemy
- asyncpg
- pandas
- yfinance
- fredapi
- structlog

## Database Schema Details

All tables include proper indexes and constraints:
- Unique constraints on natural keys (symbol+date, series_id+date)
- Timestamps with timezone awareness (DateTime(timezone=True))
- Nullable conid field for cross-referencing with IBKR data
- Source tracking for multi-source data lineage

## Next Steps

1. Add these dependencies to requirements.txt
2. Set FRED_API_KEY environment variable
3. Initialize database with `init_phase1_db()`
4. Schedule `run_daily_jobs()` to run daily (e.g., via cron or APScheduler)
5. Connect Redis client for event publishing
