# Trading Workstation

Real-time portfolio dashboard that streams positions from Interactive Brokers, enriches them with sector/country metadata, and displays live exposure breakdowns.

---

## Architecture

```
+--------------+       +----------------+       +------------+       +------------+
|              |       |                |------>|  Postgres   |------>|            |
|  IB Gateway  |------>|  broker-bridge |       | (positions) |       | api-server |---> REST + WebSocket
|  (TWS/IB GW) |  TCP  |   (Python)     |       +------------+       |  (FastAPI)  |
|              |       |                |                             |            |
+--------------+       |                |------>+------------+------->|            |
                       +----------------+       |   Redis    |       +------------+
                                                | (pub/sub)  |             |
                                                +------------+             |
                                                                           v
                                                                     +----------+
                                                                     |   web    |
                                                                     | (Next.js)|
                                                                     +----------+
                                                                     localhost:3000
```

| Service | Role |
|---------|------|
| **broker-bridge** (Python) | Connects to IB Gateway via `ib_insync`, streams positions, enriches with sector/country data, persists to Postgres, publishes to Redis |
| **api-server** (FastAPI) | REST API (`/portfolio`, `/portfolio/exposures`, `/risk/*`, `/macro`, `/health`) and WebSocket (`/stream`) for live updates |
| **web** (Next.js) | Dark-theme dashboard with positions table and sector/country pie charts |
| **Postgres** | Persistence layer (`positions_current` and `positions_events` tables) |
| **Redis** | Pub/sub channel (`positions`) for real-time update propagation |

---

## Prerequisites

- **Docker & Docker Compose** -- [Install Docker Desktop](https://docs.docker.com/get-docker/)
- **IB Gateway or Trader Workstation (TWS)** -- [Download from IBKR](https://www.interactivebrokers.com/en/trading/ibgateway-stable.php)
- **An IBKR account** -- paper trading works fine for development and testing

---

## IB Gateway Setup

1. **Download and install** IB Gateway (or TWS) from the link above.

2. **Login** with your IBKR credentials. Paper trading is recommended for testing -- select the paper trading option on the login screen.

3. **Configure API settings** (Edit > Configuration > API > Settings):

   - **Enable ActiveX and Socket Clients** -- check this box.
   - **Socket port** -- set to `4002` for paper trading, or `4001` for live trading.
   - **Trusted IPs** -- `127.0.0.1` (localhost) is usually sufficient on macOS. On Linux, you may also need to add the Docker bridge gateway IP (typically `172.17.0.1`).
   - **Read-Only API** -- checking this box is fine for Phase 0 (we only read positions, never place orders).

4. **Leave IB Gateway running.** The broker-bridge service will connect to it over TCP.

---

## Quick Start

```bash
# 1. Copy the example environment file
cp .env.example .env

# 2. Edit .env if needed (see variable reference below)

# 3. Start all services
cd infra/docker && docker compose up --build

# 4. Open the dashboard
open http://localhost:3000
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `IB_HOST` | `host.docker.internal` | Hostname where IB Gateway is running. On macOS/Windows Docker Desktop, `host.docker.internal` resolves to the host machine. On Linux, use `172.17.0.1` or the Docker bridge gateway IP. |
| `IB_PORT` | `4002` | IB Gateway API socket port. `4002` = paper, `4001` = live. |
| `IB_CLIENT_ID` | `1` | Client ID for the IB API connection. Change if running multiple clients. |
| `POSTGRES_USER` | `trading` | Postgres username. |
| `POSTGRES_PASSWORD` | `trading_dev` | Postgres password. |
| `POSTGRES_DB` | `trading` | Postgres database name. |
| `POSTGRES_URL` | `postgresql+asyncpg://trading:trading_dev@postgres:5432/trading` | Full async Postgres connection string (used by services inside Docker). |
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection string (used by services inside Docker). |
| `API_HOST` | `0.0.0.0` | Bind address for the API server. |
| `API_PORT` | `8000` | Port for the API server. |
| `NEXT_PUBLIC_API_URL` | `http://localhost:8000` | API base URL as seen by the browser. |
| `NEXT_PUBLIC_WS_URL` | `ws://localhost:8000` | WebSocket base URL as seen by the browser. |

---

## Verifying It Works

1. **Check broker-bridge logs:**

   ```bash
   docker compose logs broker-bridge
   ```

   You should see `ib_connected` followed by `position_received` log entries for each position in your account.

2. **Health check:**

   ```bash
   curl http://localhost:8000/health
   ```

   Expected response: `{"status": "ok"}`

3. **Portfolio endpoint:**

   ```bash
   curl http://localhost:8000/portfolio
   ```

   Should return a JSON array of your current positions.

---

## Adding A Macro Tile

Macro tiles are configured in a single file:

- `packages/shared/data/macro_series.py`

To add a new tile:

1. Add a new `MacroSeriesConfig` entry to `MACRO_SERIES`.
2. For a direct FRED series, set `fred_series_id`.
3. For a computed series (e.g., a spread), set `series_ids` and `computed="spread"`.
4. Choose `category`, `format`, `recommended_change_windows`, and `refresh_policy`.
5. If the series is only used as an input, set `display=False`.

The Macro tab will automatically pick up the change after rebuild.

4. **Dashboard:**

   Open [http://localhost:3000](http://localhost:3000) in your browser. You should see a positions table and sector/country allocation pie charts.

5. **Live updates:**

   Change a position in IBKR paper trading (buy or sell something). The dashboard should update within seconds -- the WebSocket connection status indicator in the header will show `connected`.

---

## Security Mappings

The file `apps/broker-bridge/mappings/security_master.json` maps securities to their sector and country for exposure charts.

### Format

Each entry is keyed by either a **conid** (IB contract ID, as a string) or a **composite key** in the format `SYMBOL:SEC_TYPE:CURRENCY`:

```json
{
  "265598":          { "sector": "Technology", "country": "US" },
  "AAPL:STK:USD":   { "sector": "Technology", "country": "US" },
  "272093":          { "sector": "Technology", "country": "US" },
  "MSFT:STK:USD":   { "sector": "Technology", "country": "US" }
}
```

Lookup order: conid first, then the composite key as a fallback.

### Adding Entries

1. Open `apps/broker-bridge/mappings/security_master.json`.
2. Add your security with the appropriate key format and sector/country values.
3. Restart broker-bridge to pick up the changes:

   ```bash
   cd infra/docker && docker compose restart broker-bridge
   ```

Securities that are not mapped will appear as **"Unknown"** in the pie charts. They are still visible in the positions table -- they are not hidden.

---

## Running Tests

```bash
# broker-bridge tests
cd apps/broker-bridge && pip install -r requirements.txt && pytest

# api-server tests
cd apps/api-server && pip install -r requirements.txt && pytest
```

---

## Common Failure Modes & Fixes

| Problem | Cause | Fix |
|---------|-------|-----|
| `Connection refused` from broker-bridge | IB Gateway is not running, or the port in `.env` does not match the IB Gateway configuration. | Start IB Gateway and verify `IB_PORT` matches the socket port in IB Gateway settings (4002 for paper, 4001 for live). |
| IB Gateway auto-logoff | IB Gateway logs you out once per day by default. | Restart IB Gateway and log back in. broker-bridge will reconnect automatically with exponential backoff. |
| No positions showing | Your IBKR account has no open positions, or `reqPositions` did not fire. | Buy something in paper trading to create a position. Check broker-bridge logs for `positions_requested` and `position_received` entries. |
| Port conflicts (5432, 6379, 8000, or 3000 already in use) | Another service is using one of these ports. | Stop the conflicting service, or change the port mapping in `.env` and `infra/docker/docker-compose.yml`. |
| `host.docker.internal` does not resolve (Linux) | Docker on Linux does not always define `host.docker.internal`. | Set `IB_HOST=172.17.0.1` (or your Docker bridge gateway IP) in `.env`. The docker-compose file includes `extra_hosts` for this, but the env var may still need updating. |
| "Unknown" everywhere in charts | Your securities are not in `security_master.json`. | Add entries to `apps/broker-bridge/mappings/security_master.json` and restart broker-bridge (see Security Mappings above). |

---

## Project Structure

```
Trading/
├── .env.example                          # Environment variable template
├── README.md
├── apps/
│   ├── broker-bridge/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── pytest.ini
│   │   ├── broker_bridge/
│   │   │   ├── main.py                   # Entrypoint
│   │   │   ├── bridge.py                 # IB connection, position streaming
│   │   │   ├── config.py                 # Settings from env vars
│   │   │   ├── db.py                     # Postgres upsert logic
│   │   │   ├── enrichment.py             # Sector/country lookup from mappings
│   │   │   ├── models.py                 # PositionEvent Pydantic model
│   │   │   └── publisher.py              # Redis pub/sub publisher
│   │   ├── mappings/
│   │   │   └── security_master.json      # Security -> sector/country mappings
│   │   └── tests/
│   ├── api-server/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── pytest.ini
│   │   ├── api_server/
│   │   │   ├── main.py                   # FastAPI app, REST + WebSocket + scheduler
│   │   │   ├── config.py                 # Settings from env vars
│   │   │   ├── db.py                     # Postgres query logic
│   │   │   ├── exposures.py              # Sector/country exposure computation
│   │   │   ├── routers/
│   │   │   │   ├── risk.py               # Risk analytics REST endpoints
│   │   │   │   └── macro.py              # Macro backdrop REST endpoints
│   │   │   └── services/
│   │   │       ├── risk_service.py       # Risk computation orchestration + caching
│   │   │       └── data_service.py       # Data freshness checks + position weights
│   │   └── tests/
│   └── web/
│       ├── Dockerfile
│       ├── package.json
│       ├── next.config.js
│       ├── tsconfig.json
│       └── src/
│           ├── app/
│           │   ├── layout.tsx            # Root layout, dark theme
│           │   ├── page.tsx              # Dashboard with tabs: Overview | Risk | Stress | Macro
│           │   └── globals.css           # Global styles
│           ├── components/
│           │   ├── RiskSummaryPanel.tsx   # Vol, VaR, ES summary cards
│           │   ├── RiskContributorsTable.tsx  # Per-position risk contributions
│           │   ├── CorrelationPanel.tsx   # Top correlated pairs table
│           │   ├── ClustersPanel.tsx      # Hierarchical cluster analysis
│           │   ├── StressPanel.tsx        # Historical + factor stress results
│           │   └── MacroStrip.tsx         # FRED macro indicator cards
│           └── lib/
│               ├── api.ts                # API/WebSocket client helpers
│               └── risk-api.ts           # Risk + macro API client helpers
├── packages/
│   └── shared/                           # Shared risk, data, and DB modules
└── infra/
    └── docker/
        └── docker-compose.yml            # Full stack orchestration
```

---

## Phase 0 Scope Note

Phase 0 is the **foundation layer** that provides:

- Live position streaming from Interactive Brokers
- Postgres persistence with current + event history tables
- Real-time dashboard updates via Redis pub/sub and WebSocket
- Sector and country exposure breakdowns

Phase 1 builds on top of this with risk analytics, stress testing, correlation analysis, and macro backdrop. See below.

---

## Phase 1: Portfolio Risk Analytics

### What's New

Phase 1 adds institutional-grade risk analytics to the trading workstation:

- **Risk Metrics**: Portfolio volatility, Value-at-Risk (VaR), Expected Shortfall (ES), marginal and component risk contributions
- **Correlation Analysis**: Correlation matrix visualization, identification of highly correlated pairs, hierarchical clustering for sector/factor grouping
- **Stress Testing**: Historical scenario replay (COVID 2020 crash, 2022 rate shock) and factor-based parameter shocks (equity crash, rate spike, volatility spike, credit spread widening)
- **Macro Backdrop**: Integration with FRED (Federal Reserve Economic Data) for macro indicators including interest rates, inflation, unemployment trends
- **Data Pipelines**: Yahoo Finance price ingestion for portfolio holdings and factor proxies, FRED API integration for macro time series, incremental updates with caching
- **UI Enhancements**: Tab-based navigation (Overview | Risk | Stress | Macro) with Bloomberg-inspired risk panels

### Architecture Changes

The shared package (`packages/shared/`) now includes:

```
packages/shared/
├── risk/
│   ├── returns.py         # Return construction and alignment
│   ├── covariance.py      # Ledoit-Wolf and EWMA covariance estimation
│   ├── metrics.py         # VaR, ES, risk contributions, concentration
│   ├── correlation.py     # Correlation analysis and clustering
│   └── stress.py          # Historical replay and factor-based stress tests
├── data/
│   ├── yahoo.py           # Yahoo Finance price fetching
│   ├── fred.py            # FRED macro data integration
│   └── scheduler.py       # Background job scheduler for data updates
├── db/
│   ├── engine.py          # Async SQLAlchemy engine singleton
│   └── models.py          # SQLAlchemy models for price/macro storage
└── tests/                 # Comprehensive pytest suite
```

### What the Human Must Do After Phase 1

#### 1. Environment Variables

Add these to your `.env` file:

```bash
# FRED API Key (free, get from https://fred.stlouisfed.org/docs/api/api_key.html)
FRED_API_KEY=your_fred_api_key_here
```

Yahoo Finance requires no API key.

#### 2. Installation

Install new Python dependencies:

```bash
# For api-server
cd apps/api-server && pip install -r requirements.txt

# Or if using Docker:
docker-compose build api-server
```

#### 3. Database Tables

Phase 1 tables (`prices_daily`, `factor_prices_daily`, `fred_series_daily`, `data_sync_status`, `risk_results`) are created **automatically** on api-server startup via `init_phase1_db()`. No manual migration step is needed.

#### 4. Running Data Backfill

On first run, the system will automatically backfill historical data:

1. **Factor proxy prices** (2 years): SPY, QQQ, TLT, HYG, UUP, USO, BTC-USD
2. **Portfolio holdings prices** (1 year): Current positions from IB
3. **FRED macro series** (2 years): Interest rates, inflation, unemployment

This runs as a background task on api-server startup. You can also trigger manually:

```bash
# Via API endpoint
curl -X POST http://localhost:8000/risk/recompute
```

Initial backfill may take 5-10 minutes depending on portfolio size. Progress is logged to the api-server console.

#### 5. Verifying Results Sanity

Use these checks to ensure Phase 1 is working correctly:

##### Risk Metrics Sanity Checks

1. **Gross Exposure Alignment**: Compare the "Gross Exposure" value on the Overview tab with IB TWS/Gateway. They should match within 1-2%.

2. **Spot-Check Returns**: Pick 2-3 tickers and verify their 1-day return against Yahoo Finance:
   - Go to yahoo.com/quote/AAPL
   - Compare the daily % change with what's shown in the Risk tab

3. **VaR Sanity Check**:
   - Daily 95% VaR should be roughly **1.5-2.5% of portfolio value** for a diversified equity portfolio
   - VaR should **increase** when concentration increases (e.g., moving from 10 positions to 3 positions)
   - VaR should be **higher** for growth/tech portfolios than value/defensive portfolios

4. **Expected Shortfall (ES) Check**:
   - ES should always be **>= VaR** (it's the conditional expectation beyond VaR)
   - For normal distributions, ES is typically **20-30% higher** than VaR at 95% confidence

5. **Risk Contribution Check**:
   - Sum of component contributions (CCR) should equal portfolio volatility
   - Largest position should generally be a top risk contributor (but not always if it's low-volatility)

##### Stress Test Sanity Checks

1. **COVID 2020 Crash** (Feb-Mar 2020):
   - Equity-heavy portfolio: **-20% to -35%** loss
   - Tech-heavy portfolio: **-15% to -25%** loss
   - Balanced (60/40): **-12% to -18%** loss

2. **2022 Rate Shock** (Jan-Oct 2022):
   - Long-duration bonds: **-15% to -25%** loss
   - Growth stocks: **-20% to -35%** loss
   - Value/defensive: **-8% to -15%** loss

3. **Factor Stress Scenarios**:
   - Equity crash scenario (-30% SPY): Should show negative P&L for long equity portfolios
   - Rate spike scenario (+200bp): Should show negative P&L if long-duration bonds in portfolio
   - Combined scenario: Should show the largest losses (all shocks applied simultaneously)

##### Correlation Sanity Checks

1. **Highly Correlated Pairs**:
   - Tech stocks (GOOGL/META, AAPL/MSFT) should appear in top pairs with correlation > 0.7
   - Stocks in same sector should generally have correlation > 0.5

2. **Clustering**:
   - Tech stocks should cluster together
   - If you have bonds, they should form a separate cluster
   - Diversified portfolios should have 3-5 clusters

##### Macro Data Sanity Checks

1. **FRED Data Availability**:
   - Open the Macro tab
   - You should see charts for: Fed Funds Rate, 10Y Treasury, CPI, Unemployment
   - Data should extend back 2 years

2. **Current Values**:
   - Fed Funds Rate: Check against current FOMC announcements
   - 10Y Treasury: Check against current market quotes
   - CPI: Check against latest BLS release

#### 6. Troubleshooting

| Problem | Likely Cause | Fix |
|---------|-------------|-----|
| **IB Pacing Limits** | IB Gateway throttles historical data requests | System automatically falls back to Yahoo Finance. Check logs for `ib_pacing_limit` warnings. |
| **Yahoo Throttling (429 errors)** | Too many requests to Yahoo Finance | System rate-limits to 0.5s between requests. If you still see 429s, increase delay in `packages/shared/data/yahoo.py`. |
| **Missing Tickers** | Ticker not found on Yahoo Finance (wrong symbol, delisted, or international ticker) | Check `data_sync_status` table in Postgres for error details. International tickers may need exchange suffix (e.g., `0700.HK` for Tencent). |
| **Time Alignment Issues** | Mismatched trading calendars (crypto 24/7 vs equity) | All prices use adjusted close and US business day calendar. Crypto is aligned to US trading days. This is expected behavior. |
| **Missing FRED API Key** | `FRED_API_KEY` not set in `.env` | Macro tab will show a warning. Get free API key from https://fred.stlouisfed.org/docs/api/api_key.html |
| **Stale Risk Data** | Risk calculations cached from prior run | Click "Recompute" button on Risk tab to force fresh calculation. Risk auto-updates every 5 minutes by default. |
| **VaR Seems Too Low** | Using sample covariance on small window | Try switching to Ledoit-Wolf shrinkage (default) or EWMA covariance in risk settings. Sample covariance underestimates risk with <100 days of data. |
| **Clustering Looks Wrong** | Insufficient correlation structure in portfolio | Hierarchical clustering needs meaningful correlation. Portfolios with <10 positions or uncorrelated assets may produce unintuitive clusters. |

#### 7. Testing Phase 1

Run the comprehensive test suite:

```bash
# Test the shared package (risk analytics)
cd packages/shared
pip install -r ../../apps/api-server/requirements.txt  # Install dependencies
pytest -v

# Test the api-server integration
cd ../../apps/api-server
pytest -v

# Run all tests
cd ../..
pytest apps/ packages/ -v
```

All tests should pass. If any fail, check:
- Dependencies installed (`pandas`, `numpy`, `scipy`, `scikit-learn`)
- Python version >= 3.10
- Test fixtures are loading correctly

#### 8. Performance Notes

- **Initial backfill**: 5-10 minutes for 20-position portfolio
- **Daily incremental update**: <30 seconds
- **Risk calculation**: <2 seconds for portfolios up to 50 positions
- **Correlation clustering**: <1 second for portfolios up to 100 positions
- **Stress test**: <5 seconds (all scenarios combined)

If calculations take longer:
- Check Postgres query performance (add indexes if needed)
- Reduce covariance estimation window (default 252 days)
- Consider caching risk calculations (default 5-minute TTL)

#### 9. Next Steps

Phase 1 provides the risk analytics foundation. Future phases will add:

- **Phase 2**: Trade idea generation, backtesting, portfolio optimization
- **Phase 3**: Real-time alerts, risk limit monitoring, compliance checks
- **Phase 4**: Multi-account aggregation, performance attribution, client reporting

For now, focus on:
1. Validating risk metrics against known benchmarks
2. Stress-testing with historical scenarios
3. Monitoring correlation changes over time
4. Using macro backdrop to contextualize portfolio risk
