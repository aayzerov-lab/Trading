# API Quick Reference - Phase 1 Risk Analytics

## Risk Endpoints

### GET /risk/summary
```python
Parameters:
  window: int = 252  # 60 or 252 days
  method: str = "lw" # "lw" or "ewma"

Returns:
{
  "portfolio_vol": float,        # Annualized volatility
  "var_95": float,               # 1-day 95% VaR
  "var_99": float,               # 1-day 99% VaR
  "es_95": float,                # 1-day 95% Expected Shortfall
  "es_99": float,                # 1-day 99% Expected Shortfall
  "hhi": float,                  # Herfindahl index (concentration)
  "effective_n": float,          # Effective number of positions
  "metadata": {
    "window": int,
    "method": str,
    "asof_date": str,
    "portfolio_hash": str,
    "num_positions": int,
    "portfolio_value": float
  }
}
```

### GET /risk/contributors
```python
Parameters:
  window: int = 252
  method: str = "lw"

Returns:
{
  "contributors": [
    {
      "symbol": str,
      "weight": float,
      "mcr": float,        # Marginal contribution to risk
      "ccr": float,        # Component contribution to risk
      "pct_var": float     # % of portfolio variance
    },
    ...
  ],
  "metadata": {...}
}
```

### GET /risk/correlation/pairs
```python
Parameters:
  window: int = 252
  n: int = 20          # 5-50 pairs

Returns:
{
  "pairs": [
    ["AAPL", "MSFT", 0.85],
    ["GOOG", "META", 0.78],
    ...
  ],
  "metadata": {...}
}
```

### GET /risk/clusters
```python
Parameters:
  window: int = 252
  max_clusters: int = 8  # 2-20 clusters

Returns:
{
  "clusters": [
    {
      "cluster_id": int,
      "symbols": [str, ...],
      "exposure": float,
      "num_symbols": int
    },
    ...
  ],
  "metadata": {...}
}
```

### GET /risk/stress
```python
Returns:
{
  "stress": {
    "historical": {
      "crisis_2008": {"loss_pct": float, "loss_dollar": float},
      "covid_2020": {...},
      "rate_shock_2022": {...}
    },
    "factor": {
      "equity_shock": {"scenario": str, "loss_pct": float},
      "bond_shock": {...},
      "usd_shock": {...},
      "vix_spike": {...}
    }
  },
  "metadata": {...}
}
```

### POST /risk/recompute
```python
Returns:
{
  "status": "recomputation_triggered"
}

Side Effects:
  - Publishes "risk_recompute" event to Redis
  - Triggers background recomputation for all windows/methods
  - Publishes "risk_updated" event when complete
```

## Macro Endpoint

### GET /macro/overview
```python
Returns:
{
  "DGS10": {
    "name": "10-Year Treasury Constant Maturity Rate",
    "latest_value": float,
    "latest_date": str,
    "change_1m": float,
    "change_3m": float,
    "unit": "Percent"
  },
  "DGS2": {...},
  "DFF": {...},
  "CPIAUCSL": {...},
  "UNRATE": {...},
  "SP500": {...},
  "VIXCLS": {...}
}
```

## Service Layer Functions

### risk_service.py

```python
async def compute_risk_pack(
    window: int = 252,
    method: str = "lw",
    force: bool = False,
) -> dict[str, Any]:
    """Orchestrate full risk computation pipeline."""

async def get_cached_risk_result(
    result_type: str,
    asof_date: date,
    window: int,
    method: str,
    portfolio_hash: str,
) -> dict[str, Any] | None:
    """Check risk_results table for cached result."""

async def cache_risk_result(
    result_type: str,
    asof_date: date,
    window: int,
    method: str,
    portfolio_hash: str,
    result: dict[str, Any],
) -> None:
    """Store risk result in risk_results table."""
```

### data_service.py

```python
async def ensure_data_fresh(force: bool = False) -> dict[str, Any]:
    """Ensure price data is up to date.
    
    Returns:
    {
      "symbols_updated": int,
      "symbols_checked": int,
      "errors": [str, ...]
    }
    """

async def get_position_weights() -> tuple[np.ndarray, list[str], float]:
    """Get current position weights for risk computation.
    
    Returns:
      (weights_array, symbols_list, portfolio_value)
    """
```

## WebSocket Events

Connect to `ws://localhost:8000/stream` to receive real-time events:

```javascript
{
  "type": "position",
  "data": {...}  // Position update
}

{
  "type": "account_summary",
  "data": {...}  // Account summary update
}

{
  "type": "data_updated",
  "data": {
    "timestamp": "2024-01-15T21:30:00Z",
    "status": "completed"
  }
}

{
  "type": "risk_recompute",
  "data": {
    "timestamp": "2024-01-15T21:30:00Z",
    "reason": "portfolio_changed"
  }
}

{
  "type": "risk_updated",
  "data": {
    "status": "completed"
  }
}
```

## Shared Package Integration

### Database
- `shared.db.engine.get_shared_engine()` - Get shared engine
- `shared.db.engine.init_phase1_db(url)` - Initialize tables
- `shared.db.models.phase1_metadata` - SQLAlchemy metadata

### Data Fetching
- `shared.data.yahoo.fetch_prices_yahoo(symbols, lookback_days)` - Fetch from Yahoo
- `shared.data.yahoo.get_prices_from_db(symbols, lookback_days)` - Get from DB
- `shared.data.fred.compute_macro_overview()` - Compute macro overview

### Scheduler
- `shared.data.scheduler.run_daily_data_update()` - Daily update job
- `shared.data.scheduler.compute_portfolio_hash()` - Hash current positions
- `shared.data.scheduler.check_and_trigger_risk_recompute()` - Check if recompute needed

### Risk Computation
- `shared.risk.returns.build_price_matrix(df, symbols)` - Build price matrix
- `shared.risk.returns.compute_log_returns(prices)` - Compute log returns
- `shared.risk.covariance.estimate_covariance(returns, method)` - Estimate covariance
- `shared.risk.metrics.build_risk_summary(weights, cov, value)` - Risk summary
- `shared.risk.metrics.build_risk_contributors(...)` - Risk contributors
- `shared.risk.correlation.correlation_matrix(cov)` - Correlation matrix
- `shared.risk.correlation.top_correlated_pairs(corr, symbols, n)` - Top pairs
- `shared.risk.correlation.hierarchical_clusters(corr, max_clusters)` - Clusters
- `shared.risk.stress.run_all_stress_tests(weights, symbols, returns)` - Stress tests

## Error Handling

All endpoints return consistent error responses:

```python
{
  "detail": "Error message describing what went wrong"
}
```

HTTP Status Codes:
- 400: Bad Request (invalid parameters)
- 500: Internal Server Error (computation failed)

## Caching Behavior

Risk computations are cached using:
- Key: (result_type, asof_date, window, method, portfolio_hash)
- Cache hit: Returns immediately
- Cache miss: Computes and caches
- Force recompute: Bypasses cache

Portfolio hash changes when:
- Position quantities change
- Positions are added or removed
- Market values change significantly

## Performance Characteristics

- Risk computation: 2-5 seconds (30 positions, 252-day window)
- Cache lookup: <100ms
- Macro overview: <500ms
- Data freshness check: <1 second

All CPU-intensive work runs in thread pool to avoid blocking the event loop.

## Example Usage

```bash
# Get risk summary
curl "http://localhost:8000/risk/summary?window=252&method=lw"

# Get top 10 correlated pairs
curl "http://localhost:8000/risk/correlation/pairs?window=252&n=10"

# Get macro overview
curl http://localhost:8000/macro/overview

# Force recompute
curl -X POST http://localhost:8000/risk/recompute

# Check health
curl http://localhost:8000/health
```

## File Locations

- Risk Router: `/Users/aayzerov/Desktop/Trading/apps/api-server/api_server/routers/risk.py`
- Macro Router: `/Users/aayzerov/Desktop/Trading/apps/api-server/api_server/routers/macro.py`
- Risk Service: `/Users/aayzerov/Desktop/Trading/apps/api-server/api_server/services/risk_service.py`
- Data Service: `/Users/aayzerov/Desktop/Trading/apps/api-server/api_server/services/data_service.py`
- Main App: `/Users/aayzerov/Desktop/Trading/apps/api-server/api_server/main.py`
- Config: `/Users/aayzerov/Desktop/Trading/apps/api-server/api_server/config.py`
- Requirements: `/Users/aayzerov/Desktop/Trading/apps/api-server/requirements.txt`
