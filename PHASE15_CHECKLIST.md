# Phase 1.5 — Human Verification Checklist

## Phase 1.5 — Human Verification Checklist

### 1. Database Migration
- [ ] Run `CREATE TABLE fx_daily` and `CREATE TABLE security_overrides` DDL (or Alembic migration)
- [ ] Verify tables exist: `SELECT * FROM fx_daily LIMIT 1;` and `SELECT * FROM security_overrides LIMIT 1;`

### 2. Security Overrides (FX Configuration)
- [ ] For each non-USD or non-USD-listed position, insert a row into `security_overrides`:
  ```sql
  INSERT INTO security_overrides (symbol, currency, is_usd_listed, fx_pair)
  VALUES ('005930.KS', 'KRW', 0, 'KRWUSD');
  ```
- [ ] For ADRs listed on US exchanges (e.g., BABA, TSM), keep is_usd_listed = 1 (default)
- [ ] Verify FX pair naming: use format like EURUSD, KRWUSD, JPYUSD (always {CCY}USD)

### 3. FX Data Verification
- [ ] Trigger a data update or wait for daily scheduler
- [ ] Verify fx_daily has data: `SELECT pair, COUNT(*), MIN(date), MAX(date) FROM fx_daily GROUP BY pair;`
- [ ] Verify FX rates are reasonable (e.g., EURUSD ~1.05-1.15, JPYUSD ~0.006-0.008)

### 4. Data Quality Panel
- [ ] Open the Risk tab in the UI
- [ ] Verify "System Health" panel appears below Risk Summary
- [ ] Check that coverage metrics (60d / 252d) match expectations
- [ ] Check that warning banners appear for any issues (excluded positions, missing data)

### 5. Risk Metadata Panel
- [ ] Verify "Risk Pack Info" panel appears at the bottom of Risk tab
- [ ] Click to expand and verify metadata fields (asof_date, method, library versions)
- [ ] Verify excluded_symbols list matches any positions without sufficient price history

### 6. Stress Test Quality Labels
- [ ] Open Stress tab
- [ ] Verify factor stress results include regression_diagnostics (check API response)
- [ ] Verify symbols with < 60 day overlap show quality="invalid" and beta=0

### 7. Ingestion Verification
- [ ] Verify daily scheduler fetches prices with 10-business-day overlap buffer
- [ ] Confirm no "full-history refetch" happens on incremental runs
- [ ] Test weekly sweep manually: ensure it re-fetches ~60 trading days
- [ ] Verify bulk upsert works (no row-by-row inserts in logs)

### 8. PSD Safety
- [ ] Verify covariance matrix eigenvalues are all positive (check logs for "clamping" messages)
- [ ] Verify portfolio variance is non-negative in risk summary

### 9. Running Tests
```bash
cd packages/shared
python -m pytest tests/test_phase15.py -v
python -m pytest tests/ -v  # full suite
```

### 10. Weekly Sweep Schedule
- [ ] Configure cron or scheduler to run `run_weekly_adjustment_sweep()` weekly (e.g., Saturday 6am)
- [ ] Example cron: `0 6 * * 6 curl -X POST http://localhost:8000/risk/recompute`

---

**Note:** Phase 1.5 does NOT change:
- Factor list (SPY, QQQ, TLT, GLD, UUP, HYG)
- UI layout or styling (dark Bloomberg theme preserved)
- Any Phase 2 features (Greeks, scenario builder, etc.)
