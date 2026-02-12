# Phase 1 QA Checklist

## Quick Start

After Phase 1 deployment, run these commands to validate everything:

```bash
# 1. Run the test suite
cd /Users/aayzerov/Desktop/Trading/packages/shared
pytest -v

# Expected output: 112 tests passed

# 2. Check environment variables
grep FRED_API_KEY .env
# Should show: FRED_API_KEY=your_key_here

# 3. Trigger data backfill
curl -X POST http://localhost:8000/risk/recompute

# 4. Verify risk endpoint
curl http://localhost:8000/risk/summary
# Should return JSON with vol_1d, var_95_1d, etc.
```

---

## Human Verification Checklist

### ✅ Installation & Setup
- [ ] FRED_API_KEY added to .env file
- [ ] Dependencies installed: `pip install -r apps/api-server/requirements.txt`
- [ ] Database migrations run: `alembic upgrade head`
- [ ] Initial backfill completed (check api-server logs)

### ✅ Risk Metrics Sanity
- [ ] Gross exposure matches IB TWS/Gateway (within 1-2%)
- [ ] Daily 95% VaR is 1.5-2.5% of portfolio value for diversified equity portfolio
- [ ] ES (Expected Shortfall) >= VaR
- [ ] Sum of component contributions (CCR) equals portfolio volatility
- [ ] Spot-check: Pick 2 tickers, verify 1-day return vs Yahoo Finance

### ✅ Stress Testing Sanity
- [ ] COVID 2020 crash shows -20% to -35% loss for equity-heavy portfolios
- [ ] 2022 rate shock shows -15% to -25% loss for long-duration bonds
- [ ] Equity crash scenario (-30% SPY) shows negative P&L for long equity
- [ ] Combined scenario shows largest losses

### ✅ Correlation & Clustering
- [ ] Tech stocks (GOOGL/META, AAPL/MSFT) appear in top pairs with corr > 0.7
- [ ] Hierarchical clustering groups similar sectors together
- [ ] Gross exposure percentages across clusters sum to 100%

### ✅ Macro Data
- [ ] FRED data visible on Macro tab (Fed Funds, 10Y Treasury, CPI, Unemployment)
- [ ] Data extends back 2 years
- [ ] Current values match latest public releases

### ✅ Performance
- [ ] Initial backfill completes in < 10 minutes
- [ ] Daily incremental update < 30 seconds
- [ ] Risk calculation < 2 seconds (up to 50 positions)
- [ ] Stress test < 5 seconds (all scenarios)

### ✅ Test Suite
- [ ] All 112 tests pass: `pytest -v packages/shared/tests/`
- [ ] No import errors
- [ ] No deprecation warnings from core libraries

---

## Troubleshooting Quick Reference

| Symptom | Quick Fix |
|---------|-----------|
| Tests fail with ModuleNotFoundError | `pip install pandas numpy scipy scikit-learn structlog` |
| "Missing FRED_API_KEY" warning | Add `FRED_API_KEY=...` to .env and restart api-server |
| Yahoo 429 errors | Increase delay in `packages/shared/data/yahoo.py` (line ~50) |
| VaR seems too low | Switch to Ledoit-Wolf method (default) or increase window to 252 days |
| Clustering looks wrong | Normal for portfolios < 10 positions or low correlation |
| Stress test shows 0 P&L | No historical data for stress dates - run backfill with earlier start date |

---

## Test Execution Examples

```bash
# Run all tests
cd /Users/aayzerov/Desktop/Trading/packages/shared
pytest -v

# Run specific test file
pytest tests/test_returns.py -v

# Run specific test class
pytest tests/test_metrics.py::TestPortfolioVolatility -v

# Run with coverage report
pytest --cov=shared.risk --cov-report=html

# Run with detailed output
pytest -vv -s
```

---

## Expected Test Output

```
tests/test_returns.py::TestBuildPriceMatrix::test_build_price_matrix_alignment PASSED
tests/test_returns.py::TestBuildPriceMatrix::test_build_price_matrix_drops_short_history PASSED
tests/test_returns.py::TestBuildPriceMatrix::test_build_price_matrix_no_forward_fill PASSED
... (112 tests total)

===================== 112 passed in 2.45s =====================
```

If any test fails, check:
1. Dependencies installed correctly
2. Python version >= 3.10
3. No conflicting versions of numpy/pandas/scipy

---

## Files Created by QA Agent

```
packages/shared/
├── tests/
│   ├── __init__.py              # 1 line
│   ├── conftest.py              # 133 lines - shared fixtures
│   ├── test_returns.py          # 256 lines - 28 tests
│   ├── test_covariance.py       # 252 lines - 25 tests
│   ├── test_metrics.py          # 440 lines - 34 tests
│   └── test_correlation.py      # 365 lines - 25 tests
└── pytest.ini                   # 3 lines

Total: 1,450 lines of production-quality test code
```

README.md also updated with comprehensive Phase 1 documentation (150+ lines added).

---

## Next Steps After Validation

1. **Monitor production**: Watch api-server logs for data sync errors
2. **Calibrate thresholds**: Adjust VaR confidence levels based on risk appetite
3. **Custom stress scenarios**: Add firm-specific historical scenarios
4. **Alert setup** (Future): Configure risk limit breach notifications

---

## Contact Points

- Test failures: Check pytest output and verify fixtures load correctly
- Risk metric issues: Validate covariance estimation method and window size
- Data sync issues: Check `data_sync_status` table in Postgres
- Performance issues: Enable query logging and check for missing indexes

---

Generated by QA Agent - Phase 1 Deliverable
