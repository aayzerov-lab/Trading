"""Data quality and coverage metrics for risk monitoring (Phase 1.5).

Computes coverage, data integrity, and system health metrics to display
in the UI's System Health / Data Quality panel.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import structlog

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# Warning thresholds (configurable constants)
# ---------------------------------------------------------------------------

WARN_EXCLUDED_EXPOSURE_PCT = 10.0     # Warn if >10% gross exposure excluded
WARN_MISSING_PRICE_PCT = 5.0          # Warn if >5% has missing recent prices
WARN_UNKNOWN_SECTOR_PCT = 20.0        # Warn if >20% unknown sector
WARN_UNKNOWN_COUNTRY_PCT = 20.0       # Warn if >20% unknown country
WARN_FX_COVERAGE_PCT = 95.0           # Warn if FX coverage < 95%
WARN_INVALID_BETA_PCT = 10.0          # Warn if >10% invalid betas for core factors
OUTLIER_RETURN_THRESHOLD = 0.30       # |return| > 30% flagged as outlier
FLAT_STREAK_THRESHOLD = 5             # >=5 days of flat returns flagged


def compute_coverage_metrics(
    positions: List[Dict[str, Any]],
    returns_dict: Dict[str, pd.Series],
    symbols: List[str],
    valid_symbols: List[str],
    window: int = 252,
    min_overlap: int = 60,
) -> Dict[str, Any]:
    """Compute covariance coverage metrics for a given window.

    Args:
        positions: List of position dicts with symbol, market_value
        returns_dict: {symbol: Series} of log returns
        symbols: All portfolio symbols
        valid_symbols: Symbols included in covariance
        window: Lookback window
        min_overlap: Minimum overlap required

    Returns:
        Dict with coverage metrics
    """
    excluded = [s for s in symbols if s not in valid_symbols]
    gross_exposure = sum(abs(float(p.get("market_value", 0) or 0)) for p in positions)

    excluded_exposure = 0.0
    excluded_details = []
    for sym in excluded:
        for p in positions:
            if p["symbol"] == sym:
                mv = abs(float(p.get("market_value", 0) or 0))
                excluded_exposure += mv
                n_returns = len(returns_dict.get(sym, []))
                reason = f"insufficient_history ({n_returns} < {min_overlap})" if sym in returns_dict else "no_price_data"
                excluded_details.append({
                    "symbol": sym,
                    "exposure": mv,
                    "exposure_pct": (mv / gross_exposure * 100) if gross_exposure > 0 else 0,
                    "reason": reason,
                })
                break

    excluded_details.sort(key=lambda x: x["exposure"], reverse=True)

    return {
        "window": window,
        "included_count": len(valid_symbols),
        "excluded_count": len(excluded),
        "excluded_exposure_pct": (excluded_exposure / gross_exposure * 100) if gross_exposure > 0 else 0,
        "top_excluded": excluded_details[:5],
    }


def compute_data_integrity_metrics(
    positions: List[Dict[str, Any]],
    prices: Dict[str, pd.DataFrame],
    returns_dict: Dict[str, pd.Series],
) -> Dict[str, Any]:
    """Compute data integrity metrics.

    Checks for:
    - Missing price data in last 60 days
    - Outlier returns (|return| > 30%)
    - Flat return streaks (>=5 days same return)
    """
    gross_exposure = sum(abs(float(p.get("market_value", 0) or 0)) for p in positions)
    symbols = [p["symbol"] for p in positions]

    # Missing price data check
    missing_price_exposure = 0.0
    nan_rows_skipped = 0
    outlier_days = 0
    flat_streak_count = 0

    today = date.today()
    cutoff_60d = pd.Timestamp(today) - pd.Timedelta(days=90)  # ~60 trading days

    for sym in symbols:
        mv = 0.0
        for p in positions:
            if p["symbol"] == sym:
                mv = abs(float(p.get("market_value", 0) or 0))
                break

        price_df = prices.get(sym)
        if price_df is None or price_df.empty:
            missing_price_exposure += mv
            continue

        df = price_df.copy()
        if "date" in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df["date"]):
                df["date"] = pd.to_datetime(df["date"])
            recent = df[df["date"] >= cutoff_60d]
            if recent.empty:
                missing_price_exposure += mv

            # Count NaN rows
            nan_rows_skipped += int(df["adj_close"].isna().sum()) if "adj_close" in df.columns else 0

        # Check returns for outliers and flat streaks
        ret_series = returns_dict.get(sym)
        if ret_series is not None and len(ret_series) > 0:
            # Outlier days
            outlier_mask = ret_series.abs() > OUTLIER_RETURN_THRESHOLD
            outlier_days += int(outlier_mask.sum())

            # Flat streak detection
            abs_rets = ret_series.abs()
            streak = 0
            for r in abs_rets:
                if r < 1e-8:  # essentially zero return
                    streak += 1
                    if streak >= FLAT_STREAK_THRESHOLD:
                        flat_streak_count += 1
                        streak = 0
                else:
                    streak = 0

    return {
        "missing_price_exposure_pct": (missing_price_exposure / gross_exposure * 100) if gross_exposure > 0 else 0,
        "nan_rows_skipped": nan_rows_skipped,
        "outlier_return_days": outlier_days,
        "flat_streak_flags": flat_streak_count,
    }


def compute_classification_metrics(
    positions: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Compute sector/country classification coverage."""
    gross_exposure = sum(abs(float(p.get("market_value", 0) or 0)) for p in positions)

    unknown_sector_exposure = 0.0
    unknown_country_exposure = 0.0

    for p in positions:
        mv = abs(float(p.get("market_value", 0) or 0))
        sector = p.get("sector", "Unknown") or "Unknown"
        country = p.get("country", "Unknown") or "Unknown"

        if sector == "Unknown":
            unknown_sector_exposure += mv
        if country == "Unknown":
            unknown_country_exposure += mv

    return {
        "unknown_sector_pct": (unknown_sector_exposure / gross_exposure * 100) if gross_exposure > 0 else 0,
        "unknown_country_pct": (unknown_country_exposure / gross_exposure * 100) if gross_exposure > 0 else 0,
    }


def compute_fx_coverage_metrics(
    positions: List[Dict[str, Any]],
    security_info: Dict[str, dict],
    fx_flags: Dict[str, str],
) -> Dict[str, Any]:
    """Compute FX coverage metrics.

    Args:
        positions: Position list
        security_info: {symbol: {currency, is_usd_listed, fx_pair}}
        fx_flags: {symbol: flag_reason} for FX issues
    """
    gross_exposure = sum(abs(float(p.get("market_value", 0) or 0)) for p in positions)

    non_usd_exposure = 0.0
    fx_covered_exposure = 0.0

    for p in positions:
        sym = p["symbol"]
        mv = abs(float(p.get("market_value", 0) or 0))
        info = security_info.get(sym, {})

        if info.get("currency", "USD") != "USD" and not info.get("is_usd_listed", True):
            non_usd_exposure += mv
            if sym not in fx_flags:
                fx_covered_exposure += mv

    non_usd_pct = (non_usd_exposure / gross_exposure * 100) if gross_exposure > 0 else 0
    fx_coverage_pct = (fx_covered_exposure / non_usd_exposure * 100) if non_usd_exposure > 0 else 100.0

    return {
        "non_usd_exposure_pct": non_usd_pct,
        "fx_coverage_pct": fx_coverage_pct,
        "fx_issues": {sym: reason for sym, reason in fx_flags.items()},
    }


def compute_timestamp_info(
    engine_available: bool = True,
) -> Dict[str, Any]:
    """Return placeholder timestamp info (actual values set by caller)."""
    return {
        "last_positions_update": None,
        "last_prices_update": None,
        "last_fx_update": None,
        "last_risk_compute": None,
    }


def generate_warnings(
    coverage_60d: Dict[str, Any],
    coverage_252d: Dict[str, Any],
    integrity: Dict[str, Any],
    classification: Dict[str, Any],
    fx_coverage: Dict[str, Any],
    beta_diagnostics: Dict[str, Any] | None = None,
) -> List[Dict[str, str]]:
    """Generate warning banners based on thresholds.

    Returns:
        List of {level: 'warning'|'error', message: str}
    """
    warnings = []

    # Coverage warnings
    for cov in [coverage_60d, coverage_252d]:
        w = cov.get("window", "?")
        excl_pct = cov.get("excluded_exposure_pct", 0)
        if excl_pct > WARN_EXCLUDED_EXPOSURE_PCT:
            warnings.append({
                "level": "warning",
                "message": f"{w}d window: {excl_pct:.1f}% gross exposure excluded from covariance",
            })

    # Missing price data
    if integrity.get("missing_price_exposure_pct", 0) > WARN_MISSING_PRICE_PCT:
        warnings.append({
            "level": "warning",
            "message": f"{integrity['missing_price_exposure_pct']:.1f}% gross exposure missing recent price data",
        })

    # Outlier returns
    if integrity.get("outlier_return_days", 0) > 0:
        warnings.append({
            "level": "info",
            "message": f"{integrity['outlier_return_days']} outlier return days detected (|return| > 30%)",
        })

    # Classification
    if classification.get("unknown_sector_pct", 0) > WARN_UNKNOWN_SECTOR_PCT:
        warnings.append({
            "level": "warning",
            "message": f"{classification['unknown_sector_pct']:.1f}% exposure has Unknown sector",
        })

    if classification.get("unknown_country_pct", 0) > WARN_UNKNOWN_COUNTRY_PCT:
        warnings.append({
            "level": "warning",
            "message": f"{classification['unknown_country_pct']:.1f}% exposure has Unknown country",
        })

    # FX coverage
    non_usd = fx_coverage.get("non_usd_exposure_pct", 0)
    if non_usd > 0 and fx_coverage.get("fx_coverage_pct", 100) < WARN_FX_COVERAGE_PCT:
        warnings.append({
            "level": "error",
            "message": f"FX coverage {fx_coverage['fx_coverage_pct']:.1f}% for {non_usd:.1f}% non-USD exposure",
        })

    # Beta diagnostics
    if beta_diagnostics:
        invalid_pct = beta_diagnostics.get("invalid_exposure_pct", 0)
        if invalid_pct > WARN_INVALID_BETA_PCT:
            warnings.append({
                "level": "warning",
                "message": f"{invalid_pct:.1f}% gross exposure has invalid betas for core equity factors",
            })

    return warnings


def compute_beta_quality_summary(
    stress_results: Dict[str, Any],
    positions: List[Dict[str, Any]],
    valid_symbols: List[str],
) -> Dict[str, Any]:
    """Summarize beta quality across factor stress results.

    Looks at regression_diagnostics from factor stress tests to compute
    exposure-weighted quality distribution.
    """
    gross_exposure = sum(abs(float(p.get("market_value", 0) or 0)) for p in positions)
    pos_mv = {p["symbol"]: abs(float(p.get("market_value", 0) or 0)) for p in positions}

    # Check core equity factors (SPY, QQQ)
    core_factors = ["SPY", "QQQ"]
    invalid_exposure = 0.0
    weak_exposure = 0.0
    good_exposure = 0.0

    factor_results = stress_results.get("factor", {})

    # Look at equity_crash scenario which uses SPY/QQQ
    for scenario_key, scenario_data in factor_results.items():
        diags = scenario_data.get("regression_diagnostics", {})
        for sym in valid_symbols:
            mv = pos_mv.get(sym, 0)
            sym_diags = diags.get(sym, {})

            worst_quality = "good"
            for factor in core_factors:
                d = sym_diags.get(factor)
                if d is None:
                    continue
                q = d.get("quality", "good")
                if q == "invalid":
                    worst_quality = "invalid"
                    break
                elif q == "weak" and worst_quality != "invalid":
                    worst_quality = "weak"

            if worst_quality == "invalid":
                invalid_exposure += mv
            elif worst_quality == "weak":
                weak_exposure += mv
            else:
                good_exposure += mv

        # Only need one scenario to get diagnostics
        break

    return {
        "good_exposure_pct": (good_exposure / gross_exposure * 100) if gross_exposure > 0 else 0,
        "weak_exposure_pct": (weak_exposure / gross_exposure * 100) if gross_exposure > 0 else 0,
        "invalid_exposure_pct": (invalid_exposure / gross_exposure * 100) if gross_exposure > 0 else 0,
    }


def build_data_quality_pack(
    positions: List[Dict[str, Any]],
    prices: Dict[str, pd.DataFrame],
    returns_dict: Dict[str, pd.Series],
    symbols: List[str],
    valid_symbols_60: List[str],
    valid_symbols_252: List[str],
    security_info: Dict[str, dict] | None = None,
    fx_flags: Dict[str, str] | None = None,
    stress_results: Dict[str, Any] | None = None,
    timestamps: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Build complete data quality pack for the UI.

    Returns a structured dict with all health metrics, warnings, etc.
    """
    if security_info is None:
        security_info = {}
    if fx_flags is None:
        fx_flags = {}
    if timestamps is None:
        timestamps = {}

    coverage_60 = compute_coverage_metrics(
        positions, returns_dict, symbols, valid_symbols_60,
        window=60, min_overlap=60,
    )
    coverage_252 = compute_coverage_metrics(
        positions, returns_dict, symbols, valid_symbols_252,
        window=252, min_overlap=60,
    )
    integrity = compute_data_integrity_metrics(positions, prices, returns_dict)
    classification = compute_classification_metrics(positions)
    fx_coverage = compute_fx_coverage_metrics(positions, security_info, fx_flags)

    beta_diags = None
    if stress_results:
        beta_diags = compute_beta_quality_summary(stress_results, positions, valid_symbols_252)

    warnings = generate_warnings(
        coverage_60, coverage_252, integrity, classification, fx_coverage, beta_diags,
    )

    return {
        "coverage": {
            "60d": coverage_60,
            "252d": coverage_252,
        },
        "integrity": integrity,
        "classification": classification,
        "fx": fx_coverage,
        "beta_quality": beta_diags or {},
        "timestamps": timestamps,
        "warnings": warnings,
        "computed_at": datetime.now(timezone.utc).isoformat() + "Z",
    }
