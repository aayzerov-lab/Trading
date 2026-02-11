"use client";

import { useState } from "react";
import { DataQualityPack } from "@/lib/risk-api";

interface DataQualityPanelProps {
  dataQuality: DataQualityPack | null;
  loading?: boolean;
}

function fmtPct(v: number): string {
  return `${v.toFixed(2)}%`;
}

function fmtTimestamp(iso: string | null): string {
  if (!iso) return "—";
  try {
    const d = new Date(iso);
    return d.toLocaleString("en-US", {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    });
  } catch {
    return iso;
  }
}

export default function DataQualityPanel({
  dataQuality,
  loading = false,
}: DataQualityPanelProps) {
  const [expanded, setExpanded] = useState(false);

  if (loading) {
    return (
      <div className="positions-panel" style={{ marginTop: "1px" }}>
        <div className="positions-header">
          <span className="positions-title">System Health</span>
        </div>
        <div className="empty-state">Loading data quality metrics...</div>
      </div>
    );
  }

  if (!dataQuality) {
    return (
      <div className="positions-panel" style={{ marginTop: "1px" }}>
        <div className="positions-header">
          <span className="positions-title">System Health</span>
        </div>
        <div className="empty-state">No data quality information available</div>
      </div>
    );
  }

  const coverage60d = dataQuality.coverage?.["60d"];
  const coverage252d = dataQuality.coverage?.["252d"];
  const integrity = dataQuality.integrity;
  const fx = dataQuality.fx;
  const betaQuality = dataQuality.beta_quality;
  const timestamps = dataQuality.timestamps;
  const warningsList = dataQuality.warnings ?? [];
  const hasWarnings = warningsList.length > 0;
  const errors = warningsList.filter((w) => w.level === "error");
  const warnings = warningsList.filter((w) => w.level === "warning");

  const warningCount = errors.length + warnings.length;
  const coverageLabel = coverage60d
    ? `${coverage60d.included_count}/${coverage60d.included_count + coverage60d.excluded_count}`
    : "—";

  return (
    <div className="positions-panel" style={{ marginTop: "1px" }}>
      <div className="positions-header">
        <span className="positions-title">System Health</span>
        <button
          className="recompute-btn"
          onClick={() => setExpanded(!expanded)}
        >
          {expanded ? "Collapse" : "Expand"}
        </button>
      </div>

      {!expanded && (
        <div
          style={{
            padding: "8px 12px",
            fontFamily: "var(--font-mono)",
            fontSize: "10px",
            color: "var(--text-secondary)",
            display: "flex",
            gap: "16px",
            flexWrap: "wrap",
            borderBottom: "1px solid var(--border-subtle)",
          }}
        >
          <div>
            <span style={{ color: "var(--text-dimmed)" }}>Coverage: </span>
            {coverageLabel}
          </div>
          {integrity && (
            <div>
              <span style={{ color: "var(--text-dimmed)" }}>Integrity: </span>
              {integrity.outlier_return_days === 0 &&
                integrity.flat_streak_flags === 0 &&
                integrity.missing_price_exposure_pct === 0
                ? "Clean"
                : `${integrity.outlier_return_days} outliers, ${integrity.flat_streak_flags} flat`}
            </div>
          )}
          {warningCount > 0 && (
            <div style={{ color: "#fbbf24" }}>
              {warningCount} warning{warningCount !== 1 ? "s" : ""}
            </div>
          )}
          {warningCount === 0 && (
            <div style={{ color: "var(--positive)" }}>No warnings</div>
          )}
        </div>
      )}

      {expanded && (
        <div className="table-container">
          <div style={{ padding: "12px" }}>
            {/* Warning banners */}
            {hasWarnings && (
              <div style={{ marginBottom: "10px" }}>
                {errors.map((w, i) => (
                  <div
                    key={`error-${i}`}
                    style={{
                      background: "rgba(239, 68, 68, 0.15)",
                      border: "1px solid rgba(239, 68, 68, 0.4)",
                      borderRadius: "2px",
                      padding: "6px 10px",
                      marginBottom: "4px",
                      fontSize: "10px",
                      fontFamily: "var(--font-mono)",
                      color: "#fca5a5",
                    }}
                  >
                    {w.message}
                  </div>
                ))}
                {warnings.map((w, i) => (
                  <div
                    key={`warning-${i}`}
                    style={{
                      background: "rgba(245, 158, 11, 0.12)",
                      border: "1px solid rgba(245, 158, 11, 0.3)",
                      borderRadius: "2px",
                      padding: "6px 10px",
                      marginBottom: "4px",
                      fontSize: "10px",
                      fontFamily: "var(--font-mono)",
                      color: "#fbbf24",
                    }}
                  >
                    {w.message}
                  </div>
                ))}
              </div>
            )}

            {/* Coverage metrics */}
            <div className="risk-grid">
        {coverage60d && (
          <div className="risk-metric-card">
            <div className="risk-metric-label">Coverage 60d</div>
            <div className="risk-metric-value">
              {coverage60d.included_count} / {coverage60d.included_count + coverage60d.excluded_count}
            </div>
            <div className="risk-metric-sub">
              {coverage60d.excluded_count > 0 && (
                <>Excluded: {fmtPct(coverage60d.excluded_exposure_pct)} exposure</>
              )}
              {coverage60d.excluded_count === 0 && <>All positions included</>}
            </div>
          </div>
        )}

        {coverage252d && (
          <div className="risk-metric-card">
            <div className="risk-metric-label">Coverage 252d</div>
            <div className="risk-metric-value">
              {coverage252d.included_count} / {coverage252d.included_count + coverage252d.excluded_count}
            </div>
            <div className="risk-metric-sub">
              {coverage252d.excluded_count > 0 && (
                <>Excluded: {fmtPct(coverage252d.excluded_exposure_pct)} exposure</>
              )}
              {coverage252d.excluded_count === 0 && <>All positions included</>}
            </div>
          </div>
        )}

        {integrity && (
          <div className="risk-metric-card">
            <div className="risk-metric-label">Data Integrity</div>
            <div className="risk-metric-value" style={{ fontSize: "13px" }}>
              {integrity.outlier_return_days > 0 && (
                <div style={{ marginBottom: "2px" }}>
                  {integrity.outlier_return_days} outlier days
                </div>
              )}
              {integrity.flat_streak_flags > 0 && (
                <div style={{ marginBottom: "2px" }}>
                  {integrity.flat_streak_flags} flat streaks
                </div>
              )}
              {integrity.missing_price_exposure_pct > 0 && (
                <div style={{ marginBottom: "2px" }}>
                  {fmtPct(integrity.missing_price_exposure_pct)} missing prices
                </div>
              )}
            </div>
            <div className="risk-metric-sub">
              {integrity.nan_rows_skipped > 0 && (
                <>{integrity.nan_rows_skipped} NaN rows skipped</>
              )}
              {integrity.nan_rows_skipped === 0 &&
                integrity.outlier_return_days === 0 &&
                integrity.flat_streak_flags === 0 &&
                integrity.missing_price_exposure_pct === 0 && (
                  <>Clean</>
                )}
            </div>
          </div>
        )}

        {fx && fx.non_usd_exposure_pct > 0 && (
          <div className="risk-metric-card">
            <div className="risk-metric-label">FX Coverage</div>
            <div className="risk-metric-value">
              {fmtPct(fx.fx_coverage_pct)}
            </div>
            <div className="risk-metric-sub">
              {fmtPct(fx.non_usd_exposure_pct)} non-USD exposure
            </div>
          </div>
        )}

        {betaQuality && (
          <div className="risk-metric-card">
            <div className="risk-metric-label">Beta Quality</div>
            <div className="risk-metric-value" style={{ fontSize: "13px" }}>
              <div style={{ marginBottom: "2px" }}>
                {fmtPct(betaQuality.good_exposure_pct)} good
              </div>
              {betaQuality.weak_exposure_pct > 0 && (
                <div style={{ marginBottom: "2px", color: "#f59e0b" }}>
                  {fmtPct(betaQuality.weak_exposure_pct)} weak
                </div>
              )}
              {betaQuality.invalid_exposure_pct > 0 && (
                <div style={{ color: "#ef4444" }}>
                  {fmtPct(betaQuality.invalid_exposure_pct)} invalid
                </div>
              )}
            </div>
            <div className="risk-metric-sub">Beta vs SPY</div>
          </div>
        )}

        {timestamps && (
          <div className="risk-metric-card">
            <div className="risk-metric-label">Data Timestamps</div>
            <div className="risk-metric-value" style={{ fontSize: "9px" }}>
              <div style={{ marginBottom: "2px" }}>
                Positions: {fmtTimestamp(timestamps.last_positions_update)}
              </div>
              <div style={{ marginBottom: "2px" }}>
                Prices: {fmtTimestamp(timestamps.last_prices_update)}
              </div>
              {fx && fx.non_usd_exposure_pct > 0 && (
                <div style={{ marginBottom: "2px" }}>
                  FX: {fmtTimestamp(timestamps.last_fx_update)}
                </div>
              )}
              <div>
                Risk: {fmtTimestamp(timestamps.last_risk_compute)}
              </div>
            </div>
          </div>
        )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
