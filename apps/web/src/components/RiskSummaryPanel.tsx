"use client";

import { RiskSummary } from "@/lib/risk-api";

interface RiskSummaryPanelProps {
  summary: RiskSummary | null;
  window: number;
  method: string;
  onWindowChange: (w: number) => void;
  onMethodChange: (m: string) => void;
  onRecompute: () => void;
  loading?: boolean;
}

function fmtPct(v: number): string {
  return `${v >= 0 ? "+" : ""}${v.toFixed(2)}%`;
}

function fmtCurrency(v: number): string {
  const abs = Math.abs(v);
  if (abs >= 1_000_000) {
    return `$${(v / 1_000_000).toFixed(2)}M`;
  }
  if (abs >= 1_000) {
    return `$${(v / 1_000).toFixed(1)}k`;
  }
  return `$${v.toFixed(0)}`;
}

export default function RiskSummaryPanel({
  summary,
  window,
  method,
  onWindowChange,
  onMethodChange,
  onRecompute,
  loading = false,
}: RiskSummaryPanelProps) {
  if (loading) {
    return (
      <div className="panel">
        <div className="panel-header">Risk Summary</div>
        <div className="empty-state">Loading risk metrics...</div>
      </div>
    );
  }

  if (!summary) {
    return (
      <div className="panel">
        <div className="panel-header">Risk Summary</div>
        <div className="empty-state">No risk data available</div>
      </div>
    );
  }

  const concentrationHigh = summary.top_5_concentration_pct > 60;

  return (
    <div className="panel">
      <div className="panel-header">Risk Summary</div>

      {/* Controls */}
      <div className="risk-controls">
        <div className="risk-toggle-group">
          <span className="risk-toggle-label">Window:</span>
          <button
            className={`risk-toggle-btn${window === 60 ? " active" : ""}`}
            onClick={() => onWindowChange(60)}
          >
            60d
          </button>
          <button
            className={`risk-toggle-btn${window === 252 ? " active" : ""}`}
            onClick={() => onWindowChange(252)}
          >
            252d
          </button>
        </div>

        <div className="risk-toggle-group">
          <span className="risk-toggle-label">Method:</span>
          <button
            className={`risk-toggle-btn${method === "lw" ? " active" : ""}`}
            onClick={() => onMethodChange("lw")}
          >
            Stable
          </button>
          <button
            className={`risk-toggle-btn${method === "ewma" ? " active" : ""}`}
            onClick={() => onMethodChange("ewma")}
          >
            Reactive
          </button>
        </div>

        <button className="recompute-btn" onClick={onRecompute}>
          Recompute
        </button>
      </div>

      {/* Metrics Grid */}
      <div className="risk-grid">
        {/* Portfolio Vol 1d */}
        <div className="risk-metric-card">
          <div className="risk-metric-label">Portfolio Vol (1d)</div>
          <div className="risk-metric-value risk-vol">
            {fmtPct(summary.vol_1d_pct)}
          </div>
          <div className="risk-metric-sub">
            {fmtCurrency(summary.vol_1d)}
          </div>
        </div>

        {/* Portfolio Vol 5d */}
        <div className="risk-metric-card">
          <div className="risk-metric-label">Portfolio Vol (5d)</div>
          <div className="risk-metric-value risk-vol">
            {fmtPct(summary.vol_5d_pct)}
          </div>
          <div className="risk-metric-sub">
            {fmtCurrency(summary.vol_5d)}
          </div>
        </div>

        {/* VaR 95% 1d */}
        <div className="risk-metric-card">
          <div className="risk-metric-label">VaR 95% (1d)</div>
          <div className="risk-metric-value risk-var">
            {fmtCurrency(summary.var_95_1d)}
          </div>
          <div className="risk-metric-sub">
            {fmtPct(summary.var_95_1d_pct)}
          </div>
        </div>

        {/* ES 95% 1d */}
        <div className="risk-metric-card">
          <div className="risk-metric-label">ES 95% (1d)</div>
          <div className="risk-metric-value risk-es">
            {fmtCurrency(summary.es_95_1d)}
          </div>
          <div className="risk-metric-sub">
            {fmtPct(summary.es_95_1d_pct)}
          </div>
        </div>

        {/* Top 5 Concentration */}
        <div className="risk-metric-card">
          <div className="risk-metric-label">Top 5 Concentration</div>
          <div
            className={`risk-metric-value${concentrationHigh ? " risk-concentration-high" : ""}`}
          >
            {summary.top_5_concentration_pct.toFixed(1)}%
          </div>
          <div className="risk-metric-sub">
            {summary.top_5_names.join(", ")}
          </div>
        </div>

        {/* HHI Score */}
        <div className="risk-metric-card">
          <div className="risk-metric-label">HHI Score</div>
          <div className="risk-metric-value">
            {summary.hhi.toFixed(0)}
          </div>
          <div className="risk-metric-sub">
            {summary.num_positions} positions
          </div>
        </div>
      </div>
    </div>
  );
}
