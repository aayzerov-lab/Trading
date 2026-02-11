"use client";

import { useState } from "react";
import { RiskMetadata } from "@/lib/risk-api";

interface RiskMetadataPanelProps {
  metadata: RiskMetadata | null;
  loading?: boolean;
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

export default function RiskMetadataPanel({
  metadata,
  loading = false,
}: RiskMetadataPanelProps) {
  const [expanded, setExpanded] = useState(false);

  if (loading) {
    return (
      <div className="positions-panel" style={{ marginTop: "1px" }}>
        <div className="positions-header">
          <span className="positions-title">Risk Pack Info</span>
        </div>
        <div className="empty-state">Loading metadata...</div>
      </div>
    );
  }

  if (!metadata) {
    return (
      <div className="positions-panel" style={{ marginTop: "1px" }}>
        <div className="positions-header">
          <span className="positions-title">Risk Pack Info</span>
        </div>
        <div className="empty-state">No metadata available</div>
      </div>
    );
  }

  const methodLabel = metadata.method === "lw" ? "Stable" : "Reactive";

  return (
    <div className="positions-panel" style={{ marginTop: "1px" }}>
      <div className="positions-header">
        <span className="positions-title">Risk Pack Info</span>
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
            <span style={{ color: "var(--text-dimmed)" }}>As of: </span>
            {metadata.asof_date}
          </div>
          <div>
            <span style={{ color: "var(--text-dimmed)" }}>Method: </span>
            {methodLabel}
          </div>
          <div>
            <span style={{ color: "var(--text-dimmed)" }}>Positions: </span>
            {metadata.num_positions}
          </div>
          <div>
            <span style={{ color: "var(--text-dimmed)" }}>Portfolio Value: </span>
            {fmtCurrency(metadata.portfolio_value)}
          </div>
        </div>
      )}

      {expanded && (
        <div className="table-container">
          <div style={{ padding: "12px" }}>
            <div className="risk-grid">
              <div className="risk-metric-card">
                <div className="risk-metric-label">Window</div>
                <div className="risk-metric-value" style={{ fontSize: "14px" }}>
                  {metadata.window}d
                </div>
                <div className="risk-metric-sub">
                  Effective: {metadata.effective_window}d
                </div>
              </div>

              <div className="risk-metric-card">
                <div className="risk-metric-label">Method</div>
                <div className="risk-metric-value" style={{ fontSize: "14px" }}>
                  {methodLabel}
                </div>
                <div className="risk-metric-sub">
                  {metadata.method}
                </div>
              </div>

              <div className="risk-metric-card">
                <div className="risk-metric-label">As of Date</div>
                <div className="risk-metric-value" style={{ fontSize: "14px" }}>
                  {metadata.asof_date}
                </div>
                <div className="risk-metric-sub">
                  {metadata.computed_at ? (
                    <>Computed: {new Date(metadata.computed_at).toLocaleString("en-US", {
                      month: "short",
                      day: "numeric",
                      hour: "2-digit",
                      minute: "2-digit",
                      hour12: false,
                    })}</>
                  ) : (
                    <>—</>
                  )}
                </div>
              </div>

              <div className="risk-metric-card">
                <div className="risk-metric-label">Portfolio Value</div>
                <div className="risk-metric-value" style={{ fontSize: "14px" }}>
                  {fmtCurrency(metadata.portfolio_value)}
                </div>
                <div className="risk-metric-sub">
                  {metadata.num_positions} positions
                </div>
              </div>

              <div className="risk-metric-card">
                <div className="risk-metric-label">Validity</div>
                <div className="risk-metric-value" style={{ fontSize: "14px" }}>
                  {metadata.num_valid_symbols} valid
                </div>
                <div className="risk-metric-sub">
                  {(metadata.num_excluded ?? 0) > 0 && (
                    <>{metadata.num_excluded} excluded</>
                  )}
                  {(metadata.num_excluded ?? 0) === 0 && <>All symbols valid</>}
                </div>
              </div>

              {metadata.fx_adjusted_count != null && (
                <div className="risk-metric-card">
                  <div className="risk-metric-label">FX Adjusted</div>
                  <div className="risk-metric-value" style={{ fontSize: "14px" }}>
                    {metadata.fx_adjusted_count}
                  </div>
                  <div className="risk-metric-sub">
                    symbols adjusted to USD
                  </div>
                </div>
              )}

              {metadata.portfolio_hash && (
                <div className="risk-metric-card">
                  <div className="risk-metric-label">Portfolio Hash</div>
                  <div className="risk-metric-value" style={{ fontSize: "10px", fontFamily: "var(--font-mono)" }}>
                    {metadata.portfolio_hash.slice(0, 12)}...
                  </div>
                </div>
              )}

              {metadata.universe_hash && (
                <div className="risk-metric-card">
                  <div className="risk-metric-label">Universe Hash</div>
                  <div className="risk-metric-value" style={{ fontSize: "10px", fontFamily: "var(--font-mono)" }}>
                    {metadata.universe_hash.slice(0, 12)}...
                  </div>
                </div>
              )}
            </div>

            {/* Excluded symbols */}
            {metadata.excluded_symbols.length > 0 && (
              <div style={{ marginTop: "12px" }}>
                <div
                  className="risk-metric-label"
                  style={{ marginBottom: "6px", display: "block" }}
                >
                  Excluded Symbols ({metadata.excluded_symbols.length})
                </div>
                <div
                  style={{
                    padding: "8px 10px",
                    background: "var(--bg-panel-alt)",
                    border: "1px solid var(--border-subtle)",
                    borderRadius: "2px",
                    fontFamily: "var(--font-mono)",
                    fontSize: "10px",
                    color: "var(--text-secondary)",
                    lineHeight: 1.5,
                  }}
                >
                  {metadata.excluded_symbols.join(", ")}
                </div>
              </div>
            )}

            {/* FX flags */}
            {metadata.fx_flags && Object.keys(metadata.fx_flags).length > 0 && (
              <div style={{ marginTop: "12px" }}>
                <div
                  className="risk-metric-label"
                  style={{ marginBottom: "6px", display: "block" }}
                >
                  FX Flags
                </div>
                <div
                  style={{
                    padding: "8px 10px",
                    background: "var(--bg-panel-alt)",
                    border: "1px solid var(--border-subtle)",
                    borderRadius: "2px",
                  }}
                >
                  {Object.entries(metadata.fx_flags).map(([symbol, flag]) => (
                    <div
                      key={symbol}
                      style={{
                        fontFamily: "var(--font-mono)",
                        fontSize: "10px",
                        color: "var(--text-secondary)",
                        marginBottom: "3px",
                      }}
                    >
                      <span style={{ color: "var(--text-primary)" }}>{symbol}:</span>{" "}
                      {flag}
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Library versions */}
            {metadata.lib_versions && Object.keys(metadata.lib_versions).length > 0 && (
              <div style={{ marginTop: "12px" }}>
                <div
                  className="risk-metric-label"
                  style={{ marginBottom: "6px", display: "block" }}
                >
                  Library Versions
                </div>
                <div
                  style={{
                    padding: "8px 10px",
                    background: "var(--bg-panel-alt)",
                    border: "1px solid var(--border-subtle)",
                    borderRadius: "2px",
                  }}
                >
                  {Object.entries(metadata.lib_versions).map(([lib, version]) => (
                    <div
                      key={lib}
                      style={{
                        fontFamily: "var(--font-mono)",
                        fontSize: "10px",
                        color: "var(--text-secondary)",
                        marginBottom: "3px",
                      }}
                    >
                      <span style={{ color: "var(--text-primary)" }}>{lib}:</span>{" "}
                      {version}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
