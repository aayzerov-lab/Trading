"use client";

import { useState } from "react";
import { StressTests, StressResult } from "@/lib/risk-api";

interface StressPanelProps {
  stressTests: StressTests | null;
  loading?: boolean;
}

function fmtCurrency(v: number): string {
  const abs = Math.abs(v);
  if (abs >= 1_000_000) {
    return `${v >= 0 ? "+" : ""}${(v / 1_000_000).toFixed(2)}M`;
  }
  if (abs >= 1_000) {
    return `${v >= 0 ? "+" : ""}${(v / 1_000).toFixed(1)}k`;
  }
  return `${v >= 0 ? "+" : ""}${v.toFixed(0)}`;
}

function fmtPct(v: number): string {
  return `${v >= 0 ? "+" : ""}${v.toFixed(2)}%`;
}

interface StressCardProps {
  scenario: string;
  result: StressResult;
}

function StressCard({ scenario, result }: StressCardProps) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="stress-card">
      <div className="stress-card-header">
        <div className="stress-scenario">{scenario}</div>
        {result.period && <div className="stress-period">{result.period}</div>}
      </div>
      <div
        className={`stress-pnl${result.portfolio_pnl >= 0 ? " positive" : " negative"}`}
      >
        {fmtCurrency(result.portfolio_pnl)}
      </div>
      <div className="stress-return">{fmtPct(result.portfolio_return_pct)}</div>
      <button
        className="stress-toggle-btn"
        onClick={() => setExpanded(!expanded)}
      >
        {expanded ? "Hide" : "Show"} Top Contributors
      </button>

      {expanded && result.top_contributors.length > 0 && (
        <div className="stress-contributors">
          <table>
            <thead>
              <tr>
                <th>Symbol</th>
                <th className="cell-right">Return %</th>
                <th className="cell-right">P&L Contrib</th>
                <th className="cell-right">Weight %</th>
              </tr>
            </thead>
            <tbody>
              {result.top_contributors.slice(0, 10).map((c, idx) => (
                <tr key={idx}>
                  <td className="cell-symbol">{c.symbol}</td>
                  <td
                    className={`cell-right${c.return_pct >= 0 ? " pnl-positive" : " pnl-negative"}`}
                  >
                    {fmtPct(c.return_pct)}
                  </td>
                  <td
                    className={`cell-right${c.pnl_contribution >= 0 ? " pnl-positive" : " pnl-negative"}`}
                  >
                    {fmtCurrency(c.pnl_contribution)}
                  </td>
                  <td className="cell-right">{c.weight_pct.toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

export default function StressPanel({
  stressTests,
  loading = false,
}: StressPanelProps) {
  if (loading) {
    return (
      <div className="stress-panel">
        <div className="empty-state">Loading stress tests...</div>
      </div>
    );
  }

  if (!stressTests) {
    return (
      <div className="stress-panel">
        <div className="empty-state">No stress test data available</div>
      </div>
    );
  }

  const historicalEntries = Object.entries(stressTests.historical);
  const factorEntries = Object.entries(stressTests.factor);

  return (
    <div className="stress-panel">
      {/* Historical Scenarios */}
      {historicalEntries.length > 0 && (
        <div className="stress-section">
          <div className="stress-section-header">Historical Scenarios</div>
          <div className="stress-grid">
            {historicalEntries.map(([scenario, result]) => (
              <StressCard key={scenario} scenario={scenario} result={result} />
            ))}
          </div>
        </div>
      )}

      {/* Factor Shocks */}
      {factorEntries.length > 0 && (
        <div className="stress-section">
          <div className="stress-section-header">Factor Shocks</div>
          <div className="stress-grid">
            {factorEntries.map(([scenario, result]) => (
              <StressCard key={scenario} scenario={scenario} result={result} />
            ))}
          </div>
        </div>
      )}

      {historicalEntries.length === 0 && factorEntries.length === 0 && (
        <div className="empty-state">No stress scenarios computed</div>
      )}
    </div>
  );
}
