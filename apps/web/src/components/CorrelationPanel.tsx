"use client";

import { CorrelationPair } from "@/lib/risk-api";

interface CorrelationPanelProps {
  pairs: CorrelationPair[];
  loading?: boolean;
}

function fmtCorr(v: number): string {
  return v.toFixed(3);
}

export default function CorrelationPanel({
  pairs,
  loading = false,
}: CorrelationPanelProps) {
  if (loading) {
    return (
      <div className="panel">
        <div className="panel-header">Top Correlated Pairs</div>
        <div className="empty-state">Loading correlation data...</div>
      </div>
    );
  }

  if (pairs.length === 0) {
    return (
      <div className="panel">
        <div className="panel-header">Top Correlated Pairs</div>
        <div className="empty-state">No correlation data available</div>
      </div>
    );
  }

  return (
    <div className="panel">
      <div className="panel-header">Top Correlated Pairs ({pairs.length})</div>
      <div className="table-container" style={{ maxHeight: "400px", overflowY: "auto" }}>
        <table>
          <thead>
            <tr>
              <th>Pair</th>
              <th className="cell-right">Correlation</th>
            </tr>
          </thead>
          <tbody>
            {pairs.map((p, idx) => {
              const corrClass =
                p.correlation > 0.7
                  ? "corr-value corr-positive"
                  : p.correlation < -0.3
                  ? "corr-value corr-negative"
                  : "corr-value";

              return (
                <tr key={idx}>
                  <td className="cell-symbol">
                    {p.symbol_a} ↔ {p.symbol_b}
                  </td>
                  <td className={`cell-right ${corrClass}`}>
                    {fmtCorr(p.correlation)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
