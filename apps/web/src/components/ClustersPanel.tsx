"use client";

import { ClusterInfo } from "@/lib/risk-api";

interface ClustersPanelProps {
  clusters: ClusterInfo[];
  loading?: boolean;
}

function fmtNumber(v: number, decimals = 2): string {
  return v.toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

export default function ClustersPanel({
  clusters,
  loading = false,
}: ClustersPanelProps) {
  if (loading) {
    return (
      <div className="panel">
        <div className="panel-header">Cluster Analysis</div>
        <div className="empty-state">Loading cluster data...</div>
      </div>
    );
  }

  if (clusters.length === 0) {
    return (
      <div className="panel">
        <div className="panel-header">Cluster Analysis</div>
        <div className="empty-state">No cluster data available</div>
      </div>
    );
  }

  return (
    <div className="panel">
      <div className="panel-header">Cluster Analysis ({clusters.length})</div>
      <div style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
        {clusters.map((c) => (
          <div key={c.cluster_id} className="cluster-card">
            <div className="cluster-header">
              <span className="cluster-id">Cluster {c.cluster_id}</span>
              <span className="cluster-size">{c.size} positions</span>
            </div>
            <div className="cluster-members">{c.members.join(", ")}</div>
            <div className="cluster-metrics">
              <div className="cluster-metric">
                <span className="cluster-metric-label">Gross Exp:</span>
                <span className="cluster-metric-value">
                  {fmtNumber(c.gross_exposure_pct, 1)}%
                </span>
              </div>
              <div className="cluster-metric">
                <span className="cluster-metric-label">Net Exp:</span>
                <span className="cluster-metric-value">
                  {fmtNumber(c.net_exposure_pct, 1)}%
                </span>
              </div>
              <div className="cluster-metric">
                <span className="cluster-metric-label">Avg Corr:</span>
                <span className="cluster-metric-value">
                  {fmtNumber(c.avg_intra_corr, 2)}
                </span>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
