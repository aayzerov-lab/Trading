"use client";

import { useState, useMemo } from "react";
import { RiskContributor } from "@/lib/risk-api";

interface RiskContributorsTableProps {
  contributors: RiskContributor[];
  loading?: boolean;
}

type SortKey = keyof RiskContributor;
type SortDir = "asc" | "desc";

function fmtNumber(v: number, decimals = 2): string {
  return v.toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

export default function RiskContributorsTable({
  contributors,
  loading = false,
}: RiskContributorsTableProps) {
  const [sortKey, setSortKey] = useState<SortKey>("ccr_pct");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [showAll, setShowAll] = useState(false);

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(sortDir === "asc" ? "desc" : "asc");
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
  };

  const sortedContributors = useMemo(() => {
    const sorted = [...contributors].sort((a, b) => {
      const av = a[sortKey];
      const bv = b[sortKey];
      if (typeof av === "string" && typeof bv === "string") {
        return sortDir === "asc" ? av.localeCompare(bv) : bv.localeCompare(av);
      }
      const cmp = (av as number) - (bv as number);
      return sortDir === "asc" ? cmp : -cmp;
    });
    return showAll ? sorted : sorted.slice(0, 20);
  }, [contributors, sortKey, sortDir, showAll]);

  if (loading) {
    return (
      <div className="positions-panel">
        <div className="positions-header">
          <span className="positions-title">Risk Contributors</span>
        </div>
        <div className="empty-state">Loading contributors...</div>
      </div>
    );
  }

  if (contributors.length === 0) {
    return (
      <div className="positions-panel">
        <div className="positions-header">
          <span className="positions-title">Risk Contributors</span>
        </div>
        <div className="empty-state">No contributors available</div>
      </div>
    );
  }

  return (
    <div className="positions-panel">
      <div className="positions-header">
        <span className="positions-title">
          Risk Contributors ({sortedContributors.length} of {contributors.length})
        </span>
        {contributors.length > 20 && (
          <button
            className="recompute-btn"
            onClick={() => setShowAll(!showAll)}
          >
            {showAll ? "Top 20" : "Show All"}
          </button>
        )}
      </div>
      <div className="table-container">
        <table>
          <thead>
            <tr>
              <th onClick={() => handleSort("symbol")} style={{ cursor: "pointer" }}>
                Symbol
                {sortKey === "symbol" && (
                  <span className="sort-indicator">
                    {sortDir === "asc" ? " ▲" : " ▼"}
                  </span>
                )}
              </th>
              <th
                className="cell-right"
                onClick={() => handleSort("weight_pct")}
                style={{ cursor: "pointer" }}
              >
                Weight %
                {sortKey === "weight_pct" && (
                  <span className="sort-indicator">
                    {sortDir === "asc" ? " ▲" : " ▼"}
                  </span>
                )}
              </th>
              <th
                className="cell-right"
                onClick={() => handleSort("mcr")}
                style={{ cursor: "pointer" }}
              >
                MCR
                {sortKey === "mcr" && (
                  <span className="sort-indicator">
                    {sortDir === "asc" ? " ▲" : " ▼"}
                  </span>
                )}
              </th>
              <th
                className="cell-right"
                onClick={() => handleSort("ccr")}
                style={{ cursor: "pointer" }}
                title="Component Contribution to Risk (basis points)"
              >
                CCR (bps)
                {sortKey === "ccr" && (
                  <span className="sort-indicator">
                    {sortDir === "asc" ? " ▲" : " ▼"}
                  </span>
                )}
              </th>
              <th
                className="cell-right"
                onClick={() => handleSort("ccr_pct")}
                style={{ cursor: "pointer" }}
              >
                CCR %
                {sortKey === "ccr_pct" && (
                  <span className="sort-indicator">
                    {sortDir === "asc" ? " ▲" : " ▼"}
                  </span>
                )}
              </th>
              <th
                className="cell-right"
                onClick={() => handleSort("standalone_vol_ann")}
                style={{ cursor: "pointer" }}
              >
                Vol (Ann %)
                {sortKey === "standalone_vol_ann" && (
                  <span className="sort-indicator">
                    {sortDir === "asc" ? " ▲" : " ▼"}
                  </span>
                )}
              </th>
            </tr>
          </thead>
          <tbody>
            {sortedContributors.map((c) => {
              const volClass =
                c.standalone_vol_ann > 50
                  ? "risk-contrib-high"
                  : c.standalone_vol_ann > 30
                  ? "risk-contrib-med"
                  : "";

              return (
                <tr key={c.symbol}>
                  <td className="cell-symbol">{c.symbol}</td>
                  <td className="cell-right">{fmtNumber(c.weight_pct, 2)}</td>
                  <td className="cell-right">{fmtNumber(c.mcr, 4)}</td>
                  <td className="cell-right">{fmtNumber(c.ccr * 10000, 1)}</td>
                  <td className="cell-right">{fmtNumber(c.ccr_pct, 2)}</td>
                  <td className={`cell-right ${volClass}`}>
                    {fmtNumber(c.standalone_vol_ann, 1)}
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
