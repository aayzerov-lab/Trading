"use client";

import { MacroOverview } from "@/lib/risk-api";

interface MacroStripProps {
  macroData: MacroOverview | null;
  loading?: boolean;
}

function fmtNumber(v: number, decimals = 2): string {
  return v.toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

function fmtDelta(v: number | null): string {
  if (v === null) return "—";
  return `${v >= 0 ? "+" : ""}${fmtNumber(v, 2)}`;
}

export default function MacroStrip({
  macroData,
  loading = false,
}: MacroStripProps) {
  if (loading) {
    return (
      <div className="macro-strip-container">
        <div className="empty-state">Loading macro data...</div>
      </div>
    );
  }

  if (!macroData || macroData.indicators.length === 0) {
    return (
      <div className="macro-strip-container">
        <div className="empty-state">No macro data available</div>
      </div>
    );
  }

  return (
    <div className="macro-strip-container">
      <div className="macro-strip">
        {macroData.indicators.map((ind) => {
          const arrowSymbol =
            ind.direction === "up" ? "↑" : ind.direction === "down" ? "↓" : "→";

          return (
            <div key={ind.series_id} className="macro-indicator">
              <div className="macro-indicator-name">{ind.name}</div>
              <div className="macro-indicator-value">
                {fmtNumber(ind.latest_value, 2)}
                {ind.unit && <span className="macro-unit"> {ind.unit}</span>}
              </div>
              <div className="macro-indicator-date">{ind.latest_date}</div>
              <div className="macro-deltas">
                <div className={`macro-delta ${ind.direction}`}>
                  <span className="macro-delta-label">1M:</span>
                  <span className="macro-delta-value">
                    {fmtDelta(ind.change_1m)} {arrowSymbol}
                  </span>
                </div>
                <div className={`macro-delta ${ind.direction}`}>
                  <span className="macro-delta-label">3M:</span>
                  <span className="macro-delta-value">
                    {fmtDelta(ind.change_3m)} {arrowSymbol}
                  </span>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
