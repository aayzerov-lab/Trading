"use client";

import { useMemo, useState } from "react";
import { MacroSummary, MacroTile } from "@/lib/risk-api";

interface MacroStripProps {
  macroData: MacroSummary | null;
  loading?: boolean;
}

function fmtChange(value: number | null, format: string): string {
  if (value === null || value === undefined) {
    return "—";
  }
  if (format === "bp") {
    return `${value >= 0 ? "+" : ""}${(value * 100).toFixed(1)}`;
  }
  if (format === "currency") {
    return `${value >= 0 ? "+" : ""}$${value.toFixed(2)}`;
  }
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}`;
}

function changeClass(value: number | null): string {
  if (value === null || value === undefined) {
    return "flat";
  }
  if (value > 0) return "up";
  if (value < 0) return "down";
  return "flat";
}

function MacroTileCard({ tile }: { tile: MacroTile }) {
  const windows = tile.recommendedChangeWindows ?? ["1W", "1M", "3M"];
  const visibleWindows = windows.slice(0, 3);
  const hiddenWindows = windows.slice(3);
  const format = tile.format || (tile.unit === "bp" ? "bp" : "percent");
  const hiddenTooltip = hiddenWindows
    .map((window) => `${window}: ${fmtChange(tile.changes[window] ?? null, format)}`)
    .join(" | ");

  return (
    <div className="macro-indicator">
      <div className="macro-indicator-name">
        {tile.label}
        {tile.revised && (
          <span
            className="macro-revised"
            title={`Revised: ${tile.previousValue ?? "—"} → ${tile.value}`}
          >
            Revised
          </span>
        )}
      </div>
      <div className="macro-indicator-value">
        {tile.valueFormatted}
        {tile.unit && <span className="macro-unit"> {tile.unit}</span>}
      </div>
      <div className="macro-indicator-date">As of {tile.obs_date}</div>
      <div className="macro-indicator-date">
        Fetched {new Date(tile.fetched_at).toLocaleString()}
      </div>
      <div className="macro-deltas">
        {visibleWindows.map((window) => {
          const value = tile.changes[window] ?? null;
          return (
            <div key={window} className={`macro-delta ${changeClass(value)}`}>
              <span className="macro-delta-label">{window}:</span>
              <span className="macro-delta-value">{fmtChange(value, format)}</span>
            </div>
          );
        })}
        {hiddenWindows.length > 0 && (
          <span className="macro-delta macro-delta-more" title={hiddenTooltip}>
            +{hiddenWindows.length} more
          </span>
        )}
      </div>
    </div>
  );
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

  if (!macroData || macroData.categories.length === 0) {
    return (
      <div className="macro-strip-container">
        <div className="empty-state">No macro data available</div>
      </div>
    );
  }

  const categories = macroData.categories;
  const categoryNames = useMemo(
    () => ["All", ...categories.map((c) => c.name)],
    [categories]
  );
  const [activeCategory, setActiveCategory] = useState("All");

  const visibleCategories = categories.filter((c) =>
    activeCategory === "All" ? true : c.name === activeCategory
  );

  const expanded = activeCategory !== "All";

  return (
    <div className={`macro-strip-container${expanded ? " expanded" : ""}`}>
      <div className="macro-category-tabs">
        {categoryNames.map((name) => (
          <button
            key={name}
            className={`macro-category-btn${activeCategory === name ? " active" : ""}`}
            onClick={() => setActiveCategory(name)}
          >
            {name}
          </button>
        ))}
      </div>

      {visibleCategories.map((category) => (
        <div key={category.name} className="macro-category">
          <div className="macro-category-header">{category.name}</div>
          <div className="macro-strip">
            {category.tiles.map((tile) => (
              <MacroTileCard key={tile.id} tile={tile} />
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}
