"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { API_URL, fetchWithRetry } from "@/lib/api";
import { Event, EventType, EventStatus, updateEventStatus } from "@/lib/events-api";

// ---- Types ------------------------------------------------------------------

interface PositionCtx {
  symbol: string; position: number; avg_cost: number; market_price: number;
  market_value: number; unrealized_pnl: number; sector: string | null;
  ib_category: string | null; weight_pct: number;
}

interface TickerOverview {
  symbol: string;
  position: PositionCtx | null;
  events: Event[];
  upcoming: Event[];
}

// ---- Helpers ----------------------------------------------------------------

const mono = "var(--font-mono)";
const sans = "var(--font-sans)";

const TYPE_COLORS: Record<string, string> = {
  SEC_FILING: "#8b5cf6", MACRO_SCHEDULE: "#3b82f6", RSS_NEWS: "#10b981", OTHER: "#64748b",
};

function timeAgo(iso: string): string {
  const m = Math.floor((Date.now() - new Date(iso).getTime()) / 60_000);
  if (m < 1) return "just now";
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

function fmtCurrency(v: number | null | undefined): string {
  if (v == null || isNaN(v)) return "—";
  return v.toLocaleString("en-US", { style: "currency", currency: "USD" });
}

function sevColor(s: number) {
  return s >= 75 ? "var(--red)" : s >= 50 ? "var(--yellow)" : "var(--text-muted)";
}

function normalizeEvent(raw: Record<string, unknown>): Event {
  const e = raw as unknown as Event;
  if (typeof e.tickers === "string") {
    try { (e as any).tickers = JSON.parse(e.tickers as any); } catch { (e as any).tickers = null; }
  }
  if (typeof e.reason_codes === "string") {
    try { (e as any).reason_codes = JSON.parse(e.reason_codes as any); } catch { (e as any).reason_codes = null; }
  }
  return e;
}

// ---- Sub-components ---------------------------------------------------------

function SevDot({ score }: { score: number }) {
  return <span style={{ display: "inline-block", width: 7, height: 7, borderRadius: "50%", background: sevColor(score), flexShrink: 0 }} />;
}

function TypePill({ type }: { type: EventType }) {
  const c = TYPE_COLORS[type] ?? TYPE_COLORS.OTHER;
  return (
    <span style={{ fontFamily: mono, fontSize: 8, fontWeight: 600, textTransform: "uppercase",
      letterSpacing: "0.06em", padding: "1px 5px", borderRadius: 2,
      background: c + "22", color: c, border: `1px solid ${c}44`, whiteSpace: "nowrap" }}>
      {type === "SEC_FILING" ? "SEC" : type === "RSS_NEWS" ? "RSS" : type === "MACRO_SCHEDULE" ? "Macro" : "Other"}
    </span>
  );
}

function EventRow({ event: e, expanded, onToggle, onStatus }: {
  event: Event; expanded: boolean; onToggle: () => void;
  onStatus: (id: string, s: EventStatus) => void;
}) {
  return (
    <div style={{ borderBottom: "1px solid var(--border-subtle)" }}>
      <div onClick={onToggle} style={{ display: "flex", alignItems: "center", gap: 8, padding: "6px 10px", cursor: "pointer", transition: "background 0.1s" }}
        onMouseEnter={(ev) => { ev.currentTarget.style.background = "var(--bg-hover)"; }}
        onMouseLeave={(ev) => { ev.currentTarget.style.background = "transparent"; }}>
        <SevDot score={e.severity_score} />
        <span style={{ fontFamily: sans, fontSize: 10, fontWeight: 500, color: "var(--text-primary)", flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{e.title}</span>
        <TypePill type={e.type} />
        <span style={{ fontFamily: mono, fontSize: 8, color: "var(--text-dimmed)", whiteSpace: "nowrap" }}>{timeAgo(e.ts_utc)}</span>
      </div>

      {expanded && (
        <div style={{ padding: "0 10px 10px 25px", display: "flex", flexDirection: "column", gap: 6 }}>
          {e.raw_text_snippet && (
            <pre style={{ fontFamily: mono, fontSize: 9, color: "var(--text-secondary)", background: "var(--bg-base)", border: "1px solid var(--border-primary)", borderRadius: 2, padding: 8, whiteSpace: "pre-wrap", wordBreak: "break-word", maxHeight: 100, overflowY: "auto", lineHeight: 1.5 }}>
              {e.raw_text_snippet}
            </pre>
          )}
          {e.llm_summary && (
            <div style={{ fontFamily: sans, fontSize: 10, color: "var(--text-primary)", background: "var(--bg-hover)", border: "1px solid var(--border-subtle)", borderRadius: 2, padding: 8, lineHeight: 1.5 }}>
              <span style={{ fontSize: 8, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.06em", color: "var(--text-dimmed)", display: "block", marginBottom: 3 }}>LLM Summary</span>
              {e.llm_summary}
            </div>
          )}
          {e.reason_codes && e.reason_codes.length > 0 && (
            <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
              {e.reason_codes.map((rc, i) => (
                <span key={i} style={{ fontFamily: mono, fontSize: 8, padding: "1px 4px", borderRadius: 2, background: "var(--bg-hover)", color: "var(--text-secondary)", border: "1px solid var(--border-primary)" }}>{rc}</span>
              ))}
            </div>
          )}
          {e.source_url && (
            <a href={e.source_url} target="_blank" rel="noopener noreferrer" style={{ fontFamily: mono, fontSize: 9, color: "var(--accent)" }}>{e.source_name ?? "Source"}</a>
          )}
          <div style={{ display: "flex", gap: 6 }}>
            <button className="method-btn" style={e.status === "ACKED" ? { fontSize: 9, padding: "3px 10px", background: "var(--accent)", color: "#fff", borderColor: "var(--accent)" } : { fontSize: 9, padding: "3px 10px" }} onClick={() => onStatus(e.id, "ACKED")}>Ack</button>
            <button className="method-btn" style={e.status === "DISMISSED" ? { fontSize: 9, padding: "3px 10px", background: "var(--red-dim)", color: "#fff", borderColor: "var(--red-dim)" } : { fontSize: 9, padding: "3px 10px" }} onClick={() => onStatus(e.id, "DISMISSED")}>Dismiss</button>
          </div>
        </div>
      )}
    </div>
  );
}

function GroupSection({ title, events, emptyLabel, expandedId, onToggle, onStatus }: {
  title: string; events: Event[]; emptyLabel: string;
  expandedId: string | null; onToggle: (id: string) => void;
  onStatus: (id: string, s: EventStatus) => void;
}) {
  return (
    <div style={{ marginBottom: 12 }}>
      <div style={{ fontFamily: sans, fontSize: 10, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.1em", color: "var(--text-muted)", padding: "6px 10px", borderBottom: "1px solid var(--border-subtle)", background: "var(--bg-panel-alt)" }}>{title} ({events.length})</div>
      {events.length === 0 ? (
        <div style={{ fontFamily: mono, fontSize: 10, color: "var(--text-dimmed)", padding: "10px 10px" }}>{emptyLabel}</div>
      ) : events.map((e) => (
        <EventRow key={e.id} event={e} expanded={expandedId === e.id} onToggle={() => onToggle(e.id)} onStatus={onStatus} />
      ))}
    </div>
  );
}

// ---- Main component ---------------------------------------------------------

export default function TickerDesk() {
  const [tickers, setTickers] = useState<string[]>([]);
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState<string | null>(null);
  const [overview, setOverview] = useState<TickerOverview | null>(null);
  const [days, setDays] = useState(7);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);

  // Fetch tickers on mount
  useEffect(() => {
    (async () => {
      try {
        const res = await fetchWithRetry(`${API_URL}/events/portfolio-tickers`);
        if (res.ok) setTickers(await res.json());
      } catch { /* degrade */ }
      finally { setLoading(false); }
    })();
  }, []);

  // Fetch overview when ticker or days changes
  const loadOverview = useCallback(async (sym: string, d: number) => {
    setDetailLoading(true);
    setExpandedId(null);
    try {
      const res = await fetchWithRetry(`${API_URL}/events/ticker/${encodeURIComponent(sym)}/overview?days=${d}`);
      if (!res.ok) throw new Error("fetch failed");
      const raw = await res.json();
      const ov: TickerOverview = {
        ...raw,
        events: (raw.events ?? []).map((e: any) => normalizeEvent(e)),
        upcoming: (raw.upcoming ?? []).map((e: any) => normalizeEvent(e)),
      };
      setOverview(ov);
    } catch { setOverview(null); }
    finally { setDetailLoading(false); }
  }, []);

  useEffect(() => { if (selected) loadOverview(selected, days); }, [selected, days, loadOverview]);

  const handleStatus = useCallback(async (id: string, status: EventStatus) => {
    try {
      await updateEventStatus(id, status);
      setOverview((prev) => {
        if (!prev) return prev;
        const up = (arr: Event[]) => arr.map((e) => (e.id === id ? { ...e, status } : e));
        return { ...prev, events: up(prev.events), upcoming: up(prev.upcoming) };
      });
    } catch { /* degrade */ }
  }, []);

  const filtered = useMemo(
    () => tickers.filter((t) => t.toLowerCase().includes(search.toLowerCase())),
    [tickers, search],
  );

  const filings = useMemo(() => overview?.events.filter((e) => e.type === "SEC_FILING") ?? [], [overview]);
  const news = useMemo(() => overview?.events.filter((e) => e.type === "RSS_NEWS") ?? [], [overview]);
  const scheduled = useMemo(() => overview?.upcoming ?? [], [overview]);

  const pos = overview?.position;

  // ---- Stat helper ----
  const Stat = ({ label, value, color }: { label: string; value: string; color?: string }) => (
    <div style={{ display: "flex", flexDirection: "column", gap: 2, minWidth: 90 }}>
      <span style={{ fontFamily: sans, fontSize: 8, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.06em", color: "var(--text-dimmed)" }}>{label}</span>
      <span style={{ fontFamily: mono, fontSize: 12, fontWeight: 600, color: color ?? "var(--text-primary)" }}>{value}</span>
    </div>
  );

  return (
    <div style={{ display: "flex", gap: 1, minHeight: 500 }}>
      {/* ---- Left Sidebar ---- */}
      <div style={{ width: 240, flexShrink: 0, background: "var(--bg-panel)", border: "1px solid var(--border-primary)", borderRadius: 2, display: "flex", flexDirection: "column", overflow: "hidden" }}>
        <div style={{ padding: 8 }}>
          <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search tickers..."
            style={{ width: "100%", fontFamily: mono, fontSize: 10, padding: "5px 8px", background: "var(--bg-base)", color: "var(--text-primary)", border: "1px solid var(--border-primary)", borderRadius: 2, outline: "none" }} />
        </div>
        <div style={{ flex: 1, overflowY: "auto" }}>
          {loading && <div style={{ fontFamily: mono, fontSize: 10, color: "var(--text-dimmed)", padding: 12, textAlign: "center" }}>Loading...</div>}
          {!loading && filtered.length === 0 && <div style={{ fontFamily: mono, fontSize: 10, color: "var(--text-dimmed)", padding: 12, textAlign: "center" }}>No tickers</div>}
          {filtered.map((t) => {
            const active = selected === t;
            return (
              <div key={t} onClick={() => setSelected(t)}
                style={{ padding: "7px 12px", cursor: "pointer", fontFamily: mono, fontSize: 11, fontWeight: active ? 600 : 400,
                  color: active ? "var(--text-primary)" : "var(--text-secondary)",
                  background: active ? "var(--bg-active)" : "transparent",
                  borderLeft: active ? "2px solid var(--accent)" : "2px solid transparent",
                  borderBottom: "1px solid var(--border-subtle)", transition: "background 0.1s" }}
                onMouseEnter={(e) => { if (!active) e.currentTarget.style.background = "var(--bg-hover)"; }}
                onMouseLeave={(e) => { if (!active) e.currentTarget.style.background = active ? "var(--bg-active)" : "transparent"; }}>
                {t}
              </div>
            );
          })}
        </div>
      </div>

      {/* ---- Main Content ---- */}
      <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 1, overflowY: "auto" }}>
        {!selected && (
          <div style={{ background: "var(--bg-panel)", border: "1px solid var(--border-primary)", borderRadius: 2, padding: 40, textAlign: "center" }}>
            <span style={{ fontFamily: mono, fontSize: 11, color: "var(--text-dimmed)" }}>Select a ticker to view details</span>
          </div>
        )}

        {selected && detailLoading && (
          <div style={{ background: "var(--bg-panel)", border: "1px solid var(--border-primary)", borderRadius: 2, padding: 40, textAlign: "center" }}>
            <span style={{ fontFamily: mono, fontSize: 11, color: "var(--text-dimmed)" }}>Loading {selected}...</span>
          </div>
        )}

        {selected && !detailLoading && overview && (
          <>
            {/* Position Context */}
            <div style={{ background: "var(--bg-panel)", border: "1px solid var(--border-primary)", borderRadius: 2, padding: 12 }}>
              <div style={{ display: "flex", alignItems: "baseline", gap: 10, marginBottom: 8 }}>
                <span style={{ fontFamily: sans, fontSize: 16, fontWeight: 700, color: "var(--text-primary)" }}>{overview.symbol}</span>
                {pos?.sector && <span style={{ fontFamily: sans, fontSize: 10, color: "var(--text-dimmed)" }}>{pos.sector}</span>}
                {pos?.ib_category && <span style={{ fontFamily: sans, fontSize: 10, color: "var(--text-dimmed)" }}>{pos.ib_category}</span>}
              </div>
              {pos ? (
                <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
                  <Stat label="Shares" value={pos.position != null ? pos.position.toLocaleString() : "—"} />
                  <Stat label="Avg Cost" value={fmtCurrency(pos.avg_cost)} />
                  <Stat label="Mkt Price" value={fmtCurrency(pos.market_price)} />
                  <Stat label="Mkt Value" value={fmtCurrency(pos.market_value)} />
                  <Stat label="Unrealized P&L" value={fmtCurrency(pos.unrealized_pnl)} color={(pos.unrealized_pnl ?? 0) >= 0 ? "var(--green)" : "var(--red)"} />
                  <Stat label="Weight" value={pos.weight_pct != null ? `${pos.weight_pct.toFixed(2)}%` : "—"} />
                </div>
              ) : (
                <span style={{ fontFamily: mono, fontSize: 10, color: "var(--text-dimmed)" }}>No current position</span>
              )}
            </div>

            {/* What Changed */}
            <div style={{ background: "var(--bg-panel)", border: "1px solid var(--border-primary)", borderRadius: 2, overflow: "hidden" }}>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "8px 12px", borderBottom: "1px solid var(--border-subtle)" }}>
                <span style={{ fontFamily: sans, fontSize: 10, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.1em", color: "var(--text-muted)" }}>Last {days} Days</span>
                <div style={{ display: "flex", gap: 4 }}>
                  {[7, 14, 30].map((d) => (
                    <button key={d} className="method-btn" style={days === d ? { fontSize: 9, padding: "2px 8px", background: "var(--accent)", color: "#fff", borderColor: "var(--accent)" } : { fontSize: 9, padding: "2px 8px" }} onClick={() => setDays(d)}>{d}d</button>
                  ))}
                </div>
              </div>
              <GroupSection title="Filings" events={filings} emptyLabel="No filings" expandedId={expandedId} onToggle={(id) => setExpandedId(expandedId === id ? null : id)} onStatus={handleStatus} />
              <GroupSection title="News" events={news} emptyLabel="No news" expandedId={expandedId} onToggle={(id) => setExpandedId(expandedId === id ? null : id)} onStatus={handleStatus} />
              <GroupSection title="Scheduled" events={scheduled} emptyLabel="No scheduled events" expandedId={expandedId} onToggle={(id) => setExpandedId(expandedId === id ? null : id)} onStatus={handleStatus} />
            </div>

          </>
        )}
      </div>
    </div>
  );
}
