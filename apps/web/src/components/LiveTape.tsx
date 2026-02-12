"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { API_URL } from "@/lib/api";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface Event {
  id: string;
  ts_utc: string;
  scheduled_for_utc: string | null;
  type: "SEC_FILING" | "MACRO_SCHEDULE" | "RSS_NEWS" | "OTHER";
  tickers: string[] | null;
  title: string;
  source_name: string | null;
  source_url: string | null;
  raw_text_snippet: string | null;
  severity_score: number;
  reason_codes: string[] | null;
  llm_summary: string | null;
  status: "NEW" | "ACKED" | "DISMISSED";
  metadata_json: Record<string, unknown> | null;
}

type Scope = "my" | "all";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const mono = "var(--font-mono)";
const sans = "var(--font-sans)";
const MAX_ITEMS = 200;
const POLL_MS = 30_000;
const TYPE_COLORS: Record<string, string> = {
  SEC_FILING: "#8b5cf6", MACRO_SCHEDULE: "#3b82f6", RSS_NEWS: "#10b981", OTHER: "#64748b",
};
const TYPE_LABELS: Record<string, string> = {
  SEC_FILING: "SEC", MACRO_SCHEDULE: "Macro", RSS_NEWS: "News", OTHER: "Other",
};

function normalize(e: Event): Event {
  let tickers = e.tickers;
  if (typeof tickers === "string") { try { tickers = JSON.parse(tickers); } catch { tickers = null; } }
  let reason_codes = e.reason_codes;
  if (typeof reason_codes === "string") { try { reason_codes = JSON.parse(reason_codes); } catch { reason_codes = null; } }
  return { ...e, tickers, reason_codes };
}

function timeAgo(iso: string): string {
  const m = Math.floor((Date.now() - new Date(iso).getTime()) / 60_000);
  if (m < 1) return "now";
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h`;
  return `${Math.floor(h / 24)}d`;
}

function fmtET(iso: string): string {
  try {
    return new Date(iso).toLocaleString("en-US", {
      timeZone: "America/New_York", month: "short", day: "numeric",
      hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
    }) + " ET";
  } catch { return iso; }
}

function sevColor(s: number) {
  return s >= 80 ? "var(--red)" : s >= 50 ? "var(--yellow)" : "var(--text-dimmed)";
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function SevDot({ score }: { score: number }) {
  const bg = score >= 80 ? "var(--red)" : score >= 50 ? "var(--yellow)" : "var(--text-dimmed)";
  const shadow = score >= 80 ? "0 0 4px var(--red)" : "none";
  return <span style={{ display: "inline-block", width: 8, height: 8, borderRadius: "50%", background: bg, boxShadow: shadow, flexShrink: 0 }} />;
}

function TypePill({ type }: { type: string }) {
  const c = TYPE_COLORS[type] ?? TYPE_COLORS.OTHER;
  return (
    <span style={{ fontFamily: mono, fontSize: 8, fontWeight: 600, textTransform: "uppercase",
      letterSpacing: "0.06em", padding: "1px 5px", borderRadius: 2,
      background: c + "22", color: c, border: `1px solid ${c}44`, whiteSpace: "nowrap" }}>
      {TYPE_LABELS[type] ?? type}
    </span>
  );
}

function TickerChip({ t }: { t: string }) {
  return (
    <span style={{ fontFamily: mono, fontSize: 8, fontWeight: 600, padding: "1px 4px", borderRadius: 2,
      background: "var(--accent-muted)", color: "var(--accent)", border: "1px solid var(--accent)" }}>
      {t}
    </span>
  );
}

function TagChip({ label }: { label: string }) {
  return (
    <span style={{ fontFamily: mono, fontSize: 8, padding: "1px 4px", borderRadius: 2,
      background: "var(--bg-hover)", color: "var(--text-secondary)", border: "1px solid var(--border-primary)" }}>
      {label}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Feed row
// ---------------------------------------------------------------------------

function FeedRow({ evt, selected, onSelect }: { evt: Event; selected: boolean; onSelect: () => void }) {
  return (
    <div onClick={onSelect}
      style={{ display: "flex", alignItems: "flex-start", gap: 8, padding: "6px 10px",
        cursor: "pointer", background: selected ? "var(--bg-active)" : "transparent",
        borderBottom: "1px solid var(--border-subtle)", transition: "background 0.1s" }}
      onMouseEnter={(e) => { if (!selected) e.currentTarget.style.background = "var(--bg-hover)"; }}
      onMouseLeave={(e) => { if (!selected) e.currentTarget.style.background = "transparent"; }}>
      <div style={{ paddingTop: 3 }}><SevDot score={evt.severity_score} /></div>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontFamily: sans, fontSize: 10, fontWeight: 500, color: "var(--text-primary)",
          lineHeight: 1.35, display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical", overflow: "hidden" }}>
          {evt.title}
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 4, marginTop: 3, flexWrap: "wrap" }}>
          <TypePill type={evt.type} />
          {evt.tickers?.slice(0, 3).map((t) => <TickerChip key={t} t={t} />)}
          {evt.tickers && evt.tickers.length > 3 && (
            <span style={{ fontFamily: mono, fontSize: 7, color: "var(--text-dimmed)" }}>+{evt.tickers.length - 3}</span>
          )}
        </div>
      </div>
      <div style={{ flexShrink: 0, textAlign: "right", display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 2 }}>
        <span style={{ fontFamily: mono, fontSize: 9, color: "var(--text-muted)", whiteSpace: "nowrap" }}>{timeAgo(evt.ts_utc)}</span>
        {evt.source_name && (
          <span style={{ fontFamily: sans, fontSize: 8, color: "var(--text-dimmed)", maxWidth: 80,
            overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{evt.source_name}</span>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Preview pane
// ---------------------------------------------------------------------------

function Preview({ evt, onStatus }: { evt: Event; onStatus: (id: string, s: "ACKED" | "DISMISSED") => void }) {
  const btnBase: React.CSSProperties = { fontFamily: mono, fontSize: 9, padding: "3px 10px", borderRadius: 2, cursor: "pointer", transition: "all 0.15s", border: "1px solid var(--border-primary)", background: "transparent", color: "var(--text-muted)" };
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10, overflowY: "auto", flex: 1, padding: 12 }}>
      <div style={{ fontFamily: sans, fontSize: 12, fontWeight: 600, color: "var(--text-primary)", lineHeight: 1.4 }}>{evt.title}</div>

      <div style={{ display: "flex", flexWrap: "wrap", gap: 10, fontFamily: mono, fontSize: 9, color: "var(--text-muted)" }}>
        <span>{fmtET(evt.ts_utc)}</span>
        <span>Severity: <strong style={{ color: sevColor(evt.severity_score) }}>{evt.severity_score}</strong></span>
        {evt.source_name && (
          evt.source_url
            ? <a href={evt.source_url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--accent)" }}>{evt.source_name}</a>
            : <span>{evt.source_name}</span>
        )}
      </div>

      {evt.tickers && evt.tickers.length > 0 && (
        <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
          {evt.tickers.map((t) => <TickerChip key={t} t={t} />)}
        </div>
      )}

      {evt.reason_codes && evt.reason_codes.length > 0 && (
        <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
          {evt.reason_codes.map((rc, i) => <TagChip key={i} label={rc} />)}
        </div>
      )}

      {evt.raw_text_snippet && (
        <pre style={{ fontFamily: mono, fontSize: 9, color: "var(--text-secondary)", background: "var(--bg-base)",
          border: "1px solid var(--border-primary)", borderRadius: 2, padding: 8,
          whiteSpace: "pre-wrap", wordBreak: "break-word", maxHeight: 150, overflowY: "auto", lineHeight: 1.5, margin: 0 }}>
          {evt.raw_text_snippet}
        </pre>
      )}

      {evt.llm_summary && (
        <div style={{ fontFamily: sans, fontSize: 10, color: "var(--text-primary)", background: "rgba(59,130,246,0.08)",
          border: "1px solid rgba(59,130,246,0.2)", borderRadius: 2, padding: 8, lineHeight: 1.5 }}>
          <span style={{ fontFamily: sans, fontSize: 8, fontWeight: 600, textTransform: "uppercase",
            letterSpacing: "0.06em", color: "var(--accent)", display: "block", marginBottom: 4 }}>LLM Summary</span>
          {evt.llm_summary}
        </div>
      )}

      <div style={{ display: "flex", gap: 6, marginTop: 2 }}>
        <button className="method-btn"
          style={evt.status === "ACKED" ? { ...btnBase, background: "var(--accent)", color: "#fff", borderColor: "var(--accent)" } : btnBase}
          onClick={() => onStatus(evt.id, "ACKED")}>Ack</button>
        <button className="method-btn"
          style={evt.status === "DISMISSED" ? { ...btnBase, background: "var(--red-dim)", color: "#fff", borderColor: "var(--red-dim)" } : btnBase}
          onClick={() => onStatus(evt.id, "DISMISSED")}>Dismiss</button>
        <span style={{ marginLeft: "auto", fontFamily: mono, fontSize: 8, color: "var(--text-dimmed)", alignSelf: "center", textTransform: "uppercase" }}>{evt.status}</span>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export default function LiveTape() {
  const [events, setEvents] = useState<Event[]>([]);
  const [scope, setScope] = useState<Scope>("my");
  const [highOnly, setHighOnly] = useState(false);
  const [search, setSearch] = useState("");
  const [selId, setSelId] = useState<string | null>(null);
  const [newCount, setNewCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const listRef = useRef<HTMLDivElement>(null);
  const latestTs = useRef<string | null>(null);

  // -- fetch today's events ------------------------------------------------
  const fetchToday = useCallback(async () => {
    const minSev = highOnly ? 60 : 0;
    const url = `${API_URL}/events/today?scope=${scope}&min_severity=${minSev}&types=RSS_NEWS,SEC_FILING&limit=${MAX_ITEMS}`;
    try {
      const res = await fetch(url);
      if (!res.ok) return;
      const data: Event[] = await res.json();
      const normed = data.map(normalize).slice(0, MAX_ITEMS);
      setEvents(normed);
      if (normed.length > 0) latestTs.current = normed[0].ts_utc;
      setNewCount(0);
    } catch { /* degrade */ }
    finally { setLoading(false); }
  }, [scope, highOnly]);

  // -- poll for new events -------------------------------------------------
  const poll = useCallback(async () => {
    if (!latestTs.current) return;
    const minSev = highOnly ? 60 : 0;
    const url = `${API_URL}/events/since?since_ts=${encodeURIComponent(latestTs.current)}&scope=${scope}&min_severity=${minSev}`;
    try {
      const res = await fetch(url);
      if (!res.ok) return;
      const fresh: Event[] = await res.json();
      if (fresh.length === 0) return;
      const normed = fresh.map(normalize);
      setEvents((prev) => [...normed, ...prev].slice(0, MAX_ITEMS));
      latestTs.current = normed[0].ts_utc;
      setNewCount((n) => n + normed.length);
    } catch { /* degrade */ }
  }, [scope, highOnly]);

  // -- lifecycle -----------------------------------------------------------
  useEffect(() => { fetchToday(); }, [fetchToday]);
  useEffect(() => { const id = setInterval(poll, POLL_MS); return () => clearInterval(id); }, [poll]);

  // -- status update -------------------------------------------------------
  const handleStatus = useCallback(async (id: string, status: "ACKED" | "DISMISSED") => {
    setEvents((prev) => prev.map((e) => (e.id === id ? { ...e, status } : e)));
    try { await fetch(`${API_URL}/events/${id}/status`, { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ status }) }); }
    catch { /* revert on error would go here */ }
  }, []);

  // -- filtered list -------------------------------------------------------
  const filtered = useMemo(() => {
    if (!search) return events;
    const q = search.toLowerCase();
    return events.filter((e) =>
      e.title.toLowerCase().includes(q) ||
      e.tickers?.some((t) => t.toLowerCase().includes(q)) ||
      e.source_name?.toLowerCase().includes(q)
    );
  }, [events, search]);

  const selEvt = useMemo(() => events.find((e) => e.id === selId) ?? null, [events, selId]);

  const scrollToTop = () => { listRef.current?.scrollTo({ top: 0, behavior: "smooth" }); setNewCount(0); };

  // -- render --------------------------------------------------------------
  if (loading && events.length === 0) {
    return <div className="panel" style={{ padding: 12 }}><div className="empty-state">Loading news tape...</div></div>;
  }

  return (
    <div style={{ display: "flex", gap: 1, minHeight: 420, background: "var(--border-primary)" }}>
      {/* ---- Feed list ---- */}
      <div style={{ flex: 1, background: "var(--bg-panel)", border: "1px solid var(--border-primary)", borderRadius: 2, display: "flex", flexDirection: "column", overflow: "hidden" }}>
        {/* Toolbar */}
        <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "6px 10px", borderBottom: "1px solid var(--border-subtle)", background: "var(--bg-panel-alt)", flexWrap: "wrap" }}>
          <span style={{ fontFamily: sans, fontSize: 8, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.06em", color: "var(--text-dimmed)" }}>Scope:</span>
          <button className={`method-btn${scope === "my" ? " active" : ""}`} style={{ fontSize: 9, padding: "2px 8px" }} onClick={() => setScope("my")}>My Book</button>
          <button className={`method-btn${scope === "all" ? " active" : ""}`} style={{ fontSize: 9, padding: "2px 8px" }} onClick={() => setScope("all")}>All</button>

          <span style={{ width: 1, height: 14, background: "var(--border-primary)", margin: "0 2px" }} />

          <span style={{ fontFamily: sans, fontSize: 8, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.06em", color: "var(--text-dimmed)" }}>Impact:</span>
          <button className={`method-btn${!highOnly ? " active" : ""}`} style={{ fontSize: 9, padding: "2px 8px" }} onClick={() => setHighOnly(false)}>All</button>
          <button className={`method-btn${highOnly ? " active" : ""}`} style={{ fontSize: 9, padding: "2px 8px" }} onClick={() => setHighOnly(true)}>High</button>

          <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search..."
            style={{ marginLeft: "auto", fontFamily: mono, fontSize: 9, padding: "2px 6px", width: 110,
              background: "var(--bg-base)", border: "1px solid var(--border-primary)", borderRadius: 2,
              color: "var(--text-secondary)", outline: "none" }} />
        </div>

        {/* New-events banner */}
        {newCount > 0 && (
          <div onClick={scrollToTop}
            style={{ padding: "4px 10px", background: "var(--accent-muted)", borderBottom: "1px solid var(--accent)",
              fontFamily: mono, fontSize: 9, color: "var(--accent)", cursor: "pointer", textAlign: "center" }}>
            {newCount} new event{newCount > 1 ? "s" : ""} -- click to scroll up
          </div>
        )}

        {/* List */}
        <div ref={listRef} style={{ flex: 1, overflowY: "auto" }}>
          {filtered.length === 0 && <div className="empty-state" style={{ padding: 24 }}>No events</div>}
          {filtered.map((evt) => (
            <FeedRow key={evt.id} evt={evt} selected={selId === evt.id} onSelect={() => setSelId(evt.id)} />
          ))}
        </div>

        {/* Count bar */}
        <div style={{ padding: "3px 10px", borderTop: "1px solid var(--border-subtle)", fontFamily: mono, fontSize: 8, color: "var(--text-dimmed)", background: "var(--bg-panel-alt)" }}>
          {filtered.length} event{filtered.length !== 1 ? "s" : ""}
        </div>
      </div>

      {/* ---- Preview pane ---- */}
      {selEvt && (
        <div style={{ width: 380, flexShrink: 0, background: "var(--bg-panel)", border: "1px solid var(--border-primary)", borderRadius: 2, display: "flex", flexDirection: "column", overflow: "hidden" }}>
          <div className="panel-header" style={{ margin: 0, padding: "8px 12px", borderBottom: "1px solid var(--border-subtle)" }}>
            Detail
          </div>
          <Preview evt={selEvt} onStatus={handleStatus} />
        </div>
      )}
    </div>
  );
}
