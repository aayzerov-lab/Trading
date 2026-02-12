"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Event, EventType, EventStatus,
  fetchEvents, fetchHighPriorityEvents, updateEventStatus, seedEvents,
} from "@/lib/events-api";

interface EventsPanelProps { loading?: boolean; }

const TYPE_LABELS: Record<EventType, string> = { SEC_FILING: "SEC", MACRO_SCHEDULE: "Macro", RSS_NEWS: "RSS", OTHER: "Other" };
const TYPE_COLORS: Record<EventType, string> = { SEC_FILING: "#8b5cf6", MACRO_SCHEDULE: "#3b82f6", RSS_NEWS: "#10b981", OTHER: "#64748b" };

function sevColor(s: number) { return s >= 80 ? "var(--red)" : s >= 50 ? "var(--yellow)" : "var(--text-muted)"; }

function timeAgo(iso: string): string {
  const m = Math.floor((Date.now() - new Date(iso).getTime()) / 60_000);
  if (m < 1) return "just now";
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

const mono = "var(--font-mono)";
const sans = "var(--font-sans)";
const ellipsis: React.CSSProperties = { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" };

function TypeBadge({ type }: { type: EventType }) {
  const c = TYPE_COLORS[type];
  return (
    <span style={{ fontFamily: mono, fontSize: 8, fontWeight: 600, textTransform: "uppercase",
      letterSpacing: "0.06em", padding: "1px 5px", borderRadius: 2,
      background: c + "22", color: c, border: `1px solid ${c}44`, whiteSpace: "nowrap" }}>
      {TYPE_LABELS[type]}
    </span>
  );
}

function SevDot({ score }: { score: number }) {
  return <span style={{ display: "inline-block", width: 8, height: 8, borderRadius: "50%", background: sevColor(score), flexShrink: 0 }} />;
}

function TickerBadge({ ticker }: { ticker: string }) {
  return (
    <span style={{ fontFamily: mono, fontSize: 9, fontWeight: 600, padding: "1px 5px", borderRadius: 2,
      background: "var(--accent-muted)", color: "var(--accent)", border: "1px solid var(--accent)" }}>
      {ticker}
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

/* ---------- Detail pane ---------- */

function EventDetail({ event: e, onStatus }: { event: Event; onStatus: (id: string, s: EventStatus) => void }) {
  const btnStyle = (active: boolean, bg: string): React.CSSProperties =>
    active ? { fontSize: 9, padding: "3px 10px", background: bg, color: "#fff", borderColor: bg } : { fontSize: 9, padding: "3px 10px" };

  return (
    <div style={{ background: "var(--bg-panel-alt)", border: "1px solid var(--border-subtle)", borderRadius: 2, padding: 12, display: "flex", flexDirection: "column", gap: 8 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <SevDot score={e.severity_score} />
        <TypeBadge type={e.type} />
        <span style={{ fontFamily: sans, fontSize: 11, fontWeight: 600, color: "var(--text-primary)", flex: 1 }}>{e.title}</span>
      </div>

      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, fontFamily: mono, fontSize: 9, color: "var(--text-muted)" }}>
        <span>Severity: <strong style={{ color: sevColor(e.severity_score) }}>{e.severity_score}</strong></span>
        <span>{timeAgo(e.ts_utc)}</span>
        <span>{new Date(e.ts_utc).toLocaleString()}</span>
      </div>

      {e.tickers && e.tickers.length > 0 && (
        <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
          {e.tickers.map((t) => <TickerBadge key={t} ticker={t} />)}
        </div>
      )}

      {e.source_name && (
        <div style={{ fontFamily: sans, fontSize: 9, color: "var(--text-dimmed)" }}>
          Source:{" "}{e.source_url ? <a href={e.source_url} target="_blank" rel="noopener noreferrer">{e.source_name}</a> : e.source_name}
        </div>
      )}

      {e.reason_codes && e.reason_codes.length > 0 && (
        <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
          {e.reason_codes.map((rc, i) => <TagChip key={i} label={rc} />)}
        </div>
      )}

      {e.raw_text_snippet && (
        <pre style={{ fontFamily: mono, fontSize: 9, color: "var(--text-secondary)", background: "var(--bg-base)",
          border: "1px solid var(--border-primary)", borderRadius: 2, padding: 8,
          whiteSpace: "pre-wrap", wordBreak: "break-word", maxHeight: 120, overflowY: "auto", lineHeight: 1.5 }}>
          {e.raw_text_snippet}
        </pre>
      )}

      {e.llm_summary && (
        <div style={{ fontFamily: sans, fontSize: 10, color: "var(--text-primary)", background: "var(--bg-hover)",
          border: "1px solid var(--border-subtle)", borderRadius: 2, padding: 8, lineHeight: 1.5 }}>
          <span style={{ fontFamily: sans, fontSize: 8, fontWeight: 600, textTransform: "uppercase",
            letterSpacing: "0.06em", color: "var(--text-dimmed)", display: "block", marginBottom: 4 }}>LLM Summary</span>
          {e.llm_summary}
        </div>
      )}

      <div style={{ display: "flex", gap: 6, marginTop: 2 }}>
        <button className="method-btn" style={btnStyle(e.status === "ACKED", "var(--accent)")} onClick={() => onStatus(e.id, "ACKED")}>Ack</button>
        <button className="method-btn" style={btnStyle(e.status === "DISMISSED", "var(--red-dim)")} onClick={() => onStatus(e.id, "DISMISSED")}>Dismiss</button>
        <span style={{ marginLeft: "auto", fontFamily: mono, fontSize: 8, color: "var(--text-dimmed)", alignSelf: "center", textTransform: "uppercase" }}>{e.status}</span>
      </div>
    </div>
  );
}

/* ---------- Main component ---------- */

const TYPE_BTNS: Array<{ label: string; value: EventType | null }> = [
  { label: "All", value: null }, { label: "SEC", value: "SEC_FILING" },
  { label: "Macro", value: "MACRO_SCHEDULE" }, { label: "RSS", value: "RSS_NEWS" },
];
const STATUS_BTNS: Array<{ label: string; value: EventStatus | null }> = [
  { label: "All", value: null }, { label: "New", value: "NEW" },
  { label: "Acked", value: "ACKED" }, { label: "Dismissed", value: "DISMISSED" },
];

export default function EventsPanel({ loading: extLoad = false }: EventsPanelProps) {
  const [events, setEvents] = useState<Event[]>([]);
  const [highPri, setHighPri] = useState<Event[]>([]);
  const [selId, setSelId] = useState<string | null>(null);
  const [typeFilt, setTypeFilt] = useState<EventType | null>(null);
  const [statusFilt, setStatusFilt] = useState<EventStatus | null>(null);
  const [fetching, setFetching] = useState(true);
  const [seeding, setSeeding] = useState(false);

  const load = useCallback(async () => {
    setFetching(true);
    try {
      const [all, hi] = await Promise.all([
        fetchEvents({ type: typeFilt ?? undefined, status: statusFilt ?? undefined, limit: 100 }),
        fetchHighPriorityEvents(15),
      ]);
      setEvents(all);
      setHighPri(hi);
    } catch { /* degrade */ } finally { setFetching(false); }
  }, [typeFilt, statusFilt]);

  useEffect(() => { load(); }, [load]);

  const handleSeed = useCallback(async () => {
    setSeeding(true);
    try { await seedEvents(); await load(); } catch {} finally { setSeeding(false); }
  }, [load]);

  const handleStatus = useCallback(async (id: string, status: EventStatus) => {
    try {
      await updateEventStatus(id, status);
      const up = (prev: Event[]) => prev.map((e) => (e.id === id ? { ...e, status } : e));
      setEvents(up); setHighPri(up);
    } catch {}
  }, []);

  const selEvt = useMemo(
    () => events.find((e) => e.id === selId) ?? highPri.find((e) => e.id === selId) ?? null,
    [events, highPri, selId],
  );

  if ((extLoad || fetching) && events.length === 0) {
    return <div className="panel" style={{ padding: 12 }}><div className="empty-state">Loading events...</div></div>;
  }

  return (
    <div style={{ display: "flex", gap: 1, minHeight: 400 }}>
      {/* Left column: High priority */}
      <div style={{ width: 320, flexShrink: 0, background: "var(--bg-panel)", border: "1px solid var(--border-primary)", borderRadius: 2, display: "flex", flexDirection: "column", overflow: "hidden" }}>
        <div className="panel-header" style={{ margin: "10px 12px 0" }}>High Priority</div>
        <div style={{ flex: 1, overflowY: "auto", padding: "0 8px 8px" }}>
          {highPri.length === 0 && <div className="empty-state" style={{ padding: 16 }}>No high-priority events</div>}
          {highPri.map((evt) => {
            const active = selId === evt.id;
            return (
              <div key={evt.id} onClick={() => setSelId(evt.id)}
                style={{ display: "flex", alignItems: "flex-start", gap: 6, padding: 6, borderRadius: 2,
                  cursor: "pointer", background: active ? "var(--bg-active)" : "transparent",
                  borderBottom: "1px solid var(--border-subtle)", transition: "background 0.1s" }}
                onMouseEnter={(e) => { if (!active) e.currentTarget.style.background = "var(--bg-hover)"; }}
                onMouseLeave={(e) => { if (!active) e.currentTarget.style.background = "transparent"; }}>
                <SevDot score={evt.severity_score} />
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontFamily: sans, fontSize: 10, fontWeight: 500, color: "var(--text-primary)", ...ellipsis }}>{evt.title}</div>
                  <div style={{ display: "flex", alignItems: "center", gap: 6, marginTop: 2 }}>
                    <TypeBadge type={evt.type} />
                    <span style={{ fontFamily: mono, fontSize: 8, color: "var(--text-dimmed)" }}>{timeAgo(evt.ts_utc)}</span>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Right column: events list + detail */}
      <div style={{ flex: 1, background: "var(--bg-panel)", border: "1px solid var(--border-primary)", borderRadius: 2, display: "flex", flexDirection: "column", overflow: "hidden" }}>
        {/* Toolbar */}
        <div style={{ display: "flex", alignItems: "center", flexWrap: "wrap", gap: 8, padding: "8px 10px", borderBottom: "1px solid var(--border-subtle)", background: "var(--bg-panel-alt)" }}>
          <span className="risk-toggle-label">Type:</span>
          {TYPE_BTNS.map((b) => (
            <button key={b.label} className={`risk-toggle-btn${typeFilt === b.value ? " active" : ""}`} onClick={() => setTypeFilt(b.value)}>{b.label}</button>
          ))}
          <span style={{ width: 1, height: 16, background: "var(--border-primary)", margin: "0 4px" }} />
          <span className="risk-toggle-label">Status:</span>
          {STATUS_BTNS.map((b) => (
            <button key={b.label} className={`risk-toggle-btn${statusFilt === b.value ? " active" : ""}`} onClick={() => setStatusFilt(b.value)}>{b.label}</button>
          ))}
          <button className="recompute-btn" style={{ marginLeft: "auto" }} onClick={handleSeed} disabled={seeding}>
            {seeding ? "Seeding..." : "Seed Data"}
          </button>
        </div>

        {/* Events table */}
        <div style={{ flex: 1, overflowY: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr>
                <th style={{ width: 24 }} />
                <th>Type</th>
                <th>Title</th>
                <th>Tickers</th>
                <th>Source</th>
                <th className="cell-right">Sev</th>
                <th className="cell-right">Time</th>
              </tr>
            </thead>
            <tbody>
              {events.length === 0 && <tr><td colSpan={7} className="empty-state">No events found</td></tr>}
              {events.map((evt) => {
                const active = selId === evt.id;
                return (
                  <tr key={evt.id} onClick={() => setSelId(evt.id)} style={{ cursor: "pointer", background: active ? "var(--bg-active)" : undefined }}>
                    <td style={{ textAlign: "center", padding: "5px 4px" }}><SevDot score={evt.severity_score} /></td>
                    <td style={{ padding: "5px 6px" }}><TypeBadge type={evt.type} /></td>
                    <td style={{ padding: "5px 6px", fontFamily: sans, fontSize: 10, fontWeight: 500, color: "var(--text-primary)", maxWidth: 280, ...ellipsis }}>{evt.title}</td>
                    <td style={{ padding: "5px 6px", fontFamily: mono, fontSize: 9, color: "var(--accent)", maxWidth: 120, ...ellipsis }}>{evt.tickers?.join(", ") ?? "\u2014"}</td>
                    <td style={{ padding: "5px 6px", fontFamily: sans, fontSize: 9, color: "var(--text-dimmed)", maxWidth: 100, ...ellipsis }}>{evt.source_name ?? "\u2014"}</td>
                    <td className="cell-right" style={{ fontFamily: mono, fontSize: 10, fontWeight: 600, color: sevColor(evt.severity_score) }}>{evt.severity_score}</td>
                    <td className="cell-right" style={{ fontFamily: mono, fontSize: 9, color: "var(--text-dimmed)", whiteSpace: "nowrap" }}>{timeAgo(evt.ts_utc)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* Detail pane */}
        {selEvt && (
          <div style={{ borderTop: "1px solid var(--border-primary)", padding: 8 }}>
            <EventDetail event={selEvt} onStatus={handleStatus} />
          </div>
        )}
      </div>
    </div>
  );
}
