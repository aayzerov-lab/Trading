"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { API_URL, fetchWithRetry } from "@/lib/api";

// -- Types ------------------------------------------------------------------

type EventType = "SEC_FILING" | "MACRO_SCHEDULE" | "RSS_NEWS" | "OTHER";

interface CalendarEvent {
  id: string;
  ts_utc: string;
  scheduled_for_utc: string | null;
  type: EventType;
  tickers: string[] | null;
  title: string;
  source_name: string | null;
  source_url: string | null;
  severity_score: number;
  reason_codes: string[] | null;
  llm_summary: string | null;
  status: string;
  metadata_json: Record<string, unknown> | null;
}

// -- Constants & helpers ----------------------------------------------------

const mono = "var(--font-mono)";
const sans = "var(--font-sans)";
const TZ = "America/New_York";
const TYPE_LABELS: Record<EventType, string> = { SEC_FILING: "SEC", MACRO_SCHEDULE: "Macro", RSS_NEWS: "RSS", OTHER: "Other" };
const TYPE_COLORS: Record<EventType, string> = { SEC_FILING: "#8b5cf6", MACRO_SCHEDULE: "#3b82f6", RSS_NEWS: "#10b981", OTHER: "#64748b" };

function sevColor(s: number) { return s >= 75 ? "var(--red)" : s >= 50 ? "var(--yellow)" : "var(--text-muted)"; }

function toETDate(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleDateString("en-CA", { timeZone: TZ }); // "YYYY-MM-DD"
}

function toETTime(iso: string): string {
  return new Date(iso).toLocaleTimeString("en-US", { timeZone: TZ, hour: "2-digit", minute: "2-digit", hour12: false });
}

function formatGroupDate(key: string, today: string, tomorrow: string): string {
  if (key === today) return "Today";
  if (key === tomorrow) return "Tomorrow";
  const d = new Date(key + "T12:00:00");
  return new Intl.DateTimeFormat("en-US", { weekday: "short", month: "short", day: "numeric" }).format(d);
}

function countdown(iso: string): string {
  const diff = new Date(iso).getTime() - Date.now();
  if (diff <= 0) return "Now";
  const mins = Math.floor(diff / 60_000);
  if (mins < 60) return `In ${mins}m`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `In ${hrs}h ${mins % 60}m`;
  const days = Math.floor(hrs / 24);
  return `In ${days}d`;
}

function parseStr<T>(v: T): T {
  if (typeof v === "string") { try { return JSON.parse(v); } catch { /* pass */ } }
  return v;
}

function normalize(e: CalendarEvent): CalendarEvent {
  return { ...e, tickers: parseStr(e.tickers), reason_codes: parseStr(e.reason_codes), metadata_json: parseStr(e.metadata_json) };
}

// -- Sub-components ---------------------------------------------------------

function TypePill({ type }: { type: EventType }) {
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
      background: "var(--bg-hover)", color: "var(--accent)", border: "1px solid var(--border-primary)" }}>
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

function ToggleBtn({ active, label, onClick }: { active: boolean; label: string; onClick: () => void }) {
  return (
    <button onClick={onClick} style={{
      fontFamily: mono, fontSize: 9, fontWeight: 600, padding: "3px 10px", borderRadius: 2, cursor: "pointer",
      border: "1px solid " + (active ? "var(--accent)" : "var(--border-primary)"),
      background: active ? "var(--accent)" : "var(--bg-panel-alt)",
      color: active ? "#fff" : "var(--text-secondary)", transition: "all 0.1s",
    }}>
      {label}
    </button>
  );
}

// -- Detail expand ----------------------------------------------------------

function EventDetail({ event: e }: { event: CalendarEvent }) {
  return (
    <div style={{ background: "var(--bg-panel-alt)", border: "1px solid var(--border-subtle)", borderRadius: 2, padding: 12, display: "flex", flexDirection: "column", gap: 8, marginTop: 4 }}>
      <div style={{ fontFamily: sans, fontSize: 11, fontWeight: 600, color: "var(--text-primary)" }}>{e.title}</div>

      {e.source_name && (
        <div style={{ fontFamily: sans, fontSize: 9, color: "var(--text-dimmed)" }}>
          Source:{" "}{e.source_url ? <a href={e.source_url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--accent)" }}>{e.source_name}</a> : e.source_name}
        </div>
      )}

      {e.llm_summary && (
        <div style={{ fontFamily: sans, fontSize: 10, color: "var(--text-secondary)", lineHeight: 1.5 }}>{e.llm_summary}</div>
      )}

      {e.reason_codes && e.reason_codes.length > 0 && (
        <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
          {e.reason_codes.map((rc, i) => <TagChip key={i} label={rc} />)}
        </div>
      )}

      {e.scheduled_for_utc && (
        <div style={{ fontFamily: mono, fontSize: 9, color: "var(--text-muted)" }}>
          Scheduled: {new Date(e.scheduled_for_utc).toLocaleString("en-US", { timeZone: TZ })} ET &mdash; {countdown(e.scheduled_for_utc)}
        </div>
      )}
    </div>
  );
}

// -- Event row --------------------------------------------------------------

function EventRow({ event: e, expanded, onToggle }: { event: CalendarEvent; expanded: boolean; onToggle: () => void }) {
  const time = e.scheduled_for_utc ? toETTime(e.scheduled_for_utc) + " ET" : "\u2014";
  const cd = e.scheduled_for_utc ? countdown(e.scheduled_for_utc) : "";

  return (
    <div>
      <div onClick={onToggle} style={{
        display: "flex", alignItems: "center", gap: 8, padding: "6px 10px", cursor: "pointer",
        background: expanded ? "var(--bg-active)" : "transparent", transition: "background 0.1s",
      }}
        onMouseEnter={(ev) => { if (!expanded) ev.currentTarget.style.background = "var(--bg-hover)"; }}
        onMouseLeave={(ev) => { if (!expanded) ev.currentTarget.style.background = "transparent"; }}>
        <span style={{ fontFamily: mono, fontSize: 10, color: "var(--text-secondary)", width: 56, flexShrink: 0 }}>{time}</span>
        <SevDot score={e.severity_score} />
        <span style={{ fontFamily: sans, fontSize: 10, fontWeight: 500, color: "var(--text-primary)", flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{e.title}</span>
        <TypePill type={e.type} />
        {e.tickers && e.tickers.length > 0 && (
          <span style={{ display: "flex", gap: 3 }}>
            {e.tickers.slice(0, 3).map((t) => <TickerBadge key={t} ticker={t} />)}
          </span>
        )}
        <span style={{ fontFamily: mono, fontSize: 9, color: "var(--text-dimmed)", whiteSpace: "nowrap", minWidth: 54, textAlign: "right" }}>{cd}</span>
      </div>
      {expanded && <div style={{ padding: "0 10px 8px 10px" }}><EventDetail event={e} /></div>}
    </div>
  );
}

// -- Main component ---------------------------------------------------------

const RANGE_OPTS = [7, 30, 90] as const;
const SCOPE_OPTS = [{ label: "My Book", value: "my" }, { label: "All", value: "all" }] as const;

export default function CalendarView() {
  const [days, setDays] = useState<number>(30);
  const [scope, setScope] = useState<string>("my");
  const [events, setEvents] = useState<CalendarEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [expandedId, setExpandedId] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetchWithRetry(`${API_URL}/events/calendar?days=${days}&scope=${scope}`);
      if (!res.ok) throw new Error(res.statusText);
      const data = await res.json();
      const items: CalendarEvent[] = (data.items ?? []).map(normalize);
      setEvents(items);
    } catch { setEvents([]); } finally { setLoading(false); }
  }, [days, scope]);

  useEffect(() => { load(); }, [load]);

  // Group by ET date
  const grouped = useMemo(() => {
    const map = new Map<string, CalendarEvent[]>();
    for (const e of events) {
      const key = e.scheduled_for_utc ? toETDate(e.scheduled_for_utc) : "unscheduled";
      const arr = map.get(key);
      if (arr) arr.push(e); else map.set(key, [e]);
    }
    // Sort keys chronologically
    return [...map.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  }, [events]);

  // "Today" / "Tomorrow" detection
  const now = new Date();
  const todayKey = toETDate(now.toISOString());
  const tmrw = new Date(now); tmrw.setDate(tmrw.getDate() + 1);
  const tomorrowKey = toETDate(tmrw.toISOString());

  return (
    <div style={{ background: "var(--bg-panel)", border: "1px solid var(--border-primary)", borderRadius: 2, display: "flex", flexDirection: "column", overflow: "hidden", minHeight: 320 }}>
      {/* Toolbar */}
      <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "8px 10px", borderBottom: "1px solid var(--border-subtle)", background: "var(--bg-panel-alt)" }}>
        <span style={{ fontFamily: mono, fontSize: 9, fontWeight: 600, color: "var(--text-dimmed)", textTransform: "uppercase", letterSpacing: "0.06em" }}>Range:</span>
        {RANGE_OPTS.map((d) => <ToggleBtn key={d} active={days === d} label={`${d}d`} onClick={() => setDays(d)} />)}
        <span style={{ width: 1, height: 16, background: "var(--border-primary)", margin: "0 4px" }} />
        <span style={{ fontFamily: mono, fontSize: 9, fontWeight: 600, color: "var(--text-dimmed)", textTransform: "uppercase", letterSpacing: "0.06em" }}>Scope:</span>
        {SCOPE_OPTS.map((s) => <ToggleBtn key={s.value} active={scope === s.value} label={s.label} onClick={() => setScope(s.value)} />)}
      </div>

      {/* Body */}
      <div style={{ flex: 1, overflowY: "auto", padding: "4px 0" }}>
        {loading && events.length === 0 && (
          <div style={{ padding: 24, textAlign: "center", fontFamily: sans, fontSize: 11, color: "var(--text-dimmed)" }}>Loading calendar...</div>
        )}

        {!loading && events.length === 0 && (
          <div style={{ padding: 24, textAlign: "center", fontFamily: sans, fontSize: 11, color: "var(--text-dimmed)" }}>
            No scheduled events in the next {days} days
          </div>
        )}

        {grouped.map(([dateKey, items]) => (
          <div key={dateKey} style={{ marginBottom: 2 }}>
            {/* Day group header */}
            <div style={{
              display: "flex", alignItems: "center", gap: 8, padding: "6px 10px",
              borderLeft: "3px solid var(--accent)", background: "var(--bg-base)",
              fontFamily: mono, fontSize: 10, fontWeight: 700, color: "var(--text-secondary)",
              textTransform: "uppercase", letterSpacing: "0.04em",
            }}>
              {formatGroupDate(dateKey, todayKey, tomorrowKey)}
              <span style={{ fontFamily: mono, fontSize: 8, fontWeight: 400, color: "var(--text-dimmed)" }}>
                {items.length} event{items.length !== 1 ? "s" : ""}
              </span>
            </div>

            {/* Event rows */}
            {items
              .sort((a, b) => (a.scheduled_for_utc ?? "").localeCompare(b.scheduled_for_utc ?? ""))
              .map((evt) => (
                <EventRow key={evt.id} event={evt} expanded={expandedId === evt.id}
                  onToggle={() => setExpandedId(expandedId === evt.id ? null : evt.id)} />
              ))}
          </div>
        ))}
      </div>
    </div>
  );
}
