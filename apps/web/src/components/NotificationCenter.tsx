"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import {
  Alert, AlertStatus, Keyword, fetchAlerts, fetchUnreadAlertCount, updateAlertStatus,
  fetchKeywords, addKeyword, deleteKeyword,
} from "@/lib/events-api";

function timeAgo(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

function severityColor(s: number): string {
  if (s >= 80) return "var(--red)";
  if (s >= 60) return "var(--yellow)";
  return "var(--green)";
}

const S: Record<string, React.CSSProperties> = {
  wrap: { position: "relative" },
  bell: {
    background: "none", border: "none", cursor: "pointer",
    padding: "4px 6px", position: "relative", display: "flex", alignItems: "center",
  },
  badge: {
    position: "absolute", top: 0, right: 0, background: "var(--red)", color: "#fff",
    fontSize: 9, fontWeight: 700, fontFamily: "var(--font-mono)", lineHeight: "14px",
    minWidth: 14, height: 14, borderRadius: 7,
    display: "flex", alignItems: "center", justifyContent: "center", padding: "0 3px",
  },
  panel: {
    position: "absolute", top: "calc(100% + 6px)", right: 0, width: 360,
    maxHeight: 480, overflowY: "auto", background: "var(--bg-panel)",
    border: "1px solid var(--border-primary)", borderRadius: 2,
    boxShadow: "0 8px 24px rgba(0,0,0,0.5)", zIndex: 1000, fontFamily: "var(--font-mono)",
  },
  panelHeader: {
    display: "flex", justifyContent: "space-between", alignItems: "center",
    padding: "8px 12px", borderBottom: "1px solid var(--border-subtle)",
  },
  title: {
    fontSize: 12, fontWeight: 600, color: "var(--text-primary)",
    textTransform: "uppercase", letterSpacing: "0.5px",
  },
  markAll: {
    background: "none", border: "none", color: "var(--accent)",
    fontSize: 10, cursor: "pointer", fontFamily: "var(--font-mono)", padding: 0,
  },
  empty: {
    padding: "16px 12px", color: "var(--text-dimmed)", fontSize: 11, textAlign: "center",
  },
  row: { padding: "8px 12px", borderBottom: "1px solid var(--border-subtle)" },
  rowInner: { display: "flex", alignItems: "flex-start", gap: 8 },
  dot: { width: 7, height: 7, borderRadius: "50%", flexShrink: 0, marginTop: 4 },
  body: { flex: 1, minWidth: 0 },
  msg: { fontSize: 11, color: "var(--text-primary)", lineHeight: "16px", wordBreak: "break-word" },
  time: { fontSize: 9, color: "var(--text-dimmed)", marginTop: 2 },
  actions: { display: "flex", gap: 8, marginTop: 4, marginLeft: 15 },
  actionBtn: {
    background: "none", border: "none", color: "var(--text-muted)",
    fontSize: 10, cursor: "pointer", fontFamily: "var(--font-mono)", padding: "2px 0",
  },
  tabs: {
    display: "flex", borderBottom: "1px solid var(--border-subtle)",
  },
  tab: {
    flex: 1, padding: "6px 0", textAlign: "center", fontSize: 10,
    fontFamily: "var(--font-mono)", fontWeight: 600, cursor: "pointer",
    background: "none", border: "none", color: "var(--text-muted)",
    textTransform: "uppercase", letterSpacing: "0.5px",
  },
  tabActive: {
    color: "var(--accent)", borderBottom: "2px solid var(--accent)",
  },
  kwSection: { padding: "8px 12px" },
  kwRow: {
    display: "flex", alignItems: "center", justifyContent: "space-between",
    padding: "4px 0", borderBottom: "1px solid var(--border-subtle)",
  },
  kwText: { fontSize: 11, color: "var(--text-primary)" },
  kwDelete: {
    background: "none", border: "none", color: "var(--text-muted)",
    fontSize: 10, cursor: "pointer", fontFamily: "var(--font-mono)", padding: "2px 4px",
  },
  kwInput: {
    display: "flex", gap: 6, marginTop: 8,
  },
  kwField: {
    flex: 1, background: "var(--bg-input)", border: "1px solid var(--border-subtle)",
    borderRadius: 2, padding: "4px 8px", fontSize: 11, color: "var(--text-primary)",
    fontFamily: "var(--font-mono)", outline: "none",
  },
  kwAdd: {
    background: "var(--accent)", border: "none", color: "#000",
    borderRadius: 2, padding: "4px 10px", fontSize: 10, cursor: "pointer",
    fontFamily: "var(--font-mono)", fontWeight: 600,
  },
  error: {
    margin: "8px 12px 0 12px",
    padding: "6px 8px",
    border: "1px solid rgba(255, 59, 59, 0.45)",
    background: "rgba(255, 59, 59, 0.08)",
    color: "var(--red)",
    fontSize: 10,
    borderRadius: 2,
  },
};

type Tab = "alerts" | "keywords";

export default function NotificationCenter() {
  const [unreadCount, setUnreadCount] = useState(0);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [keywords, setKeywords] = useState<Keyword[]>([]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [tab, setTab] = useState<Tab>("alerts");
  const [newKw, setNewKw] = useState("");
  const [alertsError, setAlertsError] = useState<string | null>(null);
  const [keywordsError, setKeywordsError] = useState<string | null>(null);
  const [unreadError, setUnreadError] = useState<string | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const refreshUnreadCount = useCallback(async () => {
    try {
      setUnreadCount(await fetchUnreadAlertCount());
      setUnreadError(null);
    } catch {
      setUnreadError("Failed to refresh unread count");
    }
  }, []);

  const loadAlerts = useCallback(async () => {
    setLoading(true);
    try {
      const data = await fetchAlerts(undefined, 50, "active");
      data.sort((a, b) => new Date(b.ts_utc).getTime() - new Date(a.ts_utc).getTime());
      setAlerts(data);
      setAlertsError(null);
    } catch {
      setAlertsError("Failed to load notifications");
    } finally { setLoading(false); }
  }, []);

  const loadKeywords = useCallback(async () => {
    try {
      setKeywords(await fetchKeywords());
      setKeywordsError(null);
    } catch {
      setKeywordsError("Failed to load keywords");
    }
  }, []);

  // Poll unread count every 30s
  useEffect(() => {
    refreshUnreadCount();
    const id = setInterval(refreshUnreadCount, 30000);
    return () => clearInterval(id);
  }, [refreshUnreadCount]);

  // Load data when dropdown opens
  useEffect(() => {
    if (open) {
      loadAlerts();
      loadKeywords();
    }
  }, [open, loadAlerts, loadKeywords]);

  // While open, refresh active alerts every 15s so state stays current.
  useEffect(() => {
    if (!open || tab !== "alerts") return;
    const id = setInterval(loadAlerts, 15000);
    return () => clearInterval(id);
  }, [open, tab, loadAlerts]);

  // Close on outside click
  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  const handleAction = useCallback(async (id: number, status: AlertStatus, snoozeHours?: number) => {
    try {
      await updateAlertStatus(id, status, snoozeHours);
      setAlerts((prev) => {
        if (status === "SNOOZED") {
          return prev.map((a) => (a.id === id ? { ...a, status: "SNOOZED" } : a));
        }
        return prev.filter((a) => a.id !== id);
      });
      setAlertsError(null);
      await refreshUnreadCount();
    } catch {
      setAlertsError("Failed to update notification");
    }
  }, [refreshUnreadCount]);

  const handleMarkAllRead = useCallback(async () => {
    const unread = alerts.filter((a) => a.status === "NEW");
    if (unread.length === 0) return;
    try {
      await Promise.all(unread.map((a) => updateAlertStatus(a.id, "READ")));
      setAlerts((prev) => prev.filter((a) => a.status !== "NEW"));
      setAlertsError(null);
      await refreshUnreadCount();
    } catch {
      setAlertsError("Failed to mark all as read");
    }
  }, [alerts, refreshUnreadCount]);

  const handleAddKeyword = useCallback(async () => {
    const kw = newKw.trim();
    if (!kw) return;
    try {
      await addKeyword(kw);
      setNewKw("");
      await loadKeywords();
      setKeywordsError(null);
      // A new keyword may produce new matches; refresh active alerts now.
      await loadAlerts();
      await refreshUnreadCount();
    } catch {
      setKeywordsError("Failed to add keyword");
    }
  }, [newKw, loadKeywords, loadAlerts, refreshUnreadCount]);

  const handleDeleteKeyword = useCallback(async (id: number) => {
    try {
      await deleteKeyword(id);
      setKeywords((prev) => prev.filter((k) => k.id !== id));
      setKeywordsError(null);
    } catch {
      setKeywordsError("Failed to delete keyword");
    }
  }, []);

  return (
    <div ref={containerRef} style={S.wrap}>
      {/* Bell button */}
      <button onClick={() => setOpen((v) => !v)} aria-label="Notifications" style={S.bell}>
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none"
          stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
          style={{ color: open ? "var(--text-primary)" : "var(--text-muted)" }}
          onMouseEnter={(e) => (e.currentTarget.style.color = "var(--text-primary)")}
          onMouseLeave={(e) => { if (!open) e.currentTarget.style.color = "var(--text-muted)"; }}
        >
          <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9" />
          <path d="M13.73 21a2 2 0 0 1-3.46 0" />
        </svg>
        {unreadCount > 0 && (
          <span style={S.badge}>{unreadCount > 99 ? "99+" : unreadCount}</span>
        )}
      </button>

      {/* Dropdown */}
      {open && (
        <div style={S.panel}>
          {/* Tabs */}
          <div style={S.tabs as React.CSSProperties}>
            <button
              style={{ ...S.tab, ...(tab === "alerts" ? S.tabActive : {}) } as React.CSSProperties}
              onClick={() => setTab("alerts")}
            >
              Alerts
            </button>
            <button
              style={{ ...S.tab, ...(tab === "keywords" ? S.tabActive : {}) } as React.CSSProperties}
              onClick={() => setTab("keywords")}
            >
              Keywords
            </button>
          </div>

          {/* Alerts tab */}
          {tab === "alerts" && (
            <>
              <div style={S.panelHeader}>
                <span style={S.title as React.CSSProperties}>Notifications</span>
                <button onClick={handleMarkAllRead} style={S.markAll}>Mark all read</button>
              </div>
              {(alertsError || unreadError) && (
                <div style={S.error}>{alertsError || unreadError}</div>
              )}

              {loading && alerts.length === 0 && <div style={S.empty}>Loading...</div>}
              {!loading && alerts.length === 0 && <div style={S.empty}>No active notifications</div>}

              {alerts.map((a) => (
                <div key={a.id} style={{
                  ...S.row,
                  background: a.status === "NEW" ? "var(--bg-panel-alt)" : "transparent",
                }}>
                  <div style={S.rowInner as React.CSSProperties}>
                    <span style={{ ...S.dot, background: severityColor(a.severity) }} />
                    <div style={S.body as React.CSSProperties}>
                      <div style={S.msg as React.CSSProperties}>{a.message}</div>
                      <div style={S.time}>{timeAgo(a.ts_utc)}</div>
                    </div>
                  </div>
                  <div style={S.actions as React.CSSProperties}>
                    {a.status === "NEW" && (
                      <button onClick={() => handleAction(a.id, "READ")} style={S.actionBtn}>
                        Read
                      </button>
                    )}
                    <button onClick={() => handleAction(a.id, "SNOOZED", 4)} style={S.actionBtn}>
                      Snooze 4h
                    </button>
                    <button onClick={() => handleAction(a.id, "DISMISSED")}
                      style={{ ...S.actionBtn, color: "var(--red)" }}>
                      Dismiss
                    </button>
                  </div>
                </div>
              ))}
            </>
          )}

          {/* Keywords tab */}
          {tab === "keywords" && (
            <div style={S.kwSection}>
              <div style={{ fontSize: 10, color: "var(--text-dimmed)", marginBottom: 8 }}>
                Add keywords to get notified when they appear in news headlines.
              </div>
              {keywordsError && <div style={S.error}>{keywordsError}</div>}

              {keywords.length === 0 && (
                <div style={S.empty}>No keywords set. Add one below.</div>
              )}

              {keywords.map((k) => (
                <div key={k.id} style={S.kwRow}>
                  <span style={S.kwText}>{k.keyword}</span>
                  <button
                    onClick={() => handleDeleteKeyword(k.id)}
                    style={{ ...S.kwDelete, color: "var(--red)" }}
                  >
                    x
                  </button>
                </div>
              ))}

              <div style={S.kwInput as React.CSSProperties}>
                <input
                  style={S.kwField}
                  placeholder="e.g. tariff, layoff, FDA..."
                  value={newKw}
                  onChange={(e) => setNewKw(e.target.value)}
                  onKeyDown={(e) => { if (e.key === "Enter") handleAddKeyword(); }}
                />
                <button onClick={handleAddKeyword} style={S.kwAdd}>Add</button>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
