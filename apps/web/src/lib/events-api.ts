// ---------------------------------------------------------------------------
// Events & Alerts API client – types & fetchers for the events system
// ---------------------------------------------------------------------------

import { API_URL } from './api';

// ---- Types ----------------------------------------------------------------

export type EventType = 'SEC_FILING' | 'MACRO_SCHEDULE' | 'RSS_NEWS' | 'OTHER';
export type EventStatus = 'NEW' | 'ACKED' | 'DISMISSED';
export type AlertStatus = 'NEW' | 'READ' | 'SNOOZED' | 'DISMISSED';

export interface Event {
  id: string;
  ts_utc: string;
  scheduled_for_utc: string | null;
  type: EventType;
  tickers: string[] | null;
  title: string;
  source_name: string | null;
  source_url: string | null;
  raw_text_snippet: string | null;
  severity_score: number;
  reason_codes: string[] | null;
  llm_summary: string | null;
  status: EventStatus;
  metadata_json: Record<string, unknown> | null;
  created_at_utc: string;
  updated_at_utc: string;
}

export interface Alert {
  id: number;
  ts_utc: string;
  type: string;
  message: string;
  severity: number;
  related_event_id: string | null;
  status: AlertStatus;
  snoozed_until: string | null;
  created_at_utc: string;
}

export interface EventStats {
  total: number;
  by_type: Record<string, number>;
  by_status: Record<string, number>;
  high_priority: number;
}

// ---- Helpers --------------------------------------------------------------

/** The API stores tickers / reason_codes as JSON text.  Parse them into arrays. */
function normaliseEvent(raw: Record<string, unknown>): Event {
  const e = raw as unknown as Event;
  if (typeof e.tickers === 'string') {
    try { (e as any).tickers = JSON.parse(e.tickers as unknown as string); } catch { (e as any).tickers = null; }
  }
  if (typeof e.reason_codes === 'string') {
    try { (e as any).reason_codes = JSON.parse(e.reason_codes as unknown as string); } catch { (e as any).reason_codes = null; }
  }
  return e;
}

// ---- Fetchers -------------------------------------------------------------

export async function fetchEvents(params?: {
  type?: EventType;
  ticker?: string;
  days?: number;
  status?: EventStatus;
  limit?: number;
  offset?: number;
}): Promise<Event[]> {
  const sp = new URLSearchParams();
  if (params?.type) sp.set('type', params.type);
  if (params?.ticker) sp.set('ticker', params.ticker);
  if (params?.days) sp.set('days', String(params.days));
  if (params?.status) sp.set('status', params.status);
  if (params?.limit) sp.set('limit', String(params.limit));
  if (params?.offset) sp.set('offset', String(params.offset));
  const qs = sp.toString();
  const res = await fetch(`${API_URL}/events${qs ? '?' + qs : ''}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch events: ${res.status} ${res.statusText}`);
  }
  const rows: Record<string, unknown>[] = await res.json();
  return rows.map(normaliseEvent);
}

export async function fetchHighPriorityEvents(limit: number = 20): Promise<Event[]> {
  const res = await fetch(`${API_URL}/events/high-priority?limit=${limit}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch high-priority events: ${res.status} ${res.statusText}`);
  }
  const rows: Record<string, unknown>[] = await res.json();
  return rows.map(normaliseEvent);
}

export async function updateEventStatus(id: string, status: EventStatus): Promise<void> {
  const res = await fetch(`${API_URL}/events/${id}/status`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ status }),
  });
  if (!res.ok) {
    throw new Error(`Failed to update event status: ${res.status} ${res.statusText}`);
  }
}

export async function fetchEventStats(): Promise<EventStats> {
  const res = await fetch(`${API_URL}/events/stats`);
  if (!res.ok) {
    throw new Error(`Failed to fetch event stats: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export async function fetchAlerts(status?: AlertStatus, limit: number = 50): Promise<Alert[]> {
  const sp = new URLSearchParams();
  if (status) sp.set('status', status);
  sp.set('limit', String(limit));
  const qs = sp.toString();
  const res = await fetch(`${API_URL}/events/alerts${qs ? '?' + qs : ''}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch alerts: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export async function fetchUnreadAlertCount(): Promise<number> {
  const res = await fetch(`${API_URL}/events/alerts/unread-count`);
  if (!res.ok) {
    throw new Error(`Failed to fetch unread alert count: ${res.status} ${res.statusText}`);
  }
  const data = await res.json();
  return data.count;
}

export async function updateAlertStatus(
  id: number,
  status: AlertStatus,
  snoozeHours?: number
): Promise<void> {
  const body: Record<string, unknown> = { status };
  if (snoozeHours !== undefined) {
    body.snooze_hours = snoozeHours;
  }
  const res = await fetch(`${API_URL}/events/alerts/${id}/status`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw new Error(`Failed to update alert status: ${res.status} ${res.statusText}`);
  }
}

export async function seedEvents(): Promise<{ seeded: boolean; events: number; alerts: number }> {
  const res = await fetch(`${API_URL}/events/seed`, { method: 'POST' });
  if (!res.ok) {
    throw new Error(`Failed to seed events: ${res.status} ${res.statusText}`);
  }
  return res.json();
}
