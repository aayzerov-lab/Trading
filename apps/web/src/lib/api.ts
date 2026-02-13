// ---------------------------------------------------------------------------
// API client – types & helpers for talking to the trading API server
// ---------------------------------------------------------------------------

export const API_URL =
  process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export const WS_URL =
  process.env.NEXT_PUBLIC_WS_URL || "ws://localhost:8000";

function buildQuery(params: Record<string, string | undefined>): string {
  const qs = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value) qs.set(key, value);
  }
  const s = qs.toString();
  return s ? `?${s}` : "";
}

// ---------------------------------------------------------------------------
// Fetch with timeout + auto-retry (handles cold-start on Render/serverless)
// ---------------------------------------------------------------------------

const DEFAULT_TIMEOUT_MS = 20_000;
const RETRY_DELAY_MS = 2_000;
const MAX_RETRIES = 3;

export async function fetchWithRetry(
  url: string,
  init?: RequestInit & { timeoutMs?: number; retries?: number }
): Promise<Response> {
  const { timeoutMs = DEFAULT_TIMEOUT_MS, retries = MAX_RETRIES, ...fetchInit } = init ?? {};
  let lastError: unknown;
  for (let attempt = 0; attempt <= retries; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, { ...fetchInit, signal: controller.signal });
      clearTimeout(timer);
      return res;
    } catch (err: unknown) {
      clearTimeout(timer);
      lastError = err;
      if (attempt < retries) {
        await new Promise((r) => setTimeout(r, RETRY_DELAY_MS));
      }
    }
  }
  throw lastError;
}

// ---- Domain types ---------------------------------------------------------

export interface Position {
  id: number;
  account: string;
  conid: number | null;
  symbol: string;
  sec_type: string;
  currency: string;
  exchange: string | null;
  position: number;
  avg_cost: number | null;
  market_price: number | null;
  market_value: number | null;
  unrealized_pnl: number | null;
  realized_pnl: number | null;
  daily_pnl: number | null;
  sector: string;
  country: string;
  ib_industry: string | null;
  ib_category: string | null;
  ib_subcategory: string | null;
  updated_at: string;
}

export interface Exposure {
  name: string;
  weight: number;
  notional: number;
  symbols: string[];
}

export interface ExposureResponse {
  by_sector: Exposure[];
  by_country: Exposure[];
  weighting_method: string;
  total_gross_exposure: number;
}

export interface AccountSummaryItem {
  account: string;
  tag: string;
  value: string;
  currency: string | null;
  updated_at: string;
}

export interface DailyPnl {
  nlv_current: number | null;
  nlv_change: number | null;
  nlv_change_pct: number | null;
}

export interface Execution {
  id: number;
  exec_id: string;
  account: string;
  conid: number | null;
  symbol: string;
  sec_type: string;
  currency: string;
  exchange: string | null;
  side: string;
  order_type: string;
  quantity: number;
  filled_qty: number;
  avg_fill_price: number | null;
  lmt_price: number | null;
  commission: number | null;
  status: string;
  order_ref: string | null;
  exec_time: string;
  created_at: string;
}

// ---- Fetchers -------------------------------------------------------------

export async function fetchPortfolio(account?: string): Promise<Position[]> {
  const res = await fetchWithRetry(`${API_URL}/portfolio${buildQuery({ account })}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch portfolio: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export async function fetchExposures(
  method: string = "market_value",
  account?: string
): Promise<ExposureResponse> {
  const res = await fetch(
    `${API_URL}/portfolio/exposures${buildQuery({ method, account })}`
  );
  if (!res.ok) {
    throw new Error(`Failed to fetch exposures: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export async function fetchAccountSummary(account?: string): Promise<AccountSummaryItem[]> {
  const res = await fetchWithRetry(`${API_URL}/account/summary${buildQuery({ account })}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch account summary: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export async function fetchDailyPnl(account?: string): Promise<DailyPnl> {
  const res = await fetchWithRetry(`${API_URL}/account/daily-pnl${buildQuery({ account })}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch daily P&L: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export async function fetchExecutions(account?: string): Promise<Execution[]> {
  const res = await fetchWithRetry(`${API_URL}/executions${buildQuery({ account })}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch executions: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export async function fetchAccounts(): Promise<string[]> {
  const res = await fetchWithRetry(`${API_URL}/accounts`);
  if (!res.ok) {
    throw new Error(`Failed to fetch accounts: ${res.status} ${res.statusText}`);
  }
  return res.json();
}
