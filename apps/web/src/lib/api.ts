// ---------------------------------------------------------------------------
// API client – types & helpers for talking to the trading API server
// ---------------------------------------------------------------------------

export const API_URL =
  process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export const WS_URL =
  process.env.NEXT_PUBLIC_WS_URL || "ws://localhost:8000";

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

// ---- Fetchers -------------------------------------------------------------

export async function fetchPortfolio(): Promise<Position[]> {
  const res = await fetch(`${API_URL}/portfolio`);
  if (!res.ok) {
    throw new Error(`Failed to fetch portfolio: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export async function fetchExposures(method: string = "market_value"): Promise<ExposureResponse> {
  const res = await fetch(`${API_URL}/portfolio/exposures?method=${method}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch exposures: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export async function fetchAccountSummary(): Promise<AccountSummaryItem[]> {
  const res = await fetch(`${API_URL}/account/summary`);
  if (!res.ok) {
    throw new Error(`Failed to fetch account summary: ${res.status} ${res.statusText}`);
  }
  return res.json();
}
