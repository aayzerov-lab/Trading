// ---------------------------------------------------------------------------
// Risk API client – types & fetchers for risk endpoints
// ---------------------------------------------------------------------------

import { API_URL } from './api';

// ---- Types ----------------------------------------------------------------

export interface RiskSummary {
  vol_1d: number;
  vol_1d_pct: number;
  vol_5d: number;
  vol_5d_pct: number;
  var_95_1d: number;
  var_95_1d_pct: number;
  es_95_1d: number;
  es_95_1d_pct: number;
  var_95_5d: number;
  es_95_5d: number;
  top_5_concentration_pct: number;
  hhi: number;
  top_5_names: string[];
  num_positions: number;
  portfolio_value: number;
  window: number;
  method: string;
  asof_date: string;
}

export interface RiskContributor {
  symbol: string;
  weight_pct: number;
  mcr: number;
  ccr: number;
  ccr_pct: number;
  standalone_vol_ann: number;
}

export interface CorrelationPair {
  symbol_a: string;
  symbol_b: string;
  correlation: number;
}

export interface ClusterInfo {
  cluster_id: number;
  members: string[];
  size: number;
  avg_intra_corr: number;
  gross_exposure_pct: number;
  net_exposure_pct: number;
}

export interface StressContributor {
  symbol: string;
  return_pct: number;
  pnl_contribution: number;
  weight_pct: number;
}

export interface StressResult {
  scenario: string;
  period?: string;
  portfolio_return_pct: number;
  portfolio_pnl: number;
  top_contributors: StressContributor[];
}

export interface StressTests {
  historical: Record<string, StressResult>;
  factor: Record<string, StressResult>;
  computed_at: string;
}

export interface MacroIndicator {
  series_id: string;
  name: string;
  latest_value: number;
  latest_date: string;
  change_1m: number | null;
  change_3m: number | null;
  direction: 'up' | 'down' | 'flat';
  unit: string;
}

export interface MacroOverview {
  indicators: MacroIndicator[];
  computed_at: string;
}

// ---- Fetchers -------------------------------------------------------------

export async function fetchRiskSummary(
  window: number = 252,
  method: string = 'lw'
): Promise<RiskSummary> {
  const res = await fetch(
    `${API_URL}/risk/summary?window=${window}&method=${method}`
  );
  if (!res.ok) {
    throw new Error(`Failed to fetch risk summary: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export async function fetchRiskContributors(
  window: number = 252,
  method: string = 'lw'
): Promise<RiskContributor[]> {
  const res = await fetch(
    `${API_URL}/risk/contributors?window=${window}&method=${method}`
  );
  if (!res.ok) {
    throw new Error(`Failed to fetch risk contributors: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export async function fetchCorrelationPairs(
  window: number = 252,
  n: number = 20
): Promise<CorrelationPair[]> {
  const res = await fetch(
    `${API_URL}/risk/correlation/pairs?window=${window}&n=${n}`
  );
  if (!res.ok) {
    throw new Error(`Failed to fetch correlation pairs: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export async function fetchClusters(
  window: number = 252
): Promise<ClusterInfo[]> {
  const res = await fetch(
    `${API_URL}/risk/clusters?window=${window}`
  );
  if (!res.ok) {
    throw new Error(`Failed to fetch clusters: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export async function fetchStressTests(): Promise<StressTests> {
  const res = await fetch(`${API_URL}/risk/stress`);
  if (!res.ok) {
    throw new Error(`Failed to fetch stress tests: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export async function fetchMacroOverview(): Promise<MacroOverview> {
  const res = await fetch(`${API_URL}/macro/overview`);
  if (!res.ok) {
    throw new Error(`Failed to fetch macro overview: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export async function triggerRiskRecompute(): Promise<void> {
  const res = await fetch(`${API_URL}/risk/recompute`, { method: 'POST' });
  if (!res.ok) {
    throw new Error(`Failed to trigger risk recompute: ${res.status} ${res.statusText}`);
  }
}
