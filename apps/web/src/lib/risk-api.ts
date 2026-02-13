// ---------------------------------------------------------------------------
// Risk API client – types & fetchers for risk endpoints
// ---------------------------------------------------------------------------

import { API_URL, fetchWithRetry } from './api';

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

export interface MacroTile {
  id: string;
  label: string;
  format: string;
  value: number;
  valueFormatted: string;
  unit: string;
  obs_date: string;
  fetched_at: string;
  realtime_start: string | null;
  realtime_end: string | null;
  changes: Record<string, number | null>;
  changeDirection: 'up' | 'down' | 'flat';
  revised: boolean;
  previousValue: number | null;
  description: string;
  category: string;
  recommendedChangeWindows: string[];
  dataQuality: 'daily' | 'release';
}

export interface MacroCategory {
  name: string;
  tiles: MacroTile[];
}

export interface MacroSummary {
  generated_at: string;
  categories: MacroCategory[];
}

// ---- Fetchers -------------------------------------------------------------

export async function fetchRiskSummary(
  window: number = 252,
  method: string = 'lw'
): Promise<RiskSummary> {
  const res = await fetchWithRetry(
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
  const res = await fetchWithRetry(
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
  const res = await fetchWithRetry(
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
  const res = await fetchWithRetry(
    `${API_URL}/risk/clusters?window=${window}`
  );
  if (!res.ok) {
    throw new Error(`Failed to fetch clusters: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export async function fetchStressTests(): Promise<StressTests> {
  const res = await fetchWithRetry(`${API_URL}/risk/stress`);
  if (!res.ok) {
    throw new Error(`Failed to fetch stress tests: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export async function fetchMacroOverview(): Promise<MacroSummary> {
  const res = await fetchWithRetry(`${API_URL}/macro/summary`);
  if (!res.ok) {
    throw new Error(`Failed to fetch macro overview: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export interface DataQualityPack {
  coverage: {
    "60d": CoverageMetrics;
    "252d": CoverageMetrics;
  };
  integrity: IntegrityMetrics;
  classification: ClassificationMetrics;
  fx: FxCoverageMetrics;
  beta_quality: BetaQualityMetrics;
  timestamps: DataTimestamps;
  warnings: DataWarning[];
  computed_at: string;
}

export interface CoverageMetrics {
  window: number;
  included_count: number;
  excluded_count: number;
  excluded_exposure_pct: number;
  top_excluded: ExcludedDetail[];
}

export interface ExcludedDetail {
  symbol: string;
  exposure: number;
  exposure_pct: number;
  reason: string;
}

export interface IntegrityMetrics {
  missing_price_exposure_pct: number;
  nan_rows_skipped: number;
  outlier_return_days: number;
  flat_streak_flags: number;
}

export interface ClassificationMetrics {
  unknown_sector_pct: number;
  unknown_country_pct: number;
}

export interface FxCoverageMetrics {
  non_usd_exposure_pct: number;
  fx_coverage_pct: number;
  fx_issues: Record<string, string>;
}

export interface BetaQualityMetrics {
  good_exposure_pct: number;
  weak_exposure_pct: number;
  invalid_exposure_pct: number;
}

export interface DataTimestamps {
  last_positions_update: string | null;
  last_prices_update: string | null;
  last_fx_update: string | null;
  last_risk_compute: string | null;
}

export interface DataWarning {
  level: "info" | "warning" | "error";
  message: string;
}

export interface RiskMetadata {
  window: number;
  effective_window: number;
  method: string;
  asof_date: string;
  computed_at: string;
  portfolio_hash: string;
  universe_hash: string;
  num_positions: number;
  num_valid_symbols: number;
  num_excluded: number;
  portfolio_value: number;
  excluded_symbols: string[];
  fx_adjusted_count: number;
  fx_flags: Record<string, string>;
  lib_versions: Record<string, string>;
}

export async function fetchDataQuality(window: number = 252, method: string = 'lw'): Promise<DataQualityPack> {
  const res = await fetchWithRetry(`${API_URL}/risk/data-quality?window=${window}&method=${method}`);
  if (!res.ok) throw new Error(`Failed to fetch data quality: ${res.status} ${res.statusText}`);
  return res.json();
}

export async function fetchRiskMetadata(window: number = 252, method: string = 'lw'): Promise<RiskMetadata> {
  const res = await fetchWithRetry(`${API_URL}/risk/metadata?window=${window}&method=${method}`);
  if (!res.ok) throw new Error(`Failed to fetch risk metadata: ${res.status} ${res.statusText}`);
  return res.json();
}

export async function triggerRiskRecompute(): Promise<void> {
  const res = await fetchWithRetry(`${API_URL}/risk/recompute`, { method: 'POST' });
  if (!res.ok) {
    throw new Error(`Failed to trigger risk recompute: ${res.status} ${res.statusText}`);
  }
}
