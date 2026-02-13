"use client";

import { Fragment, useEffect, useRef, useState, useCallback, useMemo } from "react";
import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import {
  Position,
  ExposureResponse,
  AccountSummaryItem,
  DailyPnl,
  Execution,
  fetchPortfolio,
  fetchExposures,
  fetchAccountSummary,
  fetchDailyPnl,
  fetchExecutions,
  fetchAccounts,
  WS_URL,
} from "@/lib/api";
import {
  RiskSummary,
  RiskContributor,
  CorrelationPair,
  ClusterInfo,
  StressTests,
  MacroSummary,
  DataQualityPack,
  RiskMetadata,
  fetchRiskSummary,
  fetchRiskContributors,
  fetchCorrelationPairs,
  fetchClusters,
  fetchStressTests,
  fetchMacroOverview,
  fetchDataQuality,
  fetchRiskMetadata,
  triggerRiskRecompute,
} from "@/lib/risk-api";
import RiskSummaryPanel from "@/components/RiskSummaryPanel";
import RiskContributorsTable from "@/components/RiskContributorsTable";
import CorrelationPanel from "@/components/CorrelationPanel";
import ClustersPanel from "@/components/ClustersPanel";
import StressPanel from "@/components/StressPanel";
import MacroStrip from "@/components/MacroStrip";
import EventsPanel from "@/components/EventsPanel";
import LiveTape from "@/components/LiveTape";
import CalendarView from "@/components/CalendarView";
import TickerDesk from "@/components/TickerDesk";
import NotificationCenter from "@/components/NotificationCenter";
import DataQualityPanel from "@/components/DataQualityPanel";
import RiskMetadataPanel from "@/components/RiskMetadataPanel";
import AISearch from "@/components/AISearch";

// ---------------------------------------------------------------------------
// Stable color maps – keyed by name so colors never shift between methods
// ---------------------------------------------------------------------------

const SECTOR_COLORS: Record<string, string> = {
  "Information Technology": "#3b82f6",
  Financials: "#10b981",
  Materials: "#f59e0b",
  Industrials: "#f97316",
  "Communication Services": "#8b5cf6",
  "Health Care": "#06b6d4",
  "Consumer Discretionary": "#14b8a6",
  "Consumer Staples": "#84cc16",
  Energy: "#ef4444",
  Utilities: "#a78bfa",
  "Real Estate": "#fb923c",
  Cryptocurrency: "#eab308",
  ETF: "#64748b",
  SPAC: "#94a3b8",
  Unknown: "#334155",
};

const COUNTRY_COLORS: Record<string, string> = {
  US: "#3b82f6",
  Global: "#64748b",
  KR: "#ef4444",
  CA: "#10b981",
  AU: "#f59e0b",
  GB: "#8b5cf6",
  JP: "#06b6d4",
  DE: "#f97316",
  FR: "#14b8a6",
  CN: "#ef4444",
  HK: "#a78bfa",
  Unknown: "#334155",
};

const FALLBACK_COLORS = [
  "#3b82f6", "#10b981", "#0ea5e9", "#8b5cf6", "#14b8a6",
  "#f59e0b", "#6366f1", "#06b6d4", "#a78bfa", "#34d399",
  "#fb923c", "#818cf8",
];

let _fallbackIdx = 0;
function getColor(name: string, colorMap: Record<string, string>): string {
  if (colorMap[name]) return colorMap[name];
  const color = FALLBACK_COLORS[_fallbackIdx % FALLBACK_COLORS.length];
  colorMap[name] = color;
  _fallbackIdx++;
  return color;
}

// ---------------------------------------------------------------------------
// Balance tag display order & label mapping
// ---------------------------------------------------------------------------

const BALANCE_TAGS_ORDERED = [
  "EquityWithLoanValue",
  "TotalCashValue",
  "GrossPositionValue",
  "BuyingPower",
  "AvailableFunds",
  "ExcessLiquidity",
  "AccruedCash",
] as const;

const BALANCE_LABELS: Record<string, string> = {
  NetLiquidation: "Net Liquidation",
  EquityWithLoanValue: "Equity w/ Loan",
  TotalCashValue: "Total Cash",
  GrossPositionValue: "Gross Position",
  BuyingPower: "Buying Power",
  AvailableFunds: "Available Funds",
  ExcessLiquidity: "Excess Liquidity",
  AccruedCash: "Accrued Cash",
};

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

function fmtNumber(v: number | null | undefined, decimals = 2): string {
  if (v == null) return "\u2014";
  return v.toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

function fmtCurrency(v: number | null | undefined, decimals = 0): string {
  if (v == null) return "\u2014";
  const sign = v < 0 ? "-" : "";
  return sign + "$" + Math.abs(v).toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

function fmtPnl(v: number | null | undefined): string {
  if (v == null) return "\u2014";
  const abs = Math.abs(v);
  let formatted: string;
  if (abs >= 1_000_000) {
    formatted = (abs / 1_000_000).toFixed(1) + "M";
  } else if (abs >= 10_000) {
    formatted = (abs / 1_000).toFixed(1) + "k";
  } else {
    formatted = abs.toFixed(0);
  }
  return v === 0 ? formatted : v > 0 ? `+${formatted}` : `-${formatted}`;
}

function fmtPct(v: number | null | undefined): string {
  if (v == null) return "\u2014";
  const s = v >= 0 ? "+" : "";
  return `${s}${v.toFixed(1)}%`;
}

function fmtTimestampET(iso: string | null): string {
  if (!iso) return "\u2014";
  try {
    const d = new Date(iso);
    return d.toLocaleTimeString("en-US", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
      timeZone: "America/New_York",
    }) + " ET";
  } catch {
    return iso;
  }
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type WsStatus = "connected" | "disconnected" | "reconnecting";
type WeightingMethod = "market_value" | "cost_basis";
type SortDir = "asc" | "desc";
type TabName = "overview" | "risk" | "stress" | "macro" | "live" | "calendar" | "ticker" | "ai";

interface ColumnDef {
  key: string;
  label: string;
  align: "left" | "right";
  defaultWidth: number;
  sortable: boolean;
}

const COLUMNS: ColumnDef[] = [
  { key: "symbol", label: "Symbol", align: "left", defaultWidth: 110, sortable: true },
  { key: "position", label: "Qty", align: "right", defaultWidth: 80, sortable: true },
  { key: "avg_cost", label: "Avg Cost", align: "right", defaultWidth: 90, sortable: true },
  { key: "market_price", label: "Mkt Price", align: "right", defaultWidth: 90, sortable: true },
  { key: "pct_change", label: "% Chg", align: "right", defaultWidth: 75, sortable: true },
  { key: "market_value", label: "Mkt Value", align: "right", defaultWidth: 100, sortable: true },
  { key: "daily_pnl", label: "Daily P&L", align: "right", defaultWidth: 95, sortable: true },
  { key: "unrealized_pnl", label: "Unrlzd P&L", align: "right", defaultWidth: 95, sortable: true },
  { key: "realized_pnl", label: "Rlzd P&L", align: "right", defaultWidth: 85, sortable: true },
  { key: "weight", label: "Wt %", align: "right", defaultWidth: 60, sortable: true },
  { key: "sector", label: "Sector", align: "left", defaultWidth: 140, sortable: true },
  { key: "country", label: "Country", align: "left", defaultWidth: 70, sortable: true },
];

const ORDER_COLUMNS: ColumnDef[] = [
  { key: "exec_time", label: "Time", align: "left", defaultWidth: 90, sortable: false },
  { key: "symbol", label: "Symbol", align: "left", defaultWidth: 90, sortable: false },
  { key: "side", label: "Side", align: "left", defaultWidth: 60, sortable: false },
  { key: "order_type", label: "Type", align: "left", defaultWidth: 60, sortable: false },
  { key: "quantity", label: "Qty", align: "right", defaultWidth: 70, sortable: false },
  { key: "avg_fill_price", label: "Fill Price", align: "right", defaultWidth: 90, sortable: false },
  { key: "commission", label: "Comm", align: "right", defaultWidth: 70, sortable: false },
  { key: "status", label: "Status", align: "left", defaultWidth: 80, sortable: false },
];

// ---------------------------------------------------------------------------
// Custom tooltip for pie chart
// ---------------------------------------------------------------------------

interface PieTooltipProps {
  active?: boolean;
  payload?: Array<{
    name: string;
    value: number;
    payload: { name: string; weight: number; notional: number };
  }>;
}

function PieTooltipContent({ active, payload }: PieTooltipProps) {
  if (!active || !payload || payload.length === 0) return null;
  const entry = payload[0].payload;
  return (
    <div
      style={{
        background: "#111827",
        border: "1px solid #1e293b",
        borderRadius: 2,
        padding: "6px 10px",
        fontFamily: '"JetBrains Mono", "SF Mono", monospace',
        fontSize: 11,
      }}
    >
      <div style={{ fontWeight: 600, marginBottom: 3, color: "#e2e8f0" }}>
        {entry.name}
      </div>
      <div style={{ color: "#94a3b8" }}>
        Weight: {entry.weight.toFixed(1)}%
      </div>
      <div style={{ color: "#94a3b8" }}>
        Notional: ${fmtNumber(entry.notional)}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main dashboard component
// ---------------------------------------------------------------------------

const ACCOUNT_STORAGE_KEY = "trading.selectedAccount";

export default function DashboardPage() {
  // Tab navigation
  const [activeTab, setActiveTab] = useState<TabName>("overview");

  // Overview tab state
  const [positions, setPositions] = useState<Position[]>([]);
  const [exposures, setExposures] = useState<ExposureResponse | null>(null);
  const [accountSummary, setAccountSummary] = useState<AccountSummaryItem[]>([]);
  const [dailyPnl, setDailyPnl] = useState<DailyPnl | null>(null);
  const [weightingMethod, setWeightingMethod] = useState<WeightingMethod>("market_value");
  const [wsStatus, setWsStatus] = useState<WsStatus>("disconnected");
  const [lastUpdate, setLastUpdate] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [accounts, setAccounts] = useState<string[]>([]);
  const [selectedAccount, setSelectedAccount] = useState<string | null>(null);
  const [accountsLoaded, setAccountsLoaded] = useState(false);

  // Orders state
  const [executions, setExecutions] = useState<Execution[]>([]);
  const [positionsTab, setPositionsTab] = useState<"positions" | "orders">("positions");
  const [expandedOrderId, setExpandedOrderId] = useState<string | null>(null);

  // Risk tab state
  const [riskWindow, setRiskWindow] = useState(252);
  const [riskMethod, setRiskMethod] = useState("lw");
  const [riskSummary, setRiskSummary] = useState<RiskSummary | null>(null);
  const [riskContributors, setRiskContributors] = useState<RiskContributor[]>([]);
  const [correlationPairs, setCorrelationPairs] = useState<CorrelationPair[]>([]);
  const [clusters, setClusters] = useState<ClusterInfo[]>([]);
  const [dataQuality, setDataQuality] = useState<DataQualityPack | null>(null);
  const [riskMetadata, setRiskMetadata] = useState<RiskMetadata | null>(null);
  const [riskLoading, setRiskLoading] = useState(false);

  // Stress tab state
  const [stressTests, setStressTests] = useState<StressTests | null>(null);
  const [stressLoading, setStressLoading] = useState(false);

  // Track whether risk/stress data is stale due to position changes
  const riskStaleRef = useRef(false);
  const stressStaleRef = useRef(false);

  // Macro tab state
  const [macroData, setMacroData] = useState<MacroSummary | null>(null);
  const [macroLoading, setMacroLoading] = useState(false);

  // Sort state
  const [sortCol, setSortCol] = useState<string>("market_value");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  // Column widths
  const [colWidths, setColWidths] = useState<number[]>(
    COLUMNS.map((c) => c.defaultWidth)
  );

  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const reconnectDelay = useRef(1000);
  const pollingTimer = useRef<ReturnType<typeof setInterval> | null>(null);
  const weightingMethodRef = useRef<WeightingMethod>(weightingMethod);
  const activeTabRef = useRef<TabName>(activeTab);

  // Resize drag state
  const resizingCol = useRef<number | null>(null);
  const resizeStartX = useRef(0);
  const resizeStartW = useRef(0);

  useEffect(() => {
    weightingMethodRef.current = weightingMethod;
  }, [weightingMethod]);

  useEffect(() => {
    activeTabRef.current = activeTab;
  }, [activeTab]);

  useEffect(() => {
    if (selectedAccount) {
      localStorage.setItem(ACCOUNT_STORAGE_KEY, selectedAccount);
    }
  }, [selectedAccount]);

  useEffect(() => {
    let mounted = true;
    const loadAccounts = async () => {
      try {
        const list = await fetchAccounts();
        if (!mounted) return;
        setAccounts(list);
        const stored = localStorage.getItem(ACCOUNT_STORAGE_KEY);
        let nextAccount: string | null = null;
        if (stored && list.includes(stored)) {
          nextAccount = stored;
        } else if (list.length === 1) {
          nextAccount = list[0];
        } else if (list.length > 1) {
          nextAccount = list[0];
        }
        setSelectedAccount(nextAccount);
      } catch {
        // If account discovery fails, fall back to unfiltered mode.
      } finally {
        if (mounted) setAccountsLoaded(true);
      }
    };
    loadAccounts();
    return () => {
      mounted = false;
    };
  }, []);

  // ---- Data fetching ------------------------------------------------------

  const loadPortfolioAndExposures = useCallback(async (method?: WeightingMethod) => {
    try {
      const m = method ?? weightingMethodRef.current;
      const [pos, exp] = await Promise.all([
        fetchPortfolio(selectedAccount ?? undefined),
        fetchExposures(m, selectedAccount ?? undefined),
      ]);
      setPositions(pos);
      setExposures(exp);
      setLastUpdate(new Date().toISOString());
      setError(null);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(msg);
    }
  }, [selectedAccount]);

  const loadAccountSummary = useCallback(async () => {
    try {
      const [summary, pnl] = await Promise.all([
        fetchAccountSummary(selectedAccount ?? undefined),
        fetchDailyPnl(selectedAccount ?? undefined),
      ]);
      setAccountSummary(summary);
      setDailyPnl(pnl);
      setError(null);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(msg);
    }
  }, [selectedAccount]);

  const loadExecutions = useCallback(async () => {
    try {
      const execs = await fetchExecutions(selectedAccount ?? undefined);
      setExecutions(execs);
    } catch {
      // Silently degrade — orders are non-critical
    }
  }, [selectedAccount]);

  const loadAllData = useCallback(async () => {
    try {
      const [pos, exp, summary, pnl] = await Promise.all([
        fetchPortfolio(selectedAccount ?? undefined),
        fetchExposures(weightingMethodRef.current, selectedAccount ?? undefined),
        fetchAccountSummary(selectedAccount ?? undefined),
        fetchDailyPnl(selectedAccount ?? undefined),
      ]);
      setPositions(pos);
      setExposures(exp);
      setAccountSummary(summary);
      setDailyPnl(pnl);
      setLastUpdate(new Date().toISOString());
      setError(null);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(msg);
    } finally {
      setLoading(false);
    }
    // Fetch executions separately (non-blocking)
    loadExecutions();
  }, [loadExecutions, selectedAccount]);

  const loadRiskData = useCallback(async () => {
    setRiskLoading(true);
    try {
      // Core risk fetches (must succeed)
      const [summary, contributors, pairs, clusterData] = await Promise.all([
        fetchRiskSummary(riskWindow, riskMethod),
        fetchRiskContributors(riskWindow, riskMethod),
        fetchCorrelationPairs(riskWindow, 20),
        fetchClusters(riskWindow),
      ]);
      // Treat summary as null if it lacks required risk fields (no data computed yet)
      setRiskSummary(summary.vol_1d_pct != null ? summary : null);
      setRiskContributors(contributors);
      setCorrelationPairs(pairs);
      setClusters(clusterData);
      setError(null);

      // Phase 1.5 fetches (degrade gracefully if endpoints unavailable)
      try {
        const [quality, metadata] = await Promise.all([
          fetchDataQuality(riskWindow, riskMethod),
          fetchRiskMetadata(riskWindow, riskMethod),
        ]);
        setDataQuality(quality);
        setRiskMetadata(metadata);
      } catch {
        // Silently degrade — panels will show empty state
      }
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(msg);
    } finally {
      setRiskLoading(false);
    }
  }, [riskWindow, riskMethod]);

  const loadStressData = useCallback(async () => {
    setStressLoading(true);
    try {
      const tests = await fetchStressTests();
      setStressTests(tests);
      setError(null);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(msg);
    } finally {
      setStressLoading(false);
    }
  }, []);

  const loadMacroData = useCallback(async () => {
    setMacroLoading(true);
    try {
      const macro = await fetchMacroOverview();
      setMacroData(macro);
      setError(null);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(msg);
    } finally {
      setMacroLoading(false);
    }
  }, []);

  const handleRiskRecompute = useCallback(async () => {
    try {
      setRiskLoading(true);
      await triggerRiskRecompute();
      // Server recompute is async; poll after a delay to get fresh results
      const poll = async (attempts: number) => {
        if (attempts <= 0) {
          setRiskLoading(false);
          return;
        }
        await new Promise((r) => setTimeout(r, 3000));
        await loadRiskData();
      };
      await poll(5);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(msg);
      setRiskLoading(false);
    }
  }, [loadRiskData]);

  // ---- WebSocket connection with exponential backoff ----------------------

  const connectWs = useCallback(() => {
    if (wsRef.current) {
      wsRef.current.close();
    }

    setWsStatus("reconnecting");

    try {
      const accountParam = selectedAccount ? `?account=${encodeURIComponent(selectedAccount)}` : "";
      const ws = new WebSocket(`${WS_URL}/stream${accountParam}`);
      wsRef.current = ws;

      ws.onopen = () => {
        setWsStatus("connected");
        reconnectDelay.current = 1000;
        if (pollingTimer.current) {
          clearInterval(pollingTimer.current);
          pollingTimer.current = null;
        }
      };

      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          if (msg.type === "position" || msg.type === "portfolio_refresh") {
            loadPortfolioAndExposures();
            riskStaleRef.current = true;
            stressStaleRef.current = true;
          } else if (msg.type === "account_summary") {
            loadAccountSummary();
          } else if (msg.type === "executions") {
            loadExecutions();
          } else if (msg.type === "risk_updated") {
            riskStaleRef.current = true;
            stressStaleRef.current = true;
          } else if (msg.type === "data_updated") {
            loadPortfolioAndExposures();
            riskStaleRef.current = true;
            stressStaleRef.current = true;
          }
        } catch {
          loadPortfolioAndExposures();
        }
      };

      ws.onclose = () => {
        setWsStatus("disconnected");
        scheduleReconnect();
        if (!pollingTimer.current) {
          pollingTimer.current = setInterval(() => {
            loadAllData();
          }, 30000);
        }
      };

      ws.onerror = () => {};
    } catch {
      setWsStatus("disconnected");
      scheduleReconnect();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loadPortfolioAndExposures, loadAccountSummary, selectedAccount]);

  function scheduleReconnect() {
    if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
    const delay = reconnectDelay.current;
    reconnectTimer.current = setTimeout(() => {
      reconnectDelay.current = Math.min(delay * 2, 30000);
      connectWs();
    }, delay);
  }

  // ---- Lifecycle ----------------------------------------------------------

  useEffect(() => {
    if (!accountsLoaded) return;
    loadAllData();
    connectWs();

    return () => {
      if (wsRef.current) wsRef.current.close();
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
      if (pollingTimer.current) clearInterval(pollingTimer.current);
    };
  }, [loadAllData, connectWs, accountsLoaded]);

  // Load risk data when tab changes to risk
  useEffect(() => {
    if (activeTab === "risk" && (!riskSummary || riskStaleRef.current)) {
      riskStaleRef.current = false;
      loadRiskData();
    }
  }, [activeTab, riskSummary, loadRiskData]);

  // Load stress data when tab changes to stress
  useEffect(() => {
    if (activeTab === "stress" && (!stressTests || stressStaleRef.current)) {
      stressStaleRef.current = false;
      loadStressData();
    }
  }, [activeTab, stressTests, loadStressData]);

  // Load macro data when tab changes to macro
  useEffect(() => {
    if (activeTab === "macro" && !macroData) {
      loadMacroData();
    }
  }, [activeTab, macroData, loadMacroData]);

  // Reload risk data when window/method changes
  useEffect(() => {
    if (activeTab === "risk") {
      loadRiskData();
    }
  }, [riskWindow, riskMethod]); // eslint-disable-line react-hooks/exhaustive-deps

  // ---- Weighting method change --------------------------------------------

  const handleMethodChange = useCallback(async (method: WeightingMethod) => {
    setWeightingMethod(method);
    weightingMethodRef.current = method;
    try {
      const exp = await fetchExposures(method, selectedAccount ?? undefined);
      setExposures(exp);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(msg);
    }
  }, [selectedAccount]);

  // ---- Sort handler -------------------------------------------------------

  const handleSort = useCallback((col: string) => {
    setSortCol((prev) => {
      if (prev === col) {
        setSortDir((d) => (d === "asc" ? "desc" : "asc"));
        return prev;
      }
      setSortDir("desc");
      return col;
    });
  }, []);

  // ---- Column resize handlers ---------------------------------------------

  const handleResizeStart = useCallback(
    (e: React.MouseEvent, colIndex: number) => {
      e.preventDefault();
      e.stopPropagation();
      resizingCol.current = colIndex;
      resizeStartX.current = e.clientX;
      resizeStartW.current = colWidths[colIndex];

      const handleMove = (ev: MouseEvent) => {
        if (resizingCol.current == null) return;
        const diff = ev.clientX - resizeStartX.current;
        const newW = Math.max(40, resizeStartW.current + diff);
        setColWidths((prev) => {
          const next = [...prev];
          next[resizingCol.current!] = newW;
          return next;
        });
      };

      const handleUp = () => {
        resizingCol.current = null;
        document.removeEventListener("mousemove", handleMove);
        document.removeEventListener("mouseup", handleUp);
      };

      document.addEventListener("mousemove", handleMove);
      document.addEventListener("mouseup", handleUp);
    },
    [colWidths]
  );

  // ---- Derived data -------------------------------------------------------

  const grossExposure = exposures?.total_gross_exposure ?? null;

  // Sort chart data alphabetically so Recharts Cell order is stable across method changes
  const sortedSectors = useMemo(
    () => (exposures?.by_sector ?? []).slice().sort((a, b) => a.name.localeCompare(b.name)),
    [exposures],
  );
  const sortedCountries = useMemo(
    () => (exposures?.by_country ?? []).slice().sort((a, b) => a.name.localeCompare(b.name)),
    [exposures],
  );

  const sortedPositions = useMemo(() => {
    const rows = positions.map((p) => {
      const mktVal =
        p.market_value != null
          ? p.market_value
          : Math.abs(p.position * (p.avg_cost ?? 0));
      const pctChg =
        p.market_price != null && p.avg_cost != null && p.avg_cost !== 0
          ? ((p.market_price - p.avg_cost) / Math.abs(p.avg_cost)) * 100
          : null;
      const weight =
        grossExposure != null && grossExposure > 0
          ? (Math.abs(mktVal) / grossExposure) * 100
          : null;
      return { ...p, _mktVal: mktVal, _pctChg: pctChg, _weight: weight };
    });

    rows.sort((a, b) => {
      let av: number | string | null = 0;
      let bv: number | string | null = 0;

      switch (sortCol) {
        case "symbol":
          av = a.symbol;
          bv = b.symbol;
          break;
        case "position":
          av = a.position;
          bv = b.position;
          break;
        case "avg_cost":
          av = a.avg_cost;
          bv = b.avg_cost;
          break;
        case "market_price":
          av = a.market_price;
          bv = b.market_price;
          break;
        case "pct_change":
          av = a._pctChg;
          bv = b._pctChg;
          break;
        case "market_value":
          av = Math.abs(a._mktVal);
          bv = Math.abs(b._mktVal);
          break;
        case "daily_pnl":
          av = a.daily_pnl;
          bv = b.daily_pnl;
          break;
        case "unrealized_pnl":
          av = a.unrealized_pnl;
          bv = b.unrealized_pnl;
          break;
        case "realized_pnl":
          av = a.realized_pnl;
          bv = b.realized_pnl;
          break;
        case "weight":
          av = a._weight;
          bv = b._weight;
          break;
        case "sector":
          av = a.sector;
          bv = b.sector;
          break;
        case "country":
          av = a.country;
          bv = b.country;
          break;
      }

      // Nulls go last
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;

      let cmp: number;
      if (typeof av === "string" && typeof bv === "string") {
        cmp = av.localeCompare(bv);
      } else {
        cmp = (av as number) - (bv as number);
      }
      return sortDir === "asc" ? cmp : -cmp;
    });

    return rows;
  }, [positions, sortCol, sortDir, grossExposure]);

  // Build a lookup map for account summary tags
  const summaryMap = new Map<string, AccountSummaryItem>();
  for (const item of accountSummary) {
    // Prefer account-specific (U...) values over "All" aggregates
    const existing = summaryMap.get(item.tag);
    if (!existing || (item.account !== "All" && existing.account === "All")) {
      summaryMap.set(item.tag, item);
    }
  }

  const nlvItem = summaryMap.get("NetLiquidation");
  const nlvValue = nlvItem ? parseFloat(nlvItem.value) : null;
  const gpvItem = summaryMap.get("GrossPositionValue");
  const gpvValue = gpvItem ? parseFloat(gpvItem.value) : null;
  const leverageRatio = gpvValue != null && nlvValue != null && nlvValue !== 0
    ? gpvValue / nlvValue
    : null;

  // ---- Render: Loading state ----------------------------------------------

  if (loading) {
    return (
      <div className="dashboard">
        <div className="loading">LOADING PORTFOLIO DATA...</div>
      </div>
    );
  }

  // ---- Render: Dashboard --------------------------------------------------

  return (
    <div className="dashboard">
      {/* Header */}
      <header className="header">
        <div className="header-left">
          <span className="header-title">Trading Workstation</span>
          {accounts.length > 1 && (
            <div className="account-switcher">
              <span className="account-label">Account</span>
              <select
                className="account-select"
                value={selectedAccount ?? ""}
                onChange={(e) => setSelectedAccount(e.target.value || null)}
              >
                {selectedAccount == null && (
                  <option value="" disabled>
                    Select account
                  </option>
                )}
                {accounts.map((acct) => (
                  <option key={acct} value={acct}>
                    {acct}
                  </option>
                ))}
              </select>
            </div>
          )}
        </div>
        <div className="header-meta">
          <NotificationCenter />
          {lastUpdate && <span>{fmtTimestampET(lastUpdate)}</span>}
          <span className="status-indicator">
            <span className={`status-dot ${wsStatus === "connected" ? "connected" : "polling"}`} />
            {wsStatus === "connected" ? "Live" : "Polling"}
          </span>
        </div>
      </header>

      {error && <div className="error-banner">{error}</div>}

      {/* Tab Navigation */}
      <nav className="tab-nav">
        <button
          className={`tab-btn${activeTab === "overview" ? " active" : ""}`}
          onClick={() => setActiveTab("overview")}
        >
          Overview
        </button>
        <button
          className={`tab-btn${activeTab === "risk" ? " active" : ""}`}
          onClick={() => setActiveTab("risk")}
        >
          Risk
        </button>
        <button
          className={`tab-btn${activeTab === "stress" ? " active" : ""}`}
          onClick={() => setActiveTab("stress")}
        >
          Stress
        </button>
        <button
          className={`tab-btn${activeTab === "macro" ? " active" : ""}`}
          onClick={() => setActiveTab("macro")}
        >
          Macro
        </button>
        <span style={{ width: 1, height: 16, background: "var(--border-primary)", margin: "0 4px", alignSelf: "center" }} />
        <button
          className={`tab-btn${activeTab === "live" ? " active" : ""}`}
          onClick={() => setActiveTab("live")}
        >
          Live
        </button>
        <button
          className={`tab-btn${activeTab === "calendar" ? " active" : ""}`}
          onClick={() => setActiveTab("calendar")}
        >
          Calendar
        </button>
        <button
          className={`tab-btn${activeTab === "ticker" ? " active" : ""}`}
          onClick={() => setActiveTab("ticker")}
        >
          Ticker
        </button>
        <span style={{ width: 1, height: 16, background: "var(--border-primary)", margin: "0 4px", alignSelf: "center" }} />
        <button
          className={`tab-btn${activeTab === "ai" ? " active" : ""}`}
          onClick={() => setActiveTab("ai")}
        >
          AI Search
        </button>
      </nav>

      {/* Overview Tab */}
      {activeTab === "overview" && (
        <>
          {/* Top panels grid: Balances | Sector | Country */}
          <div className="panels-grid">
        {/* Balances / Margin panel with hero NLV */}
        <div className="panel">
          <div className="panel-header">Account</div>
          <div className="nlv-hero">
            <div className="nlv-label">Net Liquidation</div>
            <div className={`nlv-value${nlvValue != null && nlvValue < 0 ? " negative" : ""}`}>
              {nlvValue != null ? fmtCurrency(nlvValue) : "\u2014"}
            </div>
            {dailyPnl?.nlv_change != null && (
              <div className={`nlv-daily-change ${dailyPnl.nlv_change > 0 ? "positive" : dailyPnl.nlv_change < 0 ? "negative" : ""}`}>
                {dailyPnl.nlv_change > 0 ? "+" : ""}{fmtCurrency(dailyPnl.nlv_change)}
                {dailyPnl.nlv_change_pct != null && (
                  <span className="nlv-daily-pct">
                    {" "}({dailyPnl.nlv_change_pct > 0 ? "+" : ""}{dailyPnl.nlv_change_pct.toFixed(2)}%)
                  </span>
                )}
                {" "}today
              </div>
            )}
          </div>
          <div className="balances-grid">
            {BALANCE_TAGS_ORDERED.map((tag) => {
              const item = summaryMap.get(tag);
              const rawValue = item ? parseFloat(item.value) : null;
              const isNeg = rawValue != null && rawValue < 0;

              return (
                <div className="balance-item" key={tag}>
                  <span className="balance-label">
                    {BALANCE_LABELS[tag] || tag}
                  </span>
                  <span className={`balance-value${isNeg ? " negative" : ""}`}>
                    {rawValue != null ? fmtCurrency(rawValue) : "\u2014"}
                  </span>
                </div>
              );
            })}
            {leverageRatio != null && (
              <div className="balance-item">
                <span className="balance-label">Leverage</span>
                <span className="balance-value">{leverageRatio.toFixed(2)}x</span>
              </div>
            )}
          </div>
        </div>

        {/* Sector Allocation pie chart */}
        <div className="panel">
          <div className="panel-header">Sector Allocation</div>
          <div className="chart-container">
            {sortedSectors.length > 0 ? (
              <ResponsiveContainer width="100%" height={240}>
                <PieChart>
                  <Pie
                    data={sortedSectors}
                    dataKey="weight"
                    nameKey="name"
                    cx="50%"
                    cy="50%"
                    outerRadius={85}
                    innerRadius={40}
                    paddingAngle={1}
                    strokeWidth={0}
                  >
                    {sortedSectors.map((entry) => (
                      <Cell
                        key={entry.name}
                        fill={getColor(entry.name, SECTOR_COLORS)}
                      />
                    ))}
                  </Pie>
                  <Tooltip content={<PieTooltipContent />} />
                  <Legend
                    wrapperStyle={{ fontSize: 10, paddingTop: 4 }}
                    iconSize={8}
                  />
                </PieChart>
              </ResponsiveContainer>
            ) : (
              <div className="empty-state">No sector data</div>
            )}
          </div>
        </div>

        {/* Country Allocation pie chart */}
        <div className="panel">
          <div className="panel-header">Country Allocation</div>
          <div className="chart-container">
            {sortedCountries.length > 0 ? (
              <ResponsiveContainer width="100%" height={240}>
                <PieChart>
                  <Pie
                    data={sortedCountries}
                    dataKey="weight"
                    nameKey="name"
                    cx="50%"
                    cy="50%"
                    outerRadius={85}
                    innerRadius={40}
                    paddingAngle={1}
                    strokeWidth={0}
                  >
                    {sortedCountries.map((entry) => (
                      <Cell
                        key={entry.name}
                        fill={getColor(entry.name, COUNTRY_COLORS)}
                      />
                    ))}
                  </Pie>
                  <Tooltip content={<PieTooltipContent />} />
                  <Legend
                    wrapperStyle={{ fontSize: 10, paddingTop: 4 }}
                    iconSize={8}
                  />
                </PieChart>
              </ResponsiveContainer>
            ) : (
              <div className="empty-state">No country data</div>
            )}
          </div>
        </div>
      </div>

      {/* Shared method toggle */}
      <div className="method-toggle-row">
        <span className="method-toggle-label">Weighting:</span>
        <button
          className={`method-btn${weightingMethod === "market_value" ? " active" : ""}`}
          onClick={() => handleMethodChange("market_value")}
        >
          Mkt Value
        </button>
        <button
          className={`method-btn${weightingMethod === "cost_basis" ? " active" : ""}`}
          onClick={() => handleMethodChange("cost_basis")}
        >
          Cost Basis
        </button>
        <span className="method-toggle-exposure">
          Gross Exposure: {grossExposure != null ? fmtCurrency(grossExposure) : "\u2014"}
        </span>
      </div>

      {/* Positions / Orders panel */}
      <div className="positions-panel">
        <div className="positions-header">
          <div className="positions-mini-tabs">
            <button
              className={`mini-tab-btn${positionsTab === "positions" ? " active" : ""}`}
              onClick={() => setPositionsTab("positions")}
            >
              Positions ({sortedPositions.length})
            </button>
            <button
              className={`mini-tab-btn${positionsTab === "orders" ? " active" : ""}`}
              onClick={() => setPositionsTab("orders")}
            >
              Orders ({executions.length})
            </button>
          </div>
        </div>
        {positionsTab === "positions" && (
        <div className="table-container">
          <table style={{ tableLayout: "fixed" }}>
            <colgroup>
              {COLUMNS.map((col, i) => (
                <col key={col.key} style={{ width: colWidths[i] }} />
              ))}
            </colgroup>
            <thead>
              <tr>
                {COLUMNS.map((col, i) => (
                  <th
                    key={col.key}
                    className={col.align === "right" ? "cell-right" : ""}
                    onClick={() => col.sortable && handleSort(col.key)}
                    style={{ cursor: col.sortable ? "pointer" : "default", position: "relative" }}
                  >
                    {col.label}
                    {sortCol === col.key && (
                      <span className="sort-indicator">
                        {sortDir === "asc" ? " \u25B2" : " \u25BC"}
                      </span>
                    )}
                    <div
                      className="resize-handle"
                      onMouseDown={(e) => handleResizeStart(e, i)}
                    />
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sortedPositions.map((p) => {
                const dpnl = p.daily_pnl;
                const dpnlClass = dpnl != null && dpnl !== 0
                  ? dpnl > 0 ? "pnl-positive" : "pnl-negative"
                  : "";
                const pnl = p.unrealized_pnl;
                const pnlClass = pnl != null && pnl !== 0
                  ? pnl > 0 ? "pnl-positive" : "pnl-negative"
                  : "";
                const rpnl = p.realized_pnl;
                const rpnlClass = rpnl != null && rpnl !== 0
                  ? rpnl > 0 ? "pnl-positive" : "pnl-negative"
                  : "";
                const pctClass = p._pctChg != null && p._pctChg !== 0
                  ? p._pctChg > 0 ? "pnl-positive" : "pnl-negative"
                  : "";
                const ccySuffix = p.currency && p.currency !== "USD"
                  ? ` ${p.currency}`
                  : "";

                return (
                  <tr key={p.id}>
                    <td className="cell-symbol">
                      {p.symbol}
                      {ccySuffix && <span className="ccy-suffix">{ccySuffix}</span>}
                    </td>
                    <td className="cell-right">
                      {fmtNumber(p.position, p.position % 1 === 0 ? 0 : 4)}
                    </td>
                    <td className="cell-right">
                      {p.avg_cost != null ? fmtNumber(p.avg_cost) : "\u2014"}
                    </td>
                    <td className="cell-right">
                      {p.market_price != null ? fmtNumber(p.market_price) : "\u2014"}
                    </td>
                    <td className={`cell-right ${pctClass}`}>
                      {fmtPct(p._pctChg)}
                    </td>
                    <td className="cell-right">
                      {fmtNumber(p._mktVal, 0)}
                    </td>
                    <td className={`cell-right ${dpnlClass}`}>
                      {dpnl != null ? fmtPnl(dpnl) : "\u2014"}
                    </td>
                    <td className={`cell-right ${pnlClass}`}>
                      {pnl != null ? fmtPnl(pnl) : "\u2014"}
                    </td>
                    <td className={`cell-right ${rpnlClass}`}>
                      {rpnl != null ? fmtPnl(rpnl) : "\u2014"}
                    </td>
                    <td className="cell-right">
                      {p._weight != null ? p._weight.toFixed(1) : "\u2014"}
                    </td>
                    <td className="cell-dim">
                      {p.sector || "Unknown"}
                    </td>
                    <td className="cell-dim">
                      {p.country || "Global"}
                    </td>
                  </tr>
                );
              })}
              {sortedPositions.length === 0 && (
                <tr>
                  <td colSpan={COLUMNS.length} className="empty-state">
                    No positions found
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        )}

        {/* Orders table */}
        {positionsTab === "orders" && (
        <div className="table-container">
          <table style={{ tableLayout: "fixed" }}>
            <colgroup>
              {ORDER_COLUMNS.map((col) => (
                <col key={col.key} style={{ width: col.defaultWidth }} />
              ))}
            </colgroup>
            <thead>
              <tr>
                {ORDER_COLUMNS.map((col) => (
                  <th
                    key={col.key}
                    className={col.align === "right" ? "cell-right" : ""}
                  >
                    {col.label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {executions.map((ex) => {
                const sideClass = ex.side === "BUY" ? "pnl-positive" : "pnl-negative";
                const statusClass =
                  ex.status === "Filled" ? "pnl-positive" :
                  ex.status === "Cancelled" ? "pnl-negative" : "";
                const isExpanded = expandedOrderId === ex.exec_id;

                return (
                  <Fragment key={ex.exec_id}>
                    <tr
                      onClick={() => setExpandedOrderId(isExpanded ? null : ex.exec_id)}
                      style={{ cursor: "pointer" }}
                    >
                      <td>{fmtTimestampET(ex.exec_time)}</td>
                      <td className="cell-symbol">{ex.symbol}</td>
                      <td className={sideClass}>{ex.side}</td>
                      <td>{ex.order_type}</td>
                      <td className="cell-right">{fmtNumber(ex.quantity, 0)}</td>
                      <td className="cell-right">
                        {ex.avg_fill_price != null ? fmtNumber(ex.avg_fill_price) : "\u2014"}
                      </td>
                      <td className="cell-right">
                        {ex.commission != null ? fmtNumber(ex.commission, 2) : "\u2014"}
                      </td>
                      <td className={statusClass}>{ex.status}</td>
                    </tr>
                    {isExpanded && (
                      <tr className="order-detail-row">
                        <td colSpan={ORDER_COLUMNS.length}>
                          <div className="order-detail">
                            <div className="order-detail-grid">
                              <div className="order-detail-field">
                                <span className="order-detail-label">Order ID</span>
                                <span className="order-detail-value">{ex.exec_id}</span>
                              </div>
                              <div className="order-detail-field">
                                <span className="order-detail-label">Account</span>
                                <span className="order-detail-value">{ex.account}</span>
                              </div>
                              <div className="order-detail-field">
                                <span className="order-detail-label">Limit Price</span>
                                <span className="order-detail-value">
                                  {ex.lmt_price != null ? fmtNumber(ex.lmt_price) : "\u2014"}
                                </span>
                              </div>
                              <div className="order-detail-field">
                                <span className="order-detail-label">Filled Qty</span>
                                <span className="order-detail-value">
                                  {fmtNumber(ex.filled_qty, 0)} / {fmtNumber(ex.quantity, 0)}
                                </span>
                              </div>
                              {ex.order_ref && (
                                <div className="order-detail-field">
                                  <span className="order-detail-label">Ref</span>
                                  <span className="order-detail-value">{ex.order_ref}</span>
                                </div>
                              )}
                              <div className="order-detail-field">
                                <span className="order-detail-label">Sec Type</span>
                                <span className="order-detail-value">{ex.sec_type}</span>
                              </div>
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
              {executions.length === 0 && (
                <tr>
                  <td colSpan={ORDER_COLUMNS.length} className="empty-state">
                    No orders today
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        )}
      </div>
        </>
      )}

      {/* Risk Tab */}
      {activeTab === "risk" && (
        <div className="tab-content">
          <RiskSummaryPanel
            summary={riskSummary}
            window={riskWindow}
            method={riskMethod}
            onWindowChange={setRiskWindow}
            onMethodChange={setRiskMethod}
            onRecompute={handleRiskRecompute}
            loading={riskLoading}
          />
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1px", marginTop: "1px" }}>
            <CorrelationPanel pairs={correlationPairs} loading={riskLoading} />
            <ClustersPanel clusters={clusters} loading={riskLoading} />
          </div>
          <RiskContributorsTable
            contributors={riskContributors}
            loading={riskLoading}
          />
          <DataQualityPanel
            dataQuality={dataQuality}
            loading={riskLoading}
          />
          <RiskMetadataPanel
            metadata={riskMetadata}
            loading={riskLoading}
          />
        </div>
      )}

      {/* Stress Tab */}
      {activeTab === "stress" && (
        <div className="tab-content">
          <StressPanel stressTests={stressTests} loading={stressLoading} />
        </div>
      )}

      {/* Macro Tab */}
      {activeTab === "macro" && (
        <div className="tab-content">
          <MacroStrip macroData={macroData} loading={macroLoading} />
        </div>
      )}

      {/* Live News Tape */}
      {activeTab === "live" && (
        <div className="tab-content">
          <LiveTape />
        </div>
      )}

      {/* Calendar */}
      {activeTab === "calendar" && (
        <div className="tab-content">
          <CalendarView />
        </div>
      )}

      {/* Ticker Desk */}
      {activeTab === "ticker" && (
        <div className="tab-content">
          <TickerDesk />
        </div>
      )}

      {/* AI Search */}
      {activeTab === "ai" && (
        <AISearch />
      )}
    </div>
  );
}
