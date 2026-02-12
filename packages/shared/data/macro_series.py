"""Config-driven macro series definitions for the Macro tab."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass(frozen=True)
class MacroSeriesConfig:
    id: str
    label: str
    category: str
    format: str
    recommended_change_windows: List[str]
    refresh_policy: str
    description: str
    fred_series_id: Optional[str] = None
    series_ids: Optional[List[str]] = None
    computed: Optional[str] = None
    units_override: Optional[str] = None
    frequency_override: Optional[str] = None
    aggregation_method: Optional[str] = None
    display: bool = True


MACRO_CATEGORIES_ORDER = [
    "Rates",
    "Inflation Expectations",
    "Real Yields",
    "Inflation",
    "Labor",
    "Growth",
    "Housing",
    "Credit/Liquidity",
]


MACRO_SERIES: List[MacroSeriesConfig] = [
    # Rates/Policy
    MacroSeriesConfig(
        id="dgs2",
        label="2Y Treasury",
        category="Rates",
        format="percent",
        recommended_change_windows=["1W", "1M", "3M", "1Y"],
        refresh_policy="intraday",
        description="2-Year Treasury Constant Maturity Rate",
        fred_series_id="DGS2",
    ),
    MacroSeriesConfig(
        id="dgs10",
        label="10Y Treasury",
        category="Rates",
        format="percent",
        recommended_change_windows=["1W", "1M", "3M", "1Y"],
        refresh_policy="intraday",
        description="10-Year Treasury Constant Maturity Rate",
        fred_series_id="DGS10",
    ),
    MacroSeriesConfig(
        id="dgs30",
        label="30Y Treasury",
        category="Rates",
        format="percent",
        recommended_change_windows=["1W", "1M", "3M", "1Y"],
        refresh_policy="intraday",
        description="30-Year Treasury Constant Maturity Rate",
        fred_series_id="DGS30",
    ),
    MacroSeriesConfig(
        id="dgs3mo",
        label="3M Treasury",
        category="Rates",
        format="percent",
        recommended_change_windows=["1W", "1M", "3M", "1Y"],
        refresh_policy="intraday",
        description="3-Month Treasury Bill Rate",
        fred_series_id="DGS3MO",
        display=False,
    ),
    MacroSeriesConfig(
        id="effr",
        label="Fed Funds Effective",
        category="Rates",
        format="percent",
        recommended_change_windows=["1W", "1M", "3M", "1Y"],
        refresh_policy="intraday",
        description="Effective Federal Funds Rate",
        fred_series_id="EFFR",
    ),
    MacroSeriesConfig(
        id="sofr",
        label="SOFR",
        category="Rates",
        format="percent",
        recommended_change_windows=["1W", "1M", "3M", "1Y"],
        refresh_policy="intraday",
        description="Secured Overnight Financing Rate",
        fred_series_id="SOFR",
    ),
    # Spreads/Inflation expectations
    MacroSeriesConfig(
        id="spread_10y_2y",
        label="10Y-2Y Spread",
        category="Inflation Expectations",
        format="bp",
        recommended_change_windows=["1W", "1M", "3M", "1Y"],
        refresh_policy="intraday",
        description="10Y Treasury minus 2Y Treasury (basis points)",
        series_ids=["DGS10", "DGS2"],
        computed="spread",
    ),
    MacroSeriesConfig(
        id="spread_10y_3m",
        label="10Y-3M Spread",
        category="Inflation Expectations",
        format="bp",
        recommended_change_windows=["1W", "1M", "3M", "1Y"],
        refresh_policy="intraday",
        description="10Y Treasury minus 3M Treasury (basis points)",
        series_ids=["DGS10", "DGS3MO"],
        computed="spread",
    ),
    MacroSeriesConfig(
        id="t10yie",
        label="10Y Breakeven",
        category="Inflation Expectations",
        format="percent",
        recommended_change_windows=["1W", "1M", "3M", "1Y"],
        refresh_policy="daily",
        description="10-Year Breakeven Inflation Rate",
        fred_series_id="T10YIE",
    ),
    MacroSeriesConfig(
        id="t5yie",
        label="5Y Breakeven",
        category="Inflation Expectations",
        format="percent",
        recommended_change_windows=["1W", "1M", "3M", "1Y"],
        refresh_policy="daily",
        description="5-Year Breakeven Inflation Rate",
        fred_series_id="T5YIE",
    ),
    MacroSeriesConfig(
        id="t5yifr",
        label="5Y5Y Inflation",
        category="Inflation Expectations",
        format="percent",
        recommended_change_windows=["1W", "1M", "3M", "1Y"],
        refresh_policy="daily",
        description="5-Year, 5-Year Forward Inflation Compensation Rate",
        fred_series_id="T5YIFR",
    ),
    # Real yields (direct)
    MacroSeriesConfig(
        id="dfii10",
        label="10Y Real Yield",
        category="Real Yields",
        format="percent",
        recommended_change_windows=["1W", "1M", "3M", "1Y"],
        refresh_policy="daily",
        description="10-Year Treasury Inflation-Indexed Security, Constant Maturity",
        fred_series_id="DFII10",
    ),
    MacroSeriesConfig(
        id="dfii5",
        label="5Y Real Yield",
        category="Real Yields",
        format="percent",
        recommended_change_windows=["1W", "1M", "3M", "1Y"],
        refresh_policy="daily",
        description="5-Year Treasury Inflation-Indexed Security, Constant Maturity",
        fred_series_id="DFII5",
    ),
    # Inflation
    MacroSeriesConfig(
        id="cpiaucsl",
        label="CPI Headline",
        category="Inflation",
        format="index",
        recommended_change_windows=["1M", "3M", "1Y"],
        refresh_policy="monthly",
        description="Consumer Price Index for All Urban Consumers: All Items",
        fred_series_id="CPIAUCSL",
    ),
    MacroSeriesConfig(
        id="cpilfesl",
        label="CPI Core",
        category="Inflation",
        format="index",
        recommended_change_windows=["1M", "3M", "1Y"],
        refresh_policy="monthly",
        description="Consumer Price Index for All Urban Consumers: All Items Less Food and Energy",
        fred_series_id="CPILFESL",
    ),
    MacroSeriesConfig(
        id="pcepi",
        label="PCE Headline",
        category="Inflation",
        format="index",
        recommended_change_windows=["1M", "3M", "1Y"],
        refresh_policy="monthly",
        description="Personal Consumption Expenditures: Chain-type Price Index",
        fred_series_id="PCEPI",
    ),
    MacroSeriesConfig(
        id="pcepilfe",
        label="PCE Core",
        category="Inflation",
        format="index",
        recommended_change_windows=["1M", "3M", "1Y"],
        refresh_policy="monthly",
        description="Personal Consumption Expenditures Excluding Food and Energy",
        fred_series_id="PCEPILFE",
    ),
    # Labor
    MacroSeriesConfig(
        id="unrate",
        label="Unemployment Rate",
        category="Labor",
        format="percent",
        recommended_change_windows=["1M", "3M", "1Y"],
        refresh_policy="monthly",
        description="Unemployment Rate",
        fred_series_id="UNRATE",
    ),
    MacroSeriesConfig(
        id="payems",
        label="Nonfarm Payrolls",
        category="Labor",
        format="index",
        recommended_change_windows=["1M", "3M", "1Y"],
        refresh_policy="monthly",
        description="All Employees: Total Nonfarm Payrolls",
        fred_series_id="PAYEMS",
    ),
    MacroSeriesConfig(
        id="icsa",
        label="Initial Claims",
        category="Labor",
        format="index",
        recommended_change_windows=["1W", "1M", "3M", "1Y"],
        refresh_policy="weekly",
        description="Initial Claims",
        fred_series_id="ICSA",
    ),
    MacroSeriesConfig(
        id="civpart",
        label="Participation Rate",
        category="Labor",
        format="percent",
        recommended_change_windows=["1M", "3M", "1Y"],
        refresh_policy="monthly",
        description="Labor Force Participation Rate",
        fred_series_id="CIVPART",
    ),
    # Growth/Activity
    MacroSeriesConfig(
        id="indpro",
        label="Industrial Production",
        category="Growth",
        format="index",
        recommended_change_windows=["1M", "3M", "1Y"],
        refresh_policy="monthly",
        description="Industrial Production Index",
        fred_series_id="INDPRO",
    ),
    MacroSeriesConfig(
        id="tcu",
        label="Capacity Utilization",
        category="Growth",
        format="percent",
        recommended_change_windows=["1M", "3M", "1Y"],
        refresh_policy="monthly",
        description="Capacity Utilization: Total Industry",
        fred_series_id="TCU",
    ),
    MacroSeriesConfig(
        id="rsafs",
        label="Retail Sales",
        category="Growth",
        format="currency",
        recommended_change_windows=["1M", "3M", "1Y"],
        refresh_policy="monthly",
        description="Advance Retail Sales: Retail Trade",
        fred_series_id="RSAFS",
    ),
    # Housing
    MacroSeriesConfig(
        id="houst",
        label="Housing Starts",
        category="Housing",
        format="index",
        recommended_change_windows=["1M", "3M", "1Y"],
        refresh_policy="monthly",
        description="Housing Starts: Total",
        fred_series_id="HOUST",
    ),
    MacroSeriesConfig(
        id="permit",
        label="Building Permits",
        category="Housing",
        format="index",
        recommended_change_windows=["1M", "3M", "1Y"],
        refresh_policy="monthly",
        description="Building Permits: Total",
        fred_series_id="PERMIT",
    ),
    MacroSeriesConfig(
        id="mortgage30us",
        label="30Y Mortgage Rate",
        category="Housing",
        format="percent",
        recommended_change_windows=["1W", "1M", "3M", "1Y"],
        refresh_policy="weekly",
        description="30-Year Fixed Rate Mortgage Average",
        fred_series_id="MORTGAGE30US",
    ),
    # Credit/Liquidity
    MacroSeriesConfig(
        id="baa",
        label="BAA Corporate Yield",
        category="Credit/Liquidity",
        format="percent",
        recommended_change_windows=["1W", "1M", "3M", "1Y"],
        refresh_policy="daily",
        description="Moody's Seasoned Baa Corporate Bond Yield",
        fred_series_id="BAA",
    ),
    MacroSeriesConfig(
        id="spread_baa_10y",
        label="BAA-10Y Spread",
        category="Credit/Liquidity",
        format="bp",
        recommended_change_windows=["1W", "1M", "3M", "1Y"],
        refresh_policy="daily",
        description="Baa corporate yield minus 10Y Treasury (basis points)",
        series_ids=["BAA", "DGS10"],
        computed="spread",
    ),
]
