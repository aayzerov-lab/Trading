"""Curated RSS feed ingestion for the Trading Workstation.

Fetches articles from financial news RSS feeds, matches them against
portfolio tickers, and stores relevant articles as events in PostgreSQL.
Uses stdlib xml.etree.ElementTree for XML parsing and httpx for async HTTP.
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from shared.db.engine import get_shared_engine

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Curated RSS feed sources (all free, public feeds)
# ---------------------------------------------------------------------------

CURATED_FEEDS: list[dict[str, Any]] = [
    {
        "name": "MarketWatch Top Stories",
        "url": "https://feeds.content.dowjones.io/public/rss/mw_topstories",
        "category": "business",
        "base_severity": 50,
    },
    {
        "name": "MarketWatch Bulletins",
        "url": "https://feeds.content.dowjones.io/public/rss/mw_bulletins",
        "category": "markets",
        "base_severity": 55,
    },
    {
        "name": "CNBC Top News",
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114",
        "category": "general",
        "base_severity": 45,
    },
    {
        "name": "Bloomberg Markets",
        "url": "https://feeds.bloomberg.com/markets/news.rss",
        "category": "markets",
        "base_severity": 55,
    },
    {
        "name": "FT Markets",
        "url": "https://www.ft.com/markets?format=rss",
        "category": "markets",
        "base_severity": 55,
    },
    {
        "name": "Investing.com Stock Market",
        "url": "https://www.investing.com/rss/news_25.rss",
        "category": "markets",
        "base_severity": 50,
    },
    {
        "name": "NYT Business",
        "url": "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
        "category": "business",
        "base_severity": 50,
    },
    {
        "name": "Fed Reserve Press",
        "url": "https://www.federalreserve.gov/feeds/press_all.xml",
        "category": "central_bank",
        "base_severity": 70,
    },
    {
        "name": "SEC Press Releases",
        "url": "https://www.sec.gov/news/pressreleases.rss",
        "category": "regulatory",
        "base_severity": 65,
    },
]

# Additional feeds used ONLY for keyword alerts.
# These are intentionally excluded from CURATED_FEEDS so they do not affect
# the live tape/event feed.
KEYWORD_ONLY_FEEDS: list[dict[str, Any]] = [
    {
        "name": "BBC Business",
        "url": "https://feeds.bbci.co.uk/news/business/rss.xml",
        "category": "business",
        "base_severity": 50,
    },
    {
        "name": "CNN Business",
        "url": "http://rss.cnn.com/rss/money_latest.rss",
        "category": "business",
        "base_severity": 48,
    },
    {
        "name": "NYT Technology",
        "url": "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml",
        "category": "business",
        "base_severity": 50,
    },
    {
        "name": "NYT World",
        "url": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
        "category": "general",
        "base_severity": 48,
    },
    {
        "name": "WSJ Markets",
        "url": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
        "category": "markets",
        "base_severity": 55,
    },
    {
        "name": "NPR Business",
        "url": "https://feeds.npr.org/1006/rss.xml",
        "category": "business",
        "base_severity": 46,
    },
    {
        "name": "Guardian Business",
        "url": "https://www.theguardian.com/business/rss",
        "category": "business",
        "base_severity": 48,
    },
]

# ---------------------------------------------------------------------------
# Common English words to exclude from ticker matching
# ---------------------------------------------------------------------------

_TICKER_EXCLUSIONS: set[str] = {
    "A", "I", "AM", "IT", "AT", "BE", "DO", "GO", "IF", "IN", "IS",
    "NO", "ON", "OR", "SO", "TO", "UP", "US", "WE",
    # Additional common short words that are not real tickers in context
    "AN", "AS", "BY", "HE", "ME", "MY", "OF", "OK", "OX",
    "ALL", "AND", "ARE", "BIG", "BUT", "CAN", "CEO", "CFO",
    "COO", "CTO", "DID", "FOR", "GET", "GOT", "HAS", "HAD",
    "HER", "HIM", "HIS", "HOW", "ITS", "LET", "MAY", "NEW",
    "NOT", "NOW", "OLD", "ONE", "OUR", "OUT", "OWN", "PUT",
    "RAN", "RUN", "SAY", "SET", "SHE", "THE", "TOO", "TOP",
    "TRY", "TWO", "WAS", "WAY", "WHO", "WHY", "WIN", "WON",
    "YET", "YOU",
    # Common abbreviations that are not tickers
    "CEO", "IPO", "ETF", "GDP", "CPI", "FED", "SEC", "NYSE",
    "FDA", "FBI", "CIA", "USA", "EUR", "USD", "GBP", "JPY",
    "EST", "PST", "CST", "PDT", "EDT",
    # Common English words that are also real tickers — exclude from bare
    # matching to avoid false positives (e.g. "net income" → NET).
    # These tickers are still matched via $NET, (NASDAQ: NET), and
    # company name aliases ("cloudflare" → NET).
    "NET", "ARM", "MP",
}

# Ticker pattern: matches $AAPL, (NASDAQ: AAPL), (NYSE: TSLA), or bare AAPL
_TICKER_DOLLAR_RE = re.compile(r"\$([A-Z]{1,5})\b")
_TICKER_EXCHANGE_RE = re.compile(
    r"\(\s*(?:NASDAQ|NYSE|AMEX|ARCA|BATS)\s*:\s*([A-Z]{1,5})\s*\)"
)
_TICKER_BARE_RE = re.compile(r"\b([A-Z]{1,5})\b")

# HTML tag stripping
_HTML_TAG_RE = re.compile(r"<[^>]+>")

# Various RSS date formats
_ISO_DATE_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}"
)

# Atom namespace
_ATOM_NS = "http://www.w3.org/2005/Atom"

# ---------------------------------------------------------------------------
# Company name → ticker aliases (only used for portfolio-held tickers)
# ---------------------------------------------------------------------------

_HARDCODED_ALIASES: dict[str, str] = {
    # Mega-cap / well-known
    "apple": "AAPL", "microsoft": "MSFT", "google": "GOOGL", "alphabet": "GOOGL",
    "amazon": "AMZN", "tesla": "TSLA", "nvidia": "NVDA", "meta": "META",
    "facebook": "META", "netflix": "NFLX", "jpmorgan": "JPM", "jp morgan": "JPM",
    "goldman sachs": "GS", "goldman": "GS", "berkshire": "BRK.B",
    "johnson & johnson": "JNJ", "procter & gamble": "PG", "coca-cola": "KO",
    "exxon": "XOM", "exxonmobil": "XOM", "chevron": "CVX",
    "disney": "DIS", "walmart": "WMT", "intel": "INTC", "amd": "AMD",
    "broadcom": "AVGO", "salesforce": "CRM", "adobe": "ADBE",
    "paypal": "PYPL", "uber": "UBER", "airbnb": "ABNB",
    "palantir": "PLTR", "snowflake": "SNOW", "crowdstrike": "CRWD",
    "coinbase": "COIN", "robinhood": "HOOD", "sofi": "SOFI",
    # Semi / materials / portfolio-relevant
    "applied materials": "AMAT", "lam research": "LRCX", "synopsys": "SNPS",
    "micron": "MU", "freeport": "FCX", "freeport-mcmoran": "FCX",
    "southern copper": "SCCO", "coherent": "COHR", "lucid": "LCID",
    "lucid motors": "LCID", "cameco": "CCJ", "mp materials": "MP",
    "denison mines": "DNN", "equinox gold": "EQX",
    "ge vernova": "GEV", "genius sports": "GENI",
    "bitcoin": "BTC", "ethereum": "ETH",
    # Tickers excluded from bare-word matching — alias them by company name
    "cloudflare": "NET", "arm holdings": "ARM",
}


def _build_company_alias_map(
    portfolio_tickers: set[str],
) -> dict[str, re.Pattern[str]]:
    """Build a map of company name aliases to compiled regex patterns.

    Only includes aliases whose corresponding ticker is in the current
    portfolio, to avoid false positive matches.

    Args:
        portfolio_tickers: Set of uppercase ticker symbols from portfolio.

    Returns:
        Dict mapping ticker symbol -> compiled regex that matches any of its
        aliases as whole words (case-insensitive).
    """
    # Invert: group aliases by ticker
    ticker_to_aliases: dict[str, list[str]] = {}
    for alias, ticker in _HARDCODED_ALIASES.items():
        if ticker in portfolio_tickers:
            ticker_to_aliases.setdefault(ticker, []).append(alias)

    # Compile one regex per ticker with all its aliases OR'd together
    alias_map: dict[str, re.Pattern[str]] = {}
    for ticker, aliases in ticker_to_aliases.items():
        # Sort longest-first so "jp morgan" matches before "jp"
        aliases.sort(key=len, reverse=True)
        pattern = "|".join(r"\b" + re.escape(a) + r"\b" for a in aliases)
        alias_map[ticker] = re.compile(pattern, re.IGNORECASE)

    return alias_map


# HTTP request settings
_HTTP_TIMEOUT = 15.0
_HTTP_USER_AGENT = "TradingWorkstation-RSS/1.0"


# ---------------------------------------------------------------------------
# HTML stripping
# ---------------------------------------------------------------------------


def _strip_html(text_val: str) -> str:
    """Strip HTML tags and decode HTML entities from a string.

    Args:
        text_val: Raw HTML or plain text string.

    Returns:
        Clean plain text with tags removed and entities decoded.
    """
    if not text_val:
        return ""
    # Remove HTML tags
    cleaned = _HTML_TAG_RE.sub(" ", text_val)
    # Decode HTML entities like &amp; &lt; &gt; &quot; &#123;
    cleaned = html.unescape(cleaned)
    # Collapse multiple whitespace into single spaces
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


# ---------------------------------------------------------------------------
# RSS date parsing
# ---------------------------------------------------------------------------


def _parse_rss_date(date_str: str) -> datetime | None:
    """Parse various RSS date formats into a timezone-aware datetime.

    Handles RFC 2822 (standard RSS), ISO 8601, and common variants.

    Args:
        date_str: Date string from RSS feed.

    Returns:
        Timezone-aware datetime in UTC, or None if parsing fails.
    """
    if not date_str or not date_str.strip():
        return None

    date_str = date_str.strip()

    # Try RFC 2822 first (most common in RSS 2.0)
    # e.g., "Mon, 06 Sep 2021 01:49:00 +0000"
    try:
        dt = parsedate_to_datetime(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    # Try ISO 8601 variants
    # e.g., "2021-09-06T01:49:00Z", "2021-09-06T01:49:00+00:00"
    iso_formats = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d",
    ]
    for fmt in iso_formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            continue

    logger.debug("rss_date_parse_failed", date_str=date_str)
    return None


# ---------------------------------------------------------------------------
# XML feed parsing
# ---------------------------------------------------------------------------


def _get_text(element: ET.Element | None) -> str:
    """Safely extract text content from an XML element."""
    if element is None:
        return ""
    return (element.text or "").strip()


def _parse_xml_feed(xml_text: str) -> list[dict[str, Any]]:
    """Parse RSS 2.0 or Atom XML into a list of article dicts.

    Handles both RSS 2.0 (<channel>/<item>) and Atom (<feed>/<entry>) formats.

    Args:
        xml_text: Raw XML string from the feed.

    Returns:
        List of article dicts with keys: title, link, published, description.
    """
    articles: list[dict[str, Any]] = []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        logger.warning("xml_parse_error", error=str(exc))
        return articles

    # Detect feed format
    tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag

    if tag == "rss":
        # RSS 2.0 format
        for channel in root.findall("channel"):
            for item in channel.findall("item"):
                article = _parse_rss_item(item)
                if article:
                    articles.append(article)
    elif tag == "feed" or root.tag == f"{{{_ATOM_NS}}}feed":
        # Atom format
        # Handle namespaced Atom
        ns = ""
        if "}" in root.tag:
            ns = root.tag.split("}")[0] + "}"

        for entry in root.findall(f"{ns}entry"):
            article = _parse_atom_entry(entry, ns)
            if article:
                articles.append(article)
    elif tag == "RDF" or "rdf" in root.tag.lower():
        # RSS 1.0 / RDF format
        # Items may be direct children of <RDF> or under namespaced elements
        ns_rss = ""
        for key, val in root.attrib.items():
            if "purl.org/rss" in val:
                ns_rss = val
                break
        # Try finding items without namespace first
        items = root.findall("item")
        if not items and ns_rss:
            items = root.findall(f"{{{ns_rss}}}item")
        # Also look for items as direct children
        if not items:
            items = list(root.iter("item"))
        for item in items:
            article = _parse_rss_item(item)
            if article:
                articles.append(article)
    else:
        # Fallback: try to find any <item> or <entry> elements anywhere
        for item in root.iter("item"):
            article = _parse_rss_item(item)
            if article:
                articles.append(article)
        if not articles:
            for entry in root.iter("entry"):
                article = _parse_atom_entry(entry, "")
                if article:
                    articles.append(article)

    return articles


def _parse_rss_item(item: ET.Element) -> dict[str, Any] | None:
    """Parse a single RSS 2.0 <item> element.

    Args:
        item: XML element for a single RSS item.

    Returns:
        Article dict or None if title is missing.
    """
    title = _get_text(item.find("title"))
    if not title:
        return None

    link = _get_text(item.find("link"))
    pub_date = _get_text(item.find("pubDate"))
    # Some feeds use dc:date
    if not pub_date:
        # Try common namespace prefixes
        for ns_prefix in [
            "{http://purl.org/dc/elements/1.1/}",
            "{http://purl.org/dc/terms/}",
        ]:
            pub_date = _get_text(item.find(f"{ns_prefix}date"))
            if pub_date:
                break

    description = _get_text(item.find("description"))
    if not description:
        # Some feeds use content:encoded
        for ns_prefix in [
            "{http://purl.org/rss/1.0/modules/content/}",
        ]:
            description = _get_text(item.find(f"{ns_prefix}encoded"))
            if description:
                break

    return {
        "title": title,
        "link": link,
        "published": pub_date,
        "description": description,
    }


def _parse_atom_entry(entry: ET.Element, ns: str) -> dict[str, Any] | None:
    """Parse a single Atom <entry> element.

    Args:
        entry: XML element for a single Atom entry.
        ns: Namespace prefix string (e.g., "{http://www.w3.org/2005/Atom}").

    Returns:
        Article dict or None if title is missing.
    """
    title = _get_text(entry.find(f"{ns}title"))
    if not title:
        return None

    # Atom links are in <link> elements with href attribute
    link = ""
    link_elem = entry.find(f"{ns}link")
    if link_elem is not None:
        # Prefer alternate link
        for l_elem in entry.findall(f"{ns}link"):
            rel = l_elem.get("rel", "alternate")
            if rel == "alternate":
                link = l_elem.get("href", "")
                break
        if not link:
            link = link_elem.get("href", "")

    # Published or updated date
    published = _get_text(entry.find(f"{ns}published"))
    if not published:
        published = _get_text(entry.find(f"{ns}updated"))

    # Content or summary
    description = _get_text(entry.find(f"{ns}summary"))
    if not description:
        content_elem = entry.find(f"{ns}content")
        if content_elem is not None:
            description = content_elem.text or ""

    return {
        "title": title,
        "link": link,
        "published": published,
        "description": description,
    }


# ---------------------------------------------------------------------------
# Feed fetching
# ---------------------------------------------------------------------------


async def _fetch_feed(feed: dict[str, Any]) -> list[dict[str, Any]]:
    """Fetch and parse a single RSS feed.

    Args:
        feed: Feed config dict with 'name' and 'url' keys.

    Returns:
        List of parsed article dicts. Returns empty list on any error.
    """
    feed_name = feed["name"]
    feed_url = feed["url"]

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(_HTTP_TIMEOUT),
            follow_redirects=True,
            headers={"User-Agent": _HTTP_USER_AGENT},
        ) as client:
            response = await client.get(feed_url)
            response.raise_for_status()

        xml_text = response.text
        if not xml_text or not xml_text.strip():
            logger.warning("rss_feed_empty_response", feed=feed_name)
            return []

        articles = _parse_xml_feed(xml_text)
        logger.debug(
            "rss_feed_fetched",
            feed=feed_name,
            articles_count=len(articles),
        )
        return articles

    except httpx.TimeoutException:
        logger.warning("rss_feed_timeout", feed=feed_name, url=feed_url)
        return []
    except httpx.HTTPStatusError as exc:
        logger.warning(
            "rss_feed_http_error",
            feed=feed_name,
            status_code=exc.response.status_code,
            url=feed_url,
        )
        return []
    except httpx.RequestError as exc:
        logger.warning(
            "rss_feed_request_error",
            feed=feed_name,
            error=str(exc),
            url=feed_url,
        )
        return []
    except Exception as exc:
        logger.error(
            "rss_feed_unexpected_error",
            feed=feed_name,
            error=str(exc),
            url=feed_url,
            exc_info=True,
        )
        return []


# ---------------------------------------------------------------------------
# Ticker extraction
# ---------------------------------------------------------------------------


def _extract_tickers(
    text_val: str,
    portfolio_tickers: set[str],
    alias_map: dict[str, re.Pattern[str]] | None = None,
) -> list[str]:
    """Scan text for ticker mentions and match against portfolio holdings.

    Identifies tickers using four strategies:
    1. Dollar-prefixed: $AAPL
    2. Exchange-prefixed: (NASDAQ: AAPL), (NYSE: TSLA)
    3. Bare uppercase: AAPL (only if it matches a known portfolio ticker)
    4. Company name aliases: "Apple reports earnings" -> AAPL

    Args:
        text_val: Combined title + description text to scan.
        portfolio_tickers: Set of uppercase ticker symbols from portfolio.
        alias_map: Optional dict of ticker -> compiled regex for company name
            aliases. Built by _build_company_alias_map().

    Returns:
        Sorted list of matched ticker symbols (uppercase, deduplicated).
    """
    if not text_val:
        return []

    matched: set[str] = set()

    # Pattern 1: Dollar-prefixed tickers (high confidence)
    for match in _TICKER_DOLLAR_RE.finditer(text_val.upper()):
        ticker = match.group(1)
        if ticker not in _TICKER_EXCLUSIONS and ticker in portfolio_tickers:
            matched.add(ticker)

    # Pattern 2: Exchange-prefixed tickers (high confidence)
    for match in _TICKER_EXCHANGE_RE.finditer(text_val.upper()):
        ticker = match.group(1)
        if ticker not in _TICKER_EXCLUSIONS and ticker in portfolio_tickers:
            matched.add(ticker)

    # Pattern 3: Bare uppercase words (only match portfolio tickers, length >= 2)
    # Use the original text but search case-insensitively
    upper_text = text_val.upper()
    for match in _TICKER_BARE_RE.finditer(upper_text):
        ticker = match.group(1)
        if (
            len(ticker) >= 2
            and ticker not in _TICKER_EXCLUSIONS
            and ticker in portfolio_tickers
        ):
            matched.add(ticker)

    # Pattern 4: Company name aliases (e.g., "Apple" -> AAPL)
    if alias_map:
        lower_text = text_val.lower()
        for ticker, pattern in alias_map.items():
            if pattern.search(lower_text):
                matched.add(ticker)

    return sorted(matched)


# ---------------------------------------------------------------------------
# Article to event conversion
# ---------------------------------------------------------------------------


def _article_to_event(
    article: dict[str, Any],
    feed: dict[str, Any],
    matched_tickers: list[str],
) -> dict[str, Any]:
    """Convert a parsed article and feed config into an event dict for DB insertion.

    Args:
        article: Parsed article dict with title, link, published, description.
        feed: Feed config dict with name, url, category, base_severity.
        matched_tickers: List of portfolio tickers found in the article.

    Returns:
        Event dict ready for database insertion.
    """
    # Stable dedup ID: hash of feed name + article link
    id_input = f"rss:{feed['name']}:{article.get('link', article.get('title', ''))}"
    event_id = hashlib.sha256(id_input.encode("utf-8")).hexdigest()

    # Parse publication date, fall back to now
    pub_date_str = article.get("published", "")
    ts_utc = _parse_rss_date(pub_date_str)
    if ts_utc is None:
        ts_utc = datetime.now(timezone.utc)

    # Clean description
    raw_description = article.get("description", "")
    clean_description = _strip_html(raw_description)

    # Severity scoring — small bump for ticker mentions; the main
    # differentiation comes from portfolio scoring (large positions,
    # high vol, sector concentration) applied downstream.
    severity = feed.get("base_severity", 50)
    if len(matched_tickers) == 1:
        severity = min(severity + 5, 100)
    elif len(matched_tickers) > 1:
        severity = min(severity + 8, 100)

    # Reason codes
    reason_codes = ["rss_news"]
    if matched_tickers:
        reason_codes.append("portfolio_mention")
    category = feed.get("category", "general")
    if category == "markets":
        reason_codes.append("market_news")
    elif category == "central_bank":
        reason_codes.append("central_bank")
    elif category == "regulatory":
        reason_codes.append("regulatory")

    # Metadata
    metadata = {
        "feed_name": feed["name"],
        "feed_category": category,
        "ticker_matches": matched_tickers,
        "original_pub_date": pub_date_str,
    }

    return {
        "id": event_id,
        "ts_utc": ts_utc,
        "type": "RSS_NEWS",
        "tickers": json.dumps(matched_tickers) if matched_tickers else None,
        "title": (article.get("title", "") or "")[:500],
        "source_name": feed["name"],
        "source_url": (article.get("link", "") or "")[:2000],
        "raw_text_snippet": clean_description[:1000] if clean_description else None,
        "severity_score": severity,
        "reason_codes": json.dumps(reason_codes),
        "llm_summary": None,
        "status": "NEW",
        "metadata_json": json.dumps(metadata),
    }


# ---------------------------------------------------------------------------
# Portfolio ticker lookup
# ---------------------------------------------------------------------------


async def _get_portfolio_tickers(engine: AsyncEngine) -> set[str]:
    """Query positions table for distinct ticker symbols.

    Args:
        engine: SQLAlchemy async engine.

    Returns:
        Set of uppercase ticker symbols from current positions.
    """
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                text("SELECT DISTINCT symbol FROM positions_current WHERE position != 0")
            )
            rows = result.fetchall()
            tickers = {str(row[0]).upper() for row in rows if row[0]}
            logger.debug("portfolio_tickers_loaded", count=len(tickers))
            return tickers
    except Exception as exc:
        logger.warning(
            "portfolio_tickers_query_failed",
            error=str(exc),
        )
        return set()


# ---------------------------------------------------------------------------
# Main sync entry point
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Per-ticker Google News RSS search
# ---------------------------------------------------------------------------


def _build_ticker_news_feeds(
    portfolio_tickers: set[str],
) -> list[dict[str, Any]]:
    """Build per-ticker Google News RSS feed configs.

    For each portfolio ticker, generates a Google News RSS search URL
    targeting recent stock/financial news for that ticker.

    Args:
        portfolio_tickers: Set of uppercase ticker symbols.

    Returns:
        List of feed config dicts compatible with _fetch_feed / _parse_xml_feed.
    """
    feeds: list[dict[str, Any]] = []

    # Reverse alias map: ticker -> best company name for search enrichment
    _ticker_to_name: dict[str, str] = {}
    for alias, ticker in _HARDCODED_ALIASES.items():
        # Prefer longer aliases (more specific company names)
        if ticker not in _ticker_to_name or len(alias) > len(_ticker_to_name[ticker]):
            _ticker_to_name[ticker] = alias

    for ticker in sorted(portfolio_tickers):
        # Skip ETFs and indices (they get plenty of coverage from curated feeds)
        if ticker in {"SPY", "QQQ", "IWM", "DIA", "VXX", "TLT", "GLD", "SLV",
                       "XLF", "XLE", "XLK", "XLV", "XLI", "XLP", "XLU", "XLB",
                       "XLRE", "XLC", "BTC", "ETH"}:
            continue

        # Build search query: "TICKER stock" + optional company name
        company = _ticker_to_name.get(ticker)
        if company:
            query = f"{ticker} OR \"{company}\" stock"
        else:
            query = f"{ticker} stock"

        # URL-encode the query
        from urllib.parse import quote
        encoded_q = quote(query)
        url = (
            f"https://news.google.com/rss/search?"
            f"q={encoded_q}+when:2d&hl=en-US&gl=US&ceid=US:en"
        )

        feeds.append({
            "name": f"Google News: {ticker}",
            "url": url,
            "category": "ticker_news",
            "base_severity": 55,
            "_ticker": ticker,  # pre-tag with the ticker
        })

    return feeds


async def sync_ticker_news_feeds(
    engine: AsyncEngine | None = None,
    lookback_hours: int = 48,
) -> dict[str, Any]:
    """Fetch per-ticker Google News RSS and store relevant articles as events.

    This complements the curated RSS feeds by searching for news specifically
    about each portfolio holding. Articles are always tagged with the
    searched ticker.

    Args:
        engine: SQLAlchemy async engine. If None, uses get_shared_engine().
        lookback_hours: Only store articles published within this many hours.

    Returns:
        Stats dict with feeds_checked, articles_found, events_inserted, etc.
    """
    if engine is None:
        engine = get_shared_engine()

    stats: dict[str, Any] = {
        "feeds_checked": 0,
        "articles_found": 0,
        "events_inserted": 0,
        "ticker_matches": 0,
        "errors": [],
    }

    # Get portfolio tickers
    portfolio_tickers = await _get_portfolio_tickers(engine)
    if not portfolio_tickers:
        logger.info("ticker_news_sync_no_tickers")
        return stats

    # Build per-ticker feeds
    ticker_feeds = _build_ticker_news_feeds(portfolio_tickers)
    if not ticker_feeds:
        return stats

    # Also build alias map for additional ticker extraction
    alias_map = _build_company_alias_map(portfolio_tickers)

    logger.info(
        "ticker_news_sync_started",
        feed_count=len(ticker_feeds),
        portfolio_tickers=len(portfolio_tickers),
    )

    cutoff_utc = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)

    # Fetch feeds in batches to avoid overwhelming Google
    batch_size = 5
    all_events: list[dict[str, Any]] = []

    for batch_start in range(0, len(ticker_feeds), batch_size):
        batch = ticker_feeds[batch_start : batch_start + batch_size]
        feed_results = await asyncio.gather(
            *[_fetch_feed(feed) for feed in batch],
            return_exceptions=True,
        )

        for feed, result in zip(batch, feed_results):
            stats["feeds_checked"] += 1
            feed_name = feed["name"]
            target_ticker = feed.get("_ticker", "")

            if isinstance(result, Exception):
                stats["errors"].append(f"{feed_name}: {result}")
                logger.warning("ticker_news_feed_error", feed=feed_name, error=str(result))
                continue

            articles: list[dict[str, Any]] = result
            stats["articles_found"] += len(articles)

            for article in articles:
                # Filter by lookback window
                pub_date = _parse_rss_date(article.get("published", ""))
                if pub_date is not None and pub_date < cutoff_utc:
                    continue

                # Always include the target ticker; also check for other mentions
                search_text = " ".join(
                    filter(None, [article.get("title", ""), article.get("description", "")])
                )
                additional_tickers = _extract_tickers(search_text, portfolio_tickers, alias_map)
                matched = sorted(set([target_ticker] + additional_tickers))
                stats["ticker_matches"] += len(matched)

                event = _article_to_event(article, feed, matched)
                all_events.append(event)

        # Small delay between batches to be polite to Google
        if batch_start + batch_size < len(ticker_feeds):
            await asyncio.sleep(1.0)

    # Bulk upsert
    if all_events:
        inserted = await _bulk_upsert_events(engine, all_events)
        stats["events_inserted"] = inserted

    # Prune old RSS_NEWS events beyond the retention limit
    # keep=500 and max_age_hours=48 ensures today's curated feed
    # articles are never deleted by the fast ticker-news loop
    try:
        pruned = await prune_old_rss_events(engine)
        stats["pruned"] = pruned
    except Exception as e:
        logger.warning("ticker_news_prune_error", error=str(e))
        stats["errors"].append(f"prune: {e}")

    logger.info(
        "ticker_news_sync_completed",
        feeds_checked=stats["feeds_checked"],
        articles_found=stats["articles_found"],
        events_inserted=stats["events_inserted"],
        ticker_matches=stats["ticker_matches"],
        pruned=stats.get("pruned", 0),
        error_count=len(stats["errors"]),
    )

    return stats


async def prune_old_rss_events(
    engine: AsyncEngine,
    keep: int = 500,
    max_age_hours: int = 48,
) -> int:
    """Delete RSS_NEWS events that are both old AND beyond the keep limit.

    Only prunes events older than *max_age_hours* to avoid deleting
    today's curated feed articles. Events within the age window are
    always retained regardless of the *keep* limit.

    Args:
        engine: SQLAlchemy async engine.
        keep: Number of most recent RSS_NEWS events to retain.
        max_age_hours: Never delete events newer than this many hours.

    Returns:
        Number of rows deleted.
    """
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text("""
                DELETE FROM events
                WHERE type = 'RSS_NEWS'
                  AND ts_utc < NOW() - MAKE_INTERVAL(hours => :max_age)
                  AND id NOT IN (
                      SELECT id FROM events
                      WHERE type = 'RSS_NEWS'
                      ORDER BY ts_utc DESC
                      LIMIT :keep
                  )
            """), {"keep": keep, "max_age": max_age_hours})
            deleted = result.rowcount
            if deleted > 0:
                logger.info("rss_events_pruned", deleted=deleted, kept=keep)
            return deleted
    except Exception:
        logger.error("rss_events_prune_failed", exc_info=True)
        return 0


async def sync_rss_feeds(
    feeds: list[dict[str, Any]] | None = None,
    lookback_hours: int = 24,
    engine: AsyncEngine | None = None,
) -> dict[str, Any]:
    """Fetch curated RSS feeds and store relevant articles as events.

    This is the main entry point for RSS feed ingestion. It fetches all
    configured feeds concurrently, matches articles against portfolio
    tickers, and upserts events into the database.

    Args:
        feeds: List of feed config dicts. Defaults to CURATED_FEEDS.
        lookback_hours: Only store articles published within this many hours.
            Defaults to 24.
        engine: SQLAlchemy async engine. If None, uses get_shared_engine().

    Returns:
        Stats dict with keys: feeds_checked, articles_found, events_inserted,
        ticker_matches, errors.
    """
    if engine is None:
        engine = get_shared_engine()

    if feeds is None:
        feeds = CURATED_FEEDS

    stats: dict[str, Any] = {
        "feeds_checked": 0,
        "articles_found": 0,
        "events_inserted": 0,
        "ticker_matches": 0,
        "errors": [],
    }

    if not feeds:
        logger.info("rss_sync_no_feeds_configured")
        return stats

    # Get portfolio tickers for matching
    portfolio_tickers = await _get_portfolio_tickers(engine)

    # Build company name alias map for headline matching
    alias_map = _build_company_alias_map(portfolio_tickers)

    logger.info(
        "rss_sync_started",
        feed_count=len(feeds),
        portfolio_tickers=len(portfolio_tickers),
        company_aliases=len(alias_map),
        lookback_hours=lookback_hours,
    )

    # Cutoff time for filtering old articles
    cutoff_utc = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)

    # Fetch all feeds concurrently
    feed_results = await asyncio.gather(
        *[_fetch_feed(feed) for feed in feeds],
        return_exceptions=True,
    )

    all_events: list[dict[str, Any]] = []

    for feed, result in zip(feeds, feed_results):
        stats["feeds_checked"] += 1
        feed_name = feed["name"]

        if isinstance(result, Exception):
            error_msg = f"{feed_name}: {result}"
            stats["errors"].append(error_msg)
            logger.error("rss_feed_exception", feed=feed_name, error=str(result))
            await _update_sync_status(
                engine, feed_name, error=str(result), items_fetched=0
            )
            continue

        articles: list[dict[str, Any]] = result
        stats["articles_found"] += len(articles)

        feed_events: list[dict[str, Any]] = []
        feed_ticker_matches = 0

        for article in articles:
            # Filter by lookback window
            pub_date = _parse_rss_date(article.get("published", ""))
            if pub_date is not None and pub_date < cutoff_utc:
                continue

            # Match tickers against portfolio
            search_text = " ".join(
                filter(None, [article.get("title", ""), article.get("description", "")])
            )
            matched_tickers = _extract_tickers(search_text, portfolio_tickers, alias_map)

            if matched_tickers:
                feed_ticker_matches += len(matched_tickers)

            # Convert to event
            event = _article_to_event(article, feed, matched_tickers)
            feed_events.append(event)

        stats["ticker_matches"] += feed_ticker_matches

        if feed_events:
            all_events.extend(feed_events)

        # Update sync status for this feed
        latest_ts = None
        if feed_events:
            latest_ts = max(e["ts_utc"] for e in feed_events)

        await _update_sync_status(
            engine,
            feed_name,
            error=None,
            items_fetched=len(feed_events),
            last_item_ts=latest_ts,
        )

        logger.info(
            "rss_feed_processed",
            feed=feed_name,
            articles=len(articles),
            events=len(feed_events),
            ticker_matches=feed_ticker_matches,
        )

    # Bulk upsert events
    if all_events:
        inserted = await _bulk_upsert_events(engine, all_events)
        stats["events_inserted"] = inserted

    logger.info(
        "rss_sync_completed",
        feeds_checked=stats["feeds_checked"],
        articles_found=stats["articles_found"],
        events_inserted=stats["events_inserted"],
        ticker_matches=stats["ticker_matches"],
        error_count=len(stats["errors"]),
    )

    return stats


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------


async def _bulk_upsert_events(
    engine: AsyncEngine,
    events: list[dict[str, Any]],
) -> int:
    """Bulk upsert events into the events table with ON CONFLICT DO NOTHING.

    Args:
        engine: SQLAlchemy async engine.
        events: List of event dicts to insert.

    Returns:
        Number of rows actually inserted (excludes duplicates).
    """
    if not events:
        return 0

    inserted_count = 0

    try:
        async with engine.begin() as conn:
            # Insert in batches to avoid overly large queries
            batch_size = 50
            for i in range(0, len(events), batch_size):
                batch = events[i : i + batch_size]

                for event in batch:
                    result = await conn.execute(
                        text("""
                            INSERT INTO events (
                                id, ts_utc, type, tickers, title,
                                source_name, source_url, raw_text_snippet,
                                severity_score, reason_codes, llm_summary,
                                status, metadata_json
                            ) VALUES (
                                :id, :ts_utc, :type, :tickers, :title,
                                :source_name, :source_url, :raw_text_snippet,
                                :severity_score, :reason_codes, :llm_summary,
                                :status, :metadata_json
                            )
                            ON CONFLICT (id) DO NOTHING
                        """),
                        {
                            "id": event["id"],
                            "ts_utc": event["ts_utc"],
                            "type": event["type"],
                            "tickers": event["tickers"],
                            "title": event["title"],
                            "source_name": event["source_name"],
                            "source_url": event["source_url"],
                            "raw_text_snippet": event["raw_text_snippet"],
                            "severity_score": event["severity_score"],
                            "reason_codes": event["reason_codes"],
                            "llm_summary": event["llm_summary"],
                            "status": event["status"],
                            "metadata_json": event["metadata_json"],
                        },
                    )
                    inserted_count += result.rowcount

        logger.info(
            "rss_events_upserted",
            total=len(events),
            inserted=inserted_count,
            duplicates=len(events) - inserted_count,
        )
    except Exception as exc:
        logger.error(
            "rss_events_upsert_failed",
            error=str(exc),
            event_count=len(events),
            exc_info=True,
        )
        raise

    return inserted_count


async def _update_sync_status(
    engine: AsyncEngine,
    feed_name: str,
    error: str | None = None,
    items_fetched: int = 0,
    last_item_ts: datetime | None = None,
) -> None:
    """Update the event_sync_status table for a given RSS feed.

    Args:
        engine: SQLAlchemy async engine.
        feed_name: Name of the RSS feed (used as sync_key).
        error: Error message if the feed fetch failed, None on success.
        items_fetched: Number of items fetched from this feed.
        last_item_ts: Timestamp of the most recent item fetched.
    """
    now_utc = datetime.now(timezone.utc)

    try:
        async with engine.begin() as conn:
            if error:
                await conn.execute(
                    text("""
                        INSERT INTO event_sync_status (
                            connector, sync_key, last_sync_at,
                            last_item_ts, items_fetched, error_count, last_error
                        ) VALUES (
                            :connector, :sync_key, :last_sync_at,
                            :last_item_ts, :items_fetched, 1, :last_error
                        )
                        ON CONFLICT (connector, sync_key)
                        DO UPDATE SET
                            last_sync_at = :last_sync_at,
                            error_count = event_sync_status.error_count + 1,
                            last_error = :last_error
                    """),
                    {
                        "connector": "rss",
                        "sync_key": feed_name,
                        "last_sync_at": now_utc,
                        "last_item_ts": last_item_ts,
                        "items_fetched": items_fetched,
                        "last_error": error,
                    },
                )
            else:
                await conn.execute(
                    text("""
                        INSERT INTO event_sync_status (
                            connector, sync_key, last_sync_at,
                            last_item_ts, items_fetched, error_count, last_error
                        ) VALUES (
                            :connector, :sync_key, :last_sync_at,
                            :last_item_ts, :items_fetched, 0, NULL
                        )
                        ON CONFLICT (connector, sync_key)
                        DO UPDATE SET
                            last_sync_at = :last_sync_at,
                            last_item_ts = COALESCE(:last_item_ts, event_sync_status.last_item_ts),
                            items_fetched = :items_fetched,
                            error_count = 0,
                            last_error = NULL
                    """),
                    {
                        "connector": "rss",
                        "sync_key": feed_name,
                        "last_sync_at": now_utc,
                        "last_item_ts": last_item_ts,
                        "items_fetched": items_fetched,
                    },
                )
    except Exception as exc:
        logger.warning(
            "rss_sync_status_update_failed",
            feed=feed_name,
            error=str(exc),
        )
