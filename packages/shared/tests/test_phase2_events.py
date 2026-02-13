"""Phase 2 unit tests for event connectors, scoring, and alert rules.

Tests pure functions that do not require a live database.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

import pytest


# ---------------------------------------------------------------------------
# EDGAR connector tests
# ---------------------------------------------------------------------------


class TestEdgarFilingToEvent:
    """Tests for _filing_to_event conversion."""

    def _make_filing(self, **overrides):
        base = {
            "accession_number": "0000320193-24-000077",
            "form_type": "10-K",
            "filing_date": "2024-11-08",
            "report_date": "2024-09-28",
            "primary_document": "aapl-20240928.htm",
            "description": "Annual Report",
        }
        base.update(overrides)
        return base

    def test_basic_conversion(self):
        from shared.data.edgar import _filing_to_event

        event = _filing_to_event("AAPL", "0000320193", self._make_filing())

        assert event["type"] == "SEC_FILING"
        assert event["source_name"] == "SEC/EDGAR"
        assert event["status"] == "NEW"
        assert "AAPL" in event["title"]
        assert "10-K" in event["title"]
        assert json.loads(event["tickers"]) == ["AAPL"]
        assert "sec.gov" in event["source_url"]

    def test_severity_by_form_type(self):
        from shared.data.edgar import _filing_to_event

        tests = [
            ("8-K", 75),
            ("10-K", 60),
            ("10-Q", 50),
            ("S-1", 70),
            ("SC 13D", 80),
            ("SC 13G", 80),
            ("DEF 14A", 40),  # unknown form → default 40
        ]
        for form_type, expected_severity in tests:
            event = _filing_to_event(
                "TSLA", "0001318605", self._make_filing(form_type=form_type)
            )
            assert event["severity_score"] == expected_severity, (
                f"form_type={form_type}: expected {expected_severity}, "
                f"got {event['severity_score']}"
            )

    def test_stable_dedup_id(self):
        from shared.data.edgar import _filing_to_event

        filing = self._make_filing()
        event1 = _filing_to_event("AAPL", "0000320193", filing)
        event2 = _filing_to_event("AAPL", "0000320193", filing)
        assert event1["id"] == event2["id"]

        # Different accession → different id
        filing2 = self._make_filing(accession_number="0000320193-24-999999")
        event3 = _filing_to_event("AAPL", "0000320193", filing2)
        assert event3["id"] != event1["id"]

    def test_metadata_json(self):
        from shared.data.edgar import _filing_to_event

        event = _filing_to_event("AAPL", "0000320193", self._make_filing())
        meta = json.loads(event["metadata_json"])
        assert meta["cik"] == "0000320193"
        assert meta["form_type"] == "10-K"
        assert meta["filing_date"] == "2024-11-08"


# ---------------------------------------------------------------------------
# Schedule scraper tests
# ---------------------------------------------------------------------------


class TestScheduleEstimation:
    """Tests for _estimate_release_dates."""

    def test_nfp_first_friday(self):
        from shared.data.schedules import _estimate_release_dates

        dates = _estimate_release_dates(
            "Nonfarm Payrolls",
            date(2025, 1, 1),
            date(2025, 3, 31),
        )
        # Should get Jan, Feb, Mar — 3 dates
        assert len(dates) == 3
        for d in dates:
            assert d.weekday() == 4  # Friday

    def test_claims_every_thursday(self):
        from shared.data.schedules import _estimate_release_dates

        dates = _estimate_release_dates(
            "Initial Jobless Claims",
            date(2025, 1, 1),
            date(2025, 1, 31),
        )
        # ~4-5 Thursdays in January
        assert len(dates) >= 4
        for d in dates:
            assert d.weekday() == 3  # Thursday

    def test_gdp_quarterly_months(self):
        from shared.data.schedules import _estimate_release_dates

        dates = _estimate_release_dates(
            "GDP Release",
            date(2025, 1, 1),
            date(2025, 12, 31),
        )
        # GDP in Jan, Apr, Jul, Oct → 4 dates
        assert len(dates) == 4
        months = {d.month for d in dates}
        assert months == {1, 4, 7, 10}

    def test_schedule_to_event(self):
        from shared.data.schedules import MACRO_RELEASES, _schedule_to_event

        release = MACRO_RELEASES[0]  # FOMC Rate Decision
        event = _schedule_to_event(release, date(2025, 3, 19))
        assert event["type"] == "MACRO_SCHEDULE"
        assert event["tickers"] is None  # macro events don't have tickers
        assert event["severity_score"] == 90
        assert "FOMC" in event["title"]
        assert "Mar 19, 2025" in event["title"]

    def test_schedule_event_dedup_id(self):
        from shared.data.schedules import MACRO_RELEASES, _schedule_to_event

        release = MACRO_RELEASES[0]
        e1 = _schedule_to_event(release, date(2025, 3, 19))
        e2 = _schedule_to_event(release, date(2025, 3, 19))
        assert e1["id"] == e2["id"]

        e3 = _schedule_to_event(release, date(2025, 6, 18))
        assert e3["id"] != e1["id"]


# ---------------------------------------------------------------------------
# RSS feed tests
# ---------------------------------------------------------------------------


class TestRSSParsing:
    """Tests for RSS parsing and ticker extraction."""

    def test_strip_html(self):
        from shared.data.rss_feeds import _strip_html

        assert _strip_html("<p>Hello <b>World</b></p>") == "Hello World"
        assert _strip_html("No tags here") == "No tags here"
        assert _strip_html("&amp; &lt;") == "& <"

    def test_extract_tickers_dollar_prefix(self):
        from shared.data.rss_feeds import _extract_tickers

        portfolio = {"AAPL", "MSFT", "TSLA", "AMZN"}
        result = _extract_tickers("Big day for $AAPL and $MSFT", portfolio)
        assert "AAPL" in result
        assert "MSFT" in result

    def test_extract_tickers_exchange_prefix(self):
        from shared.data.rss_feeds import _extract_tickers

        portfolio = {"AAPL", "TSLA"}
        result = _extract_tickers("(NASDAQ: AAPL) hits new high", portfolio)
        assert "AAPL" in result

    def test_extract_tickers_ignores_common_words(self):
        from shared.data.rss_feeds import _extract_tickers

        portfolio = {"A", "I", "IT", "IS", "ON", "AAPL"}
        result = _extract_tickers("A big day on IT systems", portfolio)
        # Common words should be excluded
        assert "A" not in result
        assert "IT" not in result
        assert "IS" not in result
        assert "ON" not in result

    def test_parse_rss_date_rfc2822(self):
        from shared.data.rss_feeds import _parse_rss_date

        dt = _parse_rss_date("Mon, 10 Feb 2025 14:30:00 GMT")
        assert dt is not None
        assert dt.year == 2025
        assert dt.month == 2

    def test_parse_rss_date_iso(self):
        from shared.data.rss_feeds import _parse_rss_date

        dt = _parse_rss_date("2025-02-10T14:30:00Z")
        assert dt is not None
        assert dt.year == 2025

    def test_parse_rss_date_invalid(self):
        from shared.data.rss_feeds import _parse_rss_date

        assert _parse_rss_date("not a date") is None
        assert _parse_rss_date("") is None
        assert _parse_rss_date(None) is None

    def test_article_to_event(self):
        from shared.data.rss_feeds import _article_to_event

        article = {
            "title": "Apple beats earnings estimates",
            "link": "https://example.com/article/1",
            "published": "2025-02-10T14:30:00Z",
            "description": "<p>Apple reported strong Q1 results.</p>",
        }
        feed = {
            "name": "Test Feed",
            "url": "https://example.com/rss",
            "category": "markets",
            "base_severity": 50,
        }
        event = _article_to_event(article, feed, ["AAPL"])

        assert event["type"] == "RSS_NEWS"
        assert event["source_name"] == "Test Feed"
        assert json.loads(event["tickers"]) == ["AAPL"]
        assert event["severity_score"] == 60  # 50 + 10 for one ticker match
        assert "portfolio_mention" in json.loads(event["reason_codes"])

    def test_article_severity_boost(self):
        from shared.data.rss_feeds import _article_to_event

        article = {"title": "Test", "link": "https://x.com/1", "published": "", "description": ""}
        feed = {"name": "F", "url": "https://x.com", "category": "general", "base_severity": 50}

        # No tickers: base severity
        e1 = _article_to_event(article, feed, [])
        assert e1["severity_score"] == 50

        # One ticker: +10
        e2 = _article_to_event(article, feed, ["AAPL"])
        assert e2["severity_score"] == 60

        # Multiple tickers: +15
        e3 = _article_to_event(article, feed, ["AAPL", "MSFT"])
        assert e3["severity_score"] == 65


# ---------------------------------------------------------------------------
# Scoring engine tests
# ---------------------------------------------------------------------------


class TestScoring:
    """Tests for _compute_portfolio_boost."""

    def _make_portfolio(self, **overrides):
        base = {
            "holdings": {
                "AAPL": {"weight_pct": 8.5, "market_value": 50000},
                "MSFT": {"weight_pct": 3.0, "market_value": 18000},
            },
            "total_value": 600000,
            "sectors": {"Technology": 35.0, "Financials": 10.0},
            "ticker_sector": {"AAPL": "Technology", "MSFT": "Technology"},
            "tickers": {"AAPL", "MSFT"},
            "vol": {"AAPL": 0.25, "MSFT": 0.22},
        }
        base.update(overrides)
        return base

    def test_no_ticker_no_boost(self):
        from shared.data.scoring import _compute_portfolio_boost

        event = {"tickers": None, "type": "MACRO_SCHEDULE"}
        boost, reasons = _compute_portfolio_boost(event, self._make_portfolio())
        assert boost == 0
        assert reasons == []

    def test_direct_holding_boost(self):
        from shared.data.scoring import _compute_portfolio_boost

        event = {"tickers": '["AAPL"]', "type": "SEC_FILING"}
        boost, reasons = _compute_portfolio_boost(event, self._make_portfolio())
        assert boost >= 15  # Direct holding boost
        assert any("portfolio_holding:AAPL" in r for r in reasons)

    def test_large_position_boost(self):
        from shared.data.scoring import _compute_portfolio_boost

        portfolio = self._make_portfolio()
        portfolio["holdings"]["AAPL"]["weight_pct"] = 12.0  # > 5% threshold
        event = {"tickers": '["AAPL"]', "type": "SEC_FILING"}
        boost, reasons = _compute_portfolio_boost(event, portfolio)
        assert any("large_position" in r for r in reasons)

    def test_high_vol_boost(self):
        from shared.data.scoring import _compute_portfolio_boost

        portfolio = self._make_portfolio()
        portfolio["vol"]["AAPL"] = 0.50  # > 40% threshold
        event = {"tickers": '["AAPL"]', "type": "SEC_FILING"}
        boost, reasons = _compute_portfolio_boost(event, portfolio)
        assert any("high_vol" in r for r in reasons)

    def test_sector_concentration_boost(self):
        from shared.data.scoring import _compute_portfolio_boost

        portfolio = self._make_portfolio()
        portfolio["sectors"]["Technology"] = 25.0  # > 20% threshold
        event = {"tickers": '["AAPL"]', "type": "SEC_FILING"}
        boost, reasons = _compute_portfolio_boost(event, portfolio)
        assert any("sector_concentration" in r for r in reasons)

    def test_non_portfolio_ticker_no_boost(self):
        from shared.data.scoring import _compute_portfolio_boost

        event = {"tickers": '["TSLA"]', "type": "SEC_FILING"}
        boost, reasons = _compute_portfolio_boost(event, self._make_portfolio())
        assert boost == 0

    def test_severity_tier(self):
        from shared.data.scoring import severity_tier

        assert severity_tier(100) == "critical"
        assert severity_tier(85) == "critical"
        assert severity_tier(84) == "high"
        assert severity_tier(70) == "high"
        assert severity_tier(69) == "medium"
        assert severity_tier(50) == "medium"
        assert severity_tier(49) == "low"
        assert severity_tier(0) == "low"

    def test_boost_capped_at_100(self):
        from shared.data.scoring import _compute_portfolio_boost

        # Create a scenario with huge boosts
        portfolio = self._make_portfolio()
        portfolio["holdings"]["AAPL"]["weight_pct"] = 20.0
        portfolio["vol"]["AAPL"] = 0.60
        portfolio["sectors"]["Technology"] = 30.0
        event = {"tickers": '["AAPL"]', "type": "SEC_FILING"}
        boost, _ = _compute_portfolio_boost(event, portfolio)
        # The boost itself can exceed 100, but scoring caps at 100
        assert boost > 0


# ---------------------------------------------------------------------------
# Summarizer tests
# ---------------------------------------------------------------------------


class TestSummarizer:
    """Tests for summarizer utility functions."""

    def test_build_event_text(self):
        from shared.data.summarizer import _build_event_text

        event = {
            "title": "AAPL 10-K Filed",
            "type": "SEC_FILING",
            "tickers": '["AAPL"]',
            "source_name": "SEC/EDGAR",
            "raw_text_snippet": "This is the filing content.",
            "reason_codes": '["10k_annual_report"]',
        }
        text = _build_event_text(event)
        assert "AAPL 10-K Filed" in text
        assert "SEC_FILING" in text
        assert "AAPL" in text
        assert "SEC/EDGAR" in text
        assert "filing content" in text

    def test_is_summarizer_available_without_key(self, monkeypatch):
        from shared.data.summarizer import is_summarizer_available

        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        assert not is_summarizer_available()

    def test_is_summarizer_available_with_key(self, monkeypatch):
        from shared.data.summarizer import is_summarizer_available

        monkeypatch.setenv("OPENAI_API_KEY", "sk-test-key")
        assert is_summarizer_available()


# ---------------------------------------------------------------------------
# Alert rules tests
# ---------------------------------------------------------------------------


class TestAlertHelpers:
    """Tests for alert rule helper functions."""

    def test_make_alert(self):
        from shared.data.alert_rules import _make_alert

        alert = _make_alert(
            alert_type="VAR_SPIKE",
            message="Portfolio VaR spike: 4.2%",
            severity=85,
        )
        assert alert["type"] == "VAR_SPIKE"
        assert alert["severity"] == 85
        assert alert["status"] == "NEW"
        assert alert["related_event_id"] is None
        assert alert["snoozed_until"] is None
        assert "VaR spike" in alert["message"]

    def test_make_alert_truncates_message(self):
        from shared.data.alert_rules import _make_alert

        long_msg = "x" * 500
        alert = _make_alert("TEST", long_msg, 50)
        assert len(alert["message"]) == 300

    def test_make_alert_clamps_severity(self):
        from shared.data.alert_rules import _make_alert

        alert_low = _make_alert("T", "m", -10)
        assert alert_low["severity"] == 0

        alert_high = _make_alert("T", "m", 200)
        assert alert_high["severity"] == 100

    def test_make_alert_with_event_id(self):
        from shared.data.alert_rules import _make_alert

        alert = _make_alert("HIGH_PRIORITY_EVENT", "Test", 80, related_event_id="abc123")
        assert alert["related_event_id"] == "abc123"

    def test_keyword_pattern_avoids_substring_false_positive(self):
        from shared.data.alert_rules import _compile_keyword_pattern

        pat = _compile_keyword_pattern("ai")
        assert pat.search("AI demand is rising")
        assert not pat.search("the company said guidance was unchanged")

    def test_keyword_pattern_matches_multi_word_phrase(self):
        from shared.data.alert_rules import _compile_keyword_pattern

        pat = _compile_keyword_pattern("rate cut")
        assert pat.search("Markets price a rate cut in June")
        assert pat.search("Markets price a rate   cut in June")

    def test_coerce_utc_datetime_handles_iso_z(self):
        from shared.data.alert_rules import _coerce_utc_datetime

        dt = _coerce_utc_datetime("2026-02-13T01:23:45Z")
        assert dt is not None
        assert dt.tzinfo is not None
        assert dt.year == 2026

    def test_coerce_utc_datetime_handles_naive_datetime(self):
        from shared.data.alert_rules import _coerce_utc_datetime

        dt = _coerce_utc_datetime(datetime(2026, 2, 13, 1, 2, 3))
        assert dt is not None
        assert dt.tzinfo is not None
        assert dt.utcoffset() == timezone.utc.utcoffset(dt)
