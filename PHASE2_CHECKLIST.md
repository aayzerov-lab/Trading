# Phase 2: Awareness + Catalysts + Alerts — Verification Checklist

## Prerequisites
- PostgreSQL running with tables auto-created via `init_phase1_db()`
- Redis running for real-time updates
- API server started (`uvicorn api_server.main:app`)
- Frontend dev server running (`npm run dev` in `apps/web`)

---

## Phase 2.0: Core Event System

- [ ] Tables created: `events`, `event_sync_status`, `alerts`
- [ ] `POST /events/seed` returns `{"seeded": true, "events": 5, "alerts": 3}`
- [ ] `GET /events` returns seeded events
- [ ] `GET /events/high-priority` returns events with severity >= 80
- [ ] `GET /events/stats` returns counts by type/status
- [ ] `PATCH /events/{id}/status` with `{"status": "ACKED"}` works
- [ ] `GET /events/alerts` returns seeded alerts
- [ ] `GET /events/alerts/unread-count` returns `{"count": N}`
- [ ] `PATCH /events/alerts/{id}/status` works for READ/SNOOZED/DISMISSED
- [ ] Frontend: Events tab appears in nav bar
- [ ] Frontend: Events tab shows seeded events with filters
- [ ] Frontend: High-priority sidebar shows severity >= 80 events
- [ ] Frontend: Event detail pane shows title, severity, tickers, source link
- [ ] Frontend: Notification bell in header shows unread count badge
- [ ] Frontend: Bell dropdown shows alerts with Read/Snooze/Dismiss actions

## Phase 2.1: EDGAR SEC Filings

- [ ] `POST /events/sync/edgar` triggers EDGAR sync
- [ ] For each portfolio ticker with a valid CIK, filings are fetched
- [ ] Events created with type=SEC_FILING, correct severity by form type
- [ ] `event_sync_status` updated for connector='edgar'
- [ ] Idempotent: re-running does not create duplicate events (ON CONFLICT)

## Phase 2.2: Macro Schedule Scrapers

- [ ] `POST /events/sync/schedules` triggers schedule sync
- [ ] FOMC dates populated (scraped or hardcoded fallback)
- [ ] NFP, CPI, GDP, PCE, ISM, Claims dates estimated for next 90 days
- [ ] Events created with type=MACRO_SCHEDULE, tickers=null
- [ ] Release times correct (FOMC at 14:00 ET, others at 08:30 ET)
- [ ] `event_sync_status` updated for connector='macro_schedule'

## Phase 2.3: RSS Feed Ingestion

- [ ] `POST /events/sync/rss` triggers RSS feed sync
- [ ] Feeds fetched concurrently from CURATED_FEEDS list
- [ ] Articles parsed from RSS 2.0 and Atom formats
- [ ] Portfolio ticker mentions detected in article titles/descriptions
- [ ] Events created with type=RSS_NEWS, matched tickers in tickers field
- [ ] Severity boosted for portfolio ticker mentions (+10 for 1, +15 for 2+)
- [ ] HTML stripped from descriptions before storage
- [ ] `event_sync_status` updated per feed

## Phase 2.4A: Materiality Scoring

- [ ] `POST /events/sync/score` triggers portfolio-aware scoring
- [ ] Direct holding events get +15 boost
- [ ] Large position (>5% weight) events get +10 additional
- [ ] High vol (>40% ann.) holding events get +5 additional
- [ ] Sector concentration (>20%) events get +8 additional
- [ ] Boost reason codes appended to event reason_codes
- [ ] `metadata_json.scored_at` timestamp set after scoring
- [ ] Events already scored are not re-scored (unless rescore_all=True)
- [ ] Final score capped at 100

## Phase 2.4B: Optional OpenAI Summarizer

- [ ] Without OPENAI_API_KEY: summarizer no-ops gracefully
- [ ] With OPENAI_API_KEY set: events with severity >= 80 get LLM summaries
- [ ] Max 10 summaries per run (configurable)
- [ ] Max 900 output tokens per summary
- [ ] `metadata_json.summarized_at` and `summarizer_model` set
- [ ] Frontend: LLM summary displayed in event detail pane when available

## Phase 2.4C: Alert Rules + Notifications

- [ ] `POST /events/sync/alerts` triggers alert rule evaluation
- [ ] Rule 1: Events with severity >= 80 generate HIGH_PRIORITY_EVENT alerts
- [ ] Rule 2: MACRO_SCHEDULE events within 24h generate MACRO_UPCOMING alerts
- [ ] Rule 3: VaR > 3% generates VAR_SPIKE alert (max 1/day)
- [ ] Rule 4: Position > 15% generates CONCENTRATION_WARNING (max 1/symbol/day)
- [ ] Rule 5: Stale data (>3 days) generates DATA_STALE alert (max 1/day)
- [ ] Deduplication: no duplicate alerts for same trigger
- [ ] Expired snoozes automatically cleared back to NEW

## Full Pipeline

- [ ] `POST /events/sync` runs full pipeline: EDGAR → Schedules → RSS → Scoring → Summarizer → Alerts
- [ ] Daily scheduler (`run_daily_jobs`) includes event sync step
- [ ] Events tab shows combined results from all connectors
- [ ] Notification center shows alerts from all rules

## Tests

- [ ] `PYTHONPATH=packages pytest packages/shared/tests/test_phase2_events.py -v` — 33/33 pass
- [ ] `npx tsc --noEmit` in apps/web — zero errors

---

## New Files Created

| File | Lines | Description |
|------|-------|-------------|
| `packages/shared/db/models.py` | +40 | events, event_sync_status, alerts tables |
| `apps/api-server/api_server/routers/events.py` | ~700 | Events + alerts API (14 endpoints) |
| `apps/web/src/lib/events-api.ts` | 149 | Frontend API client |
| `apps/web/src/components/EventsPanel.tsx` | 273 | Events tab UI |
| `apps/web/src/components/NotificationCenter.tsx` | 189 | Bell icon + dropdown |
| `packages/shared/data/edgar.py` | 618 | SEC EDGAR connector |
| `packages/shared/data/schedules.py` | 627 | Macro schedule scraper |
| `packages/shared/data/rss_feeds.py` | 920 | RSS feed ingestion |
| `packages/shared/data/scoring.py` | 587 | Materiality scoring engine |
| `packages/shared/data/summarizer.py` | 195 | Optional OpenAI summarizer |
| `packages/shared/data/alert_rules.py` | 666 | Alert rules + notifications |
| `packages/shared/tests/test_phase2_events.py` | 310 | Unit tests (33 tests) |

## Modified Files

| File | Changes |
|------|---------|
| `apps/api-server/api_server/main.py` | Added events router import + include |
| `apps/api-server/api_server/config.py` | Added OPENAI_API_KEY, EDGAR_USER_AGENT |
| `apps/web/src/app/page.tsx` | Added Events tab + NotificationCenter |
| `apps/web/src/app/globals.css` | Added ~300 lines of events/notification CSS |
| `packages/shared/data/scheduler.py` | Added run_event_sync + daily jobs integration |
