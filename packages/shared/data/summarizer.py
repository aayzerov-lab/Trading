"""Optional LLM summarizer for high-priority events.

Gated behind OPENAI_API_KEY env var. Only summarises events with
severity_score >= threshold and without an existing llm_summary.

Cost controls:
- Max 10 summarisations per invocation (configurable)
- Max 900 output tokens per summary
- Only processes events that pass the gate
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from shared.db.engine import get_shared_engine

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_SEVERITY_THRESHOLD = 80
DEFAULT_MAX_PER_RUN = 10
DEFAULT_MAX_TOKENS = 900
DEFAULT_MODEL = "gpt-4o-mini"  # cheapest capable model

SYSTEM_PROMPT = (
    "You are a concise financial analyst assistant. Summarise the event below "
    "in 2-4 sentences focusing on: (1) what happened, (2) which tickers/sectors "
    "are affected, (3) potential portfolio impact. Be factual, no speculation."
)


# ---------------------------------------------------------------------------
# Gate check
# ---------------------------------------------------------------------------


def is_summarizer_available() -> bool:
    """Return True if OpenAI API key is configured."""
    return bool(os.environ.get("OPENAI_API_KEY", "").strip())


# ---------------------------------------------------------------------------
# OpenAI call (isolated for easy mocking)
# ---------------------------------------------------------------------------


async def _call_openai(
    event_text: str,
    model: str = DEFAULT_MODEL,
    max_tokens: int = DEFAULT_MAX_TOKENS,
) -> str | None:
    """Call OpenAI ChatCompletion API.

    Returns the summary string or None on failure.
    Uses httpx directly to avoid requiring the openai SDK.
    """
    import httpx

    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return None

    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "temperature": 0.3,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": event_text},
        ],
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()
            return data["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.error("openai_call_failed", error=str(e))
        return None


# ---------------------------------------------------------------------------
# Event text builder
# ---------------------------------------------------------------------------


def _build_event_text(event: dict) -> str:
    """Build a prompt-friendly text block from an event row."""
    parts = [f"Title: {event.get('title', 'N/A')}"]

    if event.get("type"):
        parts.append(f"Type: {event['type']}")

    tickers_raw = event.get("tickers")
    if tickers_raw:
        try:
            tickers = json.loads(tickers_raw) if isinstance(tickers_raw, str) else tickers_raw
            parts.append(f"Tickers: {', '.join(tickers)}")
        except (json.JSONDecodeError, TypeError):
            pass

    if event.get("source_name"):
        parts.append(f"Source: {event['source_name']}")

    snippet = event.get("raw_text_snippet")
    if snippet:
        # Truncate to ~600 chars to leave room for other context
        parts.append(f"Content: {snippet[:600]}")

    if event.get("reason_codes"):
        try:
            codes = json.loads(event["reason_codes"]) if isinstance(event["reason_codes"], str) else event["reason_codes"]
            parts.append(f"Tags: {', '.join(codes)}")
        except (json.JSONDecodeError, TypeError):
            pass

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def summarize_events(
    severity_threshold: int = DEFAULT_SEVERITY_THRESHOLD,
    max_per_run: int = DEFAULT_MAX_PER_RUN,
    model: str = DEFAULT_MODEL,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    engine: AsyncEngine | None = None,
) -> dict[str, Any]:
    """Summarise high-priority events that lack an LLM summary.

    Returns stats dict.  No-ops gracefully if OPENAI_API_KEY is not set.
    """
    stats: dict[str, Any] = {
        "available": is_summarizer_available(),
        "events_checked": 0,
        "summaries_generated": 0,
        "errors": 0,
    }

    if not is_summarizer_available():
        logger.info("summarizer_skipped", reason="no_api_key")
        return stats

    engine = engine or get_shared_engine()

    # Fetch eligible events
    async with engine.connect() as conn:
        result = await conn.execute(
            text("""
                SELECT id, type, tickers, title, source_name,
                       raw_text_snippet, reason_codes, severity_score
                FROM events
                WHERE severity_score >= :threshold
                  AND status = 'NEW'
                  AND (llm_summary IS NULL OR llm_summary = '')
                ORDER BY severity_score DESC, created_at_utc DESC
                LIMIT :limit
            """),
            {"threshold": severity_threshold, "limit": max_per_run},
        )
        rows = result.mappings().all()

    stats["events_checked"] = len(rows)
    logger.info(
        "summarizer_started",
        eligible_events=len(rows),
        threshold=severity_threshold,
        model=model,
    )

    for row in rows:
        event = dict(row)
        event_text = _build_event_text(event)

        summary = await _call_openai(event_text, model=model, max_tokens=max_tokens)
        if summary:
            # Update the event with the summary
            async with engine.begin() as conn:
                # Update llm_summary and mark as summarised in metadata
                existing_meta = event.get("metadata_json")
                meta = {}
                if existing_meta:
                    try:
                        meta = json.loads(existing_meta) if isinstance(existing_meta, str) else existing_meta
                    except (json.JSONDecodeError, TypeError):
                        meta = {}
                meta["summarized_at"] = datetime.now(timezone.utc).isoformat()
                meta["summarizer_model"] = model

                await conn.execute(
                    text("""
                        UPDATE events
                        SET llm_summary = :summary,
                            metadata_json = :metadata,
                            updated_at_utc = now()
                        WHERE id = :id
                    """),
                    {
                        "summary": summary,
                        "metadata": json.dumps(meta),
                        "id": event["id"],
                    },
                )

            stats["summaries_generated"] += 1
            logger.info(
                "event_summarized",
                event_id=event["id"],
                summary_len=len(summary),
            )
        else:
            stats["errors"] += 1

    logger.info("summarizer_completed", **stats)
    return stats
