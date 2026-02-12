"""AI Search chat endpoint powered by Perplexity Sonar Pro.

Streams responses as Server-Sent Events (SSE) with structured financial
research formatting: TL;DR, What Happened, Why It Matters, What To Watch
Next, and Sources.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import AsyncGenerator, Optional
from zoneinfo import ZoneInfo

import structlog
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

logger = structlog.get_logger()

router = APIRouter(prefix="/ai", tags=["ai"])

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT_TEMPLATE = """\
You are a senior financial research assistant embedded in a live trading workstation.
Today's date is {today_date} ({today_weekday}). Current time: {now_time} ET.

**CRITICAL ACCURACY RULES — NEVER VIOLATE:**
- ONLY state facts you can verify with a cited source. If you cannot find a source, say so.
- NEVER fabricate, assume, or guess events. If you have no sourced information for today, \
say so and provide the most recent sourced information instead, clearly stating its date.
- ALWAYS include the actual date of each event or data point.
- If search results are thin or outdated, explicitly say so.

**PRICE DATA:**
- If MARKET DATA is provided below, use those exact numbers — they are from Yahoo Finance. \
Cite "(Yahoo Finance)" once when first referencing the price, then just use the numbers.
- Do NOT repeatedly mention Yahoo Finance or discuss why it is a reliable source. \
Just use the data naturally.
- For any other price data from web search, prefer Yahoo Finance, Google Finance, MarketWatch, \
Bloomberg, CNBC, or Nasdaq.com. Avoid obscure aggregators.

**Response structure (always use this format):**

## TL;DR
- (3 bullets max — key facts with dates)

## What Happened
- (Bullet points with inline citations [1], [2]. State the date of each event explicitly.)

## Why It Matters
(Analyze the market implications — what does this mean for the stock, sector, or portfolio? \
Focus on drivers, risks, and positioning. Do NOT discuss data source quality here.)

## What To Watch Next
(Upcoming confirmed dates, catalysts, metrics — with sources if available)

## Sources
(Numbered list matching inline citations)

**Additional rules:**
- Fact-first: lead with data, then provide analysis of what the data means.
- Focus your analysis on market implications, not on data sourcing methodology.
- No filler phrases, no preamble, no sign-offs.
- Keep total response under 700 words.
"""


def _build_system_prompt() -> str:
    """Build system prompt with current date/time injected."""
    et = ZoneInfo("America/New_York")
    now = datetime.now(et)
    return _SYSTEM_PROMPT_TEMPLATE.format(
        today_date=now.strftime("%B %d, %Y"),
        today_weekday=now.strftime("%A"),
        now_time=now.strftime("%I:%M %p"),
    )

# ---------------------------------------------------------------------------
# Web search heuristic
# ---------------------------------------------------------------------------

# Signals that the query needs live/recent info from the web.
_SEARCH_SIGNALS = [
    # Recency — anything "right now" needs fresh data
    "today", "this week", "this morning", "tonight", "right now",
    "latest", "recent", "currently", "live",
    # News & events
    "news", "announc", "earning", "report", "guidance", "outlook",
    "beat", "miss", "surprise", "revision",
    # Causal / explanatory — "why did X move" needs headlines
    "why did", "why is", "why are", "why has", "what happened",
    "what's going on", "what is happening",
    # Market moves (user wants context, not just the number)
    "crash", "drop", "surge", "rally", "spike", "plunge", "dump",
    "moon", "rip", "sell off", "selloff", "tank", "soar", "slump",
    # Macro & policy
    "fed ", "fomc", "cpi", "gdp", "inflation", "unemployment",
    "payroll", "nonfarm", "ppi", "pce", "retail sales",
    "tariff", "sanction", "regulat", "lawsuit", "sec ", "doj",
    # Analyst / ratings
    "upgrade", "downgrade", "analyst", "rating", "price target",
    "initiat", "reiterat",
    # Corporate actions
    "ipo", "merger", "acqui", "buyout", "split", "spinoff",
    "dividend", "buyback", "offering",
    # Sector / thematic
    "sector", "rotation", "trend", "sentiment", "short interest",
    "insider", "institution",
]

# Pure data-lookup patterns — if the query is *only* asking for a number
# and we already fetched it from Yahoo Finance, web search adds nothing.
_PRICE_LOOKUP_WORDS = [
    "close", "open", "high", "low", "price", "volume",
    "what was", "what did", "how much", "at what",
]


def _needs_web_search(user_text: str, has_price_data: bool) -> bool:
    """Decide whether to enable web search for this query.

    Biased toward YES — only skips search when the query is clearly a
    simple price/data lookup that Yahoo Finance already answered.
    """
    lower = user_text.lower()

    # Any news/event/causal signal → always search
    for signal in _SEARCH_SIGNALS:
        if signal in lower:
            return True

    # If we don't have Yahoo data, search might help fill the gap
    if not has_price_data:
        return True

    # We have Yahoo data — check if this is a pure price lookup
    is_price_query = any(p in lower for p in _PRICE_LOOKUP_WORDS)
    if is_price_query and len(lower.split()) < 15:
        return False

    # Default: search ON (over-willing)
    return True


# ---------------------------------------------------------------------------
# Pydantic request body
# ---------------------------------------------------------------------------


class ChatRequest(BaseModel):
    """Body for POST /ai/chat."""

    messages: list[dict]
    session_summary: Optional[str] = None
    session_id: Optional[str] = None


# ---------------------------------------------------------------------------
# SSE helpers
# ---------------------------------------------------------------------------


def _sse_event(event: str, data: dict) -> str:
    """Format a single SSE event."""
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


# ---------------------------------------------------------------------------
# POST /ai/chat — streaming SSE endpoint
# ---------------------------------------------------------------------------


@router.post("/chat")
async def ai_chat(body: ChatRequest) -> StreamingResponse:
    """Stream an AI-powered research response as SSE."""
    from api_server.config import get_settings
    from api_server.providers.perplexity import PerplexityProvider

    settings = get_settings()

    if not settings.OPENAI_API_KEY and not settings.PERPLEXITY_API_KEY:
        async def _error_stream() -> AsyncGenerator[str, None]:
            yield _sse_event("error", {"message": "No AI provider API key configured (OPENAI_API_KEY or PERPLEXITY_API_KEY)"})

        return StreamingResponse(
            _error_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    async def _stream() -> AsyncGenerator[str, None]:
        t0 = time.time()
        try:
            from api_server.services.market_data import extract_tickers, extract_dates, fetch_price_context

            # Build system prompt with current date, optionally appending session context
            system_prompt = _build_system_prompt()
            if body.session_summary:
                system_prompt += (
                    "\n\n**Session context (prior conversation summary):**\n"
                    + body.session_summary
                )

            # Enrich with Yahoo Finance data if tickers detected in any message
            all_user_text = " ".join(
                m["content"] for m in body.messages if m.get("role") == "user"
            )
            tickers = extract_tickers(all_user_text)
            requested_dates = extract_dates(all_user_text)
            if tickers:
                try:
                    price_ctx = await fetch_price_context(tickers, requested_dates or None)
                    if price_ctx:
                        system_prompt += "\n\n" + price_ctx
                        logger.info("price_context_injected", tickers=tickers,
                                    dates=[d.isoformat() for d in (requested_dates or [])])
                except Exception:
                    logger.warning("price_context_failed", tickers=tickers, exc_info=True)

            # Decide if web search adds value for this query
            has_price_data = "\n" in system_prompt and "MARKET DATA" in system_prompt
            use_search = _needs_web_search(all_user_text, has_price_data)

            # Prefer OpenAI (GPT-4o-mini), fall back to Perplexity
            if settings.OPENAI_API_KEY:
                from api_server.providers.openai_provider import OpenAIProvider
                provider = OpenAIProvider(
                    api_key=settings.OPENAI_API_KEY,
                    model="gpt-4o-mini",
                )
                provider_name = "openai"
            else:
                provider = PerplexityProvider(
                    api_key=settings.PERPLEXITY_API_KEY,
                    model="sonar-pro",
                )
                provider_name = "perplexity"

            citations: list[str] = []

            logger.info(
                "ai_chat_request",
                session_id=body.session_id,
                message_count=len(body.messages),
                web_search=use_search,
            )

            async for chunk in provider.stream_chat(
                system_prompt, body.messages, web_search=use_search
            ):
                if chunk["type"] == "delta":
                    yield _sse_event("delta", {"text": chunk["text"]})
                elif chunk["type"] == "done":
                    citations = chunk.get("citations", [])
                elif chunk["type"] == "error":
                    yield _sse_event("error", {"message": chunk["message"]})
                    return

            latency_ms = int((time.time() - t0) * 1000)

            yield _sse_event("done", {
                "citations": citations,
                "meta": {
                    "provider": provider_name,
                    "model": provider.model,
                    "web_search": use_search,
                    "latency_ms": latency_ms,
                },
            })

            logger.info(
                "ai_chat_completed",
                session_id=body.session_id,
                latency_ms=latency_ms,
                citation_count=len(citations),
            )

        except Exception as exc:
            logger.exception("ai_chat_stream_error", session_id=body.session_id)
            yield _sse_event("error", {"message": str(exc)})

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
