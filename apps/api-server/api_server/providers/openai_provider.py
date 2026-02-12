"""OpenAI Responses API provider with web search grounding."""

from __future__ import annotations

import json
from typing import AsyncIterator

import httpx
import structlog

from .base import AIProvider

logger = structlog.get_logger()


class OpenAIProvider(AIProvider):
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        self.api_key = api_key
        self.model = model
        self.base_url = "https://api.openai.com/v1"

    async def stream_chat(
        self, system_prompt: str, messages: list[dict],
        *, web_search: bool = True,
    ) -> AsyncIterator[dict]:
        # Build input in Responses API format
        input_messages = [{"role": "developer", "content": system_prompt}]
        for m in messages:
            input_messages.append({"role": m["role"], "content": m["content"]})

        payload: dict = {
            "model": self.model,
            "input": input_messages,
            "stream": True,
        }
        if web_search:
            payload["tools"] = [{"type": "web_search_preview"}]

        async with httpx.AsyncClient(timeout=httpx.Timeout(90.0, connect=10.0)) as client:
            async with client.stream(
                "POST",
                f"{self.base_url}/responses",
                json=payload,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
            ) as response:
                if response.status_code != 200:
                    body = await response.aread()
                    logger.error(
                        "openai_api_error",
                        status=response.status_code,
                        body=body.decode()[:500],
                    )
                    yield {
                        "type": "error",
                        "message": f"Provider returned {response.status_code}",
                    }
                    return

                citations: list[str] = []
                seen_urls: set[str] = set()

                async for line in response.aiter_lines():
                    if not line.startswith("data: "):
                        continue

                    data_str = line[6:]
                    if data_str.strip() == "[DONE]":
                        break

                    try:
                        event = json.loads(data_str)
                    except json.JSONDecodeError:
                        continue

                    etype = event.get("type", "")

                    # Stream text deltas
                    if etype == "response.output_text.delta":
                        delta = event.get("delta", "")
                        if delta:
                            yield {"type": "delta", "text": delta}

                    # Collect citations from the completed response
                    elif etype == "response.completed":
                        resp = event.get("response", {})
                        for output_item in resp.get("output", []):
                            for content_part in output_item.get("content", []):
                                for ann in content_part.get("annotations", []):
                                    if ann.get("type") == "url_citation":
                                        url = ann.get("url", "")
                                        if url and url not in seen_urls:
                                            seen_urls.add(url)
                                            citations.append(url)

                yield {"type": "done", "citations": citations}
