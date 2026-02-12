"""Perplexity Sonar Pro provider using httpx streaming."""

from __future__ import annotations

import json
from typing import AsyncIterator

import httpx
import structlog

from .base import AIProvider

logger = structlog.get_logger()


class PerplexityProvider(AIProvider):
    def __init__(self, api_key: str, model: str = "sonar-pro"):
        self.api_key = api_key
        self.model = model
        self.base_url = "https://api.perplexity.ai"

    async def stream_chat(
        self, system_prompt: str, messages: list[dict],
        *, web_search: bool = True,
    ) -> AsyncIterator[dict]:
        payload = {
            "model": self.model,
            "messages": [{"role": "system", "content": system_prompt}] + messages,
            "stream": True,
        }

        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=10.0)) as client:
            async with client.stream(
                "POST",
                f"{self.base_url}/chat/completions",
                json=payload,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
            ) as response:
                if response.status_code != 200:
                    body = await response.aread()
                    logger.error("perplexity_api_error", status=response.status_code, body=body.decode())
                    yield {"type": "error", "message": f"Provider returned {response.status_code}"}
                    return

                citations: list[str] = []

                async for line in response.aiter_lines():
                    if not line.startswith("data: "):
                        continue

                    data = line[6:]
                    if data.strip() == "[DONE]":
                        break

                    try:
                        chunk = json.loads(data)
                    except json.JSONDecodeError:
                        continue

                    # Perplexity puts citations in the final chunk
                    if "citations" in chunk:
                        citations = chunk["citations"]

                    choices = chunk.get("choices", [])
                    if not choices:
                        continue

                    delta = choices[0].get("delta", {})
                    content = delta.get("content")
                    if content:
                        yield {"type": "delta", "text": content}

                yield {"type": "done", "citations": citations}
