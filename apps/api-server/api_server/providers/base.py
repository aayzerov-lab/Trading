"""Abstract base class for AI chat providers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import AsyncIterator


class AIProvider(ABC):
    """Provider interface for streaming chat completions.

    Implementations must yield dicts:
      {"type": "delta", "text": "..."}      — streamed text chunk
      {"type": "done", "citations": [...]}  — final event with source URLs
    """

    @abstractmethod
    async def stream_chat(
        self, system_prompt: str, messages: list[dict],
        *, web_search: bool = True,
    ) -> AsyncIterator[dict]:
        ...
