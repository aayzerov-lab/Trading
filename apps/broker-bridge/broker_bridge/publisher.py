"""Redis Pub/Sub publisher for position events."""

from __future__ import annotations

import json
from typing import Any

import redis.asyncio as aioredis
import structlog

logger = structlog.get_logger()


class RedisPublisher:
    """Async Redis publisher that serialises dicts to JSON before publishing."""

    def __init__(self, redis_url: str) -> None:
        self._redis_url = redis_url
        self._redis: aioredis.Redis | None = None

    async def connect(self) -> None:
        """Open the Redis connection."""
        self._redis = aioredis.from_url(
            self._redis_url,
            decode_responses=True,
        )
        # Verify connectivity
        await self._redis.ping()
        logger.info("redis_connected", url=self._redis_url)

    async def publish(self, channel: str, data: dict[str, Any]) -> None:
        """Serialise *data* to JSON and publish on *channel*."""
        if self._redis is None:
            raise RuntimeError("RedisPublisher is not connected. Call connect() first.")
        payload = json.dumps(data, default=str)
        await self._redis.publish(channel, payload)
        logger.debug("redis_published", channel=channel)

    async def close(self) -> None:
        """Close the underlying Redis connection."""
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None
            logger.info("redis_closed")
