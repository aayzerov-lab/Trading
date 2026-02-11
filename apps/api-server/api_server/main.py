"""FastAPI application for the Trading Workstation API server.

Provides REST endpoints for portfolio data and a WebSocket stream that
forwards real-time updates from the Redis ``positions`` and
``account_summary`` channels.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import redis.asyncio as aioredis
import structlog
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from api_server.config import get_settings
from api_server.db import close_engine, get_account_summary, get_engine, get_positions
from api_server.exposures import compute_exposures
from api_server.routers import macro, risk

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Redis connection (module-level, managed by lifespan)
# ---------------------------------------------------------------------------

_redis: aioredis.Redis | None = None

# ---------------------------------------------------------------------------
# Scheduler task (module-level, managed by lifespan)
# ---------------------------------------------------------------------------

_scheduler_task: asyncio.Task | None = None


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def get_redis() -> aioredis.Redis | None:
    """Return the module-level Redis connection."""
    return _redis


async def _run_scheduler() -> None:
    """Background task to run daily data updates and risk recomputation.

    Runs once per day at market close (4:30 PM ET / 21:30 UTC) to:
    1. Fetch updated price data for all positions
    2. Fetch updated FRED macro data
    3. Check if portfolio has changed and trigger risk recomputation if needed

    Also publishes events to Redis for real-time updates.
    """
    from datetime import datetime, timedelta, timezone

    from shared.data.scheduler import run_daily_data_update, check_and_trigger_risk_recompute
    from shared.db.engine import get_shared_engine

    logger.info("scheduler_loop_started")

    while True:
        try:
            # Calculate next run time (4:30 PM ET = 21:30 UTC)
            now = datetime.now(timezone.utc)
            target_time = now.replace(hour=21, minute=30, second=0, microsecond=0)

            # If we've passed today's target, schedule for tomorrow
            if now >= target_time:
                target_time += timedelta(days=1)

            wait_seconds = (target_time - now).total_seconds()
            logger.info(
                "scheduler_next_run",
                next_run=target_time.isoformat(),
                wait_seconds=wait_seconds,
            )

            # Wait until target time
            await asyncio.sleep(wait_seconds)

            # Run daily update
            logger.info("scheduler_triggering_daily_update")
            engine = get_shared_engine()
            await run_daily_data_update(engine=engine, redis_client=_redis)

            # Publish event to Redis
            if _redis is not None:
                await _redis.publish(
                    "data_updated",
                    json.dumps(
                        {
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "status": "completed",
                        }
                    ),
                )

            logger.info("scheduler_daily_update_completed")

            # Check if risk recomputation is needed
            result = await check_and_trigger_risk_recompute(
                engine=engine, redis_client=_redis
            )
            if result.get("recompute_needed") and _redis is not None:
                await _redis.publish(
                    "risk_recompute",
                    json.dumps(
                        {
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "reason": result.get("reason", "portfolio_changed"),
                        }
                    ),
                )
                logger.info("scheduler_triggered_risk_recompute")

        except asyncio.CancelledError:
            logger.info("scheduler_cancelled")
            raise
        except Exception:
            logger.exception("scheduler_error")
            # Wait 1 hour before retrying on error
            await asyncio.sleep(3600)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage startup / shutdown resources."""
    global _redis, _scheduler_task
    settings = get_settings()

    # Startup ---------------------------------------------------------------
    await get_engine(settings.POSTGRES_URL)
    logger.info("db_engine_ready")

    # Initialize Phase 1 database tables
    try:
        from shared.db.engine import init_phase1_db

        await init_phase1_db(settings.POSTGRES_URL)
        logger.info("phase1_db_initialized")
    except Exception:
        logger.exception("phase1_db_init_failed")

    _redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    await _redis.ping()
    logger.info("redis_connected", url=settings.REDIS_URL)

    # Start background scheduler for daily data updates
    _scheduler_task = asyncio.create_task(_run_scheduler())
    logger.info("scheduler_started")

    yield

    # Shutdown --------------------------------------------------------------
    if _scheduler_task is not None:
        _scheduler_task.cancel()
        try:
            await _scheduler_task
        except asyncio.CancelledError:
            pass
        logger.info("scheduler_stopped")

    if _redis is not None:
        await _redis.aclose()
        _redis = None
        logger.info("redis_closed")

    await close_engine()

    # Close shared engine used by risk/data services
    try:
        from shared.db.engine import close_shared_engine

        await close_shared_engine()
        logger.info("shared_engine_closed")
    except Exception:
        logger.exception("shared_engine_close_failed")

    logger.info("shutdown_complete")


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(title="Trading Workstation API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(risk.router)
app.include_router(macro.router)


# ---------------------------------------------------------------------------
# REST endpoints
# ---------------------------------------------------------------------------


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness / readiness probe."""
    return {"status": "ok"}


@app.get("/portfolio")
async def portfolio() -> list[dict]:
    """Return all current positions."""
    try:
        positions = await get_positions()
        # Serialise datetime objects to ISO strings for JSON
        for pos in positions:
            for key, value in pos.items():
                if hasattr(value, "isoformat"):
                    pos[key] = value.isoformat()
        return positions
    except Exception:
        logger.exception("portfolio_fetch_failed")
        raise


@app.get("/portfolio/exposures")
async def portfolio_exposures(method: str = "market_value") -> dict:
    """Return sector and country exposure weights."""
    try:
        positions = await get_positions()
        if method not in ("market_value", "cost_basis"):
            method = "market_value"
        return compute_exposures(positions, method=method)
    except Exception:
        logger.exception("exposure_computation_failed")
        raise


@app.get("/account/summary")
async def account_summary() -> list[dict]:
    """Return account summary tags and values."""
    try:
        rows = await get_account_summary()
        # Serialise datetime objects to ISO strings for JSON
        for row in rows:
            for key, value in row.items():
                if hasattr(value, "isoformat"):
                    row[key] = value.isoformat()
        return rows
    except Exception:
        logger.exception("account_summary_fetch_failed")
        raise


# ---------------------------------------------------------------------------
# Channel-to-type mapping for WebSocket forwarding
# ---------------------------------------------------------------------------

_CHANNEL_TYPE_MAP: dict[str, str] = {
    "positions": "position",
    "account_summary": "account_summary",
    "data_updated": "data_updated",
    "risk_recompute": "risk_recompute",
    "risk_updated": "risk_updated",
}


# ---------------------------------------------------------------------------
# WebSocket stream
# ---------------------------------------------------------------------------


@app.websocket("/stream")
async def stream(websocket: WebSocket) -> None:
    """Stream real-time updates from Redis pub/sub to the client.

    Subscribes to both ``positions`` and ``account_summary`` channels and
    forwards each message wrapped with a ``type`` field indicating the
    source channel.
    """
    await websocket.accept()
    logger.info("websocket_connected", client=str(websocket.client))

    if _redis is None:
        logger.error("websocket_no_redis")
        await websocket.close(code=1011, reason="Redis not available")
        return

    # Each WebSocket connection gets its own pubsub instance so that
    # unsubscribing on disconnect does not affect other connections.
    pubsub = _redis.pubsub()
    listener_task: asyncio.Task | None = None

    channels = list(_CHANNEL_TYPE_MAP.keys())

    try:
        await pubsub.subscribe(*channels)
        logger.info("pubsub_subscribed", channels=channels)

        async def _forward_messages() -> None:
            """Read from Redis pubsub and forward to the WebSocket client."""
            try:
                async for message in pubsub.listen():
                    if message["type"] != "message":
                        continue
                    data = message["data"]
                    channel = message["channel"]
                    msg_type = _CHANNEL_TYPE_MAP.get(channel, channel)
                    # data is already a decoded string (decode_responses=True)
                    # Validate it is proper JSON before forwarding
                    try:
                        parsed = json.loads(data)
                    except (json.JSONDecodeError, TypeError):
                        logger.warning("pubsub_invalid_json", data=data, channel=channel)
                        continue
                    await websocket.send_json({"type": msg_type, "data": parsed})
            except asyncio.CancelledError:
                # Normal shutdown path
                pass
            except Exception:
                logger.exception("pubsub_listener_error")

        listener_task = asyncio.create_task(_forward_messages())

        # Keep the WebSocket handler alive by waiting for client messages
        # (or disconnect).  We discard any inbound messages from the client.
        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        logger.info("websocket_disconnected", client=str(websocket.client))
    except Exception:
        logger.exception("websocket_error")
    finally:
        if listener_task is not None:
            listener_task.cancel()
            try:
                await listener_task
            except asyncio.CancelledError:
                pass
        await pubsub.unsubscribe(*channels)
        await pubsub.aclose()
        logger.info("pubsub_cleaned_up", client=str(websocket.client))
