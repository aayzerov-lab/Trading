"""Entry point for the broker-bridge service.

Initialises configuration, structured logging, database, Redis, and the
IB bridge, then runs the event loop until a shutdown signal is received.

Uses a single event loop without nesting — ib_insync drives the loop
via run_until_complete() calls internally (ib.sleep, ib.connect, etc.),
and async tasks created by callbacks execute during those windows.
"""

from __future__ import annotations

import asyncio
import sys

# Ensure an event loop exists before importing ib_insync / eventkit.
# Python 3.12+ removed the implicit loop creation on get_event_loop().
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import structlog

from broker_bridge.bridge import IBBridge
from broker_bridge.config import get_settings
from broker_bridge.db import close_db, init_db
from broker_bridge.publisher import RedisPublisher


def _configure_structlog() -> None:
    """Set up structlog with human-readable console output."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(0),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def main() -> None:
    """Entry point – wires everything together and starts the bridge.

    Creates a single event loop and hands it to ib_insync.  The loop
    is NOT wrapped by asyncio.run(), so ib_insync's internal
    run_until_complete() calls are non-nested and work cleanly on
    Python 3.11+ without nest_asyncio.
    """
    settings = get_settings()
    _configure_structlog()

    logger = structlog.get_logger()
    logger.info("broker_bridge_starting")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # --- Database ----------------------------------------------------------
    loop.run_until_complete(init_db(settings.POSTGRES_URL))

    # --- Redis -------------------------------------------------------------
    publisher = RedisPublisher(settings.REDIS_URL)
    loop.run_until_complete(publisher.connect())

    # --- IB Bridge ---------------------------------------------------------
    bridge = IBBridge(settings=settings, publisher=publisher)

    try:
        # bridge.run() is synchronous — ib_insync drives the event loop
        # via ib.sleep() / ib.connect(), which call run_until_complete().
        # Async tasks (DB upserts, Redis publishes) created by callbacks
        # execute during those windows.
        bridge.run()
    except KeyboardInterrupt:
        logger.info("keyboard_interrupt")
    finally:
        loop.run_until_complete(publisher.close())
        loop.run_until_complete(close_db())
        logger.info("broker_bridge_stopped")
        loop.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
