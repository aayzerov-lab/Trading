"""Core IB bridge -- connects to Interactive Brokers via ib_insync,
normalises position callbacks, enriches, persists, and publishes them.

Periodically refreshes portfolio market values and account summary data.
Fetches IB ContractDetails for GICS sector classification.
"""

from __future__ import annotations

import asyncio
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

import structlog
from ib_insync import IB, Contract, Fill, PortfolioItem, Position, Trade, util

from broker_bridge.config import Settings
from broker_bridge.db import update_enrichment, update_market_values, upsert_account_summary, upsert_execution, upsert_position
from broker_bridge.enrichment import (
    cache_contract_details,
    enrich,
    get_cached_conids,
    get_contract_cache,
    get_manual_override,
    save_contract_cache,
)
from broker_bridge.models import ExecutionEvent, PositionEvent
from broker_bridge.publisher import RedisPublisher

logger = structlog.get_logger()

_REDIS_CHANNEL = "positions"
_REDIS_ACCT_CHANNEL = "account_summary"
_REDIS_EXEC_CHANNEL = "executions"


class IBBridge:
    """Manages the IB Gateway connection, position streaming, and fan-out."""

    def __init__(
        self,
        settings: Settings,
        publisher: RedisPublisher,
    ) -> None:
        self._settings = settings
        self._publisher = publisher
        self._ib = IB()
        self._shutdown_event = asyncio.Event()
        self._contract_details_fetched: set[int] = set()
        self._pnl_subscribed_conids: set[int] = set()
        self._refresh_interval = 15  # seconds

    # ------------------------------------------------------------------
    # Connection with exponential back-off
    # ------------------------------------------------------------------

    def connect_with_backoff(self) -> None:
        """Connect to IB Gateway, retrying with exponential back-off.

        ib_insync uses its own event-loop integration, so ``ib.connect()``
        is called synchronously.  Back-off sequence: 1 s, 2 s, 4 s, ... up
        to 60 s.
        """
        delay = 1.0
        max_delay = 60.0

        while True:
            try:
                self._ib.connect(
                    host=self._settings.IB_HOST,
                    port=self._settings.IB_PORT,
                    clientId=self._settings.IB_CLIENT_ID,
                    readonly=True,
                )
                logger.info(
                    "ib_connected",
                    host=self._settings.IB_HOST,
                    port=self._settings.IB_PORT,
                    client_id=self._settings.IB_CLIENT_ID,
                )
                return
            except (ConnectionRefusedError, OSError, Exception) as exc:
                logger.warning(
                    "ib_connect_failed",
                    error=str(exc),
                    retry_in=delay,
                )
                util.sleep(delay)
                delay = min(delay * 2, max_delay)

    # ------------------------------------------------------------------
    # Position callback
    # ------------------------------------------------------------------

    def _on_position(self, position: Position) -> None:
        """Callback fired by ib_insync for every position update.

        ib_insync emits a single ``Position`` namedtuple with fields:
        account, contract, position, avgCost.

        Normalises the IB data into a ``PositionEvent``, enriches it, then
        schedules the async persistence and publishing work on the running
        event loop.
        """
        account = position.account
        contract = position.contract
        pos = position.position
        avg_cost = position.avgCost

        ts_utc = datetime.now(timezone.utc).isoformat()

        event = PositionEvent(
            ts_utc=ts_utc,
            account=account,
            conid=contract.conId if contract.conId else None,
            symbol=contract.symbol or "",
            sec_type=contract.secType or "",
            currency=contract.currency or "",
            exchange=contract.exchange or None,
            position=float(pos),
            avg_cost=float(avg_cost) if avg_cost else None,
        )

        # NOTE: Do NOT call reqContractDetails here — we are inside a
        # positionEvent callback (which fires during run_until_complete),
        # and reqContractDetails is itself synchronous/blocking.  Instead
        # contract details are fetched in the bulk pass after initial
        # position callbacks complete, and during periodic refreshes.

        event = enrich(event)

        logger.info(
            "position_received",
            account=account,
            symbol=event.symbol,
            conid=event.conid,
            position=event.position,
            sector=event.sector,
            country=event.country,
        )

        # Schedule the async DB + Redis work on the running loop.
        loop = asyncio.get_event_loop()
        loop.create_task(self._persist_and_publish(event))

    async def _persist_and_publish(self, event: PositionEvent) -> None:
        """Upsert position to Postgres and publish to Redis."""
        try:
            await upsert_position(event)
        except Exception:
            logger.exception("db_upsert_failed", symbol=event.symbol)

        try:
            await self._publisher.publish(_REDIS_CHANNEL, event.model_dump())
        except Exception:
            logger.exception("redis_publish_failed", symbol=event.symbol)

    # ------------------------------------------------------------------
    # Contract details fetching
    # ------------------------------------------------------------------

    def _fetch_contract_details(self, contract: Contract) -> None:
        """Fetch ContractDetails from IB for a single contract.

        Extracts industry, category, subcategory and caches the GICS
        mapping result.  Skips if already fetched.
        """
        conid = contract.conId
        if not conid or conid in self._contract_details_fetched:
            return

        try:
            details = self._ib.reqContractDetails(contract)
        except Exception:
            logger.exception("req_contract_details_error", conid=conid)
            return

        if details:
            d = details[0]
            industry = getattr(d, "industry", None) or ""
            category = getattr(d, "category", None) or ""
            subcategory = getattr(d, "subcategory", None) or ""
            exchange = contract.primaryExchange or contract.exchange or ""

            cache_contract_details(conid, industry, category, subcategory, exchange)

            logger.info(
                "contract_details_fetched",
                conid=conid,
                symbol=contract.symbol,
                industry=industry,
                category=category,
                subcategory=subcategory,
            )
        else:
            logger.debug("contract_details_empty", conid=conid, symbol=contract.symbol)

        self._contract_details_fetched.add(conid)

    def _fetch_all_contract_details(self) -> None:
        """Fetch contract details for all current positions.

        Iterates through ib.positions() and fetches details for each unique
        conid that has not already been fetched.  Includes a small delay
        between requests to be polite to the IB API.
        """
        try:
            positions = self._ib.positions()
        except Exception:
            logger.exception("fetch_all_positions_for_details_failed")
            return

        seen_conids: set[int] = set()
        cached_conids = get_cached_conids()
        fetched_count = 0

        for pos in positions:
            conid = pos.contract.conId
            if not conid:
                continue
            if conid in seen_conids:
                continue
            if conid in self._contract_details_fetched:
                continue
            if conid in cached_conids:
                # Already in persistent cache, just mark as fetched
                self._contract_details_fetched.add(conid)
                continue

            seen_conids.add(conid)

            try:
                self._fetch_contract_details(pos.contract)
                fetched_count += 1
            except Exception:
                logger.exception(
                    "contract_details_fetch_failed",
                    conid=conid,
                    symbol=pos.contract.symbol,
                )

            # Be polite to IB API
            self._ib.sleep(0.1)

        if fetched_count > 0:
            save_contract_cache()

        logger.info(
            "contract_details_bulk_fetch_complete",
            fetched=fetched_count,
            total_cached=len(self._contract_details_fetched),
        )

    # ------------------------------------------------------------------
    # Re-enrichment (update sector/country in DB from cache)
    # ------------------------------------------------------------------

    def _re_enrich_positions(self) -> None:
        """Update sector/country in the DB for all cached contract details.

        Called after the bulk contract details fetch to backfill positions
        that were initially saved with Unknown sector/country.

        Only updates positions where the contract cache has a known (non-Unknown)
        sector.  Positions that already received correct enrichment from
        security_master or sec_type auto-classification (crypto, ETFs) are
        left untouched.
        """
        cache = get_contract_cache()
        if not cache:
            return

        loop = asyncio.get_event_loop()
        count = 0
        skipped = 0
        for conid, details in cache.items():
            # Security master overrides always win
            override = get_manual_override(conid)
            if override is not None:
                sector = override.get("sector", "Unknown")
                country = override.get("country", "Unknown")
            else:
                sector = details.get("sector", "Unknown")
                country = details.get("country", "Unknown")

            # Skip if we still have Unknown sector
            if sector == "Unknown":
                skipped += 1
                continue

            loop.create_task(self._update_enrichment_safe(conid, sector, country))
            count += 1

        logger.info("re_enrichment_scheduled", count=count, skipped=skipped)

    async def _update_enrichment_safe(
        self, conid: int, sector: str, country: str
    ) -> None:
        """Safely update enrichment, catching exceptions."""
        try:
            await update_enrichment(conid, sector, country)
        except Exception:
            logger.exception("enrichment_update_failed", conid=conid)

    # ------------------------------------------------------------------
    # Portfolio refresh (market values)
    # ------------------------------------------------------------------

    def _refresh_portfolio(self) -> None:
        """Refresh market value data from IB portfolio.

        Reads ib.portfolio() and updates market_price, market_value,
        unrealized_pnl, realized_pnl, daily_pnl in the database.  Also
        fetches contract details for any new conids not yet in cache.

        Uses reqPnLSingle subscriptions to get exact daily P&L from IB.
        """
        try:
            items: list[PortfolioItem] = self._ib.portfolio()
        except Exception:
            logger.exception("portfolio_refresh_failed")
            return

        if not items:
            return

        loop = asyncio.get_event_loop()
        new_conids = False

        # Build a map of conId → dailyPnL from active PnLSingle subscriptions
        daily_pnl_map: dict[int, float | None] = {}
        try:
            for ps in self._ib.pnlSingle():
                if ps.conId and ps.dailyPnL is not None and ps.dailyPnL == ps.dailyPnL:
                    daily_pnl_map[ps.conId] = ps.dailyPnL
        except Exception:
            logger.debug("pnl_single_read_failed")

        # Subscribe to account-level PnL and upsert DailyPnL tag
        try:
            for pnl_obj in self._ib.pnl():
                if pnl_obj.dailyPnL is not None and pnl_obj.dailyPnL == pnl_obj.dailyPnL:
                    loop.create_task(
                        self._upsert_account_summary_safe(
                            account=pnl_obj.account,
                            tag="DailyPnL",
                            value=str(pnl_obj.dailyPnL),
                            currency="USD",
                        )
                    )
        except Exception:
            logger.debug("pnl_account_read_failed")

        for item in items:
            conid = item.contract.conId
            if not conid:
                continue

            # Subscribe to per-position PnL if not already
            if conid not in self._pnl_subscribed_conids:
                try:
                    self._ib.reqPnLSingle(item.account, "", conid)
                    self._pnl_subscribed_conids.add(conid)
                except Exception:
                    logger.debug("req_pnl_single_failed", conid=conid)

            # Update market values in DB (including daily P&L from IB)
            loop.create_task(
                self._update_market_values_safe(
                    account=item.account,
                    conid=conid,
                    market_price=item.marketPrice,
                    market_value=item.marketValue,
                    unrealized_pnl=item.unrealizedPNL,
                    realized_pnl=item.realizedPNL,
                    daily_pnl=daily_pnl_map.get(conid),
                )
            )

            # Fetch contract details for new conids
            if conid not in self._contract_details_fetched:
                cached_conids = get_cached_conids()
                if conid in cached_conids:
                    self._contract_details_fetched.add(conid)
                else:
                    try:
                        self._fetch_contract_details(item.contract)
                        new_conids = True
                    except Exception:
                        logger.exception(
                            "contract_details_from_portfolio_failed",
                            conid=conid,
                        )
                    self._ib.sleep(0.1)

        if new_conids:
            save_contract_cache()
            # Re-enrich so the new contract details update sector/country in DB
            self._re_enrich_positions()

        # Publish a summary update to Redis
        summary = {
            "type": "portfolio_refresh",
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "count": len(items),
        }
        loop.create_task(self._publish_safe(_REDIS_CHANNEL, summary))

        logger.debug("portfolio_refreshed", count=len(items), pnl_singles=len(daily_pnl_map))

    async def _update_market_values_safe(
        self,
        account: str,
        conid: int,
        market_price: float | None,
        market_value: float | None,
        unrealized_pnl: float | None,
        realized_pnl: float | None,
        daily_pnl: float | None = None,
    ) -> None:
        """Safely update market values, catching exceptions."""
        try:
            await update_market_values(
                account=account,
                conid=conid,
                market_price=market_price,
                market_value=market_value,
                unrealized_pnl=unrealized_pnl,
                realized_pnl=realized_pnl,
                daily_pnl=daily_pnl,
            )
        except Exception:
            logger.exception("market_values_update_failed", conid=conid)

    # ------------------------------------------------------------------
    # Account summary refresh
    # ------------------------------------------------------------------

    def _refresh_account_summary(self) -> None:
        """Refresh account summary data from IB.

        Reads ib.accountSummary() and upserts each value into the database.
        Publishes the full summary to Redis.
        """
        try:
            values = self._ib.accountSummary()
        except Exception:
            logger.exception("account_summary_refresh_failed")
            return

        if not values:
            return

        loop = asyncio.get_event_loop()
        summary_data: list[dict[str, Any]] = []

        for av in values:
            loop.create_task(
                self._upsert_account_summary_safe(
                    account=av.account,
                    tag=av.tag,
                    value=av.value,
                    currency=av.currency,
                )
            )
            summary_data.append({
                "account": av.account,
                "tag": av.tag,
                "value": av.value,
                "currency": av.currency,
            })

        # Publish to Redis
        payload = {
            "type": "account_summary",
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "values": summary_data,
        }
        loop.create_task(self._publish_safe(_REDIS_ACCT_CHANNEL, payload))

        logger.debug("account_summary_refreshed", count=len(values))

    async def _upsert_account_summary_safe(
        self,
        account: str,
        tag: str,
        value: str,
        currency: str | None,
    ) -> None:
        """Safely upsert account summary, catching exceptions."""
        try:
            await upsert_account_summary(
                account=account,
                tag=tag,
                value=value,
                currency=currency,
            )
        except Exception:
            logger.exception(
                "account_summary_upsert_failed",
                account=account,
                tag=tag,
            )

    async def _publish_safe(self, channel: str, data: dict[str, Any]) -> None:
        """Safely publish to Redis, catching exceptions."""
        try:
            await self._publisher.publish(channel, data)
        except Exception:
            logger.exception("redis_publish_failed", channel=channel)

    # ------------------------------------------------------------------
    # Execution / order tracking
    # ------------------------------------------------------------------

    def _trade_to_execution_event(self, trade: Trade) -> ExecutionEvent:
        """Convert an ib_insync Trade object to an ExecutionEvent."""
        contract = trade.contract
        order = trade.order
        os = trade.orderStatus

        # Use permId as exec_id (unique per order). orderId is 0 for
        # completed orders returned by reqCompletedOrders.
        perm = getattr(order, "permId", 0) or 0
        exec_id = f"{perm}" if perm else f"{order.orderId}_{contract.conId}"

        # Determine exec_time from the most recent fill, or use current time
        exec_time = datetime.now(timezone.utc).isoformat()
        if trade.fills:
            last_fill = trade.fills[-1]
            ft = getattr(last_fill.execution, "time", None)
            if ft is not None:
                exec_time = ft.isoformat() if hasattr(ft, "isoformat") else str(ft)

        # Aggregate commission from all fills (guard against IB sentinel ~1.8e308)
        total_commission: float | None = None
        if trade.fills:
            comms = [
                f.commissionReport.commission
                for f in trade.fills
                if f.commissionReport
                and f.commissionReport.commission is not None
                and f.commissionReport.commission < 1e9
            ]
            total_commission = sum(comms) if comms else None

        lmt = float(order.lmtPrice) if order.lmtPrice and order.lmtPrice < 1e9 else None

        # Prefer fill data over orderStatus — completed orders from
        # reqCompletedOrders have empty orderStatus but populated fills
        # once reqExecutions has been called.
        if trade.fills:
            last_exec = trade.fills[-1].execution
            filled = float(last_exec.cumQty) if last_exec.cumQty else 0.0
            avg_price = float(last_exec.avgPrice) if last_exec.avgPrice else None
        else:
            filled = float(os.filled) if os.filled is not None and os.filled > 0 else 0.0
            avg_price = float(os.avgFillPrice) if os.avgFillPrice is not None and os.avgFillPrice > 0 else None

        # totalQuantity: prefer from order, fall back to filled qty
        qty = float(order.totalQuantity) if order.totalQuantity is not None and float(order.totalQuantity) > 0 else filled

        return ExecutionEvent(
            exec_id=exec_id,
            account=order.account or "",
            conid=contract.conId if contract.conId else None,
            symbol=contract.symbol or "",
            sec_type=contract.secType or "",
            currency=contract.currency or "",
            exchange=contract.exchange or None,
            side=order.action,
            order_type=order.orderType,
            quantity=qty,
            filled_qty=filled,
            avg_fill_price=avg_price,
            lmt_price=lmt,
            commission=total_commission,
            status=os.status,
            order_ref=order.orderRef or None,
            exec_time=exec_time,
        )

    def _refresh_executions(self) -> None:
        """Refresh execution data from IB fills (grouped by permId).

        Uses ``ib.fills()`` which is the most reliable source of execution
        data — it works for both active orders and completed orders returned
        by ``reqCompletedOrders`` + ``reqExecutions``.
        """
        try:
            fills = self._ib.fills()
        except Exception:
            logger.exception("executions_refresh_failed")
            return

        if not fills:
            return

        # Group fills by permId to produce one ExecutionEvent per order
        from collections import defaultdict
        by_perm: dict[int, list] = defaultdict(list)
        for fill in fills:
            perm = fill.execution.permId
            if perm:
                by_perm[perm].append(fill)

        loop = asyncio.get_event_loop()
        count = 0

        for perm_id, order_fills in by_perm.items():
            try:
                event = self._fills_to_execution_event(perm_id, order_fills)
                loop.create_task(self._persist_and_publish_execution(event))
                count += 1
            except Exception:
                logger.exception(
                    "execution_conversion_failed",
                    perm_id=perm_id,
                )

        logger.debug("executions_refreshed", count=count)

    def _fills_to_execution_event(self, perm_id: int, fills: list) -> ExecutionEvent:
        """Build an ExecutionEvent from a group of fills sharing the same permId."""
        last_fill = fills[-1]
        contract = last_fill.contract
        execution = last_fill.execution

        # Use the last fill's cumQty/avgPrice (they are running totals)
        filled = float(execution.cumQty) if execution.cumQty else 0.0
        avg_price = float(execution.avgPrice) if execution.avgPrice else None

        # Determine total quantity — cumQty on the last fill IS the total for
        # fully-filled orders; for partial fills it's what's been filled so far
        qty = filled

        # Aggregate commission from all fills (guard against IB sentinel)
        comms = [
            f.commissionReport.commission
            for f in fills
            if f.commissionReport
            and f.commissionReport.commission is not None
            and f.commissionReport.commission < 1e9
        ]
        total_commission = sum(comms) if comms else None

        # Exec time from last fill
        ft = getattr(execution, "time", None)
        exec_time = ft.isoformat() if ft and hasattr(ft, "isoformat") else datetime.now(timezone.utc).isoformat()

        # Side: execution.side is "BOT" or "SLD"
        side = "BUY" if execution.side == "BOT" else "SELL"

        # Order type from the matching trade (if any), otherwise "?"
        order_type = "?"
        order_ref = None
        lmt: float | None = None
        status = "Filled"
        for trade in self._ib.trades():
            if getattr(trade.order, "permId", 0) == perm_id:
                order_type = trade.order.orderType or "?"
                order_ref = trade.order.orderRef or None
                lp = trade.order.lmtPrice
                lmt = float(lp) if lp and lp < 1e9 else None
                status = trade.orderStatus.status or "Filled"
                # Also use order's totalQuantity if available
                tq = float(trade.order.totalQuantity) if trade.order.totalQuantity else 0.0
                if tq > 0:
                    qty = tq
                break

        return ExecutionEvent(
            exec_id=f"{perm_id}",
            account=execution.acctNumber or "",
            conid=contract.conId if contract.conId else None,
            symbol=contract.symbol or "",
            sec_type=contract.secType or "",
            currency=contract.currency or "",
            exchange=contract.exchange or None,
            side=side,
            order_type=order_type,
            quantity=qty,
            filled_qty=filled,
            avg_fill_price=avg_price,
            lmt_price=lmt,
            commission=total_commission,
            status=status,
            order_ref=order_ref,
            exec_time=exec_time,
        )

    async def _persist_and_publish_execution(self, event: ExecutionEvent) -> None:
        """Upsert execution to Postgres and publish to Redis."""
        try:
            await upsert_execution(event)
        except Exception:
            logger.exception("execution_upsert_failed", exec_id=event.exec_id)

        try:
            await self._publisher.publish(_REDIS_EXEC_CHANNEL, event.model_dump())
        except Exception:
            logger.exception("execution_publish_failed", exec_id=event.exec_id)

    def _on_exec_details(self, trade: Trade, fill: Fill) -> None:
        """Callback fired by ib_insync for every new execution fill."""
        try:
            perm_id = fill.execution.permId
            if not perm_id:
                return
            # Collect all fills for this permId from the IB client
            all_fills = [f for f in self._ib.fills() if f.execution.permId == perm_id]
            event = self._fills_to_execution_event(perm_id, all_fills)
            loop = asyncio.get_event_loop()
            loop.create_task(self._persist_and_publish_execution(event))
        except Exception:
            logger.exception("on_exec_details_failed")

    # ------------------------------------------------------------------
    # Graceful shutdown
    # ------------------------------------------------------------------

    def _request_shutdown(self) -> None:
        """Signal handler that sets the shutdown event."""
        logger.info("shutdown_requested")
        self._shutdown_event.set()

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Connect to IB, subscribe to positions, and block until shutdown.

        Uses ``ib_insync.util.run()`` / the ib_insync event-loop integration.
        Registers SIGINT and SIGTERM for graceful shutdown.
        """
        # Register signal handlers (Unix only; Windows uses KeyboardInterrupt)
        if sys.platform != "win32":
            loop = asyncio.get_event_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, self._request_shutdown)

        # Connect (with back-off)
        self.connect_with_backoff()

        # Register position callback
        self._ib.positionEvent += self._on_position

        # Request current positions -- fires positionEvent for each one
        self._ib.reqPositions()
        logger.info("positions_requested")

        # Subscribe to account summary (ib_insync takes no args;
        # it subscribes to all tags internally).  The cached values
        # are read later via self._ib.accountSummary().
        self._ib.reqAccountSummary()
        logger.info("account_summary_requested")

        # Subscribe to account-level P&L for daily P&L tracking
        try:
            accounts = self._ib.managedAccounts()
            for acct in accounts:
                self._ib.reqPnL(acct)
            logger.info("pnl_subscribed", accounts=accounts)
        except Exception:
            logger.exception("pnl_subscription_failed")

        # Request completed orders for today and subscribe to execution events
        try:
            self._ib.reqCompletedOrders(apiOnly=False)
            logger.info("completed_orders_requested")
        except Exception:
            logger.exception("completed_orders_request_failed")

        # Request today's execution reports — this populates fills on Trade
        # objects created by reqCompletedOrders (which only have order data).
        try:
            fills = self._ib.reqExecutions()
            logger.info("executions_requested", fill_count=len(fills))
        except Exception:
            logger.exception("req_executions_failed")

        self._ib.execDetailsEvent += self._on_exec_details

        # Allow initial position callbacks to fire, then fetch contract details
        self._ib.sleep(2)
        self._fetch_all_contract_details()

        # Re-enrich positions now that contract details are cached
        self._re_enrich_positions()
        # Initial execution sync
        self._refresh_executions()
        # Give tasks a moment to execute
        self._ib.sleep(1)

        # Block until shutdown is requested, periodically refreshing data
        last_refresh = 0.0
        try:
            while not self._shutdown_event.is_set():
                self._ib.sleep(1)
                now = time.time()
                if now - last_refresh >= self._refresh_interval:
                    self._refresh_portfolio()
                    self._refresh_account_summary()
                    self._refresh_executions()
                    last_refresh = now
        finally:
            self._cleanup()

    def _cleanup(self) -> None:
        """Disconnect from IB Gateway and persist caches."""
        # Save contract cache on shutdown
        try:
            save_contract_cache()
        except Exception:
            logger.exception("contract_cache_save_on_shutdown_failed")

        if self._ib.isConnected():
            self._ib.disconnect()
            logger.info("ib_disconnected")
