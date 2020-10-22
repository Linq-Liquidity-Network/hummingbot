from collections import defaultdict
from libc.stdint cimport int64_t
import aiohttp
from aiokafka import (
    AIOKafkaConsumer,
    ConsumerRecord
)
import asyncio
from async_timeout import timeout
from novadax import RequestClient as NovaClient
from decimal import Decimal
from functools import partial
import logging
import pandas as pd
import re
import time
from typing import (
    Any,
    Dict,
    List,
    AsyncIterable,
    Optional,
    Coroutine,
    Tuple,
)

import conf
from hummingbot.core.utils.asyncio_throttle import Throttler
from hummingbot.core.utils.async_call_scheduler import AsyncCallScheduler
from hummingbot.core.clock cimport Clock
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.connector.exchange.novadax.novadax_api_order_book_data_source import NovadaxAPIOrderBookDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.core.event.events import (
    MarketEvent,
    BuyOrderCompletedEvent,
    SellOrderCompletedEvent,
    OrderFilledEvent,
    OrderCancelledEvent,
    BuyOrderCreatedEvent,
    SellOrderCreatedEvent,
    MarketTransactionFailureEvent,
    MarketOrderFailureEvent,
    OrderType,
    TradeType,
    TradeFee
)
from hummingbot.connector.exchange_base cimport ExchangeBase
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.data_type.order_book_tracker import OrderBookTrackerDataSourceType
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.connector.exchange.novadax.novadax_order_book_tracker import NovadaxOrderBookTracker
from hummingbot.connector.exchange.novadax.novadax_user_stream_tracker import NovadaxUserStreamTracker
from hummingbot.connector.exchange.novadax.novadax_in_flight_order import NovadaxInFlightOrder
from hummingbot.core.data_type.user_stream_tracker import UserStreamTrackerDataSourceType
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.transaction_tracker import TransactionTracker
from hummingbot.connector.trading_rule cimport TradingRule
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce
from hummingbot.client.config.fee_overrides_config_map import fee_overrides_config_map

s_logger = None
s_decimal_0 = Decimal(0)
s_decimal_NaN = Decimal("NaN")

cdef class NovadaxExchangeTransactionTracker(TransactionTracker):
    cdef:
        NovadaxExchange _owner

    def __init__(self, owner: NovadaxExchange):
        super().__init__()
        self._owner = owner

    cdef c_did_timeout_tx(self, str tx_id):
        TransactionTracker.c_did_timeout_tx(self, tx_id)
        self._owner.c_did_timeout_tx(tx_id)


cdef class NovadaxExchange(ExchangeBase):
    MARKET_RECEIVED_ASSET_EVENT_TAG = MarketEvent.ReceivedAsset.value
    MARKET_BUY_ORDER_COMPLETED_EVENT_TAG = MarketEvent.BuyOrderCompleted.value
    MARKET_SELL_ORDER_COMPLETED_EVENT_TAG = MarketEvent.SellOrderCompleted.value
    MARKET_ORDER_CANCELLED_EVENT_TAG = MarketEvent.OrderCancelled.value
    MARKET_TRANSACTION_FAILURE_EVENT_TAG = MarketEvent.TransactionFailure.value
    MARKET_ORDER_FAILURE_EVENT_TAG = MarketEvent.OrderFailure.value
    MARKET_ORDER_FILLED_EVENT_TAG = MarketEvent.OrderFilled.value
    MARKET_BUY_ORDER_CREATED_EVENT_TAG = MarketEvent.BuyOrderCreated.value
    MARKET_SELL_ORDER_CREATED_EVENT_TAG = MarketEvent.SellOrderCreated.value

    API_CALL_TIMEOUT = 10.0
    SHORT_POLL_INTERVAL = 5.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    LONG_POLL_INTERVAL = 120.0
    NOVADAX_TRADE_TOPIC_NAME = "novadax-trade.serialized"
    NOVADAX_USER_STREAM_TOPIC_NAME = "novadax-user-stream.serialized"

    ORDER_NOT_EXIST_CONFIRMATION_COUNT = 3

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global s_logger
        if s_logger is None:
            s_logger = logging.getLogger(__name__)
        return s_logger

    def __init__(self,
                 novadax_api_key: str = None,
                 novadax_api_secret: str = None,
                 novadax_uid: str = None,
                 order_book_tracker_data_source_type: OrderBookTrackerDataSourceType =
                 OrderBookTrackerDataSourceType.EXCHANGE_API,
                 user_stream_tracker_data_source_type: UserStreamTrackerDataSourceType =
                 UserStreamTrackerDataSourceType.EXCHANGE_API,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True):

        super().__init__()
        self._trading_required = trading_required
        self._order_book_tracker = NovadaxOrderBookTracker(data_source_type=order_book_tracker_data_source_type,
                                                           trading_pairs=trading_pairs)
        self._novadax_client = NovaClient(novadax_api_key, novadax_api_secret)
        self._user_stream_tracker = NovadaxUserStreamTracker(
            data_source_type=user_stream_tracker_data_source_type, novadax_client=self._novadax_client, novadax_uid=novadax_uid)
        self._ev_loop = asyncio.get_event_loop()
        self._poll_notifier = asyncio.Event()
        self._last_timestamp = 0
        self._in_flight_orders = {}  # Dict[client_order_id:str, novadaxInFlightOrder]
        self._order_not_found_records = {}  # Dict[client_order_id:str, count:int]
        self._tx_tracker = NovadaxExchangeTransactionTracker(self)
        self._trading_rules = {}  # Dict[trading_pair:str, TradingRule]
        self._last_update_trade_fees_timestamp = 0
        self._data_source_type = order_book_tracker_data_source_type
        self._status_polling_task = None
        self._user_stream_event_listener_task = None
        self._trading_rules_polling_task = None
        self._async_scheduler = AsyncCallScheduler(call_interval=0.5)
        self._last_poll_timestamp = 0
        self._throttler = Throttler((10.0, 1.0))

    @property
    def name(self) -> str:
        return "novadax"

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    @property
    def novadax_client(self) -> NovaClient:
        return self._novadax_client

    @property
    def trading_rules(self) -> Dict[str, TradingRule]:
        return self._trading_rules

    @property
    def in_flight_orders(self) -> Dict[str, NovadaxInFlightOrder]:
        return self._in_flight_orders

    @property
    def limit_orders(self) -> List[LimitOrder]:
        return [
            in_flight_order.to_limit_order()
            for in_flight_order in self._in_flight_orders.values()
        ]

    @property
    def tracking_states(self) -> Dict[str, any]:
        return {
            key: value.to_json()
            for key, value in self._in_flight_orders.items()
        }

    @property
    def order_book_tracker(self) -> NovadaxOrderBookTracker:
        return self._order_book_tracker

    @property
    def user_stream_tracker(self) -> NovadaxUserStreamTracker:
        return self._user_stream_tracker

    def restore_tracking_states(self, saved_states: Dict[str, any]):
        self._in_flight_orders.update({
            key: NovadaxInFlightOrder.from_json(value)
            for key, value in saved_states.items()
        })

    async def get_active_exchange_markets(self) -> pd.DataFrame:
        return await NovadaxAPIOrderBookDataSource.get_active_exchange_markets()

    async def schedule_async_call(
            self,
            coro: Coroutine,
            timeout_seconds: float,
            app_warning_msg: str = "novadax API call failed. Check API key and network connection.") -> any:
        return await self._async_scheduler.schedule_async_call(coro, timeout_seconds, app_warning_msg=app_warning_msg)

    async def query_url(self, url, request_weight: int = 1) -> any:
        async with self._throttler.weighted_task(request_weight=request_weight):
            async with aiohttp.ClientSession() as client:
                async with client.get(url, timeout=self.API_CALL_TIMEOUT) as response:
                    if response.status != 200:
                        raise IOError(f"Error fetching data from {url}. HTTP status is {response.status}.")
                    data = await response.json()
                    return data

    async def _update_balances(self):
        cdef:
            dict account_info
            list balances
            str asset_name
            set local_asset_names = set(self._account_balances.keys())
            set remote_asset_names = set()
            set asset_names_to_remove
        account_balances = self._novadax_client.get_account_balance()

        for balance_entry in account_balances["data"]:
            asset_name = balance_entry["currency"]
            available_balance = Decimal(balance_entry["available"])
            total_balance = available_balance + Decimal(balance_entry["hold"]) 
            self._account_available_balances[asset_name] = available_balance
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    cdef object c_get_fee(self,
                          str base_currency,
                          str quote_currency,
                          object order_type,
                          object order_side,
                          object amount,
                          object price):
        cdef:
            object maker_trade_fee = Decimal("0.001")
            object taker_trade_fee = Decimal("0.001")
            str trading_pair = base_currency + quote_currency

        if order_type is OrderType.LIMIT and fee_overrides_config_map["novadax_maker_fee"].value is not None:
            return TradeFee(percent=fee_overrides_config_map["novadax_maker_fee"].value / Decimal("100"))
        if order_type is OrderType.MARKET and fee_overrides_config_map["novadax_taker_fee"].value is not None:
            return TradeFee(percent=fee_overrides_config_map["novadax_taker_fee"].value / Decimal("100"))

        return TradeFee(percent=maker_trade_fee if order_type is OrderType.LIMIT else taker_trade_fee)

    async def _update_trading_rules(self):
        cdef:
            int64_t last_tick = <int64_t>(self._last_timestamp / 60.0)
            int64_t current_tick = <int64_t>(self._current_timestamp / 60.0)
        if current_tick > last_tick or len(self._trading_rules) < 1:
            exchange_info = self._novadax_client.list_symbols()
            trading_rules_list = self._format_trading_rules(exchange_info)
            self._trading_rules.clear()
            for trading_rule in trading_rules_list:
                self._trading_rules[trading_rule.trading_pair] = trading_rule

    def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Example:
        {
          "code": "A10000",
          "data": [
              {
                  "symbol": "BTC_BRL",
                  "baseCurrency": "BTC",
                  "quoteCurrency": "BRL",
                  "amountPrecision": 4,
                  "pricePrecision": 2,
                  "valuePrecision": 4,
                  "minOrderAmount": "0.001",
                  "minOrderValue": "5",
              },
              {
                  "symbol": "ETH_BRL",
                  "baseCurrency": "ETH",
                  "quoteCurrency": "BRL",
                  "amountPrecision": 4,
                  "pricePrecision": 2,
                  "valuePrecision": 4,
                  "minOrderAmount": "0.01",
                  "minOrderValue": "5"
              }
          ],
          "message": "Success"
        }

        """
        cdef:
            list trading_pair_rules = exchange_info_dict.get("data", [])
            list retval = []
        for rule in trading_pair_rules:
            try:
                trading_pair = rule.get("symbol")

                min_order_size = Decimal(rule.get("minOrderAmount"))
                min_price_increment = Decimal(f"1e-{rule.get('pricePrecision')}")
                min_base_amount_increment=Decimal(f"1e-{rule.get('amountPrecision')}")
                min_notional = Decimal(rule.get("minOrderValue"))

                retval.append(
                     TradingRule(
                        trading_pair=trading_pair,
                        min_order_size = min_order_size,
                        min_price_increment=min_price_increment,
                        min_base_amount_increment=min_base_amount_increment,
                        min_notional_size = min_notional,
                        supports_limit_orders = True,
                        supports_market_orders = False
                    )
                )

            except Exception:
                self.logger().error(f"Error parsing the trading pair rule {rule}. Skipping.", exc_info=True)
        return retval

    async def _update_order_status(self):
        cdef:
            # This is intended to be a backup measure to close straggler orders, in case novadax's user stream events
            # are not working.
            # The minimum poll interval for order status is 10 seconds.
            int64_t last_tick = <int64_t>(self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)
            int64_t current_tick = <int64_t>(self._current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)

        if current_tick > last_tick and len(self._in_flight_orders) > 0:
            tracked_orders = list(self._in_flight_orders.values())
            self.logger().debug("Polling for order status updates of %d orders.", len(tracked_orders))
            results = [self._novadax_client.get_order(o.exchange_order_id) for o in tracked_orders]
            for order_details, tracked_order in zip(results, tracked_orders):
                client_order_id = tracked_order.client_order_id
                # If the order has already been cancelled or has failed do nothing
                if client_order_id not in self._in_flight_orders:
                    continue

                if isinstance(order_details, Exception):
                    if order_details.message == "Order not found":
                        self._order_not_found_records[client_order_id] = \
                            self._order_not_found_records.get(client_order_id, 0) + 1
                        if self._order_not_found_records[client_order_id] < self.ORDER_NOT_EXIST_CONFIRMATION_COUNT:
                            # Wait until the order not found error have repeated a few times before actually treating
                            # it as failed. See: https://github.com/CoinAlpha/hummingbot/issues/601
                            continue
                        self.c_trigger_event(
                            self.MARKET_ORDER_FAILURE_EVENT_TAG,
                            MarketOrderFailureEvent(self._current_timestamp, client_order_id, tracked_order.order_type)
                        )
                        self.c_stop_tracking_order(client_order_id)
                    else:
                        self.logger().network(
                            f"Error fetching status update for the order {client_order_id}: {order_details}.",
                            app_warning_msg=f"Failed to fetch status update for the order {client_order_id}."
                        )
                    continue

                # Update order execution status
                tracked_order.last_state = order_details["data"]["status"]
                order_type = OrderType.LIMIT if order_details["data"]["type"] == "LIMIT" else OrderType.MARKET
                executed_amount_base = Decimal(order_details["data"]["filledAmount"])
                executed_amount_quote = Decimal(order_details["data"]["filledValue"])

                if tracked_order.is_done:
                    if not tracked_order.is_failure:
                        if tracked_order.trade_type is TradeType.BUY:
                            self.logger().info(f"The market buy order {tracked_order.client_order_id} has completed "
                                               f"according to order status API.")
                            self.c_trigger_event(self.MARKET_BUY_ORDER_COMPLETED_EVENT_TAG,
                                                 BuyOrderCompletedEvent(self._current_timestamp,
                                                                        client_order_id,
                                                                        tracked_order.base_asset,
                                                                        tracked_order.quote_asset,
                                                                        (tracked_order.fee_asset
                                                                         or tracked_order.base_asset),
                                                                        executed_amount_base,
                                                                        executed_amount_quote,
                                                                        tracked_order.fee_paid,
                                                                        order_type))
                        else:
                            self.logger().info(f"The market sell order {client_order_id} has completed "
                                               f"according to order status API.")
                            self.c_trigger_event(self.MARKET_SELL_ORDER_COMPLETED_EVENT_TAG,
                                                 SellOrderCompletedEvent(self._current_timestamp,
                                                                         client_order_id,
                                                                         tracked_order.base_asset,
                                                                         tracked_order.quote_asset,
                                                                         (tracked_order.fee_asset
                                                                          or tracked_order.quote_asset),
                                                                         executed_amount_base,
                                                                         executed_amount_quote,
                                                                         tracked_order.fee_paid,
                                                                         order_type))
                    else:
                        # check if its a cancelled order
                        # if its a cancelled order, issue cancel and stop tracking order
                        if tracked_order.is_cancelled:
                            self.logger().info(f"Successfully cancelled order {client_order_id}.")
                            self.c_trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                                 OrderCancelledEvent(
                                                     self._current_timestamp,
                                                     client_order_id))
                        else:
                            self.logger().info(f"The market order {client_order_id} has failed according to "
                                               f"order status API.")
                            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                                 MarketOrderFailureEvent(
                                                     self._current_timestamp,
                                                     client_order_id,
                                                     order_type
                                                 ))
                    self.c_stop_tracking_order(client_order_id)

    async def _iter_kafka_messages(self, topic: str) -> AsyncIterable[ConsumerRecord]:
        while True:
            try:
                consumer = AIOKafkaConsumer(topic, loop=self._ev_loop, bootstrap_servers=conf.kafka_bootstrap_server)
                await consumer.start()
                partition = list(consumer.assignment())[0]
                await consumer.seek_to_end(partition)

                while True:
                    response = await consumer.getmany(partition, timeout_ms=1000)
                    if partition in response:
                        for record in response[partition]:
                            yield record
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unknown error. Retrying after 5 seconds.",
                    exc_info=True,
                    app_warning_msg="Could not fetch message from Kafka. Check network connection."
                )
                await asyncio.sleep(5.0)

    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unknown error. Retrying after 1 seconds.",
                    exc_info=True,
                    app_warning_msg="Could not fetch user events from novadax. Check API key and network connection."
                )
                await asyncio.sleep(1.0)

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message[0]

                if event_type == "order":
                    execution_type = event_message[1].get("status")
                    exchange_id = event_message[1]["id"]
                    tracked_order = None
                    for o in self._in_flight_orders:
                        if o.exchange_order_id == exchange_id:
                            tracked_order = o

                    if tracked_order is None:
                        # Hiding the messages for now. Root cause to be investigated in later sprints.
                        self.logger().debug(f"Unrecognized order ID from user stream: {exchange_id}.")
                        self.logger().debug(f"Event: {event_message}")
                        continue

                    tracked_order.update_with_execution_report(event_message[1])

                    if execution_type == "PARTIAL_FILLED":
                        order_filled_event = order_filled_event._replace(trade_fee=self.c_get_fee(
                            tracked_order.base_asset,
                            tracked_order.quote_asset,
                            OrderType.LIMIT if event_message[1]["type"] == "LIMIT" else OrderType.MARKET,
                            TradeType.BUY if event_message[1]["side"] == "BUY" else TradeType.SELL,
                            Decimal(event_message[1]["price"]),
                            Decimal(event_message[1]["filledAmount"])
                        ))
                        if tracked_order.trade_type is TradeType.BUY:
                            self.logger().info(f"Filled {tracked_order.executed_amount_base} out of {tracked_order.amount} of the "
                                f"limit order {tracked_order.client_order_id} according to the RPC transaction logs.")
                            self.c_trigger_event(self.MARKET_ORDER_FILLED_EVENT_TAG,
                                                 OrderFilledEvent(self._current_timestamp,
                                                                  tracked_order.client_order_id,
                                                                  tracked_order.trading_pair,
                                                                  tracked_order.trade_type,
                                                                  tracked_order.order_type,
                                                                  tracked_order.price,
                                                                  tracked_order.executed_amount_base,
                                                                  tracked_order.fee_paid,
                                                                  tracked_order.exchange_order_id))
                        else:
                            self.logger().info(f"Filled {tracked_order.executed_amount_base} out of {tracked_order.amount} of the "
                                           f"limit order {tracked_order.client_order_id} according to the RPC transaction logs.")
                            self.c_trigger_event(self.MARKET_ORDER_FILLED_EVENT_TAG,
                                                 OrderFilledEvent(self._current_timestamp,
                                                                  tracked_order.client_order_id,
                                                                  tracked_order.trading_pair,
                                                                  tracked_order.trade_type,
                                                                  tracked_order.order_type,
                                                                  tracked_order.price,
                                                                  tracked_order.executed_amount_base,
                                                                  tracked_order.fee_paid,
                                                                  tracked_order.exchange_order_id))

                    if tracked_order.is_done:
                        if not tracked_order.is_failure:
                            if tracked_order.trade_type is TradeType.BUY:
                                self.logger().info(f"The market buy order {tracked_order.client_order_id} has completed "
                                                   f"according to user stream.")
                                self.c_trigger_event(self.MARKET_BUY_ORDER_COMPLETED_EVENT_TAG,
                                                     BuyOrderCompletedEvent(self._current_timestamp,
                                                                            tracked_order.client_order_id,
                                                                            tracked_order.base_asset,
                                                                            tracked_order.quote_asset,
                                                                            (tracked_order.fee_asset
                                                                             or tracked_order.base_asset),
                                                                            tracked_order.executed_amount_base,
                                                                            tracked_order.executed_amount_quote,
                                                                            tracked_order.fee_paid,
                                                                            tracked_order.order_type))
                            else:
                                self.logger().info(f"The market sell order {tracked_order.client_order_id} has completed "
                                                   f"according to user stream.")
                                self.c_trigger_event(self.MARKET_SELL_ORDER_COMPLETED_EVENT_TAG,
                                                     SellOrderCompletedEvent(self._current_timestamp,
                                                                             tracked_order.client_order_id,
                                                                             tracked_order.base_asset,
                                                                             tracked_order.quote_asset,
                                                                             (tracked_order.fee_asset
                                                                              or tracked_order.quote_asset),
                                                                             tracked_order.executed_amount_base,
                                                                             tracked_order.executed_amount_quote,
                                                                             tracked_order.fee_paid,
                                                                             tracked_order.order_type))
                        else:
                            # check if its a cancelled order
                            # if its a cancelled order, check in flight orders
                            # if present in in flight orders issue cancel and stop tracking order
                            if tracked_order.is_cancelled:
                                if tracked_order.client_order_id in self._in_flight_orders:
                                    self.logger().info(f"Successfully cancelled order {tracked_order.client_order_id}.")
                                    self.c_trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                                         OrderCancelledEvent(
                                                             self._current_timestamp,
                                                             tracked_order.client_order_id))
                            else:
                                self.logger().info(f"The market order {tracked_order.client_order_id} has failed according to "
                                                   f"order status API.")
                                self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                                     MarketOrderFailureEvent(
                                                         self._current_timestamp,
                                                         tracked_order.client_order_id,
                                                         tracked_order.order_type
                                                     ))
                        self.c_stop_tracking_order(tracked_order.client_order_id)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await asyncio.sleep(5.0)

    async def _status_polling_loop(self):
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()
                await safe_gather(
                    self._update_balances(),
                    self._update_order_status(),
                )
                self._last_poll_timestamp = self._current_timestamp
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while fetching account updates.", exc_info=True,
                                      app_warning_msg="Could not fetch account updates from novadax. "
                                                      "Check API key and network connection.")
                await asyncio.sleep(0.5)

    async def _trading_rules_polling_loop(self):
        while True:
            try:
                await safe_gather(
                    self._update_trading_rules()
                )
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while fetching trading rules.", exc_info=True,
                                      app_warning_msg="Could not fetch new trading rules from novadax. "
                                                      "Check network connection.")
                await asyncio.sleep(0.5)

    @property
    def status_dict(self) -> Dict[str, bool]:
        return {
            "order_books_initialized": self._order_book_tracker.ready,
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0
        }

    @property
    def ready(self) -> bool:
        return all(self.status_dict.values())

    async def server_time(self) -> int:
        """
        :return: The current server time in milliseconds since UNIX epoch.
        """
        result = self._novadax_client.get_timestamp()
        return result["data"]

    cdef c_start(self, Clock clock, double timestamp):
        self._tx_tracker.c_start(clock, timestamp)
        ExchangeBase.c_start(self, clock, timestamp)

    cdef c_stop(self, Clock clock):
        ExchangeBase.c_stop(self, clock)
        self._async_scheduler.stop()

    async def start_network(self):
        self._order_book_tracker.start()
        self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
        if self._trading_required:
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())
            self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
            self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())

    def _stop_network(self):
        self._order_book_tracker.stop()
        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
        if self._user_stream_tracker_task is not None:
            self._user_stream_tracker_task.cancel()
        if self._user_stream_event_listener_task is not None:
            self._user_stream_event_listener_task.cancel()
        if self._trading_rules_polling_task is not None:
            self._trading_rules_polling_task.cancel()
        self._status_polling_task = self._user_stream_tracker_task = \
            self._user_stream_event_listener_task = None

    async def stop_network(self):
        self._stop_network()

    async def check_network(self) -> NetworkStatus:
        try:
            self._novadax_client.get_timestamp()
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    cdef c_tick(self, double timestamp):
        cdef:
            double now = time.time()
            double poll_interval = (self.SHORT_POLL_INTERVAL
                                    if now - self.user_stream_tracker.last_recv_time > 60.0
                                    else self.LONG_POLL_INTERVAL)
            int64_t last_tick = <int64_t>(self._last_timestamp / poll_interval)
            int64_t current_tick = <int64_t>(timestamp / poll_interval)
        ExchangeBase.c_tick(self, timestamp)
        self._tx_tracker.c_tick(timestamp)
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp

    async def execute_buy(self,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          order_type: OrderType,
                          price: Optional[Decimal] = s_decimal_NaN):
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]
            object m = trading_pair.split("_")
            str base_currency = m[0]
            str quote_currency = m[1]
            object buy_fee = self.c_get_fee(base_currency, quote_currency, order_type, TradeType.BUY, amount, price)
            double adjusted_amount

        decimal_amount = self.c_quantize_order_amount(trading_pair, amount)
        decimal_price = (self.c_quantize_order_price(trading_pair, price)
                         if order_type is OrderType.LIMIT
                         else s_decimal_0)
        if decimal_amount < trading_rule.min_order_size:
            raise ValueError(f"Buy order amount {decimal_amount} is lower than the minimum order size "
                             f"{trading_rule.min_order_size}.")

        try:
            order_result = None
            order_decimal_amount = f"{decimal_amount:f}"
            order_decimal_price = f"{decimal_price:f}"
            if order_type is OrderType.LIMIT:
                self.c_start_tracking_order(
                    order_id,
                    "",
                    trading_pair,
                    TradeType.BUY,
                    decimal_price,
                    decimal_amount,
                    order_type
                )
                order_result = self._novadax_client.create_order(
                                                    trading_pair,
                                                    "LIMIT",
                                                    "BUY",
                                                    order_decimal_price,
                                                    order_decimal_amount)
            elif order_type is OrderType.MARKET:
                self.c_start_tracking_order(
                    order_id,
                    "",
                    trading_pair,
                    TradeType.BUY,
                    Decimal("NaN"),
                    decimal_amount,
                    order_type
                )
                order_result = self._novadax_client.create_order(
                                                    trading_pair,
                                                    "MARKET",
                                                    "BUY",
                                                    order_decimal_price,
                                                    order_decimal_amount)
            else:
                raise ValueError(f"Invalid OrderType {order_type}. Aborting.")

            exchange_order_id = str(order_result["data"]["id"])
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None:
                self.logger().info(f"Created {order_type} buy order {order_id} for "
                                   f"{decimal_amount} {trading_pair}.")
                tracked_order.exchange_order_id = exchange_order_id
            self.c_trigger_event(self.MARKET_BUY_ORDER_CREATED_EVENT_TAG,
                                 BuyOrderCreatedEvent(
                                     self._current_timestamp,
                                     order_type,
                                     trading_pair,
                                     decimal_amount,
                                     decimal_price,
                                     order_id
                                 ))

        except asyncio.CancelledError:
            raise

        except Exception as e:
            self.c_stop_tracking_order(order_id)
            order_type_str = 'MARKET' if order_type == OrderType.MARKET else 'LIMIT'
            self.logger().network(
                f"Error submitting buy {order_type_str} order to novadax for "
                f"{decimal_amount} {trading_pair} "
                f"{decimal_price if order_type is OrderType.LIMIT else ''}.",
                exc_info=True,
                app_warning_msg=f"Failed to submit buy order to novadax. Check API key and network connection."
            )
            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(self._current_timestamp, order_id, order_type))

    cdef str c_buy(self, str trading_pair, object amount, object order_type=OrderType.MARKET, object price=s_decimal_0,
                   dict kwargs={}):
        cdef:
            int64_t tracking_nonce = <int64_t> get_tracking_nonce()
            str client_order_id = str(f"buy-{trading_pair}-{tracking_nonce}")

        safe_ensure_future(self.execute_buy(client_order_id, trading_pair, amount, order_type, price))
        return client_order_id

    async def execute_sell(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           order_type: OrderType,
                           price: Optional[Decimal] = Decimal("NaN")):
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]

        decimal_amount = self.quantize_order_amount(trading_pair, amount)
        decimal_price = (self.c_quantize_order_price(trading_pair, price)
                         if order_type is OrderType.LIMIT
                         else s_decimal_0)
        if decimal_amount < trading_rule.min_order_size:
            raise ValueError(f"Sell order amount {decimal_amount} is lower than the minimum order size "
                             f"{trading_rule.min_order_size}.")

        try:
            order_result = None
            order_decimal_amount = f"{decimal_amount:f}"
            order_decimal_price = f"{decimal_price:f}"
            if order_type is OrderType.LIMIT:
                self.c_start_tracking_order(
                    order_id,
                    "",
                    trading_pair,
                    TradeType.SELL,
                    decimal_price,
                    decimal_amount,
                    order_type
                )
                order_result = self._novadax_client.create_order(
                                                    trading_pair,
                                                    "LIMIT",
                                                    "SELL",
                                                    order_decimal_price,
                                                    order_decimal_amount)
            elif order_type is OrderType.MARKET:
                self.c_start_tracking_order(
                    order_id,
                    "",
                    trading_pair,
                    TradeType.SELL,
                    Decimal("NaN"),
                    decimal_amount,
                    order_type
                )
                order_result = self._novadax_client.create_order(
                                                    trading_pair,
                                                    "MARKET",
                                                    "SELL",
                                                    order_decimal_price,
                                                    order_decimal_amount)
            else:
                raise ValueError(f"Invalid OrderType {order_type}. Aborting.")

            exchange_order_id = str(order_result["data"]["id"])
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None:
                self.logger().info(f"Created {order_type} sell order {order_id} for "
                                   f"{decimal_amount} {trading_pair}.")
                tracked_order.exchange_order_id = exchange_order_id

            self.c_trigger_event(self.MARKET_SELL_ORDER_CREATED_EVENT_TAG,
                                 SellOrderCreatedEvent(
                                     self._current_timestamp,
                                     order_type,
                                     trading_pair,
                                     decimal_amount,
                                     decimal_price,
                                     order_id
                                 ))
        except asyncio.CancelledError:
            raise
        except Exception:
            self.c_stop_tracking_order(order_id)
            order_type_str = 'MARKET' if order_type is OrderType.MARKET else 'LIMIT'
            self.logger().network(
                f"Error submitting sell {order_type_str} order to novadax for "
                f"{decimal_amount} {trading_pair} "
                f"{decimal_price if order_type is OrderType.LIMIT else ''}.",
                exc_info=True,
                app_warning_msg=f"Failed to submit sell order to novadax. Check API key and network connection."
            )
            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(self._current_timestamp, order_id, order_type))

    cdef str c_sell(self, str trading_pair, object amount, object order_type=OrderType.MARKET, object price=s_decimal_0,
                    dict kwargs={}):

        cdef:
            int64_t tracking_nonce = <int64_t> get_tracking_nonce()
            str client_order_id = str(f"sell-{trading_pair}-{tracking_nonce}")
        safe_ensure_future(self.execute_sell(client_order_id, trading_pair, amount, order_type, price))
        return client_order_id

    async def execute_cancel(self, trading_pair: str, order_id: str):
        try:
            o = self._in_flight_orders[order_id]
            #this is the spelling they use: -------\/
            cancel_result = self._novadax_client.cancle_order(o.exchange_order_id)
            
        except Exception as e:
            if e.message == "Order not found":
                # The order was never there to begin with. So cancelling it is a no-op but semantically successful.
                self.logger().debug(f"The order {order_id} does not exist on novadax. No cancellation needed.")
                self.c_stop_tracking_order(order_id)
                self.c_trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                     OrderCancelledEvent(self._current_timestamp, order_id))
                return {
                    # Required by cancel_all() below.
                    "client_order_id": order_id
                }
            else:
                raise e

        if isinstance(cancel_result, dict) and cancel_result["message"] == "Success":
            self.logger().info(f"Successfully cancelled order {order_id}.")
            self.c_stop_tracking_order(order_id)
            self.c_trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                 OrderCancelledEvent(self._current_timestamp, order_id))
            cancel_result["client_order_id"] = order_id
        return cancel_result

    cdef c_cancel(self, str trading_pair, str order_id):
        safe_ensure_future(self.execute_cancel(trading_pair, order_id))
        return order_id

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        incomplete_orders = [o for o in self._in_flight_orders.values() if not o.is_done]
        tasks = [self.execute_cancel(o.trading_pair, o.client_order_id) for o in incomplete_orders]
        order_id_set = set([o.client_order_id for o in incomplete_orders])
        successful_cancellations = []

        try:
            async with timeout(timeout_seconds):
                cancellation_results = await safe_gather(*tasks, return_exceptions=True)
                for cr in cancellation_results:
                    if isinstance(cr, Exception):
                        continue
                    else:
                        client_order_id = cr.get("client_order_id")
                        order_id_set.remove(client_order_id)
                        successful_cancellations.append(CancellationResult(client_order_id, True))
        except Exception:
            self.logger().network(
                f"Unexpected error cancelling orders.",
                exc_info=True,
                app_warning_msg="Failed to cancel order with novadax. Check API key and network connection."
            )

        failed_cancellations = [CancellationResult(oid, False) for oid in order_id_set]
        return successful_cancellations + failed_cancellations

    cdef OrderBook c_get_order_book(self, str trading_pair):
        cdef:
            dict order_books = self._order_book_tracker.order_books

        if trading_pair not in order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return order_books[trading_pair]

    cdef c_did_timeout_tx(self, str tracking_id):
        self.c_trigger_event(self.MARKET_TRANSACTION_FAILURE_EVENT_TAG,
                             MarketTransactionFailureEvent(self._current_timestamp, tracking_id))

    cdef c_start_tracking_order(self,
                                str order_id,
                                str exchange_order_id,
                                str trading_pair,
                                object trade_type,
                                object price,
                                object amount,
                                object order_type):
        self._in_flight_orders[order_id] = NovadaxInFlightOrder(
            client_order_id=order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=trading_pair,
            order_type=order_type,
            trade_type=trade_type,
            price=price,
            amount=amount
        )

    cdef c_stop_tracking_order(self, str order_id):
        if order_id in self._in_flight_orders:
            del self._in_flight_orders[order_id]
        if order_id in self._order_not_found_records:
            del self._order_not_found_records[order_id]

    cdef object c_get_order_price_quantum(self, str trading_pair, object price):
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]
        return trading_rule.min_price_increment

    cdef object c_get_order_size_quantum(self, str trading_pair, object order_size):
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]
        return Decimal(trading_rule.min_base_amount_increment)

    cdef object c_quantize_order_amount(self, str trading_pair, object amount, object price=s_decimal_0):
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]
            object current_price = self.c_get_price(trading_pair, False)
            object notional_size
        global s_decimal_0
        quantized_amount = ExchangeBase.c_quantize_order_amount(self, trading_pair, amount)

        # Check against min_order_size and min_notional_size. If not passing either check, return 0.
        if quantized_amount < trading_rule.min_order_size:
            return s_decimal_0

        if price == s_decimal_0:
            notional_size = current_price * quantized_amount
        else:
            notional_size = price * quantized_amount

        # Add 1% as a safety factor in case the prices changed while making the order.
        if notional_size < trading_rule.min_notional_size * Decimal("1.01"):
            return s_decimal_0

        return quantized_amount
