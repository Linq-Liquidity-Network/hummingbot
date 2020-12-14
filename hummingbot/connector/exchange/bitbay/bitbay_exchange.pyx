import aiohttp
import asyncio
import binascii
import json
import time
import uuid
import traceback
import urllib
import hashlib
from typing import (
    Any,
    Dict,
    List,
    Optional
)
import math
import logging
from decimal import *
import uuid
from libc.stdint cimport int64_t
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.event.event_listener cimport EventListener
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.wallet.ethereum.web3_wallet import Web3Wallet
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.exchange.bitbay.bitbay_auth import BitbayAuth
from hummingbot.connector.exchange.bitbay.bitbay_order_book_tracker import BitbayOrderBookTracker
from hummingbot.connector.exchange.bitbay.bitbay_api_order_book_data_source import BitbayAPIOrderBookDataSource
from hummingbot.connector.exchange.bitbay.bitbay_user_stream_tracker import BitbayUserStreamTracker
from hummingbot.connector.exchange.bitbay.bitbay_order_status import BitbayOrderStatus
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.core.event.events import (
    MarketEvent,
    BuyOrderCompletedEvent,
    SellOrderCompletedEvent,
    OrderCancelledEvent,
    OrderExpiredEvent,
    OrderFilledEvent,
    MarketOrderFailureEvent,
    BuyOrderCreatedEvent,
    SellOrderCreatedEvent,
    TradeType,
    OrderType,
    TradeFee,
)
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.bitbay.bitbay_in_flight_order cimport BitbayInFlightOrder
from hummingbot.connector.trading_rule cimport TradingRule
from hummingbot.core.utils.estimate_fee import estimate_fee
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce

s_logger = None
s_decimal_0 = Decimal(0)
s_decimal_NaN = Decimal("nan")


def num_d(amount):
    return abs(Decimal(amount).normalize().as_tuple().exponent)


def now():
    return int(time.time()) * 1000


BUY_ORDER_COMPLETED_EVENT = MarketEvent.BuyOrderCompleted.value
SELL_ORDER_COMPLETED_EVENT = MarketEvent.SellOrderCompleted.value
ORDER_CANCELLED_EVENT = MarketEvent.OrderCancelled.value
ORDER_EXPIRED_EVENT = MarketEvent.OrderExpired.value
ORDER_FILLED_EVENT = MarketEvent.OrderFilled.value
ORDER_FAILURE_EVENT = MarketEvent.OrderFailure.value
BUY_ORDER_CREATED_EVENT = MarketEvent.BuyOrderCreated.value
SELL_ORDER_CREATED_EVENT = MarketEvent.SellOrderCreated.value
API_CALL_TIMEOUT = 10.0

# ==========================================================

GET_ORDERS_ROUTE = "/trading/offer"
MAINNET_API_REST_ENDPOINT = "https://api.bitbay.net/rest"
MAINNET_WS_ENDPOINT = "wss://api.bitbay.net/websocket"
EXCHANGE_INFO_ROUTE = "/trading/ticker"
EXCHANGE_STATS = "/trading/stats/:trading_pair"
BALANCES_INFO_ROUTE = "/balances/BITBAY/balance"
ORDER_ROUTE = "/trading/offer/:trading_pair"
ORDER_CANCEL_ROUTE = "/trading/offer/:trading_pair/:id/:type/:price"
UNRECOGNIZED_ORDER_DEBOUCE = 20  # seconds

class LatchingEventResponder(EventListener):
    def __init__(self, callback : any, num_expected : int):
        super().__init__()
        self._callback = callback
        self._completed = asyncio.Event()
        self._num_remaining = num_expected

    def __call__(self, arg : any):
        if self._callback(arg):
            self._reduce()

    def _reduce(self):
        self._num_remaining -= 1
        if self._num_remaining <= 0:
            self._completed.set()

    async def wait_for_completion(self, timeout : float):
        try:
            await asyncio.wait_for(self._completed.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        return self._completed.is_set()

    def cancel_one(self):
        self._reduce()

cdef class BitbayExchange(ExchangeBase):
    @classmethod
    def logger(cls) -> HummingbotLogger:
        global s_logger
        if s_logger is None:
            s_logger = logging.getLogger(__name__)
        return s_logger

    def __init__(self,
                 bitbay_private_key: str,
                 bitbay_api_key: str,
                 poll_interval: float = 5.0,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True):

        super().__init__()
        self._real_time_balance_update = True
        self._bitbay_auth = BitbayAuth(api_key=bitbay_api_key, secret_key=bitbay_private_key)
        self.API_REST_ENDPOINT = MAINNET_API_REST_ENDPOINT
        self.WS_ENDPOINT = MAINNET_WS_ENDPOINT
        self._order_book_tracker = BitbayOrderBookTracker(
            trading_pairs=trading_pairs,
            rest_api_url=self.API_REST_ENDPOINT,
            websocket_url=self.WS_ENDPOINT,
        )        
        self._user_stream_tracker = BitbayUserStreamTracker(
            orderbook_tracker_data_source=self._order_book_tracker.data_source,
            bitbay_auth=self._bitbay_auth,
            trading_pairs=trading_pairs
        )
        self._user_stream_event_listener_task = None
        self._user_stream_tracker_task = None
        self._trading_required = trading_required
        self._poll_notifier = asyncio.Event()
        self._last_timestamp = 0
        self._poll_interval = poll_interval
        self._shared_client = None
        self._polling_update_task = None
        self._awaiting_response = 0
        self._user_stream_message_queue = []

        # State
        self._lock = asyncio.Lock()
        self._trading_rules = {}
        self._in_flight_orders = {}
        self._in_flight_orders_by_exchange_id = {}
        self._next_order_id = {}
        self._trading_pairs = trading_pairs

        self._order_id_lock = asyncio.Lock()

    @property
    def name(self) -> str:
        return "bitbay"

    @property
    def ready(self) -> bool:
        return all(self.status_dict.values())

    @property
    def status_dict(self) -> Dict[str, bool]:
        return {
            "order_books_initialized": len(self._order_book_tracker.order_books) > 0,
            "account_balances": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0 if self._trading_required else True,
        }

    # ----------------------------------------
    # Markets & Order Books

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    cdef OrderBook c_get_order_book(self, str trading_pair):
        cdef dict order_books = self._order_book_tracker.order_books
        if trading_pair not in order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return order_books[trading_pair]

    @property
    def limit_orders(self) -> List[LimitOrder]:
        cdef:
            list retval = []
            BitbayInFlightOrder bitbay_flight_order

        for in_flight_order in self._in_flight_orders.values():
            bitbay_flight_order = in_flight_order
            if bitbay_flight_order.order_type is OrderType.LIMIT:
                retval.append(bitbay_flight_order.to_limit_order())
        return retval

    async def get_active_exchange_markets(self) -> pd.DataFrame:
        return await BitbayAPIOrderBookDataSource.get_active_exchange_markets()

    # ----------------------------------------
    # Account Balances

    cdef object c_get_balance(self, str currency):
        return self._account_balances[currency]

    cdef object c_get_available_balance(self, str currency):
        return self._account_available_balances[currency]

    # ==========================================================
    # Order Submission
    # ----------------------------------------------------------

    @property
    def in_flight_orders(self) -> Dict[str, BitbayInFlightOrder]:
        return self._in_flight_orders

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    async def place_order(self,
                          client_order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          is_buy: bool,
                          order_type: OrderType,
                          price: Decimal) -> Dict[str, Any]:
        order_side = "buy" if is_buy else "sell"
        base, quote = trading_pair.split('-')

        order = {
            "amount": float(amount),
            "rate": float(price),
            "price": None,
            "offerType": order_side,
            "mode": "limit",
            "postOnly": False,
            "fillOrKill": False
        }
        if order_type is OrderType.LIMIT_MAKER:
            order["postOnly"] = True

        headers = self.generate_request_headers(json.dumps(order))

        return await self.api_request("POST", f"{ORDER_ROUTE}".replace(":trading_pair",trading_pair), headers=headers, data=json.dumps(order))

    async def execute_order(self, order_side, client_order_id, trading_pair, amount, order_type, price):
        """
        Completes the common tasks from execute_buy and execute_sell.  Quantizes the order's amount and price, and
        validates the order against the trading rules before placing this order.
        """
        # Quantize order

        amount = self.c_quantize_order_amount(trading_pair, amount)
        price = self.c_quantize_order_price(trading_pair, price)

        # Check trading rules
        trading_rule = self._trading_rules[trading_pair]
        if order_type == OrderType.LIMIT and trading_rule.supports_limit_orders is False:
            raise ValueError("LIMIT orders are not supported")
        elif order_type == OrderType.MARKET and trading_rule.supports_market_orders is False:
            raise ValueError("MARKET orders are not supported")

        if amount < trading_rule.min_order_size:
            raise ValueError(f"Order amount({str(amount)}) is less than the minimum allowable amount({str(trading_rule.min_order_size)})")
        if amount > trading_rule.max_order_size:
            raise ValueError(f"Order amount({str(amount)}) is greater than the maximum allowable amount({str(trading_rule.max_order_size)})")
        if amount*price < trading_rule.min_notional_size:
            raise ValueError(f"Order notional value({str(amount*price)}) is less than the minimum allowable notional value for an order ({str(trading_rule.min_notional_size)})")

        try:
            created_at: int = int(time.time())
            in_flight_order = BitbayInFlightOrder.from_bitbay_order(self, order_side, client_order_id, created_at, None, trading_pair, price, amount)
            self.start_tracking(in_flight_order)
            try:
                self._awaiting_response += 1
                creation_response = await self.place_order(in_flight_order.client_order_id, trading_pair, amount, order_side is TradeType.BUY, order_type, price)
            except asyncio.exceptions.TimeoutError:
                # We timed out while placing this order. We may have successfully submitted the order, or we may have had connection
                # issues that prevented the submission from taking place. We'll assume that the order is live and let our order status 
                # updates mark this as cancelled if it doesn't actually exist.
                self.logger().info("Time out error occurred")
                self._awaiting_response -= 1
                pass

            # Verify the response from the exchange
            if "status" not in creation_response.keys():
                raise Exception(creation_response['comment'])

            status = creation_response["status"]
            if status != 'Ok':
                self._awaiting_response -= 1
                if status == 'Fail':
                    self.c_stop_tracking_order(client_order_id)
                    self.logger().warning(
                        f"Error submitting {order_side.name} {order_type.name} order to Bitbay for "
                        f"{amount} {trading_pair} "
                        f"{price}.",
                        exc_info=True,
                    )
                    self.c_trigger_event(ORDER_FAILURE_EVENT, MarketOrderFailureEvent(now(), client_order_id, order_type))
                    return
                else:
                    raise Exception(f"bitbay api returned unexpected '{status}' as status of created order")

            bitbay_order_hash = creation_response["offerId"]
            in_flight_order.update_exchange_order_id(bitbay_order_hash)
            self._in_flight_orders_by_exchange_id[bitbay_order_hash] = in_flight_order
            self._awaiting_response -= 1
            # Begin tracking order
            self.logger().info(
                f"Created {in_flight_order.description} order {client_order_id} for {amount} {trading_pair}.")

        except Exception as e:
            self.logger().warning(f"Error submitting {order_side.name} {order_type.name} order to bitbay for "
                                  f"{amount} {trading_pair} at {price}.")
            self.logger().info(e)

    async def execute_buy(self,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          order_type: OrderType,
                          price: Optional[Decimal] = Decimal('NaN')):
        try:
            await self.execute_order(TradeType.BUY, order_id, trading_pair, amount, order_type, price)

            self.c_trigger_event(BUY_ORDER_CREATED_EVENT,
                                 BuyOrderCreatedEvent(now(), order_type, trading_pair, Decimal(amount), Decimal(price), order_id))
        except ValueError as e:
            # Stop tracking this order
            self.c_stop_tracking_order(order_id)
            self.c_trigger_event(ORDER_FAILURE_EVENT, MarketOrderFailureEvent(now(), order_id, order_type))
            raise e

    async def execute_sell(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           order_type: OrderType,
                           price: Optional[Decimal] = Decimal('NaN')):
        try:
            await self.execute_order(TradeType.SELL, order_id, trading_pair, amount, order_type, price)
            self.c_trigger_event(SELL_ORDER_CREATED_EVENT,
                                 SellOrderCreatedEvent(now(), order_type, trading_pair, Decimal(amount), Decimal(price), order_id))
        except ValueError as e:
            # Stop tracking this order
            self.c_stop_tracking_order(order_id)
            self.c_trigger_event(ORDER_FAILURE_EVENT, MarketOrderFailureEvent(now(), order_id, order_type))
            raise e

    cdef str c_buy(self, str trading_pair, object amount, object order_type = OrderType.LIMIT, object price = 0.0,
                   dict kwargs = {}):
        cdef:
            int64_t tracking_nonce = <int64_t> get_tracking_nonce()
            str client_order_id = str(f"buy-{trading_pair}-{tracking_nonce}")
        safe_ensure_future(self.execute_buy(client_order_id, trading_pair, amount, order_type, price))
        return client_order_id

    cdef str c_sell(self, str trading_pair, object amount, object order_type = OrderType.LIMIT, object price = 0.0,
                    dict kwargs = {}):
        cdef:
            int64_t tracking_nonce = <int64_t> get_tracking_nonce()
            str client_order_id = str(f"sell-{trading_pair}-{tracking_nonce}")
        safe_ensure_future(self.execute_sell(client_order_id, trading_pair, amount, order_type, price))
        return client_order_id

    # ----------------------------------------
    # Cancellation

    async def cancel_order(self, client_order_id: str):
        in_flight_order = self._in_flight_orders.get(client_order_id)
        cancellation_event = OrderCancelledEvent(now(), client_order_id)

        if in_flight_order is None:
            self.c_trigger_event(ORDER_CANCELLED_EVENT, cancellation_event)
            return

        try:
            trading_pair = in_flight_order.trading_pair
            exchange_id = in_flight_order.exchange_order_id
            trade_type = "buy" if in_flight_order.trade_type == TradeType.BUY else "sell"
            price = str(in_flight_order.price)
            url = f"{ORDER_CANCEL_ROUTE}".replace(":trading_pair/:id/:type/:price",
                                              f"{trading_pair}/{exchange_id}/{trade_type}/{price}")
            headers = self.generate_request_headers()

            res = await self.api_request("DELETE", url, headers=headers, secure=True)

            status = res['status']
            errors = res['errors']

            if 'OFFER_NOT_FOUND' in errors:
                # Order didn't exist on exchange, mark this as canceled
                self.c_trigger_event(ORDER_CANCELLED_EVENT,cancellation_event)
                self.c_stop_tracking_order(client_order_id)
            elif len(errors) > 0 and not ('OFFER_NOT_FOUND' in errors):
                raise Exception(f"Cancel order returned errors {errors}")
            else:
                self.c_trigger_event(ORDER_CANCELLED_EVENT,cancellation_event)
            
            return True

        except Exception as e:
            self.logger().warning(f"Failed to cancel order {client_order_id}")
            self.logger().info(e)
            return False

    cdef c_cancel(self, str trading_pair, str client_order_id):
        safe_ensure_future(self.cancel_order(client_order_id))

    cdef c_stop_tracking_order(self, str order_id):
        cdef:
            str exchange_id
        if order_id in self._in_flight_orders:
            exchange_id = self._in_flight_orders[order_id].exchange_order_id
            if exchange_id in self._in_flight_orders_by_exchange_id:
                del self._in_flight_orders_by_exchange_id[exchange_id]
            del self._in_flight_orders[order_id]

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        cancellation_queue = self._in_flight_orders.copy()
        if len(cancellation_queue) == 0:
            return []

        order_status = {o.client_order_id: False for o in cancellation_queue.values()}
        for o, s in order_status.items():
            self.logger().info(o + ' ' + str(s))
        
        def set_cancellation_status(oce : OrderCancelledEvent):
            if oce.order_id in order_status:
                order_status[oce.order_id] = True
                return True
            return False
            
        cancel_verifier = LatchingEventResponder(set_cancellation_status, len(cancellation_queue))
        self.c_add_listener(ORDER_CANCELLED_EVENT, cancel_verifier)

        for order_id, in_flight in cancellation_queue.iteritems():
            try:            
                if not await self.cancel_order(order_id):
                    # this order did not exist on the exchange
                    cancel_verifier.cancel_one()
                    self.c_stop_tracking_order(order_id)
            except Exception:
                cancel_verifier.cancel_one()
        
        all_completed : bool = await cancel_verifier.wait_for_completion(timeout_seconds)
        self.c_remove_listener(ORDER_CANCELLED_EVENT, cancel_verifier)

        return [CancellationResult(order_id=order_id, success=success) for order_id, success in order_status.items()]
        
    cdef object c_get_fee(self,
                          str base_currency,
                          str quote_currency,
                          object order_type,
                          object order_side,
                          object amount,
                          object price):
        is_maker = order_type is OrderType.LIMIT
        return estimate_fee("bitbay", is_maker)

    # ==========================================================
    # Runtime
    # ----------------------------------------------------------

    async def start_network(self):
        await self.stop_network()        
        self._order_book_tracker.start()        
        if self._trading_required:
            exchange_info = await self.api_request("GET", EXCHANGE_INFO_ROUTE)        
        self._polling_update_task = safe_ensure_future(self._polling_update())        
        self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())        
        self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())

    async def stop_network(self):
        self._order_book_tracker.stop()
        self._polling_update_task = None
        if self._user_stream_tracker_task is not None:
            self._user_stream_tracker_task.cancel()
        if self._user_stream_event_listener_task is not None:
            self._user_stream_event_listener_task.cancel()
        self._user_stream_tracker_task = None
        self._user_stream_event_listener_task = None

    async def check_network(self) -> NetworkStatus:
        try:
            await self.api_request("GET", f"{EXCHANGE_STATS}".replace("trading_pair", "BTC-PLN"))
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    # ----------------------------------------
    # State Management

    @property
    def tracking_states(self) -> Dict[str, any]:
        return {
            key: value.to_json()
            for key, value in self._in_flight_orders.items()
        }

    def restore_tracking_states(self, saved_states: Dict[str, any]):
        for order_id, in_flight_repr in saved_states.iteritems():
            in_flight_json: Dict[Str, Any] = json.loads(in_flight_repr)
            self._in_flight_orders[order_id] = BitbayInFlightOrder.from_json(self, in_flight_json)

    def start_tracking(self, in_flight_order):
        self._in_flight_orders[in_flight_order.client_order_id] = in_flight_order

    # ----------------------------------------
    # updates to orders and balances

    def _update_inflight_order(self, tracked_order: BitbayInFlightOrder, event: Dict[str, Any]):
        issuable_events: List[MarketEvent] = tracked_order.update(event)

        # Issue relevent events
        for (market_event, new_amount, new_price, new_fee) in issuable_events:
            if market_event == MarketEvent.OrderFilled:
                self.c_trigger_event(ORDER_FILLED_EVENT,
                                     OrderFilledEvent(self._current_timestamp,
                                                      tracked_order.client_order_id,
                                                      tracked_order.trading_pair,
                                                      tracked_order.trade_type,
                                                      tracked_order.order_type,
                                                      new_price,
                                                      new_amount,
                                                      TradeFee(Decimal(0), [(tracked_order.fee_asset, new_fee)]),
                                                      tracked_order.client_order_id))
            elif market_event == MarketEvent.OrderCancelled:
                self.logger().info(f"Successfully cancelled order {tracked_order.client_order_id}")
                self.c_stop_tracking_order(tracked_order.client_order_id)
                self.c_trigger_event(ORDER_CANCELLED_EVENT,
                                     OrderCancelledEvent(self._current_timestamp,
                                                         tracked_order.client_order_id))
            elif market_event == MarketEvent.OrderExpired:
                self.c_trigger_event(ORDER_EXPIRED_EVENT,
                                     OrderExpiredEvent(self._current_timestamp,
                                                       tracked_order.client_order_id))
            elif market_event == MarketEvent.OrderFailure:
                self.c_trigger_event(ORDER_FAILURE_EVENT,
                                     MarketOrderFailureEvent(self._current_timestamp,
                                                             tracked_order.client_order_id,
                                                             tracked_order.order_type))

            # Complete the order if relevent
            if tracked_order.is_done:
                if not tracked_order.is_failure:
                    if tracked_order.trade_type is TradeType.BUY:
                        self.logger().info(f"The market buy order {tracked_order.client_order_id} has completed "
                                           f"according to user stream.")
                        self.c_trigger_event(BUY_ORDER_COMPLETED_EVENT,
                                             BuyOrderCompletedEvent(self._current_timestamp,
                                                                    tracked_order.client_order_id,
                                                                    tracked_order.base_asset,
                                                                    tracked_order.quote_asset,
                                                                    tracked_order.fee_asset,
                                                                    tracked_order.executed_amount_base,
                                                                    tracked_order.executed_amount_quote,
                                                                    tracked_order.fee_paid,
                                                                    tracked_order.order_type))
                    else:
                        self.logger().info(f"The market sell order {tracked_order.client_order_id} has completed "
                                           f"according to user stream.")
                        self.c_trigger_event(SELL_ORDER_COMPLETED_EVENT,
                                             SellOrderCompletedEvent(self._current_timestamp,
                                                                     tracked_order.client_order_id,
                                                                     tracked_order.base_asset,
                                                                     tracked_order.quote_asset,
                                                                     tracked_order.fee_asset,
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
                    else:
                        self.logger().info(f"The market order {tracked_order.client_order_id} has failed according to "
                                           f"order status API.")

                self.c_stop_tracking_order(tracked_order.client_order_id)

    async def _set_balances(self, updates, is_snapshot=True):
        try:
            async with self._lock:
                for data in updates:
                    total_amount: Decimal = Decimal(data['totalFunds'])
                    token: str = data['currency']
                    available_amount: Decimal = Decimal(data['availableFunds'])
                    self._account_balances[token] = total_amount
                    self._account_available_balances[token] = available_amount

        except Exception as e:
            self.logger().error(f"Could not set balance {repr(e)}")

    # ----------------------------------------
    # User stream updates

    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, Any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unknown error. Retrying after 1 seconds.",
                    exc_info=True,
                    app_warning_msg="Could not fetch user events from New Connector. Check API key and network connection."
                )
                await asyncio.sleep(1.0)

    async def _user_stream_event_listener(self):
        self._check_message_queue()
        async for event_message in self._iter_user_event_queue():
            try:
                event: Dict[str, Any] = event_message
                topic: str = event['topic'].split('/')[1]
                data: Dict[str, Any] = event['message']
                if topic == 'balance':
                    await self._set_balances([data], is_snapshot=False)
                elif topic == 'offers':
                    exchange_order_id: str = data['offerId']
                    if exchange_order_id not in self._in_flight_orders_by_exchange_id:
                        self._user_stream_message_queue.append(data)
                    else:
                        tracked_order: BitbayInFlightOrder = self._in_flight_orders_by_exchange_id.get(exchange_order_id)

                        if tracked_order is None:
                            self.logger().warning(f"Unrecognized order ID from user stream: {tracked_order.client_order_id}.")
                            self.logger().warning(f"Event: {event_message}")
                            try_update_again = True

                        # update the tracked order
                        self._update_inflight_order(tracked_order, data)
                elif topic == 'sub':
                    pass
                elif topic == 'unsub':
                    pass
                else:
                    self.logger().debug(f"Unrecognized user stream event topic: {topic}.")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await asyncio.sleep(5.0)

    # ----------------------------------------
    # Polling Updates

    async def _polling_update(self):
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()

                await asyncio.gather(
                    self._update_balances(),
                    self._update_trading_rules(),
                    self._update_order_status(),
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().warning("Failed to fetch updates on Bitbay. Check network connection.")
                self.logger().info(e)

    async def _update_balances(self):
        headers = self.generate_request_headers()
        balances_response = await self.api_request("GET", BALANCES_INFO_ROUTE,
                                                   headers=headers)
        await self._set_balances(balances_response["balances"])

    async def _update_trading_rules(self):
        exchange_info = await self.api_request("GET", EXCHANGE_INFO_ROUTE)
        for market_name in exchange_info["items"].keys():
            market = exchange_info["items"][market_name]['market']
            try:
                self._trading_rules[market_name] = TradingRule(
                    trading_pair=market_name,
                    min_order_size = Decimal(market['first']['minOffer']),
                    min_price_increment=Decimal(f"1e-{market['second']['scale']}"),
                    min_base_amount_increment=Decimal(f"1e-{market['first']['scale']}"),
                    min_quote_amount_increment=Decimal(f"1e-{market['second']['scale']}"),
                    min_notional_size = Decimal(market['second']['minOffer']),
                    supports_limit_orders = True,
                    supports_market_orders = True
                )
            except Exception as e:
                self.logger().warning("Error updating trading rules")
                self.logger().warning(str(e))

    async def _check_message_queue(self):
        if self._awaiting_response == 0:
            while len(self._user_stream_message_queue) > 0:
                data = self._user_stream_message_queue.pop()
                exchange_order_id: str = data['offerId']
                tracked_order: BitbayInFlightOrder = self._in_flight_orders_by_exchange_id.get(exchange_order_id)

                if tracked_order is None:
                    self.logger().warning(f"{self._in_flight_orders_by_exchange_id}")
                    self.logger().warning(f"Unrecognized order ID from user stream: {exchange_order_id}.")
                    self.logger().warning(f"Event: {data}")
                    continue

                self._update_inflight_order(tracked_order, data)

    async def _update_order_status(self):
        await self._check_message_queue()

        tracked_orders = self._in_flight_orders.copy()

        try:
            headers = self.generate_request_headers()
            bitbay_order_request = await self.api_request("GET",
                                                            GET_ORDERS_ROUTE,
                                                            headers=headers)
            items = bitbay_order_request["items"]
        except:
            self.logger().warning("Unable to update orders")
            return
        for item in items:
            exchange_id = item["id"]
            if exchange_id in self._in_flight_orders_by_exchange_id:
                tracked_order = self._in_flight_orders_by_exchange_id[exchange_id]
                # No endpoint to get a specific order. We check all orders and remove from our
                # starting (copied) dict as we move through the response.
                try:
                    self._update_inflight_order(tracked_order, item)
                    del tracked_orders[tracked_order.client_order_id]
                except Exception as e:
                    self.logger().error(f"Failed to update Bitbay order {tracked_order.exchange_order_id}")
                    self.logger().error(e)
        #Go through the orders that were not included in the response from ORDERS_ENDPOINT    
        for client_order_id, tracked_order in tracked_orders.iteritems():
            bitbay_order_id = tracked_order.exchange_order_id
            if bitbay_order_id is None:
                # This order is still pending acknowledgement from the exchange
                if tracked_order.created_at < (int(time.time()) - UNRECOGNIZED_ORDER_DEBOUCE):
                    self.logger().warning(f"marking {client_order_id} as cancelled")
                    cancellation_event = OrderCancelledEvent(now(), client_order_id)
                    self.c_trigger_event(ORDER_CANCELLED_EVENT, cancellation_event)
                    self.c_stop_tracking_order(client_order_id)
            else:
                self._in_flight_orders_by_exchange_id[bitbay_order_id] = tracked_order

    # ==========================================================
    # Miscellaneous
    # ----------------------------------------------------------

    cdef object c_get_order_price_quantum(self, str trading_pair, object price):
        return self._trading_rules[trading_pair].min_price_increment

    cdef object c_get_order_size_quantum(self, str trading_pair, object order_size):
        return self._trading_rules[trading_pair].min_base_amount_increment

    cdef object c_quantize_order_price(self, str trading_pair, object price):
        return price.quantize(self.c_get_order_price_quantum(trading_pair, price), rounding=ROUND_DOWN)

    cdef object c_quantize_order_amount(self, str trading_pair, object amount, object price = 0.0):
        quantized_amount = amount.quantize(self.c_get_order_size_quantum(trading_pair, amount), rounding=ROUND_DOWN)
        rules = self._trading_rules[trading_pair]

        if quantized_amount < rules.min_order_size:
            return s_decimal_0

        if price > 0 and price * quantized_amount < rules.min_notional_size:
            return s_decimal_0

        return quantized_amount

    cdef c_tick(self, double timestamp):
        cdef:
            int64_t last_tick = <int64_t> (self._last_timestamp / self._poll_interval)
            int64_t current_tick = <int64_t> (timestamp / self._poll_interval)

        ExchangeBase.c_tick(self, timestamp)
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp

    def generate_request_headers(self, body: str = ""):
        auth_headers = self._bitbay_auth.generate_auth_dict(str(body))

        headers = {
          "API-Key": auth_headers["publicKey"],
          "API-Hash": auth_headers["hashSignature"],
          "operation-id": str(uuid.uuid4()),
          "Request-Timestamp": auth_headers["requestTimestamp"],
          "Content-Type": "application/json"
        }
        return headers

    async def api_request(self,
                          http_method: str,
                          url: str,
                          data: Optional[Dict[str, Any]] = None,
                          params: Optional[Dict[str, Any]] = None,
                          headers: Optional[Dict[str, str]] = {},
                          secure: bool = False) -> Dict[str, Any]:

        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession()

        full_url = f"{self.API_REST_ENDPOINT}{url}"

        # Sign requests for secure requests

        async with self._shared_client.request(http_method, url=full_url,
                                               timeout=API_CALL_TIMEOUT,
                                               data=data, params=params, headers=headers) as response:
            if response.status != 200:
                self.logger().info(f"Issue with Bitbay API {http_method} to {url}, response: ")
                self.logger().info(await response.text())
                raise IOError(f"Error fetching data from {full_url}. HTTP status is {response.status}.")
            data = await response.json()
            return data

    def get_order_book(self, trading_pair: str) -> OrderBook:
        return self.c_get_order_book(trading_pair)

    def get_price(self, trading_pair: str, is_buy: bool) -> Decimal:
        return self.c_get_price(trading_pair, is_buy)

    def buy(self, trading_pair: str, amount: Decimal, order_type=OrderType.MARKET,
            price: Decimal = s_decimal_NaN, **kwargs) -> str:
        return self.c_buy(trading_pair, amount, order_type, price, kwargs)

    def sell(self, trading_pair: str, amount: Decimal, order_type=OrderType.MARKET,
             price: Decimal = s_decimal_NaN, **kwargs) -> str:
        return self.c_sell(trading_pair, amount, order_type, price, kwargs)

    def cancel(self, trading_pair: str, client_order_id: str):
        return self.c_cancel(trading_pair, client_order_id)

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal = s_decimal_NaN) -> TradeFee:
        return self.c_get_fee(base_currency, quote_currency, order_type, order_side, amount, price)
