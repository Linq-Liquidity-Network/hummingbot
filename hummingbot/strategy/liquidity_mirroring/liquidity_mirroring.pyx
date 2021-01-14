# distutils: language=c++
from slack_pusher import SlackPusher
import logging
from decimal import Decimal
from threading import Lock
import os
import conf
import time
import pandas as pd
import random
from typing import (
    List,
    Tuple,
)
from datetime import datetime, timedelta
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.connector.exchange_base cimport ExchangeBase
from hummingbot.core.event.events import (
    TradeType,
    OrderType,
)
from hummingbot.core.data_type.limit_order cimport LimitOrder
from hummingbot.core.data_type.market_order import MarketOrder
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.strategy import market_trading_pair_tuple
from hummingbot.strategy.strategy_base import StrategyBase
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.liquidity_mirroring.balances import get_available_balances
from hummingbot.strategy.liquidity_mirroring.order_tracking.order_tracker import OrderTracker
from hummingbot.strategy.liquidity_mirroring.order_tracking.order import Order
from hummingbot.strategy.liquidity_mirroring.order_tracking.order_state import OrderState
from hummingbot.strategy.liquidity_mirroring.position import PositionManager
from hummingbot.strategy.liquidity_mirroring.book_modeling.model_book import ModelBook

NaN = Decimal("nan")
s_decimal_0 = Decimal(0)
as_logger = None


cdef class LiquidityMirroringStrategy(StrategyBase):
    OPTION_LOG_STATUS_REPORT = 1 << 0
    OPTION_LOG_CREATE_ORDER = 1 << 1
    OPTION_LOG_ORDER_COMPLETED = 1 << 2
    OPTION_LOG_PROFITABILITY_STEP = 1 << 3
    OPTION_LOG_FULL_PROFITABILITY_STEP = 1 << 4
    OPTION_LOG_INSUFFICIENT_ASSET = 1 << 5
    OPTION_LOG_ALL = 0xfffffffffffffff
    MARKET_ORDER_MAX_TRACKING_TIME = 0.4 * 10

    CANCEL_EXPIRY_DURATION = 60.0
    @classmethod
    def logger(cls):
        global as_logger
        if as_logger is None:
            as_logger = logging.getLogger(__name__)
        return as_logger

    def __init__(self,
                 primary_market_pair: MarketTradingPairTuple,
                 mirrored_market_pair: MarketTradingPairTuple,
                 two_sided_mirroring: bool,
                 order_price_markup: Decimal,
                 max_exposure_base: Decimal,
                 max_exposure_quote: Decimal,
                 max_offsetting_exposure: Decimal,
                 max_loss: Decimal,
                 max_total_loss: Decimal,
                 min_primary_amount: Decimal,
                 min_mirroring_amount: Decimal,
                 bid_amount_percents: list,
                 ask_amount_percents: list,
                 order_replacement_threshold: Decimal,
                 post_only: bool,
                 slack_hook: str,
                 slack_update_period: Decimal,
                 fee_override: Decimal,
                 logging_options: int = OPTION_LOG_ORDER_COMPLETED,
                 status_report_interval: Decimal = 60.0,
                 next_trade_delay_interval: Decimal = 15.0,
                 failed_order_tolerance: int = 2000000000):
        """
        :param market_pairs: list liquidity mirroring market pairs
        :param logging_options: select the types of logs to output
        :param status_report_interval: how often to report network connection related warnings, if any
        :param next_trade_delay_interval: cool off period between trades
        :param failed_order_tolerance: number of failed orders to force stop the strategy when exceeded
        """

        super().__init__()
        self._logging_options = logging_options
        self.primary_market_pair = primary_market_pair
        self.mirrored_market_pair = mirrored_market_pair
        self._all_markets_ready = False
        self._status_report_interval = status_report_interval
        self._last_timestamp = 0
        self._next_trade_delay = next_trade_delay_interval
        self._last_trade_timestamps = {}
        self._failed_order_tolerance = failed_order_tolerance
        self.two_sided_mirroring = two_sided_mirroring
        self.order_replacement_threshold = Decimal(order_replacement_threshold)
        self._failed_market_order_count = 0
        self._last_failed_market_order_timestamp = Decimal(0)

        self.c_add_markets([primary_market_pair.market, mirrored_market_pair.market])

        self.order_price_markup = Decimal(order_price_markup)
        self.max_exposure_base = Decimal(max_exposure_base)
        self.max_exposure_quote = Decimal(max_exposure_quote)
        self.max_offsetting_exposure = Decimal(max_offsetting_exposure)

        self.bid_amount_percents = bid_amount_percents
        self.ask_amount_percents = ask_amount_percents

        self.bid_amounts = []
        self.ask_amounts = []
        for amount in self.bid_amount_percents:
            self.bid_amounts.append(Decimal(amount * self.max_exposure_quote))
        for amount in self.ask_amount_percents:
            self.ask_amounts.append(Decimal(amount * self.max_exposure_base))

        self.outstanding_offsets = {}
        self.max_loss = Decimal(max_loss)

        self.max_total_loss = Decimal(max_total_loss)
        self.pm = PositionManager()
        self.offset_order_tracker = OrderTracker()

        self.balances_set = False     
        self.funds_message_sent = False
        self.offset_beyond_threshold_message_sent = False
        self.fail_message_sent = False
        self.crossed_books = False

        self.min_primary_amount = Decimal(min_primary_amount)
        self.min_mirroring_amount = Decimal(min_mirroring_amount)
        self.total_trading_volume = Decimal(0)
        self.trades_executed = 0
        
        cur_dir = os.getcwd()
        nonce = datetime.timestamp(datetime.now()) * 1000
        filename = os.path.join(cur_dir, 'logs', f'lm-performance-{nonce}.log')
        self.performance_logger = logging.getLogger()
        self.performance_logger.addHandler(logging.FileHandler(filename))

        self.best_bid_start = Decimal(0)
        self.slack_url = slack_hook
        self.cycle_number = 0
        self.start_time = datetime.timestamp(datetime.now())
        self.slack_update_period = slack_update_period

        self.mm_order_type = OrderType.LIMIT
        if post_only and OrderType.LIMIT_MAKER in primary_market_pair.market.supported_order_types():
            self.mm_order_type = OrderType.LIMIT_MAKER

        self.fee_override = fee_override

        self.desired_book = None
        self.current_book = OrderTracker()

    @property
    def tracked_limit_orders(self) -> List[Tuple[ExchangeBase, LimitOrder]]:
        return self._sb_order_tracker.tracked_limit_orders

    @property
    def tracked_market_orders(self) -> List[Tuple[ExchangeBase, MarketOrder]]:
        return self._sb_order_tracker.tracked_market_orders

    @property
    def tracked_taker_orders_data_frame(self) -> List[pd.DataFrame]:
        return self._sb_order_tracker.tracked_taker_orders_data_frame

    @property
    def pm(self):
        return self.pm

    def format_status(self) -> str:
        cdef:
            list lines = []
            list warning_lines = []
        total_balance = 0
        for market_pair in [self.primary_market_pair, self.mirrored_market_pair]:
            warning_lines.extend(self.network_warning([market_pair]))
            markets_df = self.market_status_data_frame([market_pair])
            lines.extend(["", "  Markets:"] +
                         ["    " + line for line in str(markets_df).split("\n")])

            assets_df = self.wallet_balance_data_frame([market_pair])
            lines.extend(["", "  Assets:"] +
                         ["    " + line for line in str(assets_df).split("\n")])
            total_balance += assets_df['Total Balance']

            warning_lines.extend(self.balance_warning([market_pair]))
        
        mirrored_market_df = self.market_status_data_frame([self.mirrored_market_pair])
        mult = mirrored_market_df["Best Bid Price"]
        profit = (total_balance[0] * float(mult)) - float(self.initial_base_amount * self.best_bid_start) + total_balance[1] - float(self.initial_quote_amount)
        change_in_base = Decimal(total_balance[0]) - self.initial_base_amount
        change_in_quote = Decimal(total_balance[1]) - self.initial_quote_amount
        current_time = datetime.now().isoformat()
        lines.extend(["", f"   Time: {current_time}"])
        lines.extend(["", f"   Executed Trades: {self.trades_executed}"])
        lines.extend(["", f"   Total Trade Volume: {self.total_trading_volume}"])
        lines.extend(["", f"   Total Balance ({self.primary_market_pair.base_asset}): {total_balance[0]}"])
        lines.extend(["", f"   Total Balance ({self.primary_market_pair.quote_asset}): {total_balance[1]}"])
        lines.extend(["", f"   Change in base: {change_in_base}"])
        lines.extend(["", f"   Change in quote: {change_in_quote}"])
        lines.extend(["", f"   Total pre-fee profit: {-self.pm.total_loss}"])
        lines.extend(["", f"   Overall Change in Holdings: {profit}"])
        lines.extend(["", f"   Amount to offset (in base currency): {self.pm.amount_to_offset}"])
        lines.extend(["", f"   Average price of position: {self.pm.avg_price}"])
        lines.extend(["", f"   Active market making orders: {len(self.current_book)}"])
        lines.extend(["", f"   Offsetting bids:"])
        for order in self.offset_order_tracker.get_bids():
            lines.extend(["", f"{order}"])
        lines.extend(["", f"   Offsetting asks:"])
        for order in self.offset_order_tracker.get_asks():
            lines.extend(["", f"{order}"])
        if len(warning_lines) > 0:
            lines.extend(["", "  *** WARNINGS ***"] + warning_lines)

        return "\n".join(lines)

    def slack_order_filled_message(self, market: str, amount: Decimal, price: Decimal, is_buy: bool):
        if is_buy:
            buy_sell = "BUY"
        else:
            buy_sell = "SELL"

        msg = {"msg_type": "order filled", "data": {"exchange": market, "pair": self.primary_market_pair.trading_pair, "price": price, "amount": amount, "buy/sell": buy_sell}}

        SlackPusher(self.slack_url, str(msg))

    def slack_insufficient_funds_message(self, market: str, asset: str):
        msg = f"{asset} balance low on {market}"
        SlackPusher(self.slack_url, msg)

    cdef c_tick(self, double timestamp):
        """
        Clock tick entry point.

        For liquidity mirroring strategy, this function simply checks for the readiness and connection status of markets, and
        then delegates the processing of each market pair to c_process_market_pair().

        :param timestamp: current tick timestamp
        """
        StrategyBase.c_tick(self, timestamp)

        cdef:
            int64_t current_tick = <int64_t>(timestamp // self._status_report_interval)
            int64_t last_tick = <int64_t>(self._last_timestamp // self._status_report_interval)
            bint should_report_warnings = ((current_tick > last_tick) and
                                           (self._logging_options & self.OPTION_LOG_STATUS_REPORT))
        try:
            if not self._all_markets_ready:
                self._all_markets_ready = all([market.ready for market in self._sb_markets])
                if not self._all_markets_ready:
                    # Markets not ready yet. Don't do anything.
                    if should_report_warnings:
                        self.logger().warning(f"Markets are not ready. No trading is permitted.")
                    return
                else:
                    if self.OPTION_LOG_STATUS_REPORT:
                        self.logger().info(f"Markets are ready. Trading started.")
            if not self.balances_set:
                primary_market = self.primary_market_pair.market
                mirrored_market = self.mirrored_market_pair.market
                primary_base_asset = self.primary_market_pair.base_asset
                primary_quote_asset = self.primary_market_pair.quote_asset
                mirrored_base_asset = self.mirrored_market_pair.base_asset
                mirrored_quote_asset = self.mirrored_market_pair.quote_asset
                while primary_market.get_balance(primary_base_asset) == 0:
                    pass
                while primary_market.get_balance(primary_quote_asset) == 0:
                    pass 
                while mirrored_market.get_balance(mirrored_base_asset) == 0:
                    pass
                while mirrored_market.get_available_balance(mirrored_quote_asset) == 0:
                    pass

                assets_df = self.wallet_balance_data_frame([self.mirrored_market_pair])
                total_balance = assets_df['Total Balance']
                assets_df = self.wallet_balance_data_frame([self.primary_market_pair])
                total_balance += assets_df['Total Balance']
                self.initial_base_amount = Decimal(total_balance[0])
                self.initial_quote_amount = Decimal(total_balance[1])
                self.balances_set = True

            if not all([market.network_status is NetworkStatus.CONNECTED for market in self._sb_markets]):
                if should_report_warnings:
                    self.logger().warning(f"Markets are not all online. No trading is permitted.")
                return
            if (self.pm.total_loss < self.max_total_loss):
                self.c_process_market_pair(self.mirrored_market_pair)
            else:
                self.logger().warning("Too much total offset loss!")
                SlackPusher(self.slack_url, "Total offset loss beyond threshold")
                safe_ensure_future(self.primary_market_pair.market.cancel_all(5.0))
                safe_ensure_future(self.mirrored_market_pair.market.cancel_all(5.0))
        finally:
            self._last_timestamp = timestamp

    cdef bint is_maker_exchange(self, object market):
        return market == self.primary_market_pair.market

    cdef bint is_taker_exchange(self, object market):
        return market == self.mirrored_market_pair.market

    cdef c_did_fill_order(self, object order_filled_event):
        cdef:
            str order_id = order_filled_event.order_id
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
            object market
            int side_multiplier = 1 if order_filled_event.trade_type is TradeType.BUY else -1
            object previous_amount_to_offset = self.pm.amount_to_offset

        # Verify this is from a market we are tracking
        if market_trading_pair_tuple is None:
            return
        market = market_trading_pair_tuple.market

        # Update our common tracking stats
        self.total_trading_volume += order_filled_event.amount
        self.trades_executed += 1
        self.pm.register_trade(order_filled_event.price, side_multiplier * order_filled_event.amount)
        if self.is_maker_exchange(market):
            self.current_book.fill(order_id, order_filled_event.amount)
        elif self.is_taker_exchange(market):
            self.offset_order_tracker.fill(order_id, order_filled_event.amount)
        else:
            # This should obviously never happen
            raise Exception(f"Unknown exchange for strategy OrderFillEvent handler: {market.name}")

        # Emit log messages for this event
        self.slack_order_filled_message(market.name, 
                                        order_filled_event.amount, 
                                        order_filled_event.price, 
                                        order_filled_event.trade_type is TradeType.BUY)
        if self._logging_options & self.OPTION_LOG_ORDER_COMPLETED:
            self.log_with_clock(logging.INFO,
                                f"Limit order filled on {market.name}: {order_id} ({order_filled_event.price}, {order_filled_event.amount}) Amount to offset: {self.pm.amount_to_offset}")

        # Adjust our offseting orders to account for this fill
        self._issue_mirrored_orderbook_update()

    cdef _did_create_order(self, object order_created_event):
        cdef:
            str order_id = order_created_event.order_id
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
        if market_trading_pair_tuple is not None:
            if self.is_maker_exchange(market_trading_pair_tuple.market):
                self.current_book.update_order(order_id, OrderState.ACTIVE, Decimal(0))
            elif self.is_taker_exchange(market_trading_pair_tuple.market):
                self.offset_order_tracker.update_order(order_id, OrderState.ACTIVE, Decimal(0))

    cdef c_did_create_buy_order(self, object buy_order_created_event):
        self._did_create_order(buy_order_created_event)

    cdef c_did_create_sell_order(self, object sell_order_created_event):
        self._did_create_order(sell_order_created_event)

    cdef _did_complete_order(self, object completed_event):
        cdef:
            str order_id = completed_event.order_id
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
        if market_trading_pair_tuple is not None:
            if self.is_maker_exchange(market_trading_pair_tuple.market):    
                price = completed_event.quote_asset_amount/completed_event.base_asset_amount
                if self._logging_options & self.OPTION_LOG_ORDER_COMPLETED:
                    self.log_with_clock(logging.INFO,
                                        f"Limit order completed on {market_trading_pair_tuple[0].name}: {order_id} ({price}, {completed_event.base_asset_amount})")
                self.current_book.complete(order_id)
            elif self.is_taker_exchange(market_trading_pair_tuple.market):
                self.offset_order_tracker.complete(order_id)

    cdef c_did_complete_buy_order(self, object buy_order_completed_event):
        self._did_complete_order(buy_order_completed_event)

    cdef c_did_complete_sell_order(self, object sell_order_completed_event):
        self._did_complete_order(sell_order_completed_event)

    cdef c_did_fail_order(self, object fail_event):
        """
        Output log for failed order.

        :param fail_event: Order failure event
        """
        cdef:
            str order_id = fail_event.order_id
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
        full_order = self._sb_order_tracker.c_get_limit_order(market_trading_pair_tuple, order_id)
        if fail_event.order_type is OrderType.LIMIT:
            # if not self.fail_message_sent:
            #     market = market_trading_pair_tuple.market.name
            #     price = full_order.price
            #     amount = full_order.quantity
            #     buy_sell = "BUY" if full_order.is_buy else "SELL"
            #     msg = {"msg_type": "order failed", "data": {"market": market, "price": price, "amount": amount, "buy/sell": buy_sell, "id": order_id}}

            #     SlackPusher(self.slack_url, "ORDER FAILED: " + str(msg))
            #     self.fail_message_sent = True
            self._failed_market_order_count += 1
            self._last_failed_market_order_timestamp = fail_event.timestamp

        if self._failed_market_order_count > self._failed_order_tolerance:
            failed_order_kill_switch_log = \
                f"Strategy is forced stop by failed order kill switch. " \
                f"Failed market order count {self._failed_market_order_count} exceeded tolerance level of " \
                f"{self._failed_order_tolerance}. Please check market connectivity before restarting."

            self.logger().network(failed_order_kill_switch_log, app_warning_msg=failed_order_kill_switch_log)
            self.c_stop(self._clock)
        if market_trading_pair_tuple is not None:
            self.log_with_clock(logging.INFO,
                f"Limit order failed on {market_trading_pair_tuple[0].name}: {order_id}")
            if self.is_maker_exchange(market_trading_pair_tuple.market):
                self.current_book.fail(order_id)
            elif self.is_taker_exchange(market_trading_pair_tuple.market):
                self.offset_order_tracker.fail(order_id)
                self._issue_mirrored_orderbook_update()

    cdef c_did_cancel_order(self, object cancel_event):
        """
        Output log for cancelled order.

        :param cancel_event: Order cancelled event.
        """
        cdef:
            str order_id = cancel_event.order_id
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
        if market_trading_pair_tuple is not None:
            full_order = self._sb_order_tracker.c_get_limit_order(market_trading_pair_tuple, order_id)
            if self.is_maker_exchange(market_trading_pair_tuple.market):
                self.current_book.cancel(order_id)
            elif self.is_taker_exchange(market_trading_pair_tuple.market):
                self.offset_order_tracker.cancel(order_id)
                self._issue_mirrored_orderbook_update()
            self.log_with_clock(logging.INFO,
                                f"Limit order canceled on {market_trading_pair_tuple[0].name}: {order_id}")

    cdef c_check_balances(self):
        current_time = datetime.timestamp(datetime.now())
        time_elapsed = current_time - self.start_time
        if (time_elapsed > 1800):
            if self.funds_message_sent == True:
                self.funds_message_sent = False
        if (time_elapsed > 60):
            if self.fail_message_sent == True:
                self.fail_message_sent = False
        if (time_elapsed > (3600 * self.slack_update_period)):
            self.start_time = current_time
            SlackPusher(self.slack_url, self.format_status())

    cdef c_process_market_pair(self, object market_pair):
        # prepare our new desired book
        bids = list(market_pair.order_book_bid_entries())
        asks = list(market_pair.order_book_ask_entries())
        new_desired_book = ModelBook(
            bids=bids, 
            asks=asks,
            min_amount=self.min_primary_amount
        )
        new_desired_book.cut_claimed_amounts(self.offset_order_tracker)
        #new_desired_book.aggregate(price_precision)    # add this if you want to aggregate price levels by a certain precision
        new_desired_book.apply_limits()
        new_desired_book.crop(len(self.bid_amounts), len(self.ask_amounts))
        #new_desired_book.scale_amounts(self.mirroring_amount_scale) # add this if we want to scale back the size of our mirrored orders vs the source order
        new_desired_book.markup(self.order_price_markup, self.order_price_markup, self.fee_override)
        new_desired_book.limit_by_ratios(self.bid_amount_percents, self.ask_amount_percents)
        new_desired_book.apply_limits()
        self.desired_book = new_desired_book

        # Increment cycle tracking
        self.cycle_number += 1
        self.cycle_number %= 10

        # Dispatch periodic tasks that run on certain cycles
        if self.cycle_number == 8:
            self.c_check_balances()

        if ((self.cycle_number % 2) == 0):
            self.logger().info(f"Amount to offset: {self.pm.amount_to_offset}")

        # Adjust the orderbooks based on our new desired orders
        self.adjust_primary_orderbook()

        if self.two_sided_mirroring:
            self.adjust_mirrored_orderbook

    def _get_available_balances(self):
        """" Returns the availabale balance that we have for use in placing new primary market orders
        this includes all calculations related to ensuring that we have enough balance on the offsetting exchange
        to cover any fills that we might recieve 
        """
        primary_market = self.primary_market_pair.market
        mirrored_market = self.mirrored_market_pair.market
        available_primary_base = primary_market.get_available_balance(self.primary_market_pair.base_asset)
        available_primary_quote = primary_market.get_available_balance(self.primary_market_pair.quote_asset)
        available_mirrored_base = mirrored_market.get_available_balance(self.mirrored_market_pair.base_asset)
        available_mirrored_quote = mirrored_market.get_available_balance(self.mirrored_market_pair.quote_asset)

        best_bid = self.desired_book.best_bid
        best_ask = self.desired_book.best_ask

        return get_available_balances(self.pm, self.offset_order_tracker, best_bid, best_ask, self.max_loss,
                                      available_primary_base, available_primary_quote, available_mirrored_base, available_mirrored_quote)

    def _place_primary_order(self, order):
        primary_market = self.primary_market_pair.market
        quant_price = primary_market.quantize_order_price(self.primary_market_pair.trading_pair, order.price)
        quant_amount = primary_market.quantize_order_amount(self.primary_market_pair.trading_pair, order.amount_remaining)
        place_order = self.buy_with_specific_market if order.side is TradeType.BUY else self.sell_with_specific_market

        try:
            order_id = place_order(self.primary_market_pair, quant_amount, OrderType.LIMIT, quant_price)
            self.current_book.add_order(Order(order_id, order.price, order.amount_remaining, order.side, OrderState.PENDING))
            return True
        except:
            return False

    def _place_primary_orders(self, orders, available_amount):
        for order in orders:
            if (order.amount_remaining <= available_amount and not self.current_book.would_cross(order)):
                if self._place_primary_order(order):
                    available_amount -= order.amount_remaining

    # TODO with these changes, we should have a process that cancells all primary orders if we have network problems getting the updated
    # orderbook from the mirrored exchange
    # TODO, limit by max exposure as well as balances
    def adjust_primary_orderbook(self): # TODO make async and take a lock like adjust_mirrored_orderbook and then call whenever appropreate events take place
        if self.desired_book is None:
            return

        available_base, available_quote = self._get_available_balances()
        bids_to_place, asks_to_place, orders_to_cancel = self.desired_book.steps_from(self.current_book)

        # Cancel any orders that we no longer want
        for order in orders_to_cancel:
            if order.is_live_uncancelled():
                try:
                    self.c_cancel_order(self.primary_market_pair, order.id)
                    order.mark_canceled()
                except Exception as e:
                    self.logger().info(f"failed to cancel order {e}")

        # Place any new orders that we have the available balances to handle
        self._place_primary_orders(bids_to_place, available_quote)
        self._place_primary_orders(asks_to_place, available_base)

    def _manage_offsetting_exposure_threashold_warning(self):
        if not self.offset_beyond_threshold_message_sent:
            if (abs(self.pm.amount_to_offset) > self.max_offsetting_exposure):
                SlackPusher(self.slack_url, "Offsetting exposure beyond threshold")
                self.offset_beyond_threshold_message_sent = True
        else:
            if (abs(self.pm.amount_to_offset) < self.max_offsetting_exposure):
                SlackPusher(self.slack_url, "Offsetting exposure within threshold")
                self.offset_beyond_threshold_message_sent = False

    def _issue_mirrored_orderbook_update(self):
        if self.two_sided_mirroring:
            safe_ensure_future(self.adjust_mirrored_orderbook())

    def _cancel_offsetting_order(self, mirrored_market_pair, order):
        if order.state != OrderState.PENDING_CANCEL:
            self.c_cancel_order(mirrored_market_pair, order.id)
            order.state = OrderState.PENDING_CANCEL

    def _limit_order_amount(self, mirrored_market: ExchangeBase, mirrored_asset: str, amount: Decimal, quant_price: Decimal, side: TradeType):
        if side is TradeType.BUY:
            return min((mirrored_market.get_balance(mirrored_asset)/quant_price), amount)
        else:
            return min(mirrored_market.get_balance(mirrored_asset), amount)
        

    async def adjust_mirrored_orderbook(self):
        async with self.offset_order_tracker:
            self._manage_offsetting_exposure_threashold_warning()
            if not self.two_sided_mirroring:
                return
            
            mirrored_market_pair = self.mirrored_market_pair
            mirrored_market: ExchangeBase = mirrored_market_pair.market
            current_offsetting_amounts = self.offset_order_tracker.get_total_amounts()
            if self.pm.amount_to_offset < Decimal(0):
                wrong_sided_orders = self.offset_order_tracker.get_asks()
                right_sided_orders = sorted(self.offset_order_tracker.get_bids(), key = lambda o: o.price, reverse=False)
                diff : Decimal = abs(self.pm.amount_to_offset) - current_offsetting_amounts.buys
                max_loss_markdown = (Decimal(1) + self.max_loss)
                new_order_side = TradeType.BUY
                place_order_fn = self.buy_with_specific_market
                mirrored_asset = mirrored_market_pair.quote_asset
            elif self.pm.amount_to_offset > Decimal(0):
                wrong_sided_orders = self.offset_order_tracker.get_bids()
                right_sided_orders = sorted(self.offset_order_tracker.get_asks(), key = lambda o: o.price, reverse=True)
                diff : Decimal = abs(self.pm.amount_to_offset) - current_offsetting_amounts.sells
                max_loss_markdown = (Decimal(1) - self.max_loss)
                new_order_side = TradeType.SELL
                place_order_fn = self.sell_with_specific_market
                mirrored_asset = mirrored_market_pair.base_asset
            else:
                return

            # Get rid of wrong sided orders if there are any
            for order in wrong_sided_orders:
                self._cancel_offsetting_order(mirrored_market_pair, order)
            
            # Compare current amount to offset vs. current offsetting amount from orders
            if diff > 0:                
                # We need to place more offsetting orders
                new_price = self.pm.avg_price * max_loss_markdown
                quant_price = mirrored_market.c_quantize_order_price(mirrored_market_pair.trading_pair, Decimal(new_price))
                amount = self._limit_order_amount(mirrored_market, mirrored_asset, diff, quant_price, new_order_side)
                if amount >= self.min_mirroring_amount:
                    quant_amount = mirrored_market.c_quantize_order_amount(mirrored_market_pair.trading_pair, Decimal(amount))
                    if quant_amount > 0:
                        try:
                            order_id = place_order_fn(mirrored_market_pair, Decimal(quant_amount), OrderType.LIMIT, Decimal(quant_price))
                            self.offset_order_tracker.add_order(Order(order_id, quant_price, quant_amount, new_order_side, OrderState.PENDING))
                        except:
                            self.logger().error(f"Failed to c_{str(new_order_side).lower()}_with_specific_market: {mirrored_market_pair.trading_pair}"\
                                            f" {Decimal(quant_amount)} {Decimal(quant_price)}")

            elif diff < 0:
                # We are trying to offset too much compared to our ammount to offset
                # Attempt to cancel enough to make up the difference, starting with the least likely to execute orders
                for order in right_sided_orders:
                    self._cancel_offsetting_order(mirrored_market_pair, order)
                    diff -= order.amount_remaining
                    if diff <= 0:
                        break
                # We might now be at a deficit of offsetting amount, but we won't place any new orders under the assumption
                # that some of these cancelled orders will execute before we can cancel them.
                # If this is not the case, it will be picked up and offset on the next call to this function
