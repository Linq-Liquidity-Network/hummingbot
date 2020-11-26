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
from hummingbot.market.market_base cimport MarketBase
from hummingbot.core.event.events import (
    TradeType,
    OrderType,
)
from hummingbot.core.data_type.market_order import MarketOrder
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.strategy import market_trading_pair_tuple
from hummingbot.strategy.strategy_base import StrategyBase
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.liquidity_mirroring.liquidity_mirroring_market_pair import LiquidityMirroringMarketPair
from hummingbot.strategy.liquidity_mirroring.position import PositionManager
from hummingbot.strategy.liquidity_mirroring.book_state import BookState, Order

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
    FAILED_ORDER_COOL_OFF_TIME = 0.0

    CANCEL_EXPIRY_DURATION = 60.0
    @classmethod
    def logger(cls):
        global as_logger
        if as_logger is None:
            as_logger = logging.getLogger(__name__)
        return as_logger

    def __init__(self,
                 primary_market_pairs: List[MarketTradingPairTuple],
                 mirrored_market_pairs: List[MarketTradingPairTuple],
                 two_sided_mirroring: bool,
                 order_price_markup: Decimal,
                 max_exposure_base: Decimal,
                 max_exposure_quote: Decimal,
                 max_offsetting_exposure: Decimal,
                 max_loss: Decimal,
                 max_total_loss: Decimal,
                 equivalent_tokens: list,
                 min_primary_amount: Decimal,
                 min_mirroring_amount: Decimal,
                 bid_amount_percents: list,
                 ask_amount_percents: list,
                 slack_hook: str,
                 slack_update_period: Decimal,
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
        self.primary_market_pairs = primary_market_pairs
        self.mirrored_market_pairs = mirrored_market_pairs
        
        self._all_markets_ready = False
        self._status_report_interval = status_report_interval
        self._last_timestamp = 0
        self._next_trade_delay = next_trade_delay_interval
        self._last_trade_timestamps = {}
        self._failed_order_tolerance = failed_order_tolerance
        self._cool_off_logged = False
        self.two_sided_mirroring = two_sided_mirroring
        self._failed_market_order_count = 0
        self._last_failed_market_order_timestamp = Decimal(0)
                                                                
        cdef:
            set all_markets = {
                primary_market_pairs[0].market,
                mirrored_market_pairs[0].market
            }

        self.c_add_markets(list(all_markets))
        
        # initialize the bounds of the orderbooks
        self.primary_best_bid = Decimal(0)
        self.primary_best_ask = Decimal("inf")
        self.mirrored_best_bid = Decimal(0)
        self.mirrored_best_ask = Decimal("inf")

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
        self.equivalent_tokens = equivalent_tokens

        self.max_total_loss = Decimal(max_total_loss)
        self.pm = PositionManager()

        self.offset_base_exposure = Decimal(0)
        self.offset_quote_exposure = Decimal(0)

        self.primary_base_balance = Decimal(0)
        self.primary_base_total_balance = Decimal(0)
        self.primary_quote_balance = Decimal(0)
        self.primary_quote_total_balance = Decimal(0)
        self.mirrored_base_balance = Decimal(0)
        self.mirrored_base_total_balance = Decimal(0)
        self.mirrored_quote_balance = Decimal(0)
        self.mirrored_quote_total_balance = Decimal(0)
        self.balances_set = False     
        self.funds_message_sent = False
        self.offset_beyond_threshold_message_sent = False
        self.fail_message_sent = False

        self.min_primary_amount = Decimal(min_primary_amount)
        self.min_mirroring_amount = Decimal(min_mirroring_amount)
        self.total_trading_volume = Decimal(0)
        self.trades_executed = 0

        self.marked_for_deletion = {}
        self.buys_to_replace = list(range(0,len(self.bid_amounts)))
        self.sells_to_replace = list(range(0,len(self.ask_amounts)))
        self.bid_replace_ranks = []
        self.ask_replace_ranks = []
        self.has_been_offset = []

        self.previous_sells = [Decimal(0) for i in range(0, len(self.ask_amounts))]
        self.previous_buys = [Decimal(0) for i in range(0, len(self.bid_amounts))]
        
        cur_dir = os.getcwd()
        nonce = datetime.timestamp(datetime.now()) * 1000
        filename = os.path.join(cur_dir, 'logs', f'lm-performance-{nonce}.log')
        self.performance_logger = logging.getLogger()
        self.performance_logger.addHandler(logging.FileHandler(filename))

        self.best_bid_start = Decimal(0)
        self.slack_url = slack_hook
        self.cycle_number = 0
        self.start_time = datetime.timestamp(datetime.now())
        self.start_wallet_check_time = self.start_time
        self.slack_update_period = slack_update_period

        self.desired_book = BookState()
        self.current_book = BookState()

    @property
    def tracked_taker_orders(self) -> List[Tuple[MarketBase, MarketOrder]]:
        return self._sb_order_tracker.tracked_taker_orders

    @property
    def tracked_maker_orders(self) -> List[Tuple[MarketBase, MarketOrder]]:
        return self._sb_order_tracker.tracked_maker_orders

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
        for market_pair in (self.primary_market_pairs + self.mirrored_market_pairs):
            warning_lines.extend(self.network_warning([market_pair]))
            markets_df = self.market_status_data_frame([market_pair])
            lines.extend(["", "  Markets:"] +
                         ["    " + line for line in str(markets_df).split("\n")])

            assets_df = self.wallet_balance_data_frame([market_pair])
            lines.extend(["", "  Assets:"] +
                         ["    " + line for line in str(assets_df).split("\n")])
            total_balance += assets_df['Total Balance']

            warning_lines.extend(self.balance_warning([market_pair]))
        
        mirrored_market_df = self.market_status_data_frame([self.mirrored_market_pairs[0]])
        mult = mirrored_market_df["Best Bid Price"]
        profit = (total_balance[0] * float(mult)) - float(self.initial_base_amount * self.best_bid_start) + total_balance[1] - float(self.initial_quote_amount)
        portfolio = (total_balance[0] * float(mult)) - (float(self.initial_base_amount) * float(mult)) + total_balance[1] - float(self.initial_quote_amount)
        current_time = datetime.now().isoformat()
        lines.extend(["", f"   Time: {current_time}"])
        lines.extend(["", f"   Executed Trades: {self.trades_executed}"])
        lines.extend(["", f"   Total Trade Volume: {self.total_trading_volume}"])
        lines.extend(["", f"   Total Balance ({self.primary_market_pairs[0].base_asset}): {total_balance[0]}"])
        lines.extend(["", f"   Total Balance ({self.primary_market_pairs[0].quote_asset}): {total_balance[1]}"])
        lines.extend(["", f"   Overall Change in Holdings: {profit}"])
        lines.extend(["", f"   Increase in Portfolio: {portfolio}"])
        lines.extend(["", f"   Amount to offset (in base currency): {self.pm.amount_to_offset}"])
        lines.extend(["", f"   Average price of position: {self.pm.avg_price}"])
        lines.extend(["", f"   Total running loss: {self.pm.total_loss}"])
        lines.extend(["", f"   Active market making orders: {len(self.marked_for_deletion.keys())}"])
        if len(warning_lines) > 0:
            lines.extend(["", "  *** WARNINGS ***"] + warning_lines)

        return "\n".join(lines)

    def slack_order_filled_message(self, market: str, amount: Decimal, price: Decimal, is_buy: bool):
        if is_buy:
            buy_sell = "BUY"
        else:
            buy_sell = "SELL"

        msg = {"msg_type": "order filled", "data": {"exchange": market, "pair": self.primary_market_pairs[0].trading_pair,"price": price, "amount": amount, "buy/sell": buy_sell}}

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
                primary_market = self.primary_market_pairs[0].market
                mirrored_market = self.mirrored_market_pairs[0].market
                primary_base_asset = self.primary_market_pairs[0].base_asset
                primary_quote_asset = self.primary_market_pairs[0].quote_asset
                mirrored_base_asset = self.mirrored_market_pairs[0].base_asset
                mirrored_quote_asset = self.mirrored_market_pairs[0].quote_asset
                while self.primary_base_balance == 0:
                    self.primary_base_balance = primary_market.get_available_balance(primary_base_asset)
                    self.primary_base_total_balance = primary_market.get_balance(primary_base_asset)
                while self.primary_quote_balance == 0:
                    self.primary_quote_balance = primary_market.get_available_balance(primary_quote_asset)
                    self.primary_quote_total_balance = primary_market.get_balance(primary_quote_asset)
                while self.mirrored_base_balance == 0:
                    self.mirrored_base_balance = mirrored_market.get_available_balance(mirrored_base_asset)
                    self.mirrored_base_total_balance = mirrored_market.get_balance(mirrored_base_asset)
                while self.mirrored_quote_balance == 0:
                    self.mirrored_quote_balance = mirrored_market.get_available_balance(mirrored_quote_asset)
                    self.mirrored_quote_total_balance = mirrored_market.get_balance(mirrored_quote_asset)
                assets_df = self.wallet_balance_data_frame([self.mirrored_market_pairs[0]])
                total_balance = assets_df['Total Balance']
                assets_df = self.wallet_balance_data_frame([self.primary_market_pairs[0]])
                total_balance += assets_df['Total Balance']
                self.initial_base_amount = Decimal(total_balance[0])
                self.initial_quote_amount = Decimal(total_balance[1])
                self.balances_set = True

            if not all([market.network_status is NetworkStatus.CONNECTED for market in self._sb_markets]):
                if should_report_warnings:
                    self.logger().warning(f"Markets are not all online. No trading is permitted.")
                return
            if (self.pm.total_loss < self.max_total_loss):
                for market_pair in self.mirrored_market_pairs:
                    self.c_process_market_pair(market_pair)
            else:
                self.logger().warning("Too much total offset loss!")
                SlackPusher(self.slack_url, "Total offset loss beyond threshold")
                safe_ensure_future(self.primary_market_pairs[0].market.cancel_all(5.0))
                safe_ensure_future(self.mirrored_market_pairs[0].market.cancel_all(5.0))
        finally:
            self._last_timestamp = timestamp

    def cancel_offsetting_orders(self):
        mirrored_market_pair: MarketTradingPairTuple = self.mirrored_market_pairs[0]
        active_orders = self._sb_order_tracker.market_pair_to_active_orders
        current_orders = []
        if mirrored_market_pair in active_orders:
            current_orders = active_orders[mirrored_market_pair][:]
        for order in current_orders:
            self.c_cancel_order(mirrored_market_pair,order.client_order_id)
        

    cdef c_did_fill_order(self, object order_filled_event):
        cdef:
            str order_id = order_filled_event.order_id
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
        if market_trading_pair_tuple is not None:
            if order_filled_event.trade_type == TradeType.BUY:
                self.total_trading_volume += order_filled_event.amount
                self.trades_executed += 1
                if market_trading_pair_tuple.market == self.primary_market_pairs[0].market:
                    if f"{order_id}COMPLETE" not in self.has_been_offset:
                        previous_amount_to_offset = self.pm.amount_to_offset
                        self.pm.register_trade(order_filled_event.price, order_filled_event.amount)
                        if abs(self.pm.amount_to_offset) < abs(previous_amount_to_offset):
                            self.cancel_offsetting_orders()

                        self.primary_base_balance += order_filled_event.amount
                        if order_id in self.marked_for_deletion.keys():
                            order = self.marked_for_deletion[order_id]
                            self.buys_to_replace.append(order["rank"])
                        self.primary_base_total_balance += order_filled_event.amount
                        self.primary_quote_total_balance -= order_filled_event.amount * order_filled_event.price
                        self.has_been_offset.append(order_id)
                        self.slack_order_filled_message(self.primary_market_pairs[0].market.name, order_filled_event.amount, order_filled_event.price, True)
                    else:
                        self.has_been_offset.remove(f"{order_id}COMPLETE")
                else:
                    if f"{order_id}COMPLETE" not in self.has_been_offset:
                        self.pm.register_trade(order_filled_event.price, order_filled_event.amount)
                        self.offset_quote_exposure -= (order_filled_event.amount * order_filled_event.price)
                        self.mirrored_base_balance += order_filled_event.amount
                        self.mirrored_base_total_balance += order_filled_event.amount
                        self.mirrored_quote_total_balance -= order_filled_event.price * order_filled_event.amount
                        self.has_been_offset.append(order_id)
                        self.slack_order_filled_message(self.mirrored_market_pairs[0].market.name, order_filled_event.amount, order_filled_event.price, True)
                    else:
                        self.has_been_offset.remove(f"{order_id}COMPLETE")
                if self._logging_options & self.OPTION_LOG_ORDER_COMPLETED:
                    self.log_with_clock(logging.INFO,
                    f"Limit order filled on {market_trading_pair_tuple[0].name}: {order_id} ({order_filled_event.price}, {order_filled_event.amount})")
            elif order_filled_event.trade_type == TradeType.SELL:
                self.total_trading_volume += order_filled_event.amount
                self.trades_executed += 1
                if market_trading_pair_tuple.market == self.primary_market_pairs[0].market:
                    if f"{order_id}COMPLETE" not in self.has_been_offset:
                        previous_amount_to_offset = self.pm.amount_to_offset
                        self.pm.register_trade(order_filled_event.price, -order_filled_event.amount)
                        if abs(self.pm.amount_to_offset) < abs(previous_amount_to_offset):
                            self.cancel_offsetting_orders()

                        self.primary_quote_balance += order_filled_event.price * order_filled_event.amount
                        if order_id in self.marked_for_deletion.keys():
                            order = self.marked_for_deletion[order_id]
                            self.sells_to_replace.append(order["rank"])
                        self.primary_quote_total_balance += order_filled_event.price * order_filled_event.amount
                        self.primary_base_total_balance -= order_filled_event.amount
                        self.has_been_offset.append(order_id)
                        self.slack_order_filled_message(self.primary_market_pairs[0].market.name, order_filled_event.amount, order_filled_event.price, False)
                    else:
                        self.has_been_offset.remove(f"{order_id}COMPLETE")
                else:
                    if f"{order_id}COMPLETE" not in self.has_been_offset:
                        self.pm.register_trade(order_filled_event.price, -order_filled_event.amount)
                        self.offset_base_exposure -= order_filled_event.amount
                        self.mirrored_quote_balance += order_filled_event.price * order_filled_event.amount
                        self.mirrored_quote_total_balance += order_filled_event.price * order_filled_event.amount
                        self.mirrored_base_total_balance -= order_filled_event.amount
                        self.has_been_offset.append(order_id)
                        self.slack_order_filled_message(self.mirrored_market_pairs[0].market.name, order_filled_event.amount, order_filled_event.price, False)
                    else:
                        self.has_been_offset.remove(f"{order_id}COMPLETE")
                if self._logging_options & self.OPTION_LOG_ORDER_COMPLETED:
                    self.log_with_clock(logging.INFO,
                        f"Limit order filled on {market_trading_pair_tuple[0].name}: {order_id} ({order_filled_event.price}, -{order_filled_event.amount})")
        for order in self.has_been_offset:
            if not (order == order_id):
                self.has_been_offset.remove(order)

    cdef c_did_create_buy_order(self, object buy_order_created_event):
        cdef:
            str order_id = buy_order_created_event.order_id
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
        if market_trading_pair_tuple is not None:
            if (market_trading_pair_tuple.market == self.primary_market_pairs[0].market):
                self.primary_quote_balance -= buy_order_created_event.amount * buy_order_created_event.price
                num_seconds = random.randint(30,50)
                expiration_time = datetime.timestamp(datetime.now() + timedelta(seconds=num_seconds)) 
                self.marked_for_deletion[order_id]["time"] = expiration_time
            elif (market_trading_pair_tuple.market == self.mirrored_market_pairs[0].market):
                self.mirrored_quote_balance -= buy_order_created_event.amount * buy_order_created_event.price
                self.offset_quote_exposure += buy_order_created_event.amount * buy_order_created_event.price

    cdef c_did_create_sell_order(self, object sell_order_created_event):
        cdef:
            str order_id = sell_order_created_event.order_id
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
        if market_trading_pair_tuple is not None:
            if (market_trading_pair_tuple.market == self.primary_market_pairs[0].market):
                self.primary_base_balance -= sell_order_created_event.amount
                num_seconds = random.randint(30,50)
                expiration_time = datetime.timestamp(datetime.now() + timedelta(seconds=num_seconds))
                self.marked_for_deletion[order_id]["time"] = expiration_time
            elif (market_trading_pair_tuple.market == self.mirrored_market_pairs[0].market):
                self.mirrored_base_balance -= sell_order_created_event.amount
                self.offset_base_exposure += sell_order_created_event.amount

    cdef c_did_complete_buy_order(self, object buy_order_completed_event):
        """
        Output log for completed buy order.

        :param buy_order_completed_event: Order completed event
        """
        cdef:
            str order_id = buy_order_completed_event.order_id
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
        if market_trading_pair_tuple is not None:         
            price = buy_order_completed_event.quote_asset_amount/buy_order_completed_event.base_asset_amount
            if market_trading_pair_tuple.market == self.primary_market_pairs[0].market:
                if order_id not in self.has_been_offset:
                    self.total_trading_volume += buy_order_completed_event.quote_asset_amount
                    self.trades_executed += 1
                    previous_amount_to_offset = self.pm.amount_to_offset
                    self.pm.register_trade(price, buy_order_completed_event.base_asset_amount)
                    if abs(self.pm.amount_to_offset) < abs(previous_amount_to_offset):
                        self.cancel_offsetting_orders()

                    self.primary_base_balance += buy_order_completed_event.base_asset_amount
                    if order_id in self.marked_for_deletion.keys():
                        order = self.marked_for_deletion[order_id]
                        self.buys_to_replace.append(order["rank"])
                    self.primary_base_total_balance += buy_order_completed_event.base_asset_amount
                    self.primary_quote_total_balance -= buy_order_completed_event.quote_asset_amount
                    self.has_been_offset.append(f"{order_id}COMPLETE")
                    self.slack_order_filled_message(self.primary_market_pairs[0].market.name, buy_order_completed_event.base_asset_amount, price, True)
            else:
                if order_id not in self.has_been_offset:
                    self.total_trading_volume += buy_order_completed_event.quote_asset_amount
                    self.trades_executed += 1
                    self.pm.register_trade(price, buy_order_completed_event.base_asset_amount)

                    self.offset_quote_exposure -= buy_order_completed_event.quote_asset_amount
                    self.mirrored_base_balance += buy_order_completed_event.base_asset_amount
                    self.mirrored_base_total_balance += buy_order_completed_event.base_asset_amount
                    self.mirrored_quote_total_balance -= buy_order_completed_event.quote_asset_amount
                    self.has_been_offset.append(f"{order_id}COMPLETE")
                    self.slack_order_filled_message(self.mirrored_market_pairs[0].market.name, buy_order_completed_event.base_asset_amount, price, True)
            if self._logging_options & self.OPTION_LOG_ORDER_COMPLETED:
                self.log_with_clock(logging.INFO,
                                    f"Limit order completed on {market_trading_pair_tuple[0].name}: {order_id} ({price}, {buy_order_completed_event.base_asset_amount})")
            if order_id in self.marked_for_deletion:
                del self.marked_for_deletion[order_id]

    cdef c_did_complete_sell_order(self, object sell_order_completed_event):
        """
        Output log for completed sell order.

        :param sell_order_completed_event: Order completed event
        """
        cdef:
            str order_id = sell_order_completed_event.order_id
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
        if market_trading_pair_tuple is not None:
            price = sell_order_completed_event.quote_asset_amount/sell_order_completed_event.base_asset_amount
            if market_trading_pair_tuple.market == self.primary_market_pairs[0].market:
                if order_id not in self.has_been_offset:
                    self.total_trading_volume += sell_order_completed_event.quote_asset_amount
                    self.trades_executed += 1
                    previous_amount_to_offset = self.pm.amount_to_offset
                    self.pm.register_trade(price, -sell_order_completed_event.base_asset_amount)
                    if abs(self.pm.amount_to_offset) < abs(previous_amount_to_offset):
                        self.cancel_offsetting_orders()

                    self.primary_quote_balance += sell_order_completed_event.quote_asset_amount
                    if order_id in self.marked_for_deletion.keys():
                        order = self.marked_for_deletion[order_id]
                        self.sells_to_replace.append(order["rank"])
                    self.primary_quote_total_balance += sell_order_completed_event.quote_asset_amount
                    self.primary_base_total_balance -= sell_order_completed_event.base_asset_amount
                    self.has_been_offset.append(f"{order_id}COMPLETE")
                    self.slack_order_filled_message(self.primary_market_pairs[0].market.name, sell_order_completed_event.base_asset_amount, price, False)
            else:
                if order_id not in self.has_been_offset:
                    self.total_trading_volume += sell_order_completed_event.quote_asset_amount
                    self.trades_executed += 1
                    self.pm.register_trade(price, -sell_order_completed_event.base_asset_amount)
                    self.offset_base_exposure -= sell_order_completed_event.base_asset_amount
                    self.mirrored_quote_balance += sell_order_completed_event.quote_asset_amount
                    self.mirrored_quote_total_balance += sell_order_completed_event.quote_asset_amount
                    self.mirrored_base_total_balance -= sell_order_completed_event.base_asset_amount
                    self.has_been_offset.append(f"{order_id}COMPLETE")
                    self.slack_order_filled_message(self.mirrored_market_pairs[0].market.name, sell_order_completed_event.base_asset_amount, price, False)
            if self._logging_options & self.OPTION_LOG_ORDER_COMPLETED:
                self.log_with_clock(logging.INFO,
                                    f"Limit order completed on {market_trading_pair_tuple[0].name}: {order_id} ({price}, -{sell_order_completed_event.base_asset_amount})")
            if order_id in self.marked_for_deletion:
                del self.marked_for_deletion[order_id]

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
            if not self.fail_message_sent:
                market = market_trading_pair_tuple.market.name
                price = full_order.price
                amount = full_order.quantity
                buy_sell = "BUY" if full_order.is_buy else "SELL"
                msg = {"msg_type": "order failed", "data": {"market": market, "price": price, "amount": amount, "buy/sell": buy_sell, "id": order_id}}

                SlackPusher(self.slack_url, "ORDER FAILED: " + str(msg))
                self.fail_message_sent = True
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
            if order_id in self.marked_for_deletion.keys():
                order = self.marked_for_deletion[order_id]
                if order["is_buy"]:
                    if not (order["rank"] in self.buys_to_replace):
                        self.buys_to_replace.append(order["rank"])
                else:
                    if not (order["rank"] in self.sells_to_replace):
                        self.sells_to_replace.append(order["rank"])
                del self.marked_for_deletion[order_id]

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
            if order_id in self.marked_for_deletion:
                order = self.marked_for_deletion[order_id]
                if order["is_buy"]:
                    self.buys_to_replace.append(order["rank"])
                else:
                    self.sells_to_replace.append(order["rank"])
                del self.marked_for_deletion[order_id]
            if market_trading_pair_tuple.market == self.primary_market_pairs[0].market:
                if full_order.is_buy:
                    self.primary_quote_balance += full_order.price * full_order.quantity
                else:
                    self.primary_base_balance += full_order.quantity
            else:
                if full_order.is_buy:
                    self.mirrored_quote_balance += full_order.price * full_order.quantity
                    self.offset_quote_exposure -= full_order.quantity * full_order.price
                else:
                    self.mirrored_base_balance += full_order.quantity
                    self.offset_base_exposure -= full_order.quantity
            self.log_with_clock(logging.INFO,
                                f"Limit order canceled on {market_trading_pair_tuple[0].name}: {order_id}")

    cdef bint c_ready_for_new_orders(self, list market_trading_pair_tuples):
        """
        Check whether we are ready for making new mirroring orders or not. Conditions where we should not make further
        new orders include:

         1. There's an in-flight market order that's still being resolved.
         2. We're still within the cool-off period from the last trade, which means the exchange balances may be not
            accurate temporarily.

        If none of the above conditions are matched, then we're ready for new orders.

        :param market_trading_pair_tuples: list of mirroring market pairs
        :return: True if ready, False if not
        """
        cdef:
            double time_left
            dict tracked_taker_orders = self._sb_order_tracker.c_get_taker_orders()

        ready_ts_from_failed_order = (self._last_failed_market_order_timestamp/1000.0) + \
            self._failed_market_order_count * self.FAILED_ORDER_COOL_OFF_TIME
        # Wait for FAILED_ORDER_COOL_OFF_TIME * failed_market_order_count before retrying
        if ready_ts_from_failed_order > self._current_timestamp:
            time_left = ready_ts_from_failed_order - self._current_timestamp
            if not self._cool_off_logged:
                self.log_with_clock(
                    logging.INFO,
                    f"Cooling off from failed order. "
                    f"Resuming in {int(time_left)} seconds."
                )
                self._cool_off_logged = True
            return False

        for market_trading_pair_tuple in market_trading_pair_tuples:
            # Do not continue if there are pending market order
            if len(tracked_taker_orders.get(market_trading_pair_tuple, {})) > 0:
                # consider market order completed if it was already x time old
                if any([order.timestamp - self._current_timestamp < self.MARKET_ORDER_MAX_TRACKING_TIME
                       for order in tracked_taker_orders[market_trading_pair_tuple].values()]):
                    return False
            # Wait for the cool off interval before the next trade, so wallet balance is up to date
            ready_to_trade_time = self._last_trade_timestamps.get(market_trading_pair_tuple, 0) + self._next_trade_delay
            if market_trading_pair_tuple in self._last_trade_timestamps and ready_to_trade_time > self._current_timestamp:
                time_left = self._current_timestamp - self._last_trade_timestamps[market_trading_pair_tuple] - self._next_trade_delay
                if not self._cool_off_logged:
                    self.log_with_clock(
                        logging.INFO,
                        f"Cooling off from previous trade on {market_trading_pair_tuple.market.name}. "
                        f"Resuming in {int(time_left)} seconds."
                    )
                    self._cool_off_logged = True
                return False

        if self._cool_off_logged:
            self.log_with_clock(
                logging.INFO,
                f"Cool off completed. Liquidity Mirroring strategy is now ready for new orders."
            )
            # reset cool off log tag when strategy is ready for new orders
            self._cool_off_logged = False

        return True

    def check_flat_fee_coverage(self, market, flat_fees):
        covered = True
        for fee in flat_fees:
            covered = covered and (market.get_available_balance(fee[0]) > fee[1])
            if covered == False:
                break
        return covered

    def factor_in_fees(self, market: MarketBase, market_pair, price, amount, is_buy, is_primary):
        fee_object = market.c_get_fee(
            market_pair.base_asset,
            market_pair.quote_asset,
            OrderType.LIMIT,
            TradeType.BUY if is_buy else TradeType.SELL,
            amount,
            price
        )
            
        total_flat_fees = self.c_sum_flat_fees(market_pair.quote_asset,
                                                   fee_object.flat_fees)
        fixed_cost_per_unit = total_flat_fees / amount                                                       
        if (is_buy and is_primary) or ((not is_buy) and (not is_primary)):
            price_tx = Decimal(price) / (Decimal(1) + fee_object.percent) - fixed_cost_per_unit
        else:
            price_tx = Decimal(price) / (Decimal(1) - fee_object.percent) + fixed_cost_per_unit
        return price_tx, fee_object

    def check_calculations(self):
        primary_market = self.primary_market_pairs[0].market
        mirrored_market = self.mirrored_market_pairs[0].market
        primary_base_asset = self.primary_market_pairs[0].base_asset
        primary_quote_asset = self.primary_market_pairs[0].quote_asset
        mirrored_base_asset = self.mirrored_market_pairs[0].base_asset
        mirrored_quote_asset = self.mirrored_market_pairs[0].quote_asset
        
        primary_base_balance = primary_market.get_balance(primary_base_asset)
        primary_quote_balance = primary_market.get_balance(primary_quote_asset)
        mirrored_base_balance = mirrored_market.get_balance(mirrored_base_asset) 
        mirrored_quote_balance = mirrored_market.get_balance(mirrored_quote_asset)

        primary_base_available_balance = primary_market.get_available_balance(primary_base_asset)
        primary_quote_available_balance = primary_market.get_available_balance(primary_quote_asset)
        mirrored_base_available_balance = mirrored_market.get_available_balance(mirrored_base_asset) 
        mirrored_quote_available_balance = mirrored_market.get_available_balance(mirrored_quote_asset)

        messages : List[str] = []
        if (abs(primary_base_balance - self.primary_base_total_balance) > Decimal(0.001)):
            messages.append(f"{primary_market.name}:{primary_base_asset} Old:{str(self.primary_base_total_balance)} New:{str(primary_base_balance)}")
            self.primary_base_total_balance = primary_base_balance
        if (abs(primary_quote_balance - self.primary_quote_total_balance) > Decimal(0.001)):
            messages.append(f"{primary_market.name}:{primary_quote_asset} Old:{str(self.primary_quote_total_balance)} New:{str(primary_quote_balance)}")
            self.primary_quote_total_balance = primary_quote_balance
        if (abs(mirrored_base_balance - self.mirrored_base_total_balance) > Decimal(0.001)):
            messages.append(f"{mirrored_market.name}:{mirrored_base_asset} Old:{str(self.mirrored_base_total_balance)} New:{str(mirrored_base_balance)}")
            self.mirrored_base_total_balance = mirrored_base_balance
        if (abs(mirrored_quote_balance - self.mirrored_quote_total_balance) > Decimal(0.001)):
            messages.append(f"{mirrored_market.name}:{mirrored_quote_asset} Old:{str(self.mirrored_quote_total_balance)} New:{str(mirrored_quote_balance)}")
            self.mirrored_quote_total_balance = mirrored_quote_balance
        
        if (abs(primary_base_available_balance - self.primary_base_balance) > Decimal(0.001)):
            #messages.append(f"{primary_market.name}:{primary_base_asset}:available Old:{str(self.primary_base_balance)} New:{str(primary_base_available_balance)}")
            self.primary_base_balance = primary_base_available_balance
        if (abs(primary_quote_available_balance - self.primary_quote_balance) > Decimal(0.001)):
            #messages.append(f"{primary_market.name}:{primary_quote_asset}:available Old:{str(self.primary_quote_balance)} New:{str(primary_quote_available_balance)}")
            self.primary_quote_balance = primary_quote_available_balance
        if (abs(mirrored_base_available_balance - self.mirrored_base_balance) > Decimal(0.001)):
            #messages.append(f"{mirrored_market.name}:{mirrored_base_asset}:available Old:{str(self.mirrored_base_balance)} New:{str(mirrored_base_available_balance)}")
            self.mirrored_base_balance = mirrored_base_available_balance
        if (abs(mirrored_quote_available_balance - self.mirrored_quote_balance) > Decimal(0.001)):
            #messages.append(f"{mirrored_market.name}:{mirrored_quote_asset}:available Old:{str(self.mirrored_quote_balance)} New:{str(mirrored_quote_available_balance)}")
            self.mirrored_quote_balance = mirrored_quote_available_balance
        if len(messages) > 0:
            SlackPusher(self.slack_url, "BALANCE DISCREPANCY: " + '\n'.join(messages))

    cdef c_check_balances(self):
        current_time = datetime.timestamp(datetime.now())
        time_elapsed = current_time - self.start_time
        wallet_check_time_elapsed = current_time - self.start_wallet_check_time
        if (wallet_check_time_elapsed > 60):
            self.start_wallet_check_time = current_time
            self.check_calculations()
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
        # TODO: pass in our min_primary_amount to keep orders below that size off the list
        new_desired_book = BookState(
            bids=list(market_pair.order_book_bid_entries()), 
            asks=list(market_pair.order_book_ask_entries())
            #price_tolerance=self.price_toerance,
            #amount_tolerance=self.amount_tolerance
        )
        #new_desired_book.aggregate(price_precision)
        new_desired_book.crop(len(self.bid_amounts), len(self.ask_amounts))
        #new_desired_book.scale_amounts(self.mirroring_amount_scale) #add this if we want to scale back the size of our mirrored orders vs the source order
        new_desired_book.markup(self.order_price_markup, self.order_price_markup)
        new_desired_book.limit_by_ratios(self.bid_amount_percents, self.ask_amount_percents)
        #new_desired_book.markup_fees(flat_fee, scale_fee)
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
        self.adjust_primary_orderbook(self.primary_market_pairs[0], best_bid, best_ask, bid_levels, ask_levels)

        if (self.two_sided_mirroring):
            self.adjust_mirrored_orderbook(market_pair, best_bid, best_ask)

    def _get_available_balances(self):
        # TODO Should return the availabale balance that we have for use in placing new primary market orders
        # this includes all calculations related to ensuring that we have enough balance on the offsetting exchange
        # to cover any fills that we might recieve

        return available_base, available_quote

    def _place_primary_order(self, order):
        quant_price = primary_market.c_quantize_order_price(primary_market_pair.trading_pair, order.price)
        quant_amount = primary_market.c_quantize_order_amount(primary_market_pair.trading_pair, order.amount)
        place_order = self.c_buy_with_specific_market if order.side is TradeType.BUY else self.c_sell_with_specific_market

        try:
            order_id = place_order(primary_market_pair, quant_amount, OrderType.LIMIT, quant_price)
            self.current_book.add(order)
            return True
        except:
            return False

    def _place_primary_orders(self, orders, available_amount):
        for order in orders:
            if order.amount <= available_amount:
                if self._place_primary_order(order):
                    available_amount -= order.amount
                    order.mark_sent()

    # TODO with these changes, we should have a process that cancells all primary orders if we have network problems getting the updated
    # orderbook from the mirrored exchange
    def adjust_primary_orderbook(self): # TODO make async and take a lock like adjust_mirrored_orderbook
        primary_market_pair = # TOOD
        available_base, available_quote = self._get_available_balances()
        bids_to_place, asks_to_place, orders_to_cancel = self.current_book.steps_to(self.desired_book)

        # Cancel any orders that we no longer want
        for order in to_cancel:
            if order.is_live_uncancelled():
                try:
                    self.c_cancel_order(primary_market_pair, order.order_id)
                    order.mark_canceled()
                except:
                    pass

        # Place any new orders that we have the available balances to handle
        self._place_primary_orders(bids_to_place, available_quote)
        self._place_primary_orders(asks_to_place, available_base)

    def adjust_mirrored_orderbook(self,mirrored_market_pair,best_bid,best_ask):
        if not self.offset_beyond_threshold_message_sent:
            if (abs(self.pm.amount_to_offset) > self.max_offsetting_exposure):
                SlackPusher(self.slack_url, "Offsetting exposure beyond threshold")
                self.offset_beyond_threshold_message_sent = True
        else:
            if (abs(self.pm.amount_to_offset) < self.max_offsetting_exposure):
                SlackPusher(self.slack_url, "Offsetting exposure within threshold")
                self.offset_beyond_threshold_message_sent = False
        mirrored_market: MarketBase = mirrored_market_pair.market
        if self.pm.amount_to_offset.is_zero():
            return
        else:
            current_exposure = Decimal(0)
            active_orders = self._sb_order_tracker.market_pair_to_active_orders
            current_orders = []
            if mirrored_market_pair in active_orders:
                current_orders = active_orders[mirrored_market_pair][:]
            
            if self.pm.amount_to_offset < Decimal(0):
                # we are at a deficit of base. get rid of sell orders
                for order in current_orders:
                    quantity = Decimal(order.quantity)
                    if not order.is_buy:
                        self.c_cancel_order(mirrored_market_pair,order.client_order_id)
                    else:
                        current_exposure += quantity
                avg_price = self.pm.avg_price
                if not avg_price.is_zero():
                    new_price = avg_price * (Decimal(1) + self.max_loss)
                else:
                    #should not hit this; if we are offsetting, there should be an extant sell price
                    new_price = Decimal(best_ask.price)

                amount = ((-1) * self.pm.amount_to_offset) - current_exposure
                amount = min((self.mirrored_quote_balance/new_price), amount)
                if (amount >= self.min_mirroring_amount):

                    quant_price = mirrored_market.c_quantize_order_price(mirrored_market_pair.trading_pair, Decimal(new_price))
                    quant_amount = mirrored_market.c_quantize_order_amount(mirrored_market_pair.trading_pair, Decimal(amount))

                    try:
                        self.c_buy_with_specific_market(mirrored_market_pair,Decimal(quant_amount),OrderType.LIMIT,Decimal(quant_price))
                    except:
                        pass

            elif self.pm.amount_to_offset > Decimal(0):
            # we are at a surplus of base. get rid of buy orders
                for order in current_orders:
                    quantity = order.quantity
                    if order.is_buy:
                        self.c_cancel_order(mirrored_market_pair,order.client_order_id)
                    else:
                        current_exposure += quantity

                amount = (self.pm.amount_to_offset) - current_exposure
                amount = min(amount, self.mirrored_base_balance)
                if (amount >= self.min_mirroring_amount):

                    avg_price = self.pm.avg_price
                    if not avg_price.is_zero():
                        new_price = avg_price * (Decimal(1) - self.max_loss)
                    else:
                        # should not hit this. there should be an extant buy
                        new_price = Decimal(best_bid.price)

                    quant_price = mirrored_market.c_quantize_order_price(mirrored_market_pair.trading_pair, Decimal(new_price))
                    quant_amount = mirrored_market.c_quantize_order_amount(mirrored_market_pair.trading_pair, Decimal(amount))
                    try:
                        self.c_sell_with_specific_market(mirrored_market_pair,Decimal(quant_amount),OrderType.LIMIT,Decimal(quant_price))
                    except:
                        pass
