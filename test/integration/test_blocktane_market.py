#!/usr/bin/env python
import os
import json 
import time
import logging
import asyncio
import requests
import unittest
import contextlib
from unittest import mock
from decimal import Decimal
from typing import List, Optional
from os.path import join, realpath
import sys; sys.path.insert(0, realpath(join(__file__, "../../../")))

import conf
from hummingbot.core.clock import (
    Clock,
    ClockMode
)
from hummingbot.model.order import Order
from hummingbot.core.event.events import (
    MarketEvent,
    BuyOrderCompletedEvent,
    SellOrderCompletedEvent,
    OrderFilledEvent,
    OrderCancelledEvent,
    BuyOrderCreatedEvent,
    SellOrderCreatedEvent,
    TradeFee,
    TradeType,
    OrderType
)
from hummingbot.model.trade_fill import TradeFill
from hummingbot.model.market_state import MarketState
from hummingbot.model.sql_connection_manager import (
    SQLConnectionManager,
    SQLConnectionType
)
from hummingbot.core.event.event_logger import EventLogger
from test.integration.humming_web_app import HummingWebApp
from hummingbot.logger.struct_logger import METRICS_LOG_LEVEL
from hummingbot.market.markets_recorder import MarketsRecorder
from test.integration.humming_ws_server import HummingWsServerFactory
from hummingbot.connector.exchange..blocktane.blocktane_exchange import BlocktaneExchange
from test.integration.assets.mock_data.fixture_blocktane import FixtureBlocktane
from hummingbot.client.config.fee_overrides_config_map import fee_overrides_config_map

logging.basicConfig(level=METRICS_LOG_LEVEL)
API_MOCK_ENABLED = conf.mock_api_enabled is not None and conf.mock_api_enabled.lower() in ['true', 'yes', '1']
API_KEY = "XXX" if API_MOCK_ENABLED else conf.blocktane_api_key
API_SECRET = "YYY" if API_MOCK_ENABLED else conf.blocktane_api_secret
API_BASE_URL = "bolsa.tokamaktech.net/api/v2/xt"
WS_BASE_URL = "wss://bolsa.tokamaktech.net/api/v2/ws/public"
logging.basicConfig(level=METRICS_LOG_LEVEL)

class BlocktaneExchangeUnitTest(unittest.TestCase):
    events: List[MarketEvent] = [
        MarketEvent.ReceivedAsset,
        MarketEvent.BuyOrderCompleted,
        MarketEvent.SellOrderCompleted,
        MarketEvent.WithdrawAsset,
        MarketEvent.OrderFilled,
        MarketEvent.OrderCancelled,
        MarketEvent.TransactionFailure,
        MarketEvent.BuyOrderCreated,
        MarketEvent.SellOrderCreated,
        MarketEvent.OrderCancelled
    ]

    market: BlocktaneExchange
    market_logger: EventLogger
    stack: contextlib.ExitStack
    base_api_url = "bolsa.tokamaktech.net"

    @classmethod
    def setUpClass(cls):
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()

        if API_MOCK_ENABLED:
            cls.web_app = HummingWebApp.get_instance()
            cls.web_app.add_host_to_mock(cls.base_api_url, [])
            cls.web_app.start()

            cls.ev_loop.run_until_complete(cls.web_app.wait_til_started())
            cls._patcher = mock.patch("aiohttp.client.URL")
            cls._url_mock = cls._patcher.start()

            cls._url_mock.side_effect = cls.web_app.reroute_local

            cls._req_patcher = unittest.mock.patch.object(requests.Session, "request", autospec=True)
            cls._req_url_mock = cls._req_patcher.start()
            cls._req_url_mock.side_effect = HummingWebApp.reroute_request

            cls.web_app.update_response("get", cls.base_api_url, "/api/v2/xt/public/health/alive", FixtureBlocktane.PING)
            cls.web_app.update_response("get", cls.base_api_url, "/api/v2/xt/public/markets", FixtureBlocktane.MARKETS)
            cls.web_app.update_response("get", cls.base_api_url, "/api/v2/xt/public/markets/tickers", FixtureBlocktane.MARKETS_TICKERS)
            cls.web_app.update_response("get", cls.base_api_url, "/api/v2/xt/account/balances", FixtureBlocktane.BALANCES)
            cls.web_app.update_response("get", cls.base_api_url, "/api/v2/xt/market/orders?state=wait", FixtureBlocktane.ORDERS_OPEN_BUY)
            cls.web_app.update_response("post", cls.base_api_url, "/api/v2/xt/market/orders/10001/cancel", FixtureBlocktane.ORDER_CANCEL)
            # cls.web_app.update_response("post", cls.base_api_url, "/api/v2/xt/market/orders/10000/cancel", FixtureBlocktane.ORDER_CANCEL_1)
            cls.web_app.update_response("post", cls.base_api_url, "/api/v2/xt/market/orders", FixtureBlocktane.ORDER_MARKET_OPEN_BUY)
            cls.web_app.update_response("post", cls.base_api_url, "/api/v2/xt/market/orders", FixtureBlocktane.ORDER_MARKET_OPEN_BUY)
            cls.web_app.update_response("get", cls.base_api_url, "/api/v2/xt/public/markets/fthusd/depth", FixtureBlocktane.MARKETS_DEPTH)

            ws_base_url = "wss://bolsa.tokamaktech.net/api/v2/ws/public"
            cls._ws_user_url = f"{ws_base_url}/?stream=order&stream=trade"
            HummingWsServerFactory.url_host_only = True
            HummingWsServerFactory.start_new_server(cls._ws_user_url)
            HummingWsServerFactory.start_new_server(f"{ws_base_url}/linketh@depth/zrxeth@depth")
            cls._ws_patcher = unittest.mock.patch("websockets.connect", autospec=True)
            cls._ws_mock = cls._ws_patcher.start()
            cls._ws_mock.side_effect = HummingWsServerFactory.reroute_ws_connect

            cls._t_nonce_patcher = unittest.mock.patch("hummingbot.connector.exchange.blocktane.blocktane_market.get_tracking_nonce")
            cls._t_nonce_mock = cls._t_nonce_patcher.start()

        cls.clock: Clock = Clock(ClockMode.REALTIME)
        cls.market: BlocktaneExchange = BlocktaneExchange(
            blocktane_api_key=API_KEY,
            blocktane_secret_key=API_SECRET,
            trading_pairs=["fthusd"]
        )

        print("Initializing Blocktane market... this will take about a minute. ")
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.clock.add_iterator(cls.market)
        cls.stack = contextlib.ExitStack()
        cls._clock = cls.stack.enter_context(cls.clock)
        cls.ev_loop.run_until_complete(cls.wait_til_ready())
        print("Ready.")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.stack.close()
        if API_MOCK_ENABLED:
            cls.web_app.stop()
            cls._patcher.stop()
            cls._t_nonce_patcher.stop()
            cls._ws_patcher.stop()

    @classmethod
    async def wait_til_ready(cls):
        while True:
            now = time.time()
            next_iteration = now // 1.0 + 1
            if cls.market.ready:
                break
            else:
                await cls._clock.run_til(next_iteration)
            await asyncio.sleep(1.0)

    def setUp(self):
        self.db_path: str = realpath(join(__file__, "../blocktane_test.sqlite"))
        try:
            os.unlink(self.db_path)
        except FileNotFoundError:
            pass

        self.market_logger = EventLogger()
        for event_tag in self.events:
            self.market.add_listener(event_tag, self.market_logger)

    def tearDown(self):
        for event_tag in self.events:
            self.market.remove_listener(event_tag, self.market_logger)
        self.market_logger = None

    async def run_parallel_async(self, *tasks):
        future: asyncio.Future = asyncio.ensure_future(asyncio.gather(*tasks))
        while not future.done():
            now = time.time()
            next_iteration = now // 1.0 + 1
            await self.clock.run_til(next_iteration)
        return future.result()

    def run_parallel(self, *tasks):
        return self.ev_loop.run_until_complete(self.run_parallel_async(*tasks))

    def test_get_fee(self):

        limit_fee: TradeFee = self.market.get_fee("fth", "usd", OrderType.LIMIT, TradeType.BUY, Decimal(1), Decimal(4000))
        self.assertGreater(limit_fee.percent, 0)
        self.assertEqual(len(limit_fee.flat_fees), 0)

        market_fee: TradeFee = self.market.get_fee("fth", "usd", OrderType.MARKET, TradeType.BUY, Decimal(1))
        self.assertGreater(market_fee.percent, 0)
        self.assertEqual(len(market_fee.flat_fees), 0)

    def test_fee_overrides_config(self):
        fee_overrides_config_map["blocktane_taker_fee"].value = None
        taker_fee: TradeFee = self.market.get_fee("fth", "usd", OrderType.MARKET, TradeType.BUY, Decimal(1), Decimal('0.1'))
        self.assertAlmostEqual(Decimal("0.002"), taker_fee.percent)
        fee_overrides_config_map["blocktane_taker_fee"].value = Decimal('0.002')
        taker_fee: TradeFee = self.market.get_fee("fth", "usd", OrderType.MARKET, TradeType.BUY, Decimal(1), Decimal('0.1'))
        self.assertAlmostEqual(Decimal("0.002"), taker_fee.percent)
        fee_overrides_config_map["blocktane_maker_fee"].value = None
        maker_fee: TradeFee = self.market.get_fee("fth", "usd", OrderType.LIMIT, TradeType.BUY, Decimal(1), Decimal('0.1'))
        self.assertAlmostEqual(Decimal("0.002"), maker_fee.percent)
        fee_overrides_config_map["blocktane_maker_fee"].value = Decimal('0.002')
        maker_fee: TradeFee = self.market.get_fee("fth", "usd", OrderType.LIMIT, TradeType.BUY, Decimal(1), Decimal('0.1'))
        self.assertAlmostEqual(Decimal("0.002"), maker_fee.percent)

    def place_order(self, is_buy, trading_pair, amount, order_type, price, nonce, fixture_resp, fixture_ws):
        order_id, exch_order_id = None, None
        if API_MOCK_ENABLED:
            self._t_nonce_mock.return_value = nonce

            resp = fixture_resp.copy()
            exch_order_id = resp["id"]
            self.web_app.update_response("post", self.base_api_url, "/api/v2/xt/market/orders", resp)
        if is_buy:
            order_id = self.market.buy(trading_pair, amount, order_type, price)
        else:
            order_id = self.market.sell(trading_pair, amount, order_type, price)
        if API_MOCK_ENABLED:
            resp = fixture_ws.copy()
            # resp["content"]["o"]["OU"] = exch_order_id
            HummingWsServerFactory.send_json_threadsafe(WS_BASE_URL, resp, delay=1.0)
        return order_id, exch_order_id

    def cancel_order(self, trading_pair, order_id, exch_order_id):
        if API_MOCK_ENABLED:
            resp = FixtureBlocktane.ORDER_CANCEL.copy()
            resp["id"] = exch_order_id
            self.web_app.update_response("delete", self.base_api_url, f"/api/v2/xt/market/orders/{exch_order_id}/cancel", resp)
        self.market.cancel(trading_pair, order_id)

    def test_limit_buy(self):
        self.assertGreater(self.market.get_balance("usd"), 20)
        trading_pair = "fthusd"

        self.run_parallel(asyncio.sleep(3))
        current_bid_price: Decimal = self.market.get_price(trading_pair, True)
        bid_price: Decimal = current_bid_price + Decimal('0.01') * current_bid_price
        quantize_bid_price: Decimal = self.market.quantize_order_price(trading_pair, bid_price)

        amount: Decimal = Decimal("0.02")
        quantized_amount: Decimal = self.market.quantize_order_amount(trading_pair, amount)

        order_id, _ = self.place_order(True, trading_pair, quantized_amount, OrderType.LIMIT, quantize_bid_price,
                                       10001, FixtureBlocktane.ORDER_MARKET_OPEN_BUY, FixtureBlocktane.WS_ORDER_FILLED_BUY_LIMIT)
        [order_completed_event] = self.run_parallel(self.market_logger.wait_for(BuyOrderCompletedEvent))
        order_completed_event: BuyOrderCompletedEvent = order_completed_event
        trade_events = [t for t in self.market_logger.event_log
                                                if isinstance(t, BuyOrderCompletedEvent)]
        base_amount_traded: Decimal = sum(t.base_asset_amount for t in trade_events)
        quote_amount_traded: Decimal = sum(t.quote_asset_amount for t in trade_events)

        self.assertTrue([evt.order_type == OrderType.LIMIT for evt in trade_events])
        self.assertEqual(order_id, order_completed_event.order_id)
        self.assertAlmostEqual(quantized_amount, order_completed_event.base_asset_amount)
        self.assertEqual("fth", order_completed_event.base_asset)
        self.assertEqual("usd", order_completed_event.quote_asset)
        self.assertAlmostEqual(base_amount_traded, order_completed_event.base_asset_amount)
        self.assertAlmostEqual(quote_amount_traded, order_completed_event.quote_asset_amount)
        self.assertTrue(any([isinstance(event, BuyOrderCreatedEvent) and event.order_id == order_id
                             for event in self.market_logger.event_log]))
        # Reset the logs
        self.market_logger.clear()

    def test_limit_sell(self):
        trading_pair = "fthusd"
        current_ask_price: Decimal = self.market.get_price(trading_pair, False)
        ask_price: Decimal = current_ask_price - Decimal('0.01') * current_ask_price
        quantize_ask_price: Decimal = self.market.quantize_order_price(trading_pair, ask_price)

        amount: Decimal = Decimal("0.02")
        quantized_amount: Decimal = self.market.quantize_order_amount(trading_pair, amount)

        order_id, _ = self.place_order(False, trading_pair, quantized_amount, OrderType.LIMIT, quantize_ask_price,
                                       10001, FixtureBlocktane.ORDER_MARKET_OPEN_SELL, FixtureBlocktane.WS_ORDER_FILLED_SELL_LIMIT)

        [order_completed_event] = self.run_parallel(self.market_logger.wait_for(SellOrderCompletedEvent))
        order_completed_event: SellOrderCompletedEvent = order_completed_event
        trade_events = [t for t in self.market_logger.event_log if isinstance(t, SellOrderCompletedEvent)]
        base_amount_traded = sum(t.base_asset_amount for t in trade_events)
        quote_amount_traded = sum(t.quote_asset_amount for t in trade_events)

        self.assertTrue([evt.order_type == OrderType.LIMIT for evt in trade_events])
        self.assertEqual(order_id, order_completed_event.order_id)
        self.assertAlmostEqual(quantized_amount, order_completed_event.base_asset_amount)
        self.assertEqual("fth", order_completed_event.base_asset)
        self.assertEqual("usd", order_completed_event.quote_asset)
        self.assertAlmostEqual(base_amount_traded, order_completed_event.base_asset_amount)
        self.assertAlmostEqual(quote_amount_traded, order_completed_event.quote_asset_amount)
        self.assertTrue(any([isinstance(event, SellOrderCreatedEvent) and event.order_id == order_id
                             for event in self.market_logger.event_log]))
        # Reset the logs
        self.market_logger.clear()

    def test_market_buy(self):
        self.assertGreater(self.market.get_balance("usd"), 20)
        trading_pair = "fthusd"

        amount: Decimal = Decimal("0.02")
        quantized_amount: Decimal = self.market.quantize_order_amount(trading_pair, amount)

        order_id, _ = self.place_order(True, trading_pair, quantized_amount, OrderType.MARKET, 0, 10001,
                                       FixtureBlocktane.ORDER_MARKET_OPEN_BUY, FixtureBlocktane.WS_ORDER_MARKET_BUY_FILLED)

        [order_completed_event] = self.run_parallel(self.market_logger.wait_for(BuyOrderCompletedEvent))


        order_completed_event: BuyOrderCompletedEvent = order_completed_event
        trade_events: List[OrderFilledEvent] = [t for t in self.market_logger.event_log
                                                if isinstance(t, BuyOrderCompletedEvent)]
        base_amount_traded: Decimal = sum(t.base_asset_amount for t in trade_events)
        quote_amount_traded: Decimal = sum(t.quote_asset_amount for t in trade_events)

        self.assertTrue([evt.order_type == OrderType.MARKET for evt in trade_events])
        self.assertEqual(order_id, order_completed_event.order_id)
        self.assertAlmostEqual(quantized_amount, order_completed_event.base_asset_amount)
        self.assertEqual("fth", order_completed_event.base_asset)
        self.assertEqual("usd", order_completed_event.quote_asset)
        self.assertAlmostEqual(base_amount_traded, order_completed_event.base_asset_amount)
        self.assertAlmostEqual(quote_amount_traded, order_completed_event.quote_asset_amount)
        self.assertTrue(any([isinstance(event, BuyOrderCreatedEvent) and event.order_id == order_id
                             for event in self.market_logger.event_log]))
        # Reset the logs
        self.market_logger.clear()

    def test_market_sell(self):
        trading_pair = "fthusd"
        self.assertGreater(self.market.get_balance("fth"), 0.02)

        amount: Decimal = Decimal("0.02")
        quantized_amount: Decimal = self.market.quantize_order_amount(trading_pair, amount)

        order_id, _ = self.place_order(False, trading_pair, quantized_amount, OrderType.MARKET, 0, 10001,
                                       FixtureBlocktane.ORDER_MARKET_OPEN_SELL, FixtureBlocktane.WS_ORDER_MARKET_SELL_FILLED)
        [order_completed_event] = self.run_parallel(self.market_logger.wait_for(SellOrderCompletedEvent))
        order_completed_event: SellOrderCompletedEvent = order_completed_event
        trade_events = [t for t in self.market_logger.event_log if isinstance(t, SellOrderCompletedEvent)]
        base_amount_traded = sum(t.base_asset_amount for t in trade_events)
        quote_amount_traded = sum(t.quote_asset_amount for t in trade_events)

        self.assertTrue([evt.order_type == OrderType.MARKET for evt in trade_events])
        self.assertEqual(order_id, order_completed_event.order_id)
        self.assertAlmostEqual(quantized_amount, order_completed_event.base_asset_amount)
        self.assertEqual("fth", order_completed_event.base_asset)
        self.assertEqual("usd", order_completed_event.quote_asset)
        self.assertAlmostEqual(base_amount_traded, order_completed_event.base_asset_amount)
        self.assertAlmostEqual(quote_amount_traded, order_completed_event.quote_asset_amount)
        self.assertTrue(any([isinstance(event, SellOrderCreatedEvent) and event.order_id == order_id
                             for event in self.market_logger.event_log]))
        # Reset the logs
        self.market_logger.clear()

    def test_cancel_order(self):
        trading_pair = "fthusd"

        current_bid_price: Decimal = self.market.get_price(trading_pair, True) * Decimal('0.80')
        quantize_bid_price: Decimal = self.market.quantize_order_price(trading_pair, current_bid_price)

        amount: Decimal = Decimal("0.02")
        quantized_amount: Decimal = self.market.quantize_order_amount(trading_pair, amount)

        order_id, exch_order_id = self.place_order(True, trading_pair, quantized_amount, OrderType.LIMIT,
                                                   quantize_bid_price, 10001, FixtureBlocktane.ORDER_CANCEL,
                                                   FixtureBlocktane.WS_ORDER_CANCEL_BUY_LIMIT)
        self.run_parallel(self.market_logger.wait_for(BuyOrderCreatedEvent))
        self.cancel_order(trading_pair, order_id, exch_order_id)
        [order_cancelled_event] = self.run_parallel(self.market_logger.wait_for(OrderCancelledEvent))
        order_cancelled_event: OrderCancelledEvent = order_cancelled_event
        self.assertEqual(order_cancelled_event.order_id, order_id)

    def test_cancel_all(self):
        self.assertGreater(self.market.get_balance("usd"), 20)
        trading_pair = "fthusd"

        current_bid_price: Decimal = self.market.get_price(trading_pair, True) * Decimal('0.80')
        
        quantize_bid_price: Decimal = self.market.quantize_order_price(trading_pair, current_bid_price)
        bid_amount: Decimal = Decimal('0.02')
        quantized_bid_amount: Decimal = self.market.quantize_order_amount(trading_pair, bid_amount)

        current_ask_price: Decimal = self.market.get_price(trading_pair, False)
        quantize_ask_price: Decimal = self.market.quantize_order_price(trading_pair, current_ask_price)
        ask_amount: Decimal = Decimal('0.02')
        quantized_ask_amount: Decimal = self.market.quantize_order_amount(trading_pair, ask_amount)

        _, exch_order_id_1 = self.place_order(True, trading_pair, quantized_bid_amount, OrderType.LIMIT,
                                              quantize_bid_price, 10001,
                                              FixtureBlocktane.ORDER_PLACE_OPEN, FixtureBlocktane.WS_ORDER_MARKET_BUY_FILLED)
        _, exch_order_id_2 = self.place_order(False, trading_pair, quantized_ask_amount, OrderType.LIMIT,
                                              quantize_ask_price, 10002,
                                              FixtureBlocktane.ORDER_PLACE_OPEN, FixtureBlocktane.WS_ORDER_MARKET_BUY_FILLED)
        self.run_parallel(asyncio.sleep(1))
        if API_MOCK_ENABLED:
            resp = FixtureBlocktane.ORDER_CANCEL.copy()
            resp["id"] = exch_order_id_1
            self.web_app.update_response("delete", self.base_api_url, f"/api/v2/xt/orders/{exch_order_id_1}", resp)
            resp = FixtureBlocktane.ORDER_CANCEL.copy()
            resp["id"] = exch_order_id_2
            self.web_app.update_response("delete", self.base_api_url, f"/api/v2/xt/orders/{exch_order_id_2}", resp)
        
        [cancellation_results] = self.run_parallel(self.market.cancel_all(5))
        for cr in cancellation_results:
            self.assertEqual(cr.success, True)

    @unittest.skipUnless(any("test_list_orders" in arg for arg in sys.argv), "List order test requires manual action.")
    def test_list_orders(self):

        self.assertGreater(self.market.get_balance("usd"), 20)
        trading_pair = "fthusd"

        current_bid_price: Decimal = self.market.get_price(trading_pair, True) * Decimal('0.80')
        quantize_bid_price: Decimal = self.market.quantize_order_price(trading_pair, current_bid_price)
        bid_amount: Decimal = Decimal('0.02')
        quantized_bid_amount: Decimal = self.market.quantize_order_amount(trading_pair, bid_amount)

        self.market.buy(trading_pair, quantized_bid_amount, OrderType.LIMIT, quantize_bid_price)
        self.run_parallel(asyncio.sleep(1))
        [order_details] = self.run_parallel(self.market.list_orders())
        self.assertGreaterEqual(len(order_details), 1)

        self.market_logger.clear()
        [cancellation_results] = self.run_parallel(self.market.cancel_all(5))
        for cr in cancellation_results:
            self.assertEqual(cr.success, True)

    def test_orders_saving_and_restoration(self):
        config_path: str = "test_config"
        strategy_name: str = "test_strategy"
        trading_pair: str = "fthusd"
        sql: SQLConnectionManager = SQLConnectionManager(SQLConnectionType.TRADE_FILLS, db_path=self.db_path)
        order_id: Optional[str] = None
        recorder: MarketsRecorder = MarketsRecorder(sql, [self.market], config_path, strategy_name)
        recorder.start()

        try:
            self.assertEqual(0, len(self.market.tracking_states))
            current_bid_price: Decimal = self.market.get_price(trading_pair, True) * Decimal('0.80')
            quantize_bid_price: Decimal = self.market.quantize_order_price(trading_pair, current_bid_price)
            bid_amount: Decimal = Decimal('0.02')
            quantized_bid_amount: Decimal = self.market.quantize_order_amount(trading_pair, bid_amount)

            order_id, exch_order_id = self.place_order(True, trading_pair, quantized_bid_amount, OrderType.LIMIT,
                                                       quantize_bid_price, 10001,
                                                       FixtureBlocktane.ORDER_PLACE_OPEN, FixtureBlocktane.WS_ORDER_MARKET_BUY_FILLED)
            [order_created_event] = self.run_parallel(self.market_logger.wait_for(BuyOrderCreatedEvent))
            order_created_event: BuyOrderCreatedEvent = order_created_event
            self.assertEqual(order_id, order_created_event.order_id)

            # Verify tracking states
            self.assertEqual(1, len(self.market.tracking_states))
            self.assertEqual(order_id, list(self.market.tracking_states.keys())[0])

            # Verify orders from recorder
            recorded_orders: List[Order] = recorder.get_orders_for_config_and_market(config_path, self.market)
            self.assertEqual(1, len(recorded_orders))
            self.assertEqual(order_id, recorded_orders[0].id)

            # Verify saved market states
            saved_market_states: MarketState = recorder.get_market_states(config_path, self.market)
            self.assertIsNotNone(saved_market_states)
            self.assertIsInstance(saved_market_states.saved_state, dict)
            self.assertGreater(len(saved_market_states.saved_state), 0)

            # Close out the current market and start another market.
            self.clock.remove_iterator(self.market)
            for event_tag in self.events:
                self.market.remove_listener(event_tag, self.market_logger)
            self.market: BlocktaneExchange = BlocktaneExchange( blocktane_api_key=API_KEY, blocktane_secret_key=API_SECRET, trading_pairs=["XRP-BTC"]
            )
            for event_tag in self.events:
                self.market.add_listener(event_tag, self.market_logger)
            recorder.stop()
            recorder = MarketsRecorder(sql, [self.market], config_path, strategy_name)
            recorder.start()
            saved_market_states = recorder.get_market_states(config_path, self.market)
            self.clock.add_iterator(self.market)
            self.assertEqual(0, len(self.market.limit_orders))
            self.assertEqual(0, len(self.market.tracking_states))
            self.market.restore_tracking_states(saved_market_states.saved_state)
            self.assertEqual(1, len(self.market.limit_orders))
            self.assertEqual(1, len(self.market.tracking_states))

            # Cancel the order and verify that the change is saved.
            self.cancel_order(trading_pair, order_id, exch_order_id)
            self.run_parallel(self.market_logger.wait_for(OrderCancelledEvent))
            order_id = None
            self.assertEqual(0, len(self.market.limit_orders))
            self.assertEqual(0, len(self.market.tracking_states))
            # saved_market_states = recorder.get_market_states(config_path, self.market)
            self.assertEqual(0, len(saved_market_states.saved_state))
        finally:
            if order_id is not None:
                self.market.cancel(trading_pair, order_id)
                self.run_parallel(self.market_logger.wait_for(OrderCancelledEvent))

            recorder.stop()
            os.unlink(self.db_path)

    def test_order_fill_record(self):
        config_path: str = "test_config"
        strategy_name: str = "test_strategy"
        trading_pair: str = "fthusd"
        sql: SQLConnectionManager = SQLConnectionManager(SQLConnectionType.TRADE_FILLS, db_path=self.db_path)
        order_id: Optional[str] = None
        recorder: MarketsRecorder = MarketsRecorder(sql, [self.market], config_path, strategy_name)
        recorder.start()

        try:
            amount: Decimal = Decimal("0.02")
            quantized_amount: Decimal = self.market.quantize_order_amount(trading_pair, amount)
            order_id, _ = self.place_order(True, trading_pair, quantized_amount, OrderType.MARKET, 0, 10001,
                                           FixtureBlocktane.ORDER_MARKET_OPEN_BUY, FixtureBlocktane.WS_ORDER_FILLED_BUY_LIMIT)
            [buy_order_completed_event] = self.run_parallel(self.market_logger.wait_for(BuyOrderCompletedEvent))

            # Reset the logs
            self.market_logger.clear()

            amount = Decimal(buy_order_completed_event.base_asset_amount)
            order_id, _ = self.place_order(False, trading_pair, amount, OrderType.MARKET, 0, 10001,
                                           FixtureBlocktane.ORDER_MARKET_OPEN_SELL, FixtureBlocktane.WS_ORDER_FILLED_SELL_LIMIT)
            [sell_order_completed_event] = self.run_parallel(self.market_logger.wait_for(SellOrderCompletedEvent))

            # Query the persisted trade logs
            # trade_fills: List[TradeFill] = [recorder.get_trades_for_config(config_path)]
            trade_fills: List[TradeFill] = [x for x in recorder.market_event_tag_map.values() if x in (MarketEvent.BuyOrderCompleted, MarketEvent.SellOrderCompleted)]
            self.assertEqual(2, len(trade_fills))
            buy_fills: List[TradeFill] = [t for t in trade_fills if t == MarketEvent.BuyOrderCompleted]
            sell_fills: List[TradeFill] = [t for t in trade_fills if t == MarketEvent.SellOrderCompleted]
            self.assertEqual(1, len(buy_fills))
            self.assertEqual(1, len(sell_fills))

            order_id = None
        finally:
            if order_id is not None:
                self.market.cancel(trading_pair, order_id)
                self.run_parallel(self.market_logger.wait_for(OrderCancelledEvent))

            recorder.stop()
            os.unlink(self.db_path)

if __name__ == "__main__":
    logging.getLogger("hummingbot.core.event.event_reporter").setLevel(logging.WARNING)
    unittest.main()
