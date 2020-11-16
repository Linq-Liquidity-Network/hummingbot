#!/usr/bin/env python

import asyncio
from decimal import Decimal

import aiohttp
import logging
# import pandas as pd
# import math

import requests
import cachetools.func

from typing import AsyncIterable, Dict, List, Optional, Any

import time
import ujson
import websockets
from websockets.exceptions import ConnectionClosed

# from hummingbot.core.utils import async_ttl_cache
# from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.connector.exchange.bitbay.bitbay_active_order_tracker import BitbayActiveOrderTracker
from hummingbot.connector.exchange.bitbay.bitbay_order_book import BitbayOrderBook
# from hummingbot.connector.exchange.bitbay.bitbay_order_book_tracker_entry import BitbayOrderBookTrackerEntry
from hummingbot.connector.exchange.bitbay.bitbay_utils import convert_from_exchange_trading_pair
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.logger import HummingbotLogger
# from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
# from hummingbot.connector.exchange.bitbay.bitbay_order_book_message import BitbayOrderBookMessage
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage


MARKETS_URL = "/trading/stats"
TICKER_URL = "/trading/ticker/"
SNAPSHOT_URL = "/trading/orderbook/"
TOKEN_INFO_URL = MARKETS_URL
WS_URL = "wss://api.bitbay.net/websocket"
REST_URL_2 = "https://bitbay.net/API/Public/:market1:market2"
REST_URL = "https://api.bitbay.net/rest"
LAST_TRADE_URL = "/trading/transactions/"
BITBAY_PRICE_URL = TICKER_URL


class BitbayAPIOrderBookDataSource(OrderBookTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    __daobds__logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls.__daobds__logger is None:
            cls.__daobds__logger = logging.getLogger(__name__)
        return cls.__daobds__logger

    def __init__(self, trading_pairs: List[str] = None, rest_api_url="", websocket_url=""):
        super().__init__(trading_pairs)
        self.REST_URL = rest_api_url
        self.WS_URL = websocket_url
        self._get_tracking_pair_done_event: asyncio.Event = asyncio.Event()
        self.order_book_create_function = lambda: OrderBook()
        self.active_order_tracker: BitbayActiveOrderTracker = BitbayActiveOrderTracker()

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str]) -> Dict[str, float]:
        async with aiohttp.ClientSession() as client:
            retval = {}
            for pair in trading_pairs:
                resp = await client.get(f"{REST_URL}{LAST_TRADE_URL}{pair}?limit=1")
                resp_json = await resp.json()
                retval[pair] = float(resp_json["items"][0]["r"])
            return retval

    @property
    def order_book_class(self) -> BitbayOrderBook:
        return BitbayOrderBook

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    async def get_snapshot(self, client: aiohttp.ClientSession, trading_pair: str, level: int = 0) -> Dict[str, any]:
        async with client.get(f"{REST_URL}{SNAPSHOT_URL}{trading_pair}") as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(
                    f"Error fetching bitbay market snapshot for {trading_pair}. " f"HTTP status is {response.status}."
                )
            data: Dict[str, Any] = await response.json()
            data["market"] = trading_pair
            return data

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        async with aiohttp.ClientSession() as client:
            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair, 1000)
            snapshot_timestamp: float = time.time()
            snapshot_msg: OrderBookMessage = BitbayOrderBook.snapshot_message_from_exchange(
                snapshot,
                snapshot_timestamp,
                metadata={"trading_pair": trading_pair}
            )
            bids, asks = self.active_order_tracker.convert_snapshot_message_to_order_book_row(snapshot_msg)
            order_book: OrderBook = self.order_book_create_function()
            order_book.apply_snapshot(bids, asks, snapshot_msg.update_id)
            return order_book

    async def _inner_messages(self, ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    yield msg
                except asyncio.TimeoutError:
                    try:
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await ws.close()

    @staticmethod
    @cachetools.func.ttl_cache(ttl=10)
    def get_mid_price(trading_pair: str) -> Optional[Decimal]:
        resp = requests.get(url=f"{REST_URL}{TICKER_URL}{trading_pair}")
        record = resp.json()
        if record["status"] == "Ok":
            data = record["ticker"]["market"]
            mid_price = (Decimal(data["lowestAsk"]) + Decimal(data["highestBid"])) / 2

            return mid_price

    @staticmethod
    async def fetch_trading_pairs() -> List[str]:
        try:
            async with aiohttp.ClientSession() as client:
                async with client.get(f"{REST_URL}{MARKETS_URL}", timeout=5) as response:
                    if response.status == 200:
                        all_trading_pairs: Dict[str, Any] = await response.json()
                        valid_trading_pairs: list = []
                        for item in all_trading_pairs["items"]:
                            valid_trading_pairs.append(item)
                        trading_pair_list: List[str] = []
                        for raw_trading_pair in valid_trading_pairs:
                            converted_trading_pair: Optional[str] = convert_from_exchange_trading_pair(raw_trading_pair)
                            if converted_trading_pair is not None:
                                trading_pair_list.append(converted_trading_pair)
                        return trading_pair_list
        except Exception:
            # Do nothing if the request fails -- there will be no autocomplete for bitbay trading pairs
            pass

        return []

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                async with websockets.connect(f"{WS_URL}") as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    for pair in self._trading_pairs:

                        subscribe_request: Dict[str, Any] = {
                            "action": "subscribe-public",
                            "module": "trading",
                            "path": f"transactions/{pair}"
                        }
                        await ws.send(ujson.dumps(subscribe_request))
                    async for raw_msg in self._inner_messages(ws):
                        if len(raw_msg) > 4:
                            msg = ujson.loads(raw_msg)
                            topic = (msg["topic"]).split('/')
                            trading_pair = topic[2].upper()
                            msg["trading_pair"] = trading_pair
                            if "action" in msg:
                                if msg["action"] == "push":
                                    for datum in msg["message"]["transactions"]:
                                        trade_msg: OrderBookMessage = BitbayOrderBook.trade_message_from_exchange(datum, msg)
                                        output.put_nowait(trade_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                async with websockets.connect(f"{WS_URL}") as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    for pair in self._trading_pairs:
                        topics: List[dict] = [{"topic": "orderbook", "market": pair, "level": 0}]
                        subscribe_request: Dict[str, Any] = {
                            "action": "subscribe-public",
                            "module": "trading",
                            "path": f"orderbook/{pair}"
                        }
                        await ws.send(ujson.dumps(subscribe_request))
                    async for raw_msg in self._inner_messages(ws):
                        if len(raw_msg) > 4:
                            msg = ujson.loads(raw_msg)
                            if "action" in msg:
                                if msg["action"] == "push":
                                    for update in msg["message"]["changes"]:
                                        order_msg: OrderBookMessage = BitbayOrderBook.diff_message_from_exchange(update)
                                        output.put_nowait(order_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        pass