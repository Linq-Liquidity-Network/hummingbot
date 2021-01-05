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
# from hummingbot.connector.exchange.upbit.upbit_active_order_tracker import UpbitActiveOrderTracker
from hummingbot.connector.exchange.upbit.upbit_order_book import UpbitOrderBook
# from hummingbot.connector.exchange.upbit.upbit_order_book_tracker_entry import UpbitOrderBookTrackerEntry
from hummingbot.connector.exchange.upbit.upbit_utils import convert_from_exchange_trading_pair, convert_to_exchange_trading_pair
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.logger import HummingbotLogger
# from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
# from hummingbot.connector.exchange.upbit.upbit_order_book_message import UpbitOrderBookMessage
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_row import ClientOrderBookRow


MARKETS_URL = "/market/all"
TICKER_URL = "/ticker?markets=:markets"
SNAPSHOT_URL = "/orderbook?markets=:trading_pair"
TOKEN_INFO_URL = "/api/v2/exchange/tokens"
WS_URL = "wss://sg-api.upbit.com/websocket/v1"
REST_URL =  "https://sg-api.upbit.com/v1"
UPBIT_PRICE_URL = TICKER_URL


class UpbitAPIOrderBookDataSource(OrderBookTrackerDataSource):

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

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str]) -> Dict[str, float]:
        pairs = [convert_to_exchange_trading_pair(pair) for pair in trading_pairs]
        async with aiohttp.ClientSession() as client:
            resp = await client.get(f"{REST_URL}{TICKER_URL}".replace(":markets", ",".join(pairs)))
            resp_json = await resp.json()
            return {convert_from_exchange_trading_pair(x["market"]): float(x["trade_price"]) for x in resp_json}

    @property
    def order_book_class(self) -> UpbitOrderBook:
        return UpbitOrderBook

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    async def get_snapshot(self, client: aiohttp.ClientSession, trading_pair: str, level: int = 0) -> Dict[str, any]:
        async with client.get(f"{REST_URL}{SNAPSHOT_URL}".replace(":trading_pair", convert_to_exchange_trading_pair(trading_pair))) as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(
                    f"Error fetching upbit market snapshot for {trading_pair}. " f"HTTP status is {response.status}."
                )
            data: Dict[str, Any] = await response.json()
            return data[0]

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        async with aiohttp.ClientSession() as client:
            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair, 1000)
            snapshot_timestamp: float = snapshot["timestamp"]

            snapshot_msg: OrderBookMessage = UpbitOrderBook.snapshot_message_from_exchange(
                snapshot,
                snapshot_timestamp,
                metadata={"rest": True, "trading_pair": convert_from_exchange_trading_pair(snapshot["market"])}
            )

            order_book: OrderBook = self.order_book_create_function()
            bids = [ClientOrderBookRow(bid["price"],bid["amount"],snapshot_msg.update_id) for bid in snapshot_msg.bids]
            asks = [ClientOrderBookRow(ask["price"],ask["amount"],snapshot_msg.update_id) for ask in snapshot_msg.asks]
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
        resp = requests.get(url=UPBIT_PRICE_URL, params={"market": trading_pair})
        record = resp.json()

        data = record[0]
        mid_price = (Decimal(data["high_price"]) + Decimal(data["low_price"])) / 2

        return mid_price

    @staticmethod
    async def fetch_trading_pairs() -> List[str]:
        try:
            async with aiohttp.ClientSession() as client:
                async with client.get(f"{REST_URL}{MARKETS_URL}", timeout=5) as response:
                    if response.status == 200:
                        all_trading_pairs: Dict[str, Any] = await response.json()
                        valid_trading_pairs: list = []
                        for item in all_trading_pairs:
                            valid_trading_pairs.append(item["market"])
                        trading_pair_list: List[str] = []
                        for raw_trading_pair in valid_trading_pairs:
                            converted_trading_pair: Optional[str] = convert_from_exchange_trading_pair(raw_trading_pair)
                            if converted_trading_pair is not None:
                                trading_pair_list.append(converted_trading_pair)
                        return trading_pair_list
        except Exception:
            # Do nothing if the request fails -- there will be no autocomplete for upbit trading pairs
            pass

        return []

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                codes: List[str] = [convert_to_exchange_trading_pair(pair) for pair in self._trading_pairs]
                subscribe_request: List[Dict[str, Any]] = [
                    {"ticket": "ram macbook"},
                    {"format": "SIMPLE"},
                    {
                        "type": "trade",
                        "codes": codes
                    }
                ]

                async with websockets.connect(f"{WS_URL}") as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    await ws.send(ujson.dumps(subscribe_request))
                    async for raw_msg in self._inner_messages(ws):
                        if len(raw_msg) > 4:
                            msg = ujson.loads(raw_msg)
                            if "ty" in msg:
                                msg["trading_pair"] = convert_from_exchange_trading_pair(msg["cd"])
                                if (msg['ty'] == "trade") and (msg["st"] == "REALTIME"):
                                    trade_msg: OrderBookMessage = UpbitOrderBook.trade_message_from_exchange(msg)
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
                codes: List[str] = [convert_to_exchange_trading_pair(pair) for pair in self._trading_pairs]
                subscribe_request: List[Dict[str, Any]] = [
                    {"ticket": "ram macbook"},
                    {"format": "SIMPLE"},
                    {
                        "type": "orderbook",
                        "codes": codes
                    }
                ]

                async with websockets.connect(f"{WS_URL}") as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    await ws.send(ujson.dumps(subscribe_request))
                    async for raw_msg in self._inner_messages(ws):
                        if len(raw_msg) > 4:
                            msg = ujson.loads(raw_msg)
                            if "ty" in msg:
                                if msg["ty"] == "orderbook":
                                    msg["trading_pair"] = convert_from_exchange_trading_pair(msg["cd"])
                                    if msg["st"] == "SNAPSHOT":
                                        order_msg: OrderBookMessage = UpbitOrderBook.snapshot_message_from_exchange(msg,msg["tms"],{"rest": False})
                                        output.put_nowait(order_msg)
                                    elif msg["st"] == "REALTIME":
                                        order_msg: OrderBookMessage = UpbitOrderBook.diff_message_from_exchange(msg, msg["tms"])
                                        output.put_nowait(order_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        pass # Upbit gets snapshots from the same subscription as for diffs
