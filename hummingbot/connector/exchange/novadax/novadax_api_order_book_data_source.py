#!/usr/bin/env python

import asyncio
import aiohttp
import cachetools.func
from decimal import Decimal
import logging
import re
import requests
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional
)
import time
import ujson
import websockets
from websockets.exceptions import ConnectionClosed

from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.novadax.novadax_order_book import NovadaxOrderBook
from hummingbot.connector.exchange.novadax.novadax_utils import convert_to_exchange_trading_pair, convert_from_exchange_trading_pair

TRADING_PAIR_FILTER = re.compile(r"(BTC|ETH|USDT)$")

SNAPSHOT_REST_URL = "https://api.novadax.com/v1/market/depth"
DIFF_STREAM_URL = "wss://ws.novadax.com/socket.io/?EIO=3&transport=websocket"
TICKER_PRICE_CHANGE_URL = "https://api.novadax.com//v1/market/tickers"
EXCHANGE_INFO_URL = "https://api.novadax.com//v1/common/symbols"

API_CALL_TIMEOUT = 5.0


class NovadaxAPIOrderBookDataSource(OrderBookTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _baobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._baobds_logger is None:
            cls._baobds_logger = logging.getLogger(__name__)
        return cls._baobds_logger

    def __init__(self, trading_pairs: Optional[List[str]] = None):
        super().__init__(trading_pairs)
        self._order_book_create_function = lambda: OrderBook()

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str]) -> Dict[str, float]:
        async with aiohttp.ClientSession() as client:
            async with await client.get(TICKER_PRICE_CHANGE_URL, timeout=API_CALL_TIMEOUT) as response:
                response_json = await response.json()
                data = response_json['data']
                return {convert_from_exchange_trading_pair(result['symbol']): float(result['lastPrice'])
                        for result in data if convert_from_exchange_trading_pair(result['symbol']) in trading_pairs}

    async def get_trading_pairs(self) -> List[str]:
        return self._trading_pairs

    @staticmethod
    @cachetools.func.ttl_cache(ttl=10)
    def get_mid_price(trading_pair: str) -> Optional[Decimal]:
        resp = requests.get(url=f"{TICKER_PRICE_CHANGE_URL}/?symbol={convert_to_exchange_trading_pair(trading_pair)}")
        record = resp.json()
        result = (Decimal(record.get("bid", "0")) + Decimal(record.get("ask", "0"))) / Decimal("2")
        return result if result else None

    @staticmethod
    async def fetch_trading_pairs() -> List[str]:
        try:
            async with aiohttp.ClientSession() as client:
                async with client.get(EXCHANGE_INFO_URL, timeout=API_CALL_TIMEOUT) as response:
                    if response.status == 200:
                        all_trading_pairs: Dict[str, Any] = await response.json()
                        valid_trading_pairs: list = []
                        for item in all_trading_pairs["data"]:
                            valid_trading_pairs.append(item["symbol"])
                        trading_pair_list: List[str] = []
                        for raw_trading_pair in valid_trading_pairs:
                            converted_trading_pair: Optional[str] = \
                                convert_from_exchange_trading_pair(raw_trading_pair)
                            if converted_trading_pair is not None:
                                trading_pair_list.append(converted_trading_pair)
                        return trading_pair_list
        except Exception:
            # Do nothing if the request fails -- there will be no autocomplete for loopring trading pairs
            pass

        return []

    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str, limit: int = 1000) -> Dict[str, Any]:
        async with client.get(f"{SNAPSHOT_REST_URL}?symbol={convert_to_exchange_trading_pair(trading_pair)}") as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching novadax market snapshot for {trading_pair}. "
                              f"HTTP status is {response.status}.")
            data: Dict[str, Any] = await response.json()
            return data["data"]

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        async with aiohttp.ClientSession() as client:
            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair, 1000)
            snapshot_timestamp: float = time.time()
            snapshot_msg: OrderBookMessage = NovadaxOrderBook.restful_snapshot_message_from_exchange(
                snapshot,
                snapshot_timestamp,
                metadata={"trading_pair": trading_pair}
            )
            order_book: OrderBook = self.order_book_create_function()
            order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
            return order_book

    async def _inner_messages(self,
                              ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    if msg[0:2] == '42':
                        yield msg[2:]
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

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                stream_url: str = f"{DIFF_STREAM_URL}"
                for trading_pair in self._trading_pairs:
                    subscription_request = f'''42["join", "{convert_to_exchange_trading_pair(trading_pair)}"]'''
                    async with websockets.connect(stream_url) as ws:
                        ws: websockets.WebSocketClientProtocol = ws
                        await ws.send(subscription_request)
                        async for raw_msg in self._inner_messages(ws):
                            msg = ujson.loads(raw_msg)
                            if msg[0] == "basic_with_trades":
                                for trade in msg[1]["trades"]:
                                    trade_msg: OrderBookMessage = NovadaxOrderBook.trade_message_from_exchange(trade, msg[1]["basic"])
                                    output.put_nowait(trade_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                stream_url: str = f"{DIFF_STREAM_URL}"
                for trading_pair in self._trading_pairs:
                    subscription_request = f'''42["join", "{convert_to_exchange_trading_pair(trading_pair)}_depth_0"]'''
                    async with websockets.connect(stream_url) as ws:
                        ws: websockets.WebSocketClientProtocol = ws
                        await ws.send(subscription_request)
                        async for raw_msg in self._inner_messages(ws):
                            msg = ujson.loads(raw_msg)
                            if msg[0] == "depth":
                                order_book_message: OrderBookMessage = NovadaxOrderBook.snapshot_message_from_exchange(
                                    msg[1]["depth"], time.time(), msg[1]["basic"])
                                output.put_nowait(order_book_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        pass
