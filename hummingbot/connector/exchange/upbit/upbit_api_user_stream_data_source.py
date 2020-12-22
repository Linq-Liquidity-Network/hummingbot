#!/usr/bin/env python

import asyncio
import aiohttp
import logging
from typing import (
    AsyncIterable,
    Dict,
    Optional,
    Any
)
import time
import ujson
import websockets
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.upbit.upbit_auth import UpbitAuth
from hummingbot.connector.exchange.upbit.upbit_api_order_book_data_source import UpbitAPIOrderBookDataSource
from hummingbot.connector.exchange.upbit.upbit_order_book import UpbitOrderBook
from hummingbot.connector.exchange.upbit.upbit_utils import get_ws_api_key

UPBIT_WS_URL = "wss://ws.upbit.io/v2/ws"

UPBIT_ROOT_API = "https://api.upbit.io"


class UpbitAPIUserStreamDataSource(UserStreamTrackerDataSource):

    _krausds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._krausds_logger is None:
            cls._krausds_logger = logging.getLogger(__name__)
        return cls._krausds_logger

    def __init__(self, orderbook_tracker_data_source: UpbitAPIOrderBookDataSource, upbit_auth: UpbitAuth):
        self._upbit_auth: UpbitAuth = upbit_auth
        self._orderbook_tracker_data_source: UpbitAPIOrderBookDataSource = orderbook_tracker_data_source
        self._shared_client: Optional[aiohttp.ClientSession] = None
        self._last_recv_time: float = 0
        super().__init__()

    @property
    def order_book_class(self):
        return UpbitOrderBook

    @property
    def last_recv_time(self):
        return self._last_recv_time

    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                ws_key: str = await get_ws_api_key()
                async with websockets.connect(f"{UPBIT_WS_URL}?wsApiKey={ws_key}") as ws:
                    ws: websockets.WebSocketClientProtocol = ws

                    topics = [{"topic": "order", "market": m} for m in self._orderbook_tracker_data_source.trading_pairs]
                    topics.append({
                        "topic": "account"
                    })

                    subscribe_request: Dict[str, Any] = {
                        "op": "sub",
                        "apiKey": self._upbit_auth.generate_auth_dict()["X-API-KEY"],
                        "unsubscribeAll": True,
                        "topics": topics
                    }
                    await ws.send(ujson.dumps(subscribe_request))

                    async for raw_msg in self._inner_messages(ws):
                        self._last_recv_time = time.time()

                        diff_msg = ujson.loads(raw_msg)
                        if 'op' in diff_msg:
                            continue  # These messages are for control of the stream, so skip sending them to the market class
                        output.put_nowait(diff_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with upbit WebSocket connection. "
                                    "Retrying after 30 seconds...", exc_info=True)
                await asyncio.sleep(30.0)

    async def _inner_messages(self,
                              ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        """
        Generator function that returns messages from the web socket stream
        :param ws: current web socket connection
        :returns: message in AsyncIterable format
        """
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                msg: str = await asyncio.wait_for(ws.recv(), timeout=None)    # This will throw the ConnectionClosed exception on disconnect
                if msg == "ping":
                    await ws.send("pong")  # skip returning this and handle this protocol level message here
                else:
                    yield msg
        except websockets.exceptions.ConnectionClosed:
            self.logger().warning("upbit websocket connection closed. Reconnecting...")
            return
        finally:
            await ws.close()

    async def stop(self):
        if self._shared_client is not None and not self._shared_client.closed:
            await self._shared_client.close()
