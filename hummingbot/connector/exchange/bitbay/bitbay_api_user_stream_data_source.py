#!/usr/bin/env python

import asyncio
import aiohttp
import logging
from typing import (
    AsyncIterable,
    Dict,
    Optional,
    Any,
    List
)
import time
import ujson
import websockets
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.bitbay.bitbay_auth import BitbayAuth
from hummingbot.connector.exchange.bitbay.bitbay_api_order_book_data_source import BitbayAPIOrderBookDataSource
from hummingbot.connector.exchange.bitbay.bitbay_order_book import BitbayOrderBook

BITBAY_WS_URL = "wss://api.bitbay.net/websocket"

BITBAY_ROOT_API = "https://api.bitbay.net/rest"


class BitbayAPIUserStreamDataSource(UserStreamTrackerDataSource):

    _krausds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._krausds_logger is None:
            cls._krausds_logger = logging.getLogger(__name__)
        return cls._krausds_logger

    def __init__(self, orderbook_tracker_data_source: BitbayAPIOrderBookDataSource, bitbay_auth: BitbayAuth, trading_pairs: Optional[List[str]]):
        self._bitbay_auth: BitbayAuth = bitbay_auth
        self._orderbook_tracker_data_source: BitbayAPIOrderBookDataSource = orderbook_tracker_data_source
        self._shared_client: Optional[aiohttp.ClientSession] = None
        self._last_recv_time: float = 0
        self._trading_pairs = trading_pairs
        super().__init__()

    @property
    def order_book_class(self):
        return BitbayOrderBook

    @property
    def last_recv_time(self):
        return self._last_recv_time

    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                async with websockets.connect(f"{BITBAY_WS_URL}") as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    creds = self._bitbay_auth.generate_auth_dict()
                    
                    for pair in self._trading_pairs:
                        subscribe_request: Dict[str, Any] = {
                            "action": "subscribe-private",
                            "module": "trading",
                            "path": f"offers/{pair}",
                            "hashSignature": f"{creds['hashSignature']}",
                            "publicKey": f"{creds['publicKey']}",
                            "requestTimestamp": f"{creds['requestTimestamp']}"
                        }
                        await ws.send(ujson.dumps(subscribe_request))

                    subscribe_request: Dict[str, Any] = {
                        "action": "subscribe-private",
                        "module": "balances",
                        "path": "balance/bitbay/updatefunds",
                        "hashSignature": f"{creds['hashSignature']}",
                        "publicKey": f"{creds['publicKey']}",
                        "requestTimestamp": f"{creds['requestTimestamp']}"
                    }
                    await ws.send(ujson.dumps(subscribe_request))

                    async for raw_msg in self._inner_messages(ws):
                        self._last_recv_time = time.time()

                        diff_msg = ujson.loads(raw_msg)
                        if 'action' in diff_msg:
                            if diff_msg['action'] == 'push':
                                output.put_nowait(diff_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with bitbay WebSocket connection. "
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
            self.logger().warning("bitbay websocket connection closed. Reconnecting...")
            return
        finally:
            await ws.close()

    #async def stop(self):
    #    if self._shared_client is not None and not self._shared_client.closed:
    #        await self._shared_client.close()
