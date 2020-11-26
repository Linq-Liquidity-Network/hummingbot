#!/usr/bin/env python

import asyncio
import aiohttp
from decimal import Decimal
import logging
import time
from typing import (
    AsyncIterable,
    Dict,
    Optional
)
import json
import simplejson
import websockets
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.ftx.ftx_auth import FtxAuth

FTX_API_ENDPOINT = "wss://ftx.com/ws/"
FTX_USER_STREAM_ENDPOINT = "userDataStream"


class FtxAPIUserStreamDataSource(UserStreamTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _bausds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._bausds_logger is None:
            cls._bausds_logger = logging.getLogger(__name__)
        return cls._bausds_logger

    def __init__(self, ftx_auth: FtxAuth):
        self._listen_for_user_stream_task = None
        self._ftx_auth: FtxAuth = ftx_auth
        self._current_listen_key = None
        self._last_recv_time: float = 0
        super().__init__()

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def _inner_messages(self, ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    self._last_recv_time = time.time()
                    yield msg
                except asyncio.TimeoutError:
                    try:
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
                        self._last_recv_time = time.time()
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except websockets.exceptions.ConnectionClosed:
            return
        finally:
            await ws.close()

    async def messages(self) -> AsyncIterable[str]:
        async with (await self.get_ws_connection()) as ws:
            subscribe = json.dumps(self._ftx_auth.generate_websocket_subscription())
            await ws.send(subscribe)

            orders = json.dumps({"op": "subscribe", "channel": "orders"})
            await ws.send(orders)
            
            fills = json.dumps({"op": "subscribe", "channel": "fills"})
            await ws.send(fills)
            
            async for msg in self._inner_messages(ws):
                yield msg

    async def get_ws_connection(self) -> websockets.WebSocketClientProtocol:
        stream_url: str = f"{FTX_API_ENDPOINT}"

        # Create the WS connection.
        return websockets.connect(stream_url)

    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        try:
            while True:
                try:
                    self._listen_for_user_stream_task = safe_ensure_future(self.log_user_stream(output))
                    await self.wait_til_next_tick(seconds=60.0)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.logger().error("Unexpected error while maintaining the user event listen key. Retrying after "
                                        "5 seconds...", exc_info=True)
                    await asyncio.sleep(5)
        finally:
            # Make sure no background task is leaked.
            if self._listen_for_user_stream_task is not None:
                self._listen_for_user_stream_task.cancel()
                self._listen_for_user_stream_task = None
            self._current_listen_key = None

    async def log_user_stream(self, output: asyncio.Queue):
        while True:
            try:
                async for message in self.messages():
                    decoded: Dict[str, any] = simplejson.loads(message, parse_float=Decimal)
                    output.put_nowait(decoded)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error. Retrying after 5 seconds...", exc_info=True)
                await asyncio.sleep(5.0)