#!/usr/bin/env python

import asyncio
import aiohttp
import logging
import time
from typing import (
    AsyncIterable,
    Dict,
    Optional
)
import ujson
import websockets
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from novadax import RequestClient as NovaClient
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.novadax.novadax_auth import NovadaxAuth

NOVADAX_API_ENDPOINT = "wss://ws.novadax.com/socket.io/?EIO=3&transport=websocket"
NOVADAX_USER_STREAM_ENDPOINT = "userDataStream"


class NovadaxAPIUserStreamDataSource(UserStreamTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _bausds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._bausds_logger is None:
            cls._bausds_logger = logging.getLogger(__name__)
        return cls._bausds_logger

    def __init__(self, novadax_client: NovaClient, novadax_uid: str):
        self._novadax_client: NovaClient = novadax_client
        self._current_listen_key = None
        self._listen_for_user_stream_task = None
        self._last_recv_time: float = 0
        self._novadax_auth: NovadaxAuth = NovadaxAuth(self._novadax_client)
        self._novadax_uid = novadax_uid
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
                    if msg[0:2] == '42':
                        yield msg[2:]
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
        try:
            async with (await self.get_ws_connection()) as ws:
                uid = self._novadax_uid
                api_key = self._novadax_auth.api_key()
                subscription = f'''42["login", "{uid}", "{api_key}"]'''
                await ws.send(subscription)
                async for msg in self._inner_messages(ws):
                    yield msg
        except asyncio.CancelledError:
            return

    async def get_ws_connection(self) -> websockets.WebSocketClientProtocol:
        stream_url: str = f"{NOVADAX_API_ENDPOINT}"
        ws = websockets.connect(stream_url)
        return ws

    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                if self._current_listen_key is None:
                    if self._listen_for_user_stream_task is not None:
                        self._listen_for_user_stream_task.cancel()
                    self._listen_for_user_stream_task = safe_ensure_future(self.log_user_stream(output))
                    await self.wait_til_next_tick(seconds=60.0)

                await self.wait_til_next_tick(seconds=60.0)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error while maintaining the user event listen key. Retrying after "
                                    "5 seconds...", exc_info=True)
                await asyncio.sleep(5)

    async def log_user_stream(self, output: asyncio.Queue):
        while True:
            try:
                async for message in self.messages():
                    decoded: List[any] = ujson.loads(message)
                    output.put_nowait(message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error. Retrying after 5 seconds...", exc_info=True)
                await asyncio.sleep(5.0)
