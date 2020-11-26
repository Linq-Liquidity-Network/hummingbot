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
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.novadax.novadax_auth import NovadaxAuth

NOVADAX_API_ENDPOINT = "wss://ws.novadax.com/socket.io/?EIO=3&transport=websocket"
NOVADAX_USER_STREAM_ENDPOINT = "userDataStream"


class NovadaxAPIUserStreamDataSource(UserStreamTrackerDataSource):

    MESSAGE_TIMEOUT = 60.0
    PING_TIMEOUT = 60.0

    _bausds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._bausds_logger is None:
            cls._bausds_logger = logging.getLogger(__name__)
        return cls._bausds_logger

    def __init__(self, novadax_auth: NovadaxAuth, novadax_uid: str):
        self._current_listen_key = None
        self._listen_for_user_stream_task = None
        self._last_recv_time: float = 0
        self._novadax_auth: NovadaxAuth = novadax_auth
        self._novadax_uid = novadax_uid
        super().__init__()

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time
    
    @staticmethod
    async def wait_til_next_tick(seconds: float = 1.0):
        now: float = time.time()
        current_tick: int = int(now // seconds)
        delay_til_next_tick: float = (current_tick + 1) * seconds - now
        await asyncio.sleep(delay_til_next_tick)

    async def _inner_messages(self, ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = str(await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT))
                    self._last_recv_time = time.time()
                    yield msg
                except asyncio.TimeoutError:
                    pong_waiter = await ws.ping()
                    await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
                    self._last_recv_time = time.time()
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            raise
        except websockets.exceptions.ConnectionClosed as e:
            raise
        finally:
            await ws.close()

    async def messages(self) -> AsyncIterable[str]:
        ws = await self.get_ws_connection()
        while (True):
            async for msg in self._inner_messages(ws):
                yield msg

    async def get_ws_connection(self) -> websockets.WebSocketClientProtocol:
        uid = self._novadax_uid
        api_key = self._novadax_auth.api_key()
        subscription = f'''42["login", "{uid}", "{api_key}"]'''
        stream_url: str = f"{NOVADAX_API_ENDPOINT}"
        ws = await websockets.connect(stream_url)
        await ws.send(subscription)
        return ws

    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                if self._listen_for_user_stream_task is not None:
                    self._listen_for_user_stream_task.cancel()
                self._listen_for_user_stream_task = safe_ensure_future(self.log_user_stream(output))
                await self.wait_til_next_tick(seconds=60.0)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().warning("Unexpected error while maintaining the user event listen key. Retrying after "
                                    "5 seconds...", exc_info=True)
                await asyncio.sleep(5)

    async def log_user_stream(self, output: asyncio.Queue):
        while True:
            try:
                async for message in self.messages():
                    if isinstance(message, str) and len(message) > 2:
                        if message[0:2] == '42':
                            decoded = ujson.loads(message[2:])
                            output.put_nowait(decoded)
                        else:
                            continue
                    else:
                        continue
            except Exception as e:
                self.logger().warning("Unexpected error. Retrying after 5 seconds...", exc_info=True)
                await asyncio.sleep(5.0)
