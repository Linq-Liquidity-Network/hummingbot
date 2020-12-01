#!/usr/bin/env python

import asyncio
import logging
from typing import (
    Optional
)
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.user_stream_tracker import (
    UserStreamTrackerDataSourceType,
    UserStreamTracker
)
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.connector.exchange.novadax.novadax_api_user_stream_data_source import NovadaxAPIUserStreamDataSource
from hummingbot.connector.exchange.novadax.novadax_auth import NovadaxAuth

class NovadaxUserStreamTracker(UserStreamTracker):
    _bust_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._bust_logger is None:
            cls._bust_logger = logging.getLogger(__name__)
        return cls._bust_logger

    def __init__(self,
                 novadax_auth: Optional[NovadaxAuth] = None,
                 novadax_uid: str = None):
        super().__init__()
        self._novadax_auth: NovadaxAuth = novadax_auth
        self._novadax_uid = novadax_uid
        self._ev_loop: asyncio.events.AbstractEventLoop = asyncio.get_event_loop()
        self._data_source: Optional[UserStreamTrackerDataSource] = None
        self._user_stream_tracking_task: Optional[asyncio.Task] = None

    @property
    def data_source(self) -> UserStreamTrackerDataSource:
        if not self._data_source:
            self._data_source = NovadaxAPIUserStreamDataSource(novadax_auth=self._novadax_auth, novadax_uid=self._novadax_uid)
        return self._data_source

    @property
    def exchange_name(self) -> str:
        return "novadax"

    async def start(self):
        self._user_stream_tracking_task = safe_ensure_future(
            self.data_source.listen_for_user_stream(self._ev_loop, self._user_stream)
        )
        await safe_gather(self._user_stream_tracking_task)
