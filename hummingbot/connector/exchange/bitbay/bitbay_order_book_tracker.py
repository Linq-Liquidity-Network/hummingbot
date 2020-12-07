import asyncio
import logging
# import sys
from collections import deque, defaultdict
from typing import (
    Optional,
    Deque,
    List,
    Dict,
    # Set
)
from hummingbot.connector.exchange.bitbay.bitbay_active_order_tracker import BitbayActiveOrderTracker
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
from hummingbot.connector.exchange.bitbay.bitbay_order_book import BitbayOrderBook
from hummingbot.connector.exchange.bitbay.bitbay_order_book_message import BitbayOrderBookMessage
# from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
# from hummingbot.core.data_type.remote_api_order_book_data_source import RemoteAPIOrderBookDataSource
from hummingbot.connector.exchange.bitbay.bitbay_api_order_book_data_source import BitbayAPIOrderBookDataSource
# from hummingbot.connector.exchange.bitbay.bitbay_order_book_tracker_entry import BitbayOrderBookTrackerEntry
from hummingbot.core.data_type.order_book_message import OrderBookMessageType
# from hummingbot.core.utils.async_utils import safe_ensure_future


class BitbayOrderBookTracker(OrderBookTracker):
    _dobt_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._dobt_logger is None:
            cls._dobt_logger = logging.getLogger(__name__)
        return cls._dobt_logger

    def __init__(
        self,
        trading_pairs: Optional[List[str]] = None,
        rest_api_url: str = "https://api.bitbay.net.io",
        websocket_url: str = "wss://ws.bitbay.net.io/v2/ws",
    ):
        super().__init__(
            BitbayAPIOrderBookDataSource(
                trading_pairs=trading_pairs,
                rest_api_url=rest_api_url,
                websocket_url=websocket_url,
            ),
            trading_pairs)
        self._order_books: Dict[str, BitbayOrderBook] = {}
        self._saved_message_queues: Dict[str, Deque[BitbayOrderBookMessage]] = defaultdict(lambda: deque(maxlen=1000))
        self._order_book_snapshot_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_diff_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_trade_stream: asyncio.Queue = asyncio.Queue()
        self._ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        self._active_order_trackers: Dict[str, BitbayActiveOrderTracker] = defaultdict(lambda: BitbayActiveOrderTracker())

    @property
    def exchange_name(self) -> str:
        return "bitbay"

    async def _track_single_book(self, trading_pair: str):
        message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]
        order_book: BitbayOrderBook = self._order_books[trading_pair]
        active_order_tracker: BitbayActiveOrderTracker = self._active_order_trackers[trading_pair]
        while True:
            try:
                message: BitbayOrderBookMessage = None
                
                saved_messages: Deque[BitbayOrderBookMessage] = self._saved_message_queues[trading_pair]

                # Process saved messages first if there are any
                if len(saved_messages) > 0:
                    message = saved_messages.popleft()
                else:
                    message = await message_queue.get()

                if message.type is OrderBookMessageType.DIFF:
                    bids, asks = active_order_tracker.convert_diff_message_to_order_book_row(message)
                    order_book.apply_diffs(bids, asks, message.timestamp)

                elif message.type is OrderBookMessageType.SNAPSHOT:
                    s_bids, s_asks = active_order_tracker.convert_snapshot_message_to_order_book_row(message)
                    order_book.apply_snapshot(s_bids, s_asks, message.timestamp)
                    self.logger().debug("Processed order book snapshot for %s.", trading_pair)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    f"Unexpected error tracking order book for {trading_pair}.",
                    exc_info=True,
                    app_warning_msg="Unexpected error tracking order book. Retrying after 5 seconds.",
                )
                await asyncio.sleep(5.0)
