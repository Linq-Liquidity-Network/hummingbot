from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.connector.exchange.upbit.upbit_active_order_tracker import UpbitActiveOrderTracker


class UpbitOrderBookTrackerEntry(OrderBookTrackerEntry):
    def __init__(self,
                 trading_pair: str,
                 timestamp: float,
                 order_book: OrderBook,):

        super(UpbitOrderBookTrackerEntry, self).__init__(trading_pair, timestamp, order_book)

    def __repr__(self) -> str:
        return f"UpbitOrderBookTrackerEntry(trading_pair='{self._trading_pair}', timestamp='{self._timestamp}', " \
            f"order_book='{self._order_book}')"

