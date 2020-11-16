# distutils: language=c++
# distutils: sources=hummingbot/core/cpp/OrderBookEntry.cpp

import logging

import numpy as np
import math
from decimal import Decimal

from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book_row import ClientOrderBookRow

s_empty_diff = np.ndarray(shape=(0, 4), dtype="float64")
_ddaot_logger = None

cdef class BitbayActiveOrderTracker:
    def __init__(self, active_asks=None, active_bids=None):
        super().__init__()
        self._active_asks = active_asks or {}
        self._active_bids = active_bids or {}

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _ddaot_logger
        if _ddaot_logger is None:
            _ddaot_logger = logging.getLogger(__name__)
        return _ddaot_logger

    @property
    def active_asks(self):
        return self._active_asks

    @property
    def active_bids(self):
        return self._active_bids

    cdef tuple c_convert_snapshot_message_to_np_arrays(self, object message):
        cdef:
            object price
            str order_id

        # Refresh all order tracking.
        self._active_bids.clear()
        self._active_asks.clear()

        for bid_order in message.bids:
            order_id = str(message.timestamp)
            price, amount = Decimal(bid_order['ra']), Decimal(bid_order['ca'])
            order_dict = {
                "availableAmount": Decimal(amount),
                "orderId": order_id
            }
            if price in self._active_bids:
                self._active_bids[price][order_id] += order_dict
            else:
                self._active_bids[price] = {
                    order_id: order_dict
                }

        for ask_order in message.asks:
            price, amount = Decimal(ask_order['ra']), Decimal(ask_order['ca'])
            order_id = str(message.timestamp)
            order_dict = {
                "availableAmount": amount,
                "orderId": order_id
            }

            if price in self._active_asks:
                self._active_asks[price][order_id] = order_dict
            else:
                self._active_asks[price] = {
                    order_id: order_dict
                }

        # Return the sorted snapshot tables.
        cdef:
            np.ndarray[np.float64_t, ndim=2] bids = np.array(
                [[message.timestamp,
                  Decimal(price),
                  sum([Decimal(order_dict["availableAmount"])
                       for order_dict in self._active_bids[price].values()]),
                  order_id]
                 for price in sorted(self._active_bids.keys(), reverse=True)], dtype="float64", ndmin=2)

            np.ndarray[np.float64_t, ndim=2] asks = np.array(
                [[message.timestamp,
                  Decimal(price),
                  sum([Decimal(order_dict["availableAmount"])
                       for order_dict in self._active_asks[price].values()]),
                  order_id]
                 for price in sorted(self._active_asks.keys(), reverse=True)], dtype="float64", ndmin=2)

        # If there're no rows, the shape would become (1, 0) and not (0, 4).
        # Reshape to fix that.
        if bids.shape[1] != 4:
            bids = bids.reshape((0, 4))
        if asks.shape[1] != 4:
            asks = asks.reshape((0, 4))
        return bids, asks

    cdef tuple c_convert_diff_message_to_np_arrays(self, object message):
        cdef:
            dict content = message.content
            str market = content["trading_pair"]
            str order_id
            str order_side
            str price_raw
            object price
            dict order_dict
            double timestamp = message.timestamp
            double quantity = 0

        bids = s_empty_diff
        asks = s_empty_diff
        price = content["price"]
        quantity = content["amount"]
        order_side = content["entryType"]

        if order_side == 'Buy':
            bids = np.array(
                [[timestamp,
                  float(price),
                  float(quantity),
                  timestamp]],
                dtype="float64",
                ndmin=2
            )

        if order_side == 'Sell':
            asks = np.array(
                [[timestamp,
                  float(price),
                  float(quantity),
                  timestamp]],
                dtype="float64",
                ndmin=2
            )

        return bids, asks

    def convert_diff_message_to_order_book_row(self, message):
        np_bids, np_asks = self.c_convert_diff_message_to_np_arrays(message)
        bids_row = [ClientOrderBookRow(price, qty, update_id) for ts, price, qty, update_id in np_bids]
        asks_row = [ClientOrderBookRow(price, qty, update_id) for ts, price, qty, update_id in np_asks]
        return bids_row, asks_row

    def convert_snapshot_message_to_order_book_row(self, message):
        np_bids, np_asks = self.c_convert_snapshot_message_to_np_arrays(message)
        bids_row = [ClientOrderBookRow(price, qty, update_id) for ts, price, qty, update_id in np_bids]
        asks_row = [ClientOrderBookRow(price, qty, update_id) for ts, price, qty, update_id in np_asks]
        return bids_row, asks_row
