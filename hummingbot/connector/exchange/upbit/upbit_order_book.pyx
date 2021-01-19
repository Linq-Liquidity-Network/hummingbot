#!/usr/bin/env python

from aiokafka import ConsumerRecord
import logging
from sqlalchemy.engine import RowProxy
from typing import (
    Dict,
    List,
    Optional,
)
import ujson

from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.upbit.upbit_order_book_message import UpbitOrderBookMessage
from hummingbot.core.event.events import TradeType
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessage,
    OrderBookMessageType,
)

_dob_logger = None

cdef class UpbitOrderBook(OrderBook):

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _dob_logger
        if _dob_logger is None:
            _dob_logger = logging.getLogger(__name__)
        return _dob_logger

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> UpbitOrderBookMessage:
        bids = []
        asks = []
        if metadata["rest"]:
            for bid_ask in msg["orderbook_units"]:
                bids.append({"price": bid_ask["bid_price"], "amount": bid_ask["bid_size"]})
                asks.append({"price": bid_ask["ask_price"], "amount": bid_ask["ask_size"]})
        else:
            for bid_ask in msg["obu"]:
                bids.append({"price": bid_ask["bp"], "amount": bid_ask["bs"]})
                asks.append({"price": bid_ask["ap"], "amount": bid_ask["as"]})
        msg["bids"] = bids
        msg["asks"] = asks
        return UpbitOrderBookMessage(OrderBookMessageType.SNAPSHOT, msg, timestamp)

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        bids, asks = [], []
        for bid_ask in msg["obu"]:
            bids.append({"price": bid_ask["bp"], "amount": bid_ask["bs"]})
            asks.append({"price": bid_ask["ap"], "amount": bid_ask["as"]})
        msg["bids"] = bids
        msg["asks"] = asks
        return UpbitOrderBookMessage(OrderBookMessageType.DIFF, msg, timestamp)

    @classmethod
    def trade_message_from_exchange(cls, msg: Dict[str, any], metadata: Optional[Dict] = None):
        ts = msg["tms"]
        return OrderBookMessage(OrderBookMessageType.TRADE, {
            "trading_pair": msg["trading_pair"],
            "trade_type": float(TradeType.BUY.value) if (msg["ab"] == "BID") else float(TradeType.SELL.value),
            "trade_id": msg["sid"],
            "update_id": ts,
            "price": msg["tp"],
            "amount": msg["tv"]
        }, timestamp=ts * 1e-3)

    @classmethod
    def snapshot_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None) -> OrderBookMessage:
        msg = record.json if type(record.json)==dict else ujson.loads(record.json)
        return UpbitOrderBookMessage(OrderBookMessageType.SNAPSHOT, msg, timestamp=record.timestamp * 1e-3)

    @classmethod
    def diff_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None) -> OrderBookMessage:
        return UpbitOrderBookMessage(OrderBookMessageType.DIFF, record.json)

    @classmethod
    def snapshot_message_from_kafka(cls, record: ConsumerRecord, metadata: Optional[Dict] = None) -> OrderBookMessage:
        msg = ujson.loads(record.value.decode())
        return UpbitOrderBookMessage(OrderBookMessageType.SNAPSHOT, msg, timestamp=record.timestamp * 1e-3)

    @classmethod
    def diff_message_from_kafka(cls, record: ConsumerRecord, metadata: Optional[Dict] = None) -> OrderBookMessage:
        msg = ujson.loads(record.value.decode())
        return UpbitOrderBookMessage(OrderBookMessageType.DIFF, msg)

    @classmethod
    def trade_receive_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None):
        return UpbitOrderBookMessage(OrderBookMessageType.TRADE, record.json)

    @classmethod
    def from_snapshot(cls, snapshot: OrderBookMessage):
        raise NotImplementedError("upbit order book needs to retain individual order data.")

    @classmethod
    def restore_from_snapshot_and_diffs(self, snapshot: OrderBookMessage, diffs: List[OrderBookMessage]):
        raise NotImplementedError("upbit order book needs to retain individual order data.")
