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

from decimal import Decimal

from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.bitbay.bitbay_order_book_message import BitbayOrderBookMessage
from hummingbot.core.event.events import TradeType
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessage,
    OrderBookMessageType,
)

_dob_logger = None

cdef class BitbayOrderBook(OrderBook):

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
                                       metadata: Optional[Dict] = None) -> BitbayOrderBookMessage:
        if metadata:
            msg.update(metadata)
        return BitbayOrderBookMessage(OrderBookMessageType.SNAPSHOT, msg, timestamp)

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        if msg["action"] == "remove":
            msg["amount"] = Decimal('0')
        else:
            msg["amount"] = Decimal(msg["state"]["ca"])

        msg["price"] = msg["rate"]
        msg["trading_pair"] = msg["marketCode"]
        return BitbayOrderBookMessage(OrderBookMessageType.DIFF, msg, timestamp)

    @classmethod
    def trade_message_from_exchange(cls, msg: Dict[str, any], metadata: Optional[Dict] = None):
        ts = metadata["timestamp"]
        return OrderBookMessage(OrderBookMessageType.TRADE, {
            "trading_pair": metadata["trading_pair"],
            "trade_type": float(TradeType.BUY.value) if (msg["ty"] == "Buy") else float(TradeType.SELL.value),
            "trade_id": msg["id"],
            "update_id": ts,
            "price": msg["r"],
            "amount": msg["a"]
        }, timestamp=ts * 1e-3)

    @classmethod
    def snapshot_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None) -> OrderBookMessage:
        msg = record.json if type(record.json)==dict else ujson.loads(record.json)
        return BitbayOrderBookMessage(OrderBookMessageType.SNAPSHOT, msg, timestamp=record.timestamp * 1e-3)

    @classmethod
    def diff_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None) -> OrderBookMessage:
        return BitbayOrderBookMessage(OrderBookMessageType.DIFF, record.json)

    @classmethod
    def snapshot_message_from_kafka(cls, record: ConsumerRecord, metadata: Optional[Dict] = None) -> OrderBookMessage:
        msg = ujson.loads(record.value.decode())
        return BitbayOrderBookMessage(OrderBookMessageType.SNAPSHOT, msg, timestamp=record.timestamp * 1e-3)

    @classmethod
    def diff_message_from_kafka(cls, record: ConsumerRecord, metadata: Optional[Dict] = None) -> OrderBookMessage:
        msg = ujson.loads(record.value.decode())
        return BitbayOrderBookMessage(OrderBookMessageType.DIFF, msg)

    @classmethod
    def trade_receive_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None):
        return BitbayOrderBookMessage(OrderBookMessageType.TRADE, record.json)

    @classmethod
    def from_snapshot(cls, snapshot: OrderBookMessage):
        raise NotImplementedError("new connector order book needs to retain individual order data.")

    @classmethod
    def restore_from_snapshot_and_diffs(self, snapshot: OrderBookMessage, diffs: List[OrderBookMessage]):
        raise NotImplementedError("new connector order book needs to retain individual order data.")
