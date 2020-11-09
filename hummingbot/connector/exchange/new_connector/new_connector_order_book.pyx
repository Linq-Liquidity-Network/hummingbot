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
from hummingbot.connector.exchange.new_connector.new_connector_order_book_message import classNewConnectorOrderBookMessage
from hummingbot.core.event.events import TradeType
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessage,
    OrderBookMessageType,
)

_dob_logger = None

cdef class classNewConnectorOrderBook(OrderBook):

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
                                       metadata: Optional[Dict] = None) -> classNewConnectorOrderBookMessage:
        if metadata:
            msg.update(metadata)
        return classNewConnectorOrderBookMessage(OrderBookMessageType.SNAPSHOT, msg, timestamp)

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        return classNewConnectorOrderBookMessage(OrderBookMessageType.DIFF, msg, timestamp)

    @classmethod
    def trade_message_from_exchange(cls, msg: Dict[str, any], metadata: Optional[Dict] = None):
        ts = metadata["ts"]
        return OrderBookMessage(OrderBookMessageType.TRADE, {
            "trading_pair": metadata["topic"]["market"],
            "trade_type": float(TradeType.SELL.value) if (msg[2] == "SELL") else float(TradeType.BUY.value),
            "trade_id": msg[1],
            "update_id": ts,
            "price": msg[4],
            "amount": msg[3]
        }, timestamp=ts * 1e-3)

    @classmethod
    def snapshot_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None) -> OrderBookMessage:
        msg = record.json if type(record.json)==dict else ujson.loads(record.json)
        return classNewConnectorOrderBookMessage(OrderBookMessageType.SNAPSHOT, msg, timestamp=record.timestamp * 1e-3)

    @classmethod
    def diff_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None) -> OrderBookMessage:
        return classNewConnectorOrderBookMessage(OrderBookMessageType.DIFF, record.json)

    @classmethod
    def snapshot_message_from_kafka(cls, record: ConsumerRecord, metadata: Optional[Dict] = None) -> OrderBookMessage:
        msg = ujson.loads(record.value.decode())
        return classNewConnectorOrderBookMessage(OrderBookMessageType.SNAPSHOT, msg, timestamp=record.timestamp * 1e-3)

    @classmethod
    def diff_message_from_kafka(cls, record: ConsumerRecord, metadata: Optional[Dict] = None) -> OrderBookMessage:
        msg = ujson.loads(record.value.decode())
        return classNewConnectorOrderBookMessage(OrderBookMessageType.DIFF, msg)

    @classmethod
    def trade_receive_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None):
        return classNewConnectorOrderBookMessage(OrderBookMessageType.TRADE, record.json)

    @classmethod
    def from_snapshot(cls, snapshot: OrderBookMessage):
        raise NotImplementedError("new connector order book needs to retain individual order data.")

    @classmethod
    def restore_from_snapshot_and_diffs(self, snapshot: OrderBookMessage, diffs: List[OrderBookMessage]):
        raise NotImplementedError("new connector order book needs to retain individual order data.")
