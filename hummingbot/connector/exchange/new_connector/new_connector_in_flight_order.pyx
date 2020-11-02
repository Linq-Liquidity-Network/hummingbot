import copy
import json
import time
from typing import (Any, Dict, List, Tuple)
from decimal import Decimal
from hummingbot.connector.exchange.new_connector.new_connector_order_status import NewConnectorOrderStatus
from hummingbot.connector.in_flight_order_base cimport InFlightOrderBase
from hummingbot.connector.exchange.new_connector.new_connector_exchange cimport NewConnectorExchange
from hummingbot.core.event.events import (OrderFilledEvent, TradeType, OrderType, TradeFee, MarketEvent)

cdef class NewConnectorInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 market: NewConnectorExchange,
                 client_order_id: str,
                 exchange_order_id: str,
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 initial_state: NewConnectorOrderStatus,
                 filled_size: Decimal,
                 filled_volume: Decimal,
                 filled_fee: Decimal,
                 created_at: int):

        super().__init__(client_order_id=client_order_id,
                         exchange_order_id=exchange_order_id,
                         trading_pair=trading_pair,
                         order_type=order_type,
                         trade_type=trade_type,
                         price=price,
                         amount=amount,
                         initial_state = str(initial_state))
        self.market = market
        self.status = initial_state
        self.created_at = created_at
        self.executed_amount_base = filled_size
        self.executed_amount_quote = filled_volume
        self.fee_paid = filled_fee

        (base, quote) = self.market.split_trading_pair(trading_pair)
        self.fee_asset = base if trade_type is TradeType.BUY else quote

    @property
    def is_done(self) -> bool:
        return self.status >= NewConnectorOrderStatus.DONE

    @property
    def is_cancelled(self) -> bool:
        return self.status == NewConnectorOrderStatus.cancelled

    @property
    def is_failure(self) -> bool:
        return self.status >= NewConnectorOrderStatus.failed

    @property
    def is_expired(self) -> bool:
        return self.status == NewConnectorOrderStatus.expired

    @property
    def description(self):
        return f"{str(self.order_type).lower()} {str(self.trade_type).lower()}"

    def to_json(self):
        return json.dumps({
            "client_order_id": self.client_order_id,
            "exchange_order_id": self.exchange_order_id,
            "trading_pair": self.trading_pair,
            "order_type": self.order_type.name,
            "trade_type": self.trade_type.name,
            "price": str(self.price),
            "amount": str(self.amount),
            "status": self.status.name,
            "executed_amount_base": str(self.executed_amount_base),
            "executed_amount_quote": str(self.executed_amount_quote),
            "fee_paid": str(self.fee_paid),
            "created_at": self.created_at
        })

    @classmethod
    def from_json(cls, market, data: Dict[str, Any]) -> NewConnectorInFlightOrder:
        return NewConnectorInFlightOrder(
            market,
            data["client_order_id"],
            data["exchange_order_id"],
            data["trading_pair"],
            OrderType[data["order_type"]],
            TradeType[data["trade_type"]],
            Decimal(data["price"]),
            Decimal(data["amount"]),
            NewConnectorOrderStatus[data["status"]],
            Decimal(data["executed_amount_base"]),
            Decimal(data["executed_amount_quote"]),
            Decimal(data["fee_paid"]),
            data["created_at"]
        )

    @classmethod
    def from_new_connector_order(cls,
                            market: NewConnectorExchange,
                            side: TradeType,
                            client_order_id: str,
                            created_at: int,
                            hash: str,
                            trading_pair: str,
                            price: float,
                            amount: float) -> NewConnectorInFlightOrder:
        return NewConnectorInFlightOrder(
            market,
            client_order_id,
            hash,
            trading_pair,
            OrderType.LIMIT, # TODO: fix this to the actual type (ie. LIMIT_MAKER)
            side,
            Decimal(price),
            Decimal(amount),
            NewConnectorOrderStatus.waiting,
            Decimal(0),
            Decimal(0),
            Decimal(0),
            created_at
        )

    def update(self, data: Dict[str, Any]) -> List[Any]:
        events: List[Any] = []

        base: str
        quote: str
        trading_pair: str = data["market"]
        (base, quote) = self.market.split_trading_pair(trading_pair)
        base_id: int = self.market.token_configuration.get_tokenid(base)
        quote_id: int = self.market.token_configuration.get_tokenid(quote)
        fee_currency_id: int = self.market.token_configuration.get_tokenid(self.fee_asset)

        new_status: NewConnectorOrderStatus = NewConnectorOrderStatus[data["status"]]
        new_executed_amount_base: Decimal = self.market.token_configuration.unpad(data["filledSize"], base_id)
        new_executed_amount_quote: Decimal = self.market.token_configuration.unpad(data["filledVolume"], quote_id)
        new_fee_paid: Decimal = self.market.token_configuration.unpad(data["filledFee"], fee_currency_id)

        if new_executed_amount_base > self.executed_amount_base or new_executed_amount_quote > self.executed_amount_quote:
            diff_base: Decimal = new_executed_amount_base - self.executed_amount_base
            diff_quote: Decimal = new_executed_amount_quote - self.executed_amount_quote
            diff_fee: Decimal = new_fee_paid - self.fee_paid
            if diff_quote > Decimal(0):
                price: Decimal = diff_quote / diff_base
            else:
                price: Decimal = self.executed_amount_quote / self.executed_amount_base

            events.append((MarketEvent.OrderFilled, diff_base, price, diff_fee))

        if not self.is_done and new_status == NewConnectorOrderStatus.cancelled:
            events.append((MarketEvent.OrderCancelled, None, None, None))

        if not self.is_done and new_status == NewConnectorOrderStatus.expired:
            events.append((MarketEvent.OrderExpired, None, None, None))

        if not self.is_done and new_status == NewConnectorOrderStatus.failed:
            events.append( (MarketEvent.OrderFailure, None, None, None) )

        self.status = new_status
        self.last_state = str(new_status)
        self.executed_amount_base = new_executed_amount_base
        self.executed_amount_quote = new_executed_amount_quote
        self.fee_paid = new_fee_paid

        if self.exchange_order_id is None:
            self.update_exchange_order_id(data.get('hash', None))

        return events
