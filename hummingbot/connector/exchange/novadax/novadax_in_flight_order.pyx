from decimal import Decimal
from typing import (
    Any,
    Dict,
    Optional,
    List
)


from hummingbot.core.event.events import (OrderFilledEvent, TradeType, OrderType, TradeFee, MarketEvent)
from hummingbot.connector.exchange.novadax.novadax_exchange import NovadaxExchange
from hummingbot.connector.exchange.novadax.novadax_order_status import NovadaxOrderStatus
from hummingbot.connector.in_flight_order_base import InFlightOrderBase


cdef class NovadaxInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: str,
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 initial_state: str = NovadaxOrderStatus.pending.name,
                 executed_amount_base: Decimal = Decimal(0),
                 executed_amount_quote: Decimal = Decimal(0),
                 fee_paid: Decimal = Decimal(0)):
        super().__init__(
            client_order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
            initial_state
        )
        
        (base, quote) = NovadaxExchange.split_trading_pair(trading_pair)
        self.fee_asset = base if trade_type is TradeType.BUY else quote
        self.trade_id_set = set()
        self.status = NovadaxOrderStatus[initial_state]
        self.executed_amount_base = executed_amount_base
        self.executed_amount_quote = executed_amount_quote
        self.fee_paid = fee_paid

    @property
    def is_done(self) -> bool:
        return self.status >= NovadaxOrderStatus.done

    @property
    def is_failure(self) -> bool:
        return self.status >= NovadaxOrderStatus.failed

    @property
    def is_cancelled(self) -> bool:
        return self.status is NovadaxOrderStatus.CANCELED

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> InFlightOrderBase:
        return NovadaxInFlightOrder(
            client_order_id=data["client_order_id"],
            exchange_order_id=data["exchange_order_id"],
            trading_pair=data["trading_pair"],
            order_type=getattr(OrderType, data["order_type"]),
            trade_type=getattr(TradeType, data["trade_type"]),
            price=Decimal(data["price"]),
            amount=Decimal(data["amount"]),
            initial_state=data["last_state"],
            executed_amount_base=Decimal(data["executed_amount_base"]),
            executed_amount_quote=Decimal(data["executed_amount_quote"]),
            fee_paid=Decimal(data["fee_paid"])
        )

    def update(self, data: Dict[str, Any]) -> List[Any]:
        events: List[Any] = []

        new_status: NovadaxOrderStatus = NovadaxOrderStatus[data["status"]]
        new_executed_amount_base: Decimal = Decimal(data["filledAmount"])
        new_executed_amount_quote: Decimal = Decimal(data["filledValue"])
        new_fee_paid: Decimal = Decimal(data["filledFee"])

        if new_executed_amount_base > self.executed_amount_base or new_executed_amount_quote > self.executed_amount_quote:
            diff_base: Decimal = new_executed_amount_base - self.executed_amount_base
            diff_quote: Decimal = new_executed_amount_quote - self.executed_amount_quote
            diff_fee: Decimal = new_fee_paid - self.fee_paid
            if diff_quote > Decimal(0):
                price: Decimal = diff_quote / diff_base
            else:
                price: Decimal = self.executed_amount_quote / self.executed_amount_base

            events.append((MarketEvent.OrderFilled, diff_base, price, diff_fee))

        if not self.is_done and new_status is NovadaxOrderStatus.CANCELED:
            events.append((MarketEvent.OrderCancelled, None, None, None))

        if not self.is_done and new_status is NovadaxOrderStatus.REJECTED:
            events.append( (MarketEvent.OrderFailure, None, None, None) )

        self.status = new_status
        self.last_state = str(new_status)
        self.executed_amount_base = new_executed_amount_base
        self.executed_amount_quote = new_executed_amount_quote
        self.fee_paid = new_fee_paid

        if self.exchange_order_id is None:
            self.update_exchange_order_id(data.get('id', None))

        return events
