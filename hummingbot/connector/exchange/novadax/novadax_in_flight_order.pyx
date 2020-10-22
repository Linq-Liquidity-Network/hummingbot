from decimal import Decimal
from typing import (
    Any,
    Dict,
    Optional
)

from hummingbot.core.event.events import (
    OrderType,
    TradeType
)
from hummingbot.connector.exchange.novadax.novadax_exchange import NovadaxExchange
from hummingbot.connector.in_flight_order_base import InFlightOrderBase

s_decimal_0 = Decimal(0)


cdef class NovadaxInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: str,
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 initial_state: str = "NEW"):
        super().__init__(
            NovadaxExchange,
            client_order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
            initial_state
        )
        self.trade_id_set = set()

    @property
    def is_done(self) -> bool:
        return self.last_state in {"FILLED", "CANCELED", "CANCELING", "REJECTED"}

    @property
    def is_failure(self) -> bool:
        return self.last_state in {"CANCELED", "CANCELING", "REJECTED"}

    @property
    def is_cancelled(self) -> bool:
        return self.last_state in {"CANCELED"}

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> InFlightOrderBase:
        cdef:
            NovadaxInFlightOrder retval = NovadaxInFlightOrder(
                client_order_id=data["client_order_id"],
                exchange_order_id=data["exchange_order_id"],
                trading_pair=data["trading_pair"],
                order_type=getattr(OrderType, data["order_type"]),
                trade_type=getattr(TradeType, data["trade_type"]),
                price=Decimal(data["price"]),
                amount=Decimal(data["amount"]),
                initial_state=data["last_state"]
            )
        retval.executed_amount_base = Decimal(data["executed_amount_base"])
        retval.executed_amount_quote = Decimal(data["executed_amount_quote"])
        retval.fee_asset = data["fee_asset"]
        retval.fee_paid = Decimal(data["fee_paid"])
        return retval

    def update_with_execution_report(self, execution_report: Dict[str, Any]):
        trade_id = execution_report["id"]
        if trade_id in self.trade_id_set:
            # trade already recorded
            return
        self.trade_id_set.add(trade_id)
        self.executed_amount_base = Decimal(execution_report["filledAmount"])
        self.executed_amount_quote = Decimal(execution_report["filledValue"])
        self.fee_paid = Decimal(execution_report["filledFee"])
        self.last_state = execution_report["status"]
