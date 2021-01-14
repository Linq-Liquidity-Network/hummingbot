from decimal import Decimal

from hummingbot.core.event.events import TradeType
from hummingbot.strategy.liquidity_mirroring.order_tracking.order_state import OrderState


class Order:
    def __init__(self, id: str, price: Decimal, amount: Decimal, side: TradeType, state: OrderState = OrderState.UNSENT):
        self.id = id
        self.amount_remaining = amount
        self.state = state
        self._price = price
        self._side = side

    @property
    def side(self):
        return self._side

    @property
    def price(self) -> Decimal:
        return self._price

    @property
    def total(self) -> Decimal:
        return self._price * self.amount_remaining

    def is_live_uncancelled(self) -> bool:
        return self.state in [OrderState.ACTIVE, OrderState.PENDING]

    def mark_canceled(self):
        if self.state in [OrderState.ACTIVE, OrderState.PENDING, OrderState.UNSENT]:
            self.state = OrderState.PENDING_CANCEL

    def __repr__(self):
        return f"(id={self.id}, price={self.price}, amount_remaining={self.amount_remaining}, state={self.state}, side={self.side})"
