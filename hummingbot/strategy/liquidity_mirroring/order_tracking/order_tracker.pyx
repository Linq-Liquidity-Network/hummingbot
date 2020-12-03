import asyncio
from decimal import Decimal

from hummingbot.core.event.events import TradeType

from hummingbot.strategy.liquidity_mirroring.order_tracking.exposure_tuple import ExposureTuple
from hummingbot.strategy.liquidity_mirroring.order_tracking.offsetting_amounts_tuple import OffsettingAmountsTuple
from hummingbot.strategy.liquidity_mirroring.order_tracking.order import Order
from hummingbot.strategy.liquidity_mirroring.order_tracking.order_state import OrderState


class OrderTracker:
    def __init__(self):
        self._orders = {}
        self._bids = {}
        self._asks = {}
        self._lock = asyncio.Lock()

    async def __aenter__(self):
        await self._lock.acquire()

    async def __aexit__(self, exc_type, exc, tb):
        self._lock.release()

    def add_order(self, order: Order):
        if order.id in self._orders:
            raise ValueError(f"Order {order.id} already tracked")
        self._orders[order.id] = order
        if order.side is TradeType.BUY:
            self._bids[order.id] = order
        else:
            self._asks[order.id] = order

    def remove_order(self, id: str):
        try:
            order = self._orders.pop(id) 
        except KeyError:
            return None

        try:
            if order.side is TradeType.BUY:
                del self._bids[id]
            else:
                del self._asks[id]
        except KeyError:
            pass # already not in this dict, so don't worry about it

        return order

    def get_bids(self):
        return list(self._bids.values())

    def get_asks(self):
        return list(self._asks.values())
    
    def cancel(self, id: str):
        order = self.remove_order(id)
        if order is None:
            return
        order.state = OrderState.CANCELED

    def fail(self, id: str):
        order = self.remove_order(id)
        if order is None:
            return
        order.state = OrderState.FAILED

    def complete(self, id: str):
        order = self.remove_order(id)
        if order is None:
            return
        order.state = OrderState.COMPLETE
        order.amount_remaining = Decimal(0)

    def update_order(self, id: str, new_state: OrderState, amount_filled: Decimal):
        order = self.get_order(id)
        if order is None:
            return
        order.state = new_state
        order.amount_remaining -= amount_filled

    def fill(self, id: str, amount_filled: Decimal):
        order = self.get_order(id)
        if order is None:
            return
        order.amount_remaining -= amount_filled

    def get_order(self, id: str):
        return self._orders.get(id, None)

    def _get_exposures(self, check_fn):
        exposures = ExposureTuple()
        for order in self._orders.values():
            if check_fn(order):
                if order.side is TradeType.BUY:
                    exposures.quote += order.amount_remaining * order.price
                else:
                    exposures.base += order.amount_remaining

        return exposures

    def get_total_exposures(self):
        return self._get_exposures(lambda o: o.state < OrderState.COMPLETE)
        
    def get_active_exposures(self):
        return self._get_exposures(lambda o: o.state is OrderState.ACTIVE or o.state is OrderState.PENDING_CANCEL)

    def get_pending_exposures(self):
        return self._get_exposures(lambda o: o.state is OrderState.PENDING or o.state is OrderState.UNSENT)

    def get_total_amounts(self):
        amounts = OffsettingAmountsTuple()
        for order in self._orders.values():
            if order.side is TradeType.BUY:
                amounts.buys += order.amount_remaining
            else:
                amounts.sells += order.amount_remaining

        return amounts
