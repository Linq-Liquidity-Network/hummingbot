from decimal import Decimal
from enum import Enum
from typing import Dict, List, Iterator, Tuple
from hummingbot.core.data_type.order_book_row import ClientOrderBookRow
from hummingbot.core.event.events import TradeType
from hummingbot.strategy.liquidity_mirroring.order_tracking.order_tracker import OrderTracker
from hummingbot.strategy.liquidity_mirroring.order_tracking.order import Order


class ModelOrder:
    def __init__(self, price: Decimal, amount: Decimal, side: TradeType):
        self.price: Decimal = price
        self.amount: Decimal = amount
        self.side: TradeType = side

    def __repr__(self):
        return f"{self.side.name}: {self.amount}@{self.price}"

cdef class ModelBook:
    def __init__(self, bids: List[ClientOrderBookRow] = [], asks: List[ClientOrderBookRow] = [], min_amount: Decimal = Decimal(0)):
        self.bids = self.get_book_levels(bids, TradeType.BUY)
        self.asks = self.get_book_levels(asks, TradeType.SELL)
        self._original_best_bid = self.bids[0].price
        self._original_best_ask = self.asks[0].price
        self._min_amount = min_amount

    def __repr__(self):
        return f"bids: {self.bids}\nasks: {self.asks}"

    @property
    def best_bid(self) -> Decimal:
        return self._original_best_bid

    @property
    def best_ask(self) -> Decimal:
        return self._original_best_ask

    def aggregate(self, price_precision: Decimal):
        self.bids = self._aggregate_book(self.bids, price_precision, TradeType.BUY)
        self.asks = self._aggregate_book(self.asks, price_precision, TradeType.SELL)

    def apply_limits(self):
        self.bids = [o for o in self.bids if o.amount >= self._min_amount]
        self.asks = [o for o in self.asks if o.amount >= self._min_amount]

    def crop(self, bid_levels: int, ask_levels: int):
        self.bids = self.bids[:bid_levels]
        self.asks = self.asks[:ask_levels]

    def cut_claimed_amounts(self, offsets: OrderTracker):
        offsettings_amounts = offsets.get_total_amounts()
        self.bids = self._cut_book_amount(self.bids, offsettings_amounts.sells)
        self.asks = self._cut_book_amount(self.asks, offsettings_amounts.buys)

    def limit_by_ratios(self, bids_ratios: List[Decimal], asks_ratios: List[Decimal]):
        self.bids = self._limit_book(self.bids, bids_ratios, TradeType.BUY)
        self.asks = self._limit_book(self.asks, asks_ratios, TradeType.SELL)

    def markup(self, bids_markup: Decimal = Decimal(0), asks_markup: Decimal = Decimal(0), total_fee_rate: Decimal = Decimal(0)):
        self._markup_book(self.bids, -(bids_markup + total_fee_rate))
        self._markup_book(self.asks, asks_markup + total_fee_rate)
        
    def scale_amounts(self, bids_scale: Decimal = Decimal(1), asks_scale: Decimal = Decimal(1)):
        self._scale_book(self.bids, bids_scale)
        self._scale_book(self.asks, asks_scale)

    def _aggregate_book(self, book: List[ModelOrder], precision: Decimal, side: TradeType):
        aggregate_book = {}
        for order in book:
            quant_price = (order.price // precision) * precision
            level = aggregate_book.get(quant_price, ModelOrder(quant_price, 0, side))
            level.amount += order.amount
            aggregate_book[quant_price] = level
        return list(aggregate_book.values())

    def _cut_book_amount(self, book: List[ModelOrder], amount: Decimal) -> List[ModelOrder]:
        i: int = 0
        reserve: Decimal = Decimal(0)
        while amount > 0 and i < len(book):
            if amount >= book[i].amount:
                amount -= book[i].amount
                i += 1
            else:
                reserve = amount
                amount = Decimal(0)
        result = book[i:]
        if reserve > 0:
            result[0].amount = result[0].amount - reserve

        return result

    def _scale_book(self, book, scale):
        for order in book:
            order.amount *= scale

    def _markup_book(self, book, markup):
        for order in book:
            order.price *= 1 + markup

    def _limit_book(self, book, ratios: List[Decimal], side: TradeType):
        new_desired_book = []
        leftover_amount = Decimal(0)
        for order, max_amount in zip(book, ratios):
            max_amount += leftover_amount
            if side is TradeType.BUY:
                limited_amount = max_amount / order.price
                desired_order = ModelOrder(order.price, min(order.amount, limited_amount), side)
                leftover_amount = max_amount - desired_order.amount * desired_order.price # guaranteed max_amount >= desired_order.amount due to above line of code
            else:
                desired_order = ModelOrder(order.price, min(order.amount, max_amount), side)
                leftover_amount = max_amount - desired_order.amount # guaranteed max_amount >= desired_order.amount due to above line of code
            new_desired_book.append(desired_order)
        return new_desired_book

    cdef get_book_levels(self, list book, object side):
        levels: List[ModelOrder] = []
        current_price: Optional[Decimal] = None
        current_level_index: int = 0
        for entry in book:
            if current_price == entry.price:
                levels[current_level_index].amount += entry.amount
            else:
                levels.append(ModelOrder(entry.price, entry.amount, side))
                current_price = entry.price
        return levels

    @classmethod
    def _get_void_orders(cls, current_book: List[Order], desired_book: List[ModelOrder]) -> List[Order]:
        void_orders: List[Order] = []
        desired_book_lookup = {order.price: order for order in desired_book}
        for order in current_book:
            if (order.price not in desired_book_lookup or order.amount_remaining > desired_book_lookup[order.price].amount):
                void_orders.append(order)

        return void_orders

    @classmethod
    def _get_new_orders(cls, current_book: List[Order], desired_book: List[ModelOrder]) -> Tuple[List[Order], List[Order]]:
        void_orders: List[Order] = []
        new_orders: List[Order] = []
        current_book_lookup = {order.price: order for order in current_book}
        for order in desired_book:
            current_book_order = current_book_lookup.get(order.price, None)
            if current_book_order is None:
                new_orders.append(Order(None, order.price, order.amount, order.side))
            elif current_book_order.amount_remaining < order.amount:
                void_orders.append(current_book_order)
                # note that we don't add this order to the new_orders, as we must ensure that this void order is clear from
                # the books before we replace this new order or else, we may overcommit. This desire for the new order will 
                # be picked up on the next cycle after the void order has been cancelled successfuly 
        
        return void_orders, new_orders

    def steps_from(self, current_orders: OrderTracker) -> Tuple[List[Order], List[Order], List[Order]]:
        bids_to_place: List[Order] = []
        asks_to_place: List[Order] = []
        orders_to_cancel: List[Order] = []

        # get the orders that we have that the other doesn't have - ie. the 
        orders_to_cancel.extend(ModelBook._get_void_orders(current_orders.get_bids(), self.bids))
        orders_to_cancel.extend(ModelBook._get_void_orders(current_orders.get_asks(), self.asks))

        # Get new orders that we'd like to add to the book, and orders that we need to replace
        void_orders, new_orders = ModelBook._get_new_orders(current_orders.get_bids(), self.bids)
        orders_to_cancel.extend(void_orders)
        bids_to_place = sorted(new_orders, key=lambda o: o.price, reverse=True)

        void_orders, new_orders = ModelBook._get_new_orders(current_orders.get_asks(), self.asks)
        orders_to_cancel.extend(void_orders)
        asks_to_place = sorted(new_orders, key=lambda o: o.price, reverse=False)

        return bids_to_place, asks_to_place, orders_to_cancel



    
