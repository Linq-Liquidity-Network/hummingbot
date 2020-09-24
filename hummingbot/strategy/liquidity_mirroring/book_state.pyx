from decimal import Decimal
from enum import Enum
from typing import Dict, List, Iterator
from hummingbot.core.data_type.order_book_row import ClientOrderBookRow

class OrderState(Enum):
    UNSENT = 1
    PENDING = 2
    ACTIVE = 3
    CANCEL_REQUESTED = 4
    COMPLETED = 5

# TODO: add side to the order object
class Order:
    def __init__(self, price : Decimal, amount : Decimal, order_id : str):
        self.price : Decimal = price
        self.amount : Decimal = amount
        self.order_id : str = order_id
        self.status : OrderState = OrderState.UNSENT

    def mark_sent(self):
        self.status = OrderState.PENDING

    def mark_active(self):
        self.status = OrderState.ACTIVE

    def mark_canceled(self):
        self.status = OrderState.CANCEL_REQUESTED

    def mark_complete(self):
        self.status = OrderState.COMPLETED

    def fill(self, amount):
        self.amount -= amount

class BookState:
    def __init__(self, bids : List[ClientOrderBookRow] = [], asks : List[ClientOrderBookRow] = []):
        self.bids = self.get_book_levels(bids)
        self.asks = self.get_book_levels(asks)

    def aggregate(self, price_precision : Double):
        self.bids = self._aggregate_book(self.bids, price_precision)
        self.asks = self._aggregate_book(self.asks, price_precision)

    def crop(self, bid_levels : int, ask_levels : int):
        self.bids = self.bids[:bid_levels]
        self.asks = self.asks[:ask_levels]

    def limit_by_ratios(self, bids_ratios : List[float], asks_ratios : List[float]):
        self.bids = self._limit_book(self.bids, bids_ratios)
        self.asks = self._limit_book(self.asks, asks_ratios)

    def markup(self, bids_markup : Decimal = Decimal(0), asks_markup : Decimal = Decimal(0)):
        self._markup_book(self.bids, -bids_markup)
        self._markup_book(self.asks, asks_markup)
        
    def scale_amounts(self, bids_scale : Decimal = Decimal(1), asks_scale : Decimal = Decimal(1)):
        self._scale_book(bids, bids_scale)
        self._scale_book(asks, asks_scale)
    
    def _aggregate_book(self, book : List[Order], precision : Decimal, sort_direction : bool):
        aggregate_book = {}
        for order in book:
            quant_price = (order.price // precision) * precision
            level = aggregate_book.get(quant_price, Order(quant_price, 0, None))
            level.amount += order.amount
            aggregate_book[quant_price] = level
        return list(aggregate_book.values).

    def _scale_book(self, book, scale):
        for order in book:
            order.amount *= scale

    def _markup_book(self, book, markup):
        for order in book:
            order.price *= 1 + markup

    def _limit_book(self, book, ratios : List[float]):
        new_desired_book = []
        leftover_amount = Decimal(0)
        for order, max_amount in zip(book, self.ratios):
            max_amount += leftover_amount
            desired_order = Order(order.price, min(order.amount, max_amount))
            leftover_amount = max_amount - desired_order.amount # guaranteed max_amount >= desired_order.amount due to above line of code
            new_desired_book.orders.append(desired_order)
        return new_desired_book

    cdef get_book_levels(self, book : List[ClientOrderBookRow]):
        levels : List[Order] = []
        current_price : Optional[Decimal] = None
        current_level_index : int = 0
        for entry in book:
            if current_price == entry.price:
                levels[current_level_index].amount += entry.amount
            else:
                levels.append(Order(entry.price, entry.amount, None))
                current_price = entry.price
        return levels

    def steps_to(self, other):
        to_place = []
        to_cancel = []

        for order in self.orders:
            other_order = other.orders.get(order.price)
            if other_order is None and order.state == OrderState.ACTIVE or other_order.amount != order.amount:
                to_cancel.append(order)
        for order in other.orders:
            this_order = self.orders.get(order.price)
            if this_order is None and order.state == OrderState.ACTIVE:
                to_place.append(order)

        return to_place, to_cancel


    
