from decimal import Decimal
from functools import reduce
from typing import List


class Position:
    def __init__(self, price: Decimal, amount: Decimal):
        self.price: Decimal = price
        self.amount: Decimal = amount

    def __add__(self, other):
        if not isinstance(other, Position):
            raise RuntimeError(f"Position._add__ does not support addition with type {type(other).__name__}")

        total_base: Decimal = self.amount + other.amount
        if total_base.is_zero():
            return Position(Decimal(0), Decimal(0))

        total_quote: Decimal = self.amount * self.price + other.amount * other.price
        return Position(total_quote / total_base, total_base)

    def __str__(self):
        return f"({self.price}, {self.amount})"


zero_pos = Position(Decimal(0), Decimal(0))


class PositionManager:
    def __init__(self):
        self.total_loss = Decimal(0)
        self._trades: List[Position] = []
        self._offsets: List[Position] = []

        self._trade_cache_invalid: bool = True
        self._trades_sum: Position = None

        self._offset_cache_invalid: bool = True
        self._offsets_sum: Position = None

    @property
    def avg_price(self) -> Decimal:
        if not self._trade_cache_invalid:
            return self._trades_sum.price
        else:
            return self._sum_trades().price

    @property
    def amount_to_offset(self) -> Decimal:
        if self._trade_cache_invalid:
            self._sum_trades()
        if self._offset_cache_invalid:
            self._sum_offsets()

        return self._trades_sum.amount + self._offsets_sum.amount

    def register_trade(self, price: Decimal, amount: Decimal):
        current_amount: Decimal = self.amount_to_offset
        if current_amount < 0 and amount > 0 or current_amount > 0 and amount < 0:
            # this is an offsetting trade
            self._register_offset(price, amount, current_amount)
        else:
            self._trades.append(Position(price, amount))

        self._trade_cache_invalid = True

    def _register_offset(self, price: Decimal, amount: Decimal, current_amount: Decimal):
        if current_amount.copy_abs() <= amount.copy_abs():
            # this will swap or eliminate our position
            new_amount: Decimal = current_amount + amount
            self._offsets.append(Position(price, -current_amount))
            self._clean()
            if abs(new_amount) > 0:
                self._trades.append(Position(price, new_amount))
        else:
            self._offsets.append(Position(price, amount))
            self._offset_cache_invalid = True

    def _clean(self):
        total_in = self._sum_trades()
        total_out = self._sum_offsets()
        diff = (total_out.price * total_out.amount + total_in.price * total_in.amount)
        self.total_loss += diff

        self._trades.clear()
        self._offsets.clear()
        self._offset_cache_invalid = True
        self._trade_cache_invalid = True

    def _sum_trades(self):
        self._trades_sum = reduce(lambda x, y: x + y, self._trades, zero_pos)
        self._trade_cache_invalid = False
        return self._trades_sum

    def _sum_offsets(self):
        self._offsets_sum = reduce(lambda x, y: x + y, self._offsets, zero_pos)
        self._offset_cache_invalid = False
        return self._offsets_sum
