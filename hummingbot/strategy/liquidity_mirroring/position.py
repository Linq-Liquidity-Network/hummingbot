from decimal import Decimal


def _has_different_sign(a: Decimal, b: Decimal):
    return a * b < 0


class PositionManager:
    def __init__(self):
        self._total_loss = Decimal(0)
        self._position = Decimal(0)
        self._price = Decimal(0)

    @property
    def avg_price(self) -> Decimal:
        return self._price

    @property
    def amount_to_offset(self) -> Decimal:
        return self._position

    @property
    def total_loss(self) -> Decimal:
        return self._total_loss

    def _update_loss(self, price: Decimal, amount: Decimal):
        if self._position < 0:
            self._total_loss += (price - self._price) * abs(amount)
        else:
            self._total_loss += (self._price - price) * abs(amount)

    def register_trade(self, price: Decimal, amount: Decimal):
        if self._position.is_zero():
            self._position = amount
            self._price = price
        else:
            new_position = self._position + amount
            if new_position.is_zero():
                self._update_loss(price, amount)
                self._price = Decimal(0)
            elif _has_different_sign(new_position, self._position):
                self._update_loss(price, self._position)
                self._price = price
            elif not _has_different_sign(amount, self._position):
                self._price = (price * amount + self._price * self._position) / new_position
            else:
                self._update_loss(price, amount)

            self._position = new_position
