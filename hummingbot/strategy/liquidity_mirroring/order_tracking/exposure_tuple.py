from decimal import Decimal


class ExposureTuple:
    def __init__(self, base_amount: Decimal = Decimal(0), quote_amount: Decimal = Decimal(0)):
        self.base = base_amount
        self.quote = quote_amount

    def __repr__(self):
        return f"base={self.base}, quote={self.quote}"
