from decimal import Decimal


class ExposureTuple:
    def __init__(self, base_amount: Decimal = Decimal(0), quote_amount: Decimal = Decimal(0)):
        self.base = base_amount
        self.quote = quote_amount

    def __repr__(self):
        return f"base[sells]={self.base}, quote[buys]={self.quote}"

    @property
    def buys(self):
        return self.quote

    @buys.setter
    def buys(self, amount: Decimal):
        self.quote += amount

    @property
    def sells(self):
        return self.base

    @sells.setter
    def sells(self, amount: Decimal):
        self.base += amount
