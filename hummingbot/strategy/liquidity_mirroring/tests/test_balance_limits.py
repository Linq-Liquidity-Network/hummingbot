import unittest
from decimal import Decimal

from hummingbot.core.event.events import TradeType
from hummingbot.strategy.liquidity_mirroring.balances import get_available_balances
from hummingbot.strategy.liquidity_mirroring.order_tracking.order_tracker import OrderTracker
from hummingbot.strategy.liquidity_mirroring.order_tracking.order import Order
from hummingbot.strategy.liquidity_mirroring.order_tracking.order_state import OrderState
from hummingbot.strategy.liquidity_mirroring.position import PositionManager


class TestBalanceLimits(unittest.TestCase):

    def setUp(self):
        self.pm = PositionManager()
        self.offset_order_tracker = OrderTracker()
        self.best_bid = Decimal("1")
        self.best_ask = Decimal("1.1")
        self.max_loss = Decimal("0.01")

    def test_basic_excessive_mirrored(self):
        """ Test with NO position, NO offsetting orders, and EXCESSIVE mirrored exchange balances """
        base, quote = get_available_balances(self.pm, self.offset_order_tracker, self.best_bid, self.best_ask, self.max_loss,
                                             available_primary_base = Decimal(100),
                                             available_primary_quote = Decimal(300),
                                             available_mirrored_base = Decimal(100000),
                                             available_mirrored_quote = Decimal(300000))
        self.assertEqual(Decimal(100), base)
        self.assertEqual(Decimal(300), quote)

    def test_basic_limited_mirrored(self):
        """ Test with NO position, NO offsetting orders, and LIMITED offsetting exchange balances """
        base, quote = get_available_balances(self.pm, self.offset_order_tracker, self.best_bid, self.best_ask, self.max_loss,
                                             available_primary_base = Decimal(100),
                                             available_primary_quote = Decimal(300),
                                             available_mirrored_base = Decimal(30),
                                             available_mirrored_quote = Decimal(10))
        self.assertEqual(Decimal(10) / Decimal("1.111"), base)
        self.assertEqual(Decimal(30) * Decimal("0.99"), quote)

    def test_long_excessive_mirrored(self):
        """ Test with a LONG position, NO offsetting orders, and EXCESSIVE mirrored exchange balances """
        self.pm.register_trade(Decimal(1), Decimal(15))
        base, quote = get_available_balances(self.pm, self.offset_order_tracker, self.best_bid, self.best_ask, self.max_loss,
                                             available_primary_base = Decimal(100),
                                             available_primary_quote = Decimal(300),
                                             available_mirrored_base = Decimal(100000),
                                             available_mirrored_quote = Decimal(300000))
        self.assertEqual(Decimal(100), base)
        self.assertEqual(Decimal(300), quote)

    def test_long_limited_mirrored(self):
        """ Test with a LONG position, NO offsetting orders, and LIMITED offsetting exchange balances """
        self.pm.register_trade(Decimal(1), Decimal(15))
        base, quote = get_available_balances(self.pm, self.offset_order_tracker, self.best_bid, self.best_ask, self.max_loss,
                                             available_primary_base = Decimal(100),
                                             available_primary_quote = Decimal(300),
                                             available_mirrored_base = Decimal(30),
                                             available_mirrored_quote = Decimal(10))
        self.assertEqual(Decimal(10) / Decimal("1.111"), base)
        self.assertEqual(Decimal(30 - 15) * Decimal("0.99"), quote)

    def test_short_excessive_mirrored(self):
        """ Test with a SHORT position, NO offsetting orders, and EXCESSIVE mirrored exchange balances """
        self.pm.register_trade(Decimal(1), -Decimal(15))
        base, quote = get_available_balances(self.pm, self.offset_order_tracker, self.best_bid, self.best_ask, self.max_loss,
                                             available_primary_base = Decimal(100),
                                             available_primary_quote = Decimal(300),
                                             available_mirrored_base = Decimal(100000),
                                             available_mirrored_quote = Decimal(300000))
        self.assertEqual(Decimal(100), base)
        self.assertEqual(Decimal(300), quote)

    def test_short_limited_mirrored(self):
        """ Test with a SHORT position, NO offsetting orders, and LIMITED offsetting exchange balances """
        self.pm.register_trade(Decimal(1), -Decimal(5))
        base, quote = get_available_balances(self.pm, self.offset_order_tracker, self.best_bid, self.best_ask, self.max_loss,
                                             available_primary_base = Decimal(100),
                                             available_primary_quote = Decimal(300),
                                             available_mirrored_base = Decimal(30),
                                             available_mirrored_quote = Decimal(10))
        self.assertEqual(Decimal(10 - 5 * Decimal("1.01")) / Decimal("1.111"), base)
        self.assertEqual(Decimal(30) * Decimal("0.99"), quote)

    def test_long_limited_mirrored_with_pending_offsets(self):
        """ Test with a LONG position, A PENDING SELL offsetting order, and LIMITED offsetting exchange balances """
        self.pm.register_trade(Decimal(1), Decimal(5))

        offsetting_order = Order("", Decimal(1), Decimal(4), TradeType.SELL, OrderState.ACTIVE)
        self.offset_order_tracker.add_order(offsetting_order)

        base, quote = get_available_balances(self.pm, self.offset_order_tracker, self.best_bid, self.best_ask, self.max_loss,
                                             available_primary_base = Decimal(100),
                                             available_primary_quote = Decimal(300),
                                             available_mirrored_base = Decimal(4),
                                             available_mirrored_quote = Decimal(10))
        self.assertEqual(Decimal(10) / Decimal("1.111"), base)
        self.assertEqual(Decimal(3) * Decimal("0.99"), quote)

    def test_short_limited_mirrored_with_pending_offsets(self):
        """ Test with a SHORT position, A PENDING BUY offsetting order, and LIMITED offsetting exchange balances """
        self.pm.register_trade(Decimal(1), -Decimal(5))

        offsetting_order = Order("", Decimal(1), Decimal(4), TradeType.BUY, OrderState.ACTIVE)
        self.offset_order_tracker.add_order(offsetting_order)

        base, quote = get_available_balances(self.pm, self.offset_order_tracker, self.best_bid, self.best_ask, self.max_loss,
                                             available_primary_base = Decimal(100),
                                             available_primary_quote = Decimal(300),
                                             available_mirrored_base = Decimal(4),
                                             available_mirrored_quote = Decimal(10))
        self.assertEqual(Decimal("8.99") / Decimal("1.111"), base)
        self.assertEqual(Decimal(4) * Decimal("0.99"), quote)

    def test_basic_excessive_mirrored_with_pending_offsets(self):
        """ Test with NO position, A PENDING BUY offsetting order, and EXCESSIVE mirrored exchange balances """
        offsetting_order = Order("", Decimal(1), Decimal(4), TradeType.BUY, OrderState.ACTIVE)
        self.offset_order_tracker.add_order(offsetting_order)

        base, quote = get_available_balances(self.pm, self.offset_order_tracker, self.best_bid, self.best_ask, self.max_loss,
                                             available_primary_base = Decimal(100),
                                             available_primary_quote = Decimal(300),
                                             available_mirrored_base = Decimal(100000),
                                             available_mirrored_quote = Decimal(300000))
        self.assertEqual(Decimal(100), base)
        self.assertEqual(Decimal(300), quote)

    def test_basic_limited_mirrored_with_pending_offsets(self):
        """ Test with NO position, A PENDING SELL offsetting order, and LIMITED offsetting exchange balances """
        offsetting_order = Order("", Decimal(1), Decimal(4), TradeType.SELL, OrderState.ACTIVE)
        self.offset_order_tracker.add_order(offsetting_order)

        base, quote = get_available_balances(self.pm, self.offset_order_tracker, self.best_bid, self.best_ask, self.max_loss,
                                             available_primary_base = Decimal(100),
                                             available_primary_quote = Decimal(300),
                                             available_mirrored_base = Decimal(30),
                                             available_mirrored_quote = Decimal(10))
        self.assertEqual(Decimal(10) / Decimal("1.111"), base)
        self.assertEqual(Decimal(30) * Decimal("0.99"), quote)


if __name__ == '__main__':
    unittest.main()
