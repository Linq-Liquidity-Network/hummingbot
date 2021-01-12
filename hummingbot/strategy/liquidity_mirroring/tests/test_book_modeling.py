import unittest
from decimal import Decimal
from typing import List

from hummingbot.core.data_type.order_book_row import ClientOrderBookRow
from hummingbot.core.event.events import TradeType
from hummingbot.strategy.liquidity_mirroring.book_modeling.model_book import ModelBook
from hummingbot.strategy.liquidity_mirroring.order_tracking.order_state import OrderState
from hummingbot.strategy.liquidity_mirroring.order_tracking.order_tracker import OrderTracker
from hummingbot.strategy.liquidity_mirroring.order_tracking.order import Order


class TestModelBook(unittest.TestCase):

    def setUp(self):
        self.original_bids: List[ClientOrderBookRow] = [
            ClientOrderBookRow(Decimal("30150.00"), Decimal("0.1"), 0),
            ClientOrderBookRow(Decimal("30140.00"), Decimal("0.2"), 0),
            ClientOrderBookRow(Decimal("30130.00"), Decimal("0.3"), 0),
            ClientOrderBookRow(Decimal("30120.00"), Decimal("0.4"), 0),
            ClientOrderBookRow(Decimal("30110.00"), Decimal("0.5"), 0),
            ClientOrderBookRow(Decimal("30090.00"), Decimal("0.6"), 0),
            ClientOrderBookRow(Decimal("30080.00"), Decimal("0.7"), 0),
            ClientOrderBookRow(Decimal("30070.00"), Decimal("0.8"), 0),
            ClientOrderBookRow(Decimal("30060.00"), Decimal("0.9"), 0),
            ClientOrderBookRow(Decimal("30050.00"), Decimal("1.0"), 0),
            ClientOrderBookRow(Decimal("30040.00"), Decimal("1.1"), 0),
            ClientOrderBookRow(Decimal("30035.00"), Decimal("1.2"), 0),
            ClientOrderBookRow(Decimal("30034.00"), Decimal("1.3"), 0),
            ClientOrderBookRow(Decimal("30033.10"), Decimal("1.4"), 0),
            ClientOrderBookRow(Decimal("30033.11"), Decimal("1.5"), 0),
        ]
        self.original_asks: List[ClientOrderBookRow] = [
            ClientOrderBookRow(Decimal("30200.00"), Decimal("0.1"), 0),
            ClientOrderBookRow(Decimal("30210.00"), Decimal("0.2"), 0),
            ClientOrderBookRow(Decimal("30220.00"), Decimal("0.3"), 0),
            ClientOrderBookRow(Decimal("30230.00"), Decimal("0.4"), 0),
            ClientOrderBookRow(Decimal("30240.00"), Decimal("0.5"), 0),
            ClientOrderBookRow(Decimal("30250.00"), Decimal("0.6"), 0),
            ClientOrderBookRow(Decimal("30260.00"), Decimal("0.7"), 0),
            ClientOrderBookRow(Decimal("30270.00"), Decimal("0.8"), 0),
            ClientOrderBookRow(Decimal("30280.00"), Decimal("0.9"), 0),
            ClientOrderBookRow(Decimal("30290.00"), Decimal("1.0"), 0),
            ClientOrderBookRow(Decimal("30300.00"), Decimal("1.1"), 0),
            ClientOrderBookRow(Decimal("30310.00"), Decimal("1.2"), 0),
            ClientOrderBookRow(Decimal("30320.00"), Decimal("1.3"), 0),
            ClientOrderBookRow(Decimal("30330.00"), Decimal("1.4"), 0),
            ClientOrderBookRow(Decimal("30340.00"), Decimal("1.5"), 0),
        ]
        self.book = ModelBook(self.original_bids, self.original_asks, Decimal("0.2"))

    def _verify_results(self, bids, asks):
        self.assertEqual(len(bids), len(self.book.bids))
        self.assertEqual(len(asks), len(self.book.asks))
        for original, model in zip(bids, self.book.bids):
            self.assertEqual(original.price, model.price)  # bids
            self.assertEqual(original.amount, model.amount)  # bids
        for original, model in zip(asks, self.book.asks):
            self.assertEqual(original.price, model.price)  # asks
            self.assertEqual(original.amount, model.amount)  # asks

    def test_unedited_book(self):
        """ Test a basic, unedited orderbook, directly translated from arrays of ClientOrderBookRows """
        self._verify_results(self.original_bids, self.original_asks)

    def test_cropped_book(self):
        """ Test the crop function """
        self.book.crop(4, 7)
        self._verify_results(self.original_bids[:4], self.original_asks[:7])

    def test_aggregated_book_10(self):
        """ Test the aggregate function with an aggregate level of $10 """
        self.book.aggregate(Decimal(10))
        bids = self.original_bids[:11]
        bids.append(ClientOrderBookRow(Decimal("30030.00"), Decimal("5.4"), 0))
        self._verify_results(bids, self.original_asks)

    def test_aggregated_book_p10(self):
        """ Test the aggregate function with an aggregate level of $0.10 """
        self.book.aggregate(Decimal("0.1"))
        bids = self.original_bids[:13]
        bids.append(ClientOrderBookRow(Decimal("30033.10"), Decimal("2.9"), 0))
        self._verify_results(bids, self.original_asks)

    def test_limits(self):
        """ Test the apply_limits function """
        self.book.apply_limits()
        self._verify_results(self.original_bids[1:], self.original_asks[1:])

    def test_cut_claimed_amounts(self):
        """ Test the cut_claimed_amounts function """
        ot = OrderTracker()
        ot.add_order(Order("b", Decimal("30000"), Decimal(1), TradeType.SELL, OrderState.PENDING))
        ot.add_order(Order("a", Decimal("30300"), Decimal("1.2"), TradeType.BUY, OrderState.PENDING))

        self.book.cut_claimed_amounts(ot)
        self._verify_results(self.original_bids[4:], [ClientOrderBookRow(Decimal("30240.00"), Decimal("0.3"), 0), *self.original_asks[5:]])

    def test_limit_by_ratios(self):
        """ Test the limit_by_ratio function """
        self.book.limit_by_ratios([Decimal("0.1"), Decimal("0.1"), Decimal("0.2"), Decimal("0.3")], [Decimal("0.01"), Decimal("0.1"), Decimal("2")])
        bids = [
            ClientOrderBookRow(Decimal("30150.00"), Decimal("0.1"), 0),
            ClientOrderBookRow(Decimal("30140.00"), Decimal("0.1"), 0),
            ClientOrderBookRow(Decimal("30130.00"), Decimal("0.2"), 0),
            ClientOrderBookRow(Decimal("30120.00"), Decimal("0.3"), 0),
        ]
        asks = [
            ClientOrderBookRow(Decimal("30200.00"), Decimal("0.01"), 0),
            ClientOrderBookRow(Decimal("30210.00"), Decimal("0.1"), 0),
            ClientOrderBookRow(Decimal("30220.00"), Decimal("0.3"), 0),
        ]
        self._verify_results(bids, asks)

    def test_markup(self):
        """ Test price markup function """
        self.book.markup(Decimal("0.001"), Decimal("0.002"), Decimal("0.0005"))
        bids = [ClientOrderBookRow(bid.price * (Decimal(1) - Decimal("0.0015")), bid.amount, 0) for bid in self.original_bids]
        asks = [ClientOrderBookRow(ask.price * (Decimal(1) + Decimal("0.0025")), ask.amount, 0) for ask in self.original_asks]
        self._verify_results(bids, asks)

    def test_scale_amounts(self):
        """ Test scale_amounts function """
        self.book.scale_amounts(Decimal("0.5"), Decimal("0.1"))
        bids = [ClientOrderBookRow(bid.price, bid.amount * Decimal("0.5"), 0) for bid in self.original_bids]
        asks = [ClientOrderBookRow(ask.price, ask.amount * Decimal("0.1"), 0) for ask in self.original_asks]
        self._verify_results(bids, asks)

    def test_composite_functions(self):
        """ Test a combination of model book functions """
        self.book.apply_limits()

        ot = OrderTracker()
        ot.add_order(Order("b", Decimal("30000"), Decimal(1), TradeType.SELL, OrderState.PENDING))
        ot.add_order(Order("a", Decimal("30300"), Decimal("1.2"), TradeType.BUY, OrderState.PENDING))
        self.book.cut_claimed_amounts(ot)

        self.book.scale_amounts(Decimal("0.9"), Decimal("0.9"))
        self.book.limit_by_ratios([Decimal("0.1"), Decimal("0.1"), Decimal("0.2"), Decimal("10")], [Decimal("0.01"), Decimal("0.1"), Decimal("1"), Decimal("0.2")])
        self.book.markup(Decimal("0.0015"), Decimal("0.0015"), Decimal("0.00075"))
        self.book.apply_limits()

        bids = [
            ClientOrderBookRow(Decimal("30012.32"), Decimal("0.2"), 0),
            ClientOrderBookRow(Decimal("30002.3425"), Decimal("0.72"), 0),
        ]
        asks = [
            ClientOrderBookRow(Decimal("30328.085"), Decimal("0.63"), 0),
            ClientOrderBookRow(Decimal("30338.1075"), Decimal("0.57"), 0),
        ]
        self._verify_results(bids, asks)

    def test_steps_from(self):
        self.book.crop(3, 3)
        # bids:
        #     Decimal("30150.00"), Decimal("0.1")
        #     Decimal("30140.00"), Decimal("0.2")
        #     Decimal("30130.00"), Decimal("0.3")
        # asks:
        #     Decimal("30200.00"), Decimal("0.1")
        #     Decimal("30210.00"), Decimal("0.2")
        #     Decimal("30220.00"), Decimal("0.3")

        current_book = OrderTracker()

        current_book.add_order(Order("b-a", Decimal("30150.00"), Decimal("0.1"), TradeType.BUY, OrderState.ACTIVE))  # match
        current_book.add_order(Order("b-b", Decimal("30145.00"), Decimal("0.2"), TradeType.BUY, OrderState.ACTIVE))  # no price match
        current_book.add_order(Order("b-c", Decimal("30130.00"), Decimal("0.6"), TradeType.BUY, OrderState.ACTIVE))  # amount too large
        current_book.add_order(Order("b-d", Decimal("30120.00"), Decimal("0.4"), TradeType.BUY, OrderState.ACTIVE))  # not in list
        current_book.add_order(Order("b-e", Decimal("30110.00"), Decimal("0.5"), TradeType.BUY, OrderState.ACTIVE))  # not in list

        current_book.add_order(Order("a-a", Decimal("30220.00"), Decimal("0.4"), TradeType.SELL, OrderState.ACTIVE))  # amount too large

        bids_to_place, asks_to_place, orders_to_cancel = self.book.steps_from(current_book)

        self.assertEqual(len(bids_to_place), 1)
        self.assertEqual(bids_to_place[0].price, Decimal("30140.00"))
        self.assertEqual(bids_to_place[0].amount_remaining, Decimal("0.2"))

        self.assertEqual(len(asks_to_place), 2)
        self.assertEqual(asks_to_place[0].price, Decimal("30200.00"))
        self.assertEqual(asks_to_place[0].amount_remaining, Decimal("0.1"))
        self.assertEqual(asks_to_place[1].price, Decimal("30210.00"))
        self.assertEqual(asks_to_place[1].amount_remaining, Decimal("0.2"))

        self.assertEqual(len(orders_to_cancel), 5)
        ids = [o.id for o in orders_to_cancel]
        self.assertIn('b-b', ids)
        self.assertIn('b-c', ids)
        self.assertIn('b-d', ids)
        self.assertIn('b-e', ids)
        self.assertIn('a-a', ids)


if __name__ == '__main__':
    unittest.main()
