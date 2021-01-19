import unittest
from decimal import Decimal

from hummingbot.strategy.liquidity_mirroring.position import PositionManager

trade_data = [
    {'msg_type': 'order filled', 'data': {'exchange': 'ftx', 'pair': 'BTC-BRL', 'price': Decimal('193000.00'), 'amount': Decimal('0.00206195'), 'buy/sell': 'BUY'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'blocktane', 'pair': 'BTC-BRL', 'price': Decimal('194851.52'), 'amount': Decimal('0.00017876'), 'buy/sell': 'SELL'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'blocktane', 'pair': 'BTC-BRL', 'price': Decimal('194851.52'), 'amount': Decimal('0.00081096'), 'buy/sell': 'SELL'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'ftx', 'pair': 'BTC-BRL', 'price': Decimal('176560.0'), 'amount': Decimal('0.0002'), 'buy/sell': 'BUY'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'blocktane', 'pair': 'BTC-BRL', 'price': Decimal('177389.83'), 'amount': Decimal('0.00021766'), 'buy/sell': 'SELL'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'ftx', 'pair': 'BTC-BRL', 'price': Decimal('176560.0'), 'amount': Decimal('0.0007'), 'buy/sell': 'BUY'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'blocktane', 'pair': 'BTC-BRL', 'price': Decimal('177389.83'), 'amount': Decimal('0.00074962'), 'buy/sell': 'SELL'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'ftx', 'pair': 'BTC-BRL', 'price': Decimal('176560.0'), 'amount': Decimal('0.0026'), 'buy/sell': 'BUY'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'blocktane', 'pair': 'BTC-BRL', 'price': Decimal('177389.83'), 'amount': Decimal('0.00258348'), 'buy/sell': 'SELL'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'ftx', 'pair': 'BTC-BRL', 'price': Decimal('176560.0'), 'amount': Decimal('0.004'), 'buy/sell': 'BUY'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'blocktane', 'pair': 'BTC-BRL', 'price': Decimal('177389.83'), 'amount': Decimal('0.00400012'), 'buy/sell': 'SELL'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'ftx', 'pair': 'BTC-BRL', 'price': Decimal('178330.0'), 'amount': Decimal('0.002'), 'buy/sell': 'BUY'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'blocktane', 'pair': 'BTC-BRL', 'price': Decimal('179173.17'), 'amount': Decimal('0.00200012'), 'buy/sell': 'SELL'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'ftx', 'pair': 'BTC-BRL', 'price': Decimal('179915.0'), 'amount': Decimal('0.004'), 'buy/sell': 'BUY'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'blocktane', 'pair': 'BTC-BRL', 'price': Decimal('180760.6'), 'amount': Decimal('0.00400012'), 'buy/sell': 'SELL'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'ftx', 'pair': 'BTC-BRL', 'price': Decimal('179835.0'), 'amount': Decimal('0.004'), 'buy/sell': 'BUY'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'blocktane', 'pair': 'BTC-BRL', 'price': Decimal('180579.75'), 'amount': Decimal('0.00400008'), 'buy/sell': 'SELL'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'ftx', 'pair': 'BTC-BRL', 'price': Decimal('178295.0'), 'amount': Decimal('0.004'), 'buy/sell': 'BUY'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'blocktane', 'pair': 'BTC-BRL', 'price': Decimal('179132.99'), 'amount': Decimal('0.00400008'), 'buy/sell': 'SELL'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'ftx', 'pair': 'BTC-BRL', 'price': Decimal('177955.0'), 'amount': Decimal('0.0029'), 'buy/sell': 'BUY'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'ftx', 'pair': 'BTC-BRL', 'price': Decimal('178405.0'), 'amount': Decimal('0.009'), 'buy/sell': 'BUY'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'blocktane', 'pair': 'BTC-BRL', 'price': Decimal('178711.01'), 'amount': Decimal('0.00283241'), 'buy/sell': 'SELL'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'blocktane', 'pair': 'BTC-BRL', 'price': Decimal('177610.87'), 'amount': Decimal('0.00916763'), 'buy/sell': 'SELL'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'ftx', 'pair': 'BTC-BRL', 'price': Decimal('173350.0'), 'amount': Decimal('0.0009'), 'buy/sell': 'SELL'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'blocktane', 'pair': 'BTC-BRL', 'price': Decimal('172535.25'), 'amount': Decimal('0.001'), 'buy/sell': 'BUY'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'ftx', 'pair': 'BTC-BRL', 'price': Decimal('208685.0'), 'amount': Decimal('0.0005'), 'buy/sell': 'BUY'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'blocktane', 'pair': 'BTC-BRL', 'price': Decimal('209620.61'), 'amount': Decimal('0.00052091'), 'buy/sell': 'SELL'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'ftx', 'pair': 'BTC-BRL', 'price': Decimal('209720.0'), 'amount': Decimal('0.0001'), 'buy/sell': 'BUY'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'blocktane', 'pair': 'BTC-BRL', 'price': Decimal('210846.34'), 'amount': Decimal('0.0001'), 'buy/sell': 'SELL'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'ftx', 'pair': 'BTC-BRL', 'price': Decimal('208260.0'), 'amount': Decimal('0.0001'), 'buy/sell': 'SELL'}},
    {'msg_type': 'order filled', 'data': {'exchange': 'blocktane', 'pair': 'BTC-BRL', 'price': Decimal('207251.32'), 'amount': Decimal('0.0001'), 'buy/sell': 'BUY'}},
]
trade_data.reverse()

expected_results = [
    (range(0, 2), (Decimal(0), Decimal("0"), Decimal("-0.100868"))),
    (range(0, 4), (Decimal(0), Decimal("0"), Decimal("-0.213502"))),
    (range(0, 8), (Decimal("0.00007909"), Decimal("172535.25"), Decimal("-2.1900368776"))),
    (range(0, 12), (Decimal("-0.00002095"), Decimal("177872.2625515500023068631275"), Decimal("2.44310797315497254832878275"))),
    (range(0, 16), (Decimal("-0.00002111"), Decimal("180572.1492382945298228630035"), Decimal("-3.83117487607960247543936165"))),
    (range(0, 22), (Decimal("-0.00002147"), Decimal("177399.3862187335082632889724"), Decimal("-12.28682695638379157758718545"))),
    (range(0, 28), (Decimal("-0.00007223"), Decimal("177389.8301035720016069354536"), Decimal("-15.19143712091899432393105193"))),
    (range(0, 30), (Decimal("-0.00106195"), Decimal("193663.8389780884275870511303"), Decimal("-15.19143712091899432393105193"))),
    (range(0, 31), (Decimal("0.001"), Decimal("193000.00"), Decimal("-15.89640092369999999999999975"))),
]


class TestPM(unittest.TestCase):

    def setUp(self):
        self.pm = PositionManager()

    def test_basic_short_loss(self):
        self.pm.register_trade(Decimal(100), Decimal(-1))
        self.assertEqual(f"{str(self.pm.avg_price)} {str(self.pm.amount_to_offset)} {str(self.pm.total_loss)}", "100 -1 0")
        self.pm.register_trade(Decimal(101), Decimal(1))
        self.assertEqual(f"{str(self.pm.avg_price)} {str(self.pm.amount_to_offset)} {str(self.pm.total_loss)}", "0 0 1")

    def test_basic_long_loss(self):
        self.pm.register_trade(Decimal(101), Decimal(1))
        self.assertEqual(f"{str(self.pm.avg_price)} {str(self.pm.amount_to_offset)} {str(self.pm.total_loss)}", "101 1 0")
        self.pm.register_trade(Decimal(100), Decimal(-1))
        self.assertEqual(f"{str(self.pm.avg_price)} {str(self.pm.amount_to_offset)} {str(self.pm.total_loss)}", "0 0 1")

    def test_basic_short_gain(self):
        self.pm.register_trade(Decimal(101), Decimal(-1))
        self.assertEqual(f"{str(self.pm.avg_price)} {str(self.pm.amount_to_offset)} {str(self.pm.total_loss)}", "101 -1 0")
        self.pm.register_trade(Decimal(100), Decimal(1))
        self.assertEqual(f"{str(self.pm.avg_price)} {str(self.pm.amount_to_offset)} {str(self.pm.total_loss)}", "0 0 -1")

    def test_basic_long_gain(self):
        self.pm.register_trade(Decimal(100), Decimal(1))
        self.assertEqual(f"{str(self.pm.avg_price)} {str(self.pm.amount_to_offset)} {str(self.pm.total_loss)}", "100 1 0")
        self.pm.register_trade(Decimal(101), Decimal(-1))
        self.assertEqual(f"{str(self.pm.avg_price)} {str(self.pm.amount_to_offset)} {str(self.pm.total_loss)}", "0 0 -1")

    def _test_range(self, range_index):
        r, (amount_to_offset, vwap, loss) = expected_results[range_index]
        for i in r:
            t = trade_data[i]['data']
            side_mult = 1 if t['buy/sell'] == 'BUY' else -1
            self.pm.register_trade(t['price'], side_mult * t['amount'])

        self.assertEqual(self.pm.amount_to_offset, amount_to_offset)
        self.assertEqual(self.pm.avg_price, vwap)
        self.assertEqual(self.pm.total_loss, loss)

    def test_range0(self):
        """ Test first range """
        self._test_range(0)

    def test_range1(self):
        """ Test second range """
        self._test_range(1)

    def test_range2(self):
        """ Test third range """
        self._test_range(2)

    def test_range3(self):
        """ Test fourth range """
        self._test_range(3)

    def test_range4(self):
        """ Test fifth range """
        self._test_range(4)

    def test_range5(self):
        """ Test fifth range """
        self._test_range(5)

    def test_range6(self):
        """ Test fifth range """
        self._test_range(6)

    def test_range7(self):
        """ Test fifth range """
        self._test_range(7)

    def test_range8(self):
        """ Test final range """
        self._test_range(8)


if __name__ == '__main__':
    unittest.main()
