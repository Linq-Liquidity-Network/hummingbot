import pandas as pd
from typing import TYPE_CHECKING
from decimal import Decimal

from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.strategy_base import StrategyBase
from hummingbot.core.event.events import OrderType
if TYPE_CHECKING:
    from hummingbot.client.hummingbot_application import HummingbotApplication


class OffsetCommand:
    def zero_out_offset_amount(self):
        if (self.strategy_name in ["liquidity_mirroring"]):
            self.strategy.pm._clean()
            self.logger().info("Amount to offset set to 0")
        else:
            self.logger().warning("Amount to offset not an available parameter")