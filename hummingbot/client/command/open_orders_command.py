from typing import TYPE_CHECKING
from decimal import Decimal

from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.strategy_base import StrategyBase
from hummingbot.core.event.events import OrderType
if TYPE_CHECKING:
    from hummingbot.client.hummingbot_application import HummingbotApplication


class OpenOrdersCommand:
    def open_orders(self):
        open_orders = self.strategy.tracked_limit_orders + self.strategy.tracked_market_orders
        self._notify("Market    Client Order ID                             Price   Amount")
        for order in open_orders:
            self._notify(f"{order[0].name}: {order[1].client_order_id}  {order[1].price}  {order[1].quantity}")
