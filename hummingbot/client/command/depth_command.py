from typing import TYPE_CHECKING
from decimal import Decimal

from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.strategy_base import StrategyBase
from hummingbot.core.event.events import OrderType
if TYPE_CHECKING:
    from hummingbot.client.hummingbot_application import HummingbotApplication


class DepthCommand:
    def depth(self, exchange: str, levels: int):
        trading_tuples = self.market_trading_pair_tuples
        market: ExchangeBase = self.markets[exchange[0]]
        relevant_tuples = []

        for trading_tuple in trading_tuples:
            if trading_tuple.market == market:
                relevant_tuples.append(trading_tuple)
        for trading_tuple in relevant_tuples:
            bids = list(trading_tuple.order_book_bid_entries())
            asks = list(trading_tuple.order_book_ask_entries())
            self._notify(f"{trading_tuple.trading_pair}: \n")
            self._notify("  Bids:\n")
            self._notify("    Price   Amount")
            for i in range(0,levels):
                self._notify(f"   {bids[i].price}   {bids[i].amount}")
            self._notify("\n  Asks:\n")
            self._notify("    Price   Amount")
            for i in range(0,levels):
                self._notify(f"   {asks[i].price}   {asks[i].amount}")
        