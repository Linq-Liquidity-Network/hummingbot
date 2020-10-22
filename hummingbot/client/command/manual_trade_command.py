from typing import TYPE_CHECKING
from decimal import Decimal

from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.strategy_base import StrategyBase
from hummingbot.client.config.security import Security
from hummingbot.core.event.events import OrderType
from hummingbot.user.user_balances import UserBalances
from hummingbot.core.utils.async_utils import safe_ensure_future

if TYPE_CHECKING:
    from hummingbot.client.hummingbot_application import HummingbotApplication

class ManualTradeCommand:
    def trade(self, exchange_name: str, base_asset: str, quote_asset: str, amount: Decimal, buy_sell: str, price: Decimal = None):
        safe_ensure_future(self.async_trade(exchange_name[0], base_asset[0], quote_asset[0], amount[0], buy_sell[0], price))   


    async def async_trade(self, exchange_name: str, base_asset:str, quote_asset: str, amount: Decimal, buy_sell:str, price: Decimal = None):
        if exchange_name in self.markets:
            market: ExchangeBase = self.markets[exchange_name]
            for trading_tuple in self.market_trading_pair_tuples:
                if (trading_tuple.market == market) and (trading_tuple.base_asset.upper() == base_asset.upper()) and (trading_tuple.quote_asset.upper() == quote_asset.upper()):
                    market_trading_pair_tuple = trading_tuple
                    break
        else:
            trading_pair = f"{base_asset.upper()}-{quote_asset.upper()}"
            market_trading_pair: str = self._convert_to_exchange_trading_pair(exchange_name, [trading_pair])[0]
            self._initialize_markets([(exchange_name,[market_trading_pair])])
            
            api_keys = await Security.api_keys(exchange_name)
            
            if api_keys:
                market = UserBalances.connect_market(exchange_name, *api_keys.values())
            else:
                self._notify("API keys have not been added.")
                return

            await market._update_trading_rules()

            market_trading_pair_tuple = MarketTradingPairTuple(market,market_trading_pair,base_asset,quote_asset)              
        
        strategy = StrategyBase()
        strategy.add_markets([market])
        if (buy_sell.lower() == "buy"):
            if price == None:
                if (is_stopped):
                    self._notify("Please use limit order when strategy is stopped. Append price to command")
                else:
                    order_id = strategy.buy_with_specific_market(market_trading_pair_tuple,Decimal(amount),OrderType.MARKET)
            else:
                order_id = strategy.buy_with_specific_market(market_trading_pair_tuple,Decimal(amount),OrderType.LIMIT,Decimal(price))
        elif (buy_sell.lower() == "sell"):
            if price == None:
                if (is_stopped):
                    self._notify("Please use limit order when strategy is stopped. Append price to command")
                else:
                    order_id = strategy.sell_with_specific_market(market_trading_pair_tuple,Decimal(amount),OrderType.MARKET)
            else:
                order_id = strategy.sell_with_specific_market(market_trading_pair_tuple,Decimal(amount),OrderType.LIMIT,Decimal(price)) 
        self._notify(f"{order_id}")        
