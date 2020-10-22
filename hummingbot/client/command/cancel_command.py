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


class CancelCommand:
    def cancel(self, exchange: str, base_asset: str = None, quote_asset: str = None, order_id: str = None):
        safe_ensure_future(self.async_cancel(exchange[0], base_asset, quote_asset, order_id))

    async def async_cancel(self, exchange_name: str, base_asset: str = None, quote_asset: str = None, order_id: str = None):
        if exchange_name in self.markets:
            market: ExchangeBase = self.markets[exchange_name]
            if order_id is not None:
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
        
        if order_id is None:
            await market.cancel_all(10)
            self._notify(f"Canceled all orders on {exchange_name}")
        else:
            for o in market.limit_orders:
                if o.client_order_id.lower() == order_id:
                    order_id = o.client_order_id
                    break
            market.cancel(market_trading_pair_tuple.trading_pair, order_id)
            self._notify(f"Canceled order {order_id}")