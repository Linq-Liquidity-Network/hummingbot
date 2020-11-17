import aiohttp
from typing import Dict, Any

from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_methods import using_exchange

CENTRALIZED = True

EXAMPLE_PAIR = "LRC-ETH"

DEFAULT_FEES = [0.0, 0.2]

BITBAY_ROOT_API = "https://bitbay.net/API/Public/"

KEYS = {
    "bitbay_private_key":
        ConfigVar(key="bitbay_private_key",
                  prompt="Enter your bitbay private key >>> ",
                  required_if=using_exchange("bitbay"),
                  is_secure=True,
                  is_connect_key=True),
    "bitbay_api_key":
        ConfigVar(key="bitbay_api_key",
                  prompt="Enter your bitbay api key >>> ",
                  required_if=using_exchange("bitbay"),
                  is_secure=True,
                  is_connect_key=True)
}


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> str:
    # new connector returns trading pairs in the correct format natively
    return exchange_trading_pair


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    # new connector expects trading pairs in the same format as hummingbot internally represents them
    return hb_trading_pair

