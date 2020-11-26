from typing import (
    Optional,
    Tuple)

from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_methods import using_exchange


CENTRALIZED = True


EXAMPLE_PAIR = "ZRX-ETH"


DEFAULT_FEES = [0.1, 0.1]


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> Optional[str]:
    return exchange_trading_pair.replace("_", "-")


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    # novadax does not split BASEQUOTE (BTCUSDT)
    return hb_trading_pair.replace("-", "_")


KEYS = {
    "novadax_api_key":
        ConfigVar(key="novadax_api_key",
                  prompt="Enter your Novadax API key >>> ",
                  required_if=using_exchange("novadax"),
                  is_secure=True,
                  is_connect_key=True),
    "novadax_api_secret":
        ConfigVar(key="novadax_api_secret",
                  prompt="Enter your Novadax API secret >>> ",
                  required_if=using_exchange("novadax"),
                  is_secure=True,
                  is_connect_key=True),
    "novadax_uid":
        ConfigVar(key="novadax_uid",
                  prompt="Enter your Novadax UID >>> ",
                  required_if=using_exchange("novadax"),
                  is_secure=True,
                  is_connect_key=True),
}