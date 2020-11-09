import aiohttp
from typing import Dict, Any

from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_methods import using_exchange

CENTRALIZED = True

EXAMPLE_PAIR = "LRC-ETH"

DEFAULT_FEES = [0.0, 0.2]

urlNEW_CONNECTOR_ROOT_API = "https://api.url_new_connector.io"
urlNEW_CONNECTOR_WS_KEY_PATH = "/v2/ws/key"

KEYS = {
    "new_connector_accountid":
        ConfigVar(key="new_connector_accountid",
                  prompt="Enter your new_connector account id >>> ",
                  required_if=using_exchange("new_connector"),
                  is_secure=True,
                  is_connect_key=True),
    "new_connector_exchangeid":
        ConfigVar(key="new_connector_exchangeid",
                  prompt="Enter the new_connector exchange id >>> ",
                  required_if=using_exchange("new_connector"),
                  is_secure=True,
                  is_connect_key=True),
    "new_connector_private_key":
        ConfigVar(key="new_connector_private_key",
                  prompt="Enter your new_connector private key >>> ",
                  required_if=using_exchange("new_connector"),
                  is_secure=True,
                  is_connect_key=True),
    "new_connector_api_key":
        ConfigVar(key="new_connector_api_key",
                  prompt="Enter your new_connector api key >>> ",
                  required_if=using_exchange("new_connector"),
                  is_secure=True,
                  is_connect_key=True)
}


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> str:
    # new connector returns trading pairs in the correct format natively
    return exchange_trading_pair


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    # new connector expects trading pairs in the same format as hummingbot internally represents them
    return hb_trading_pair


async def get_ws_api_key():
    async with aiohttp.ClientSession() as client:
        response: aiohttp.ClientResponse = await client.get(
            f"{urlNEW_CONNECTOR_ROOT_API}{urlNEW_CONNECTOR_WS_KEY_PATH}"
        )
        if response.status != 200:
            raise IOError(f"Error getting WS key. Server responded with status: {response.status}.")

        response_dict: Dict[str, Any] = await response.json()
        return response_dict['data']
