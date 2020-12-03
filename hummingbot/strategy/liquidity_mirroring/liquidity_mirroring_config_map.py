from hummingbot.client.config.global_config_map import global_config_map
from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_validators import (
    validate_exchange,
    validate_market_trading_pair,
)
from hummingbot.client.settings import (
    required_exchanges,
    EXAMPLE_PAIRS,
)

def configure_offsetting_exchange(value: bool):
    if (value):
        if not global_config_map.get("paper_trade_enabled").value:
            mirrored_market = liquidity_mirroring_config_map.get("mirrored_market").value
            required_exchanges.remove(mirrored_market)

def is_valid_mirroring_market_trading_pair(value: str) -> bool:
    mirrored_market = liquidity_mirroring_config_map.get("mirrored_market").value
    in_mirrored_market = validate_market_trading_pair(mirrored_market, value)
    return in_mirrored_market

def is_valid_primary_market_trading_pair(value: str) -> bool:
    primary_market = liquidity_mirroring_config_map.get("primary_market").value
    in_primary_market = validate_market_trading_pair(primary_market, value)
    return in_primary_market

def mirror_trading_pair_prompt():
    mirrored_market = liquidity_mirroring_config_map.get("mirrored_market").value
    example = EXAMPLE_PAIRS.get(mirrored_market)
    return "Enter the token trading pair from %s you would like to mirror %s >>> " \
           % (mirrored_market, f" (e.g. {example})" if example else "")

def primary_trading_pair_prompt():
    primary_market = liquidity_mirroring_config_map.get("primary_market").value
    example = EXAMPLE_PAIRS.get(primary_market)
    return "Enter the token trading pair you would like to trade on %s%s >>> " \
           % (primary_market, f" (e.g. {example})" if example else "")

liquidity_mirroring_config_map = {
    "strategy": ConfigVar(key="strategy",
                          prompt="",
                          default="liquidity_mirroring"
                          ),
    "primary_market": ConfigVar(
        key="primary_market",
        prompt="Enter your primary exchange name >>> ",
        validator=validate_exchange,
        prompt_on_new=True,
        on_validated=lambda value: required_exchanges.append(value)),
    "mirrored_market": ConfigVar(
        key="mirrored_market",
        prompt="Enter the name of the exchange which you would like to mirror >>> ",
        validator=validate_exchange,
        prompt_on_new=True,
        on_validated=lambda value: required_exchanges.append(value)),
    "market_trading_pair_to_mirror": ConfigVar(
        key="market_trading_pair_to_mirror",
        prompt=mirror_trading_pair_prompt,
        prompt_on_new=True,
        validator=lambda value: is_valid_mirroring_market_trading_pair(value),
    ),
    "primary_market_trading_pair": ConfigVar(
        key="primary_market_trading_pair",
        prompt=primary_trading_pair_prompt,
        prompt_on_new=True,
        validator=lambda value: is_valid_primary_market_trading_pair(value),
    ),
    "two_sided_mirroring": ConfigVar(
        key="two_sided_mirroring",
        prompt="Two-sided mirroring enabled (True/False) >>> ",
        default=False,
        prompt_on_new=True,
        type_str="bool" 
    ),
    "order_price_markup": ConfigVar(
        key="order_price_markup",
        prompt="Enter your desired order price markup applied after fees (0.01 is 1%) >>> ",
        default=0.001,
        prompt_on_new=True,
        type_str="decimal"
    ),
    "max_exposure_base": ConfigVar(
        key="max_exposure_base",
        prompt="Enter the max desired exposure for the base asset >>> ",
        prompt_on_new=True,
        type_str="decimal"
    ),
    "max_exposure_quote": ConfigVar(
        key="max_exposure_quote",
        prompt="Enter the max desired exposure for the quote asset >>> ",
        prompt_on_new=True,
        type_str="decimal"
    ),
    "max_offsetting_exposure": ConfigVar(
        key="max_offsetting_exposure",
        prompt="Enter the maximum allowable outstanding amount to offset >>> ",
        prompt_on_new=True,
        type_str="decimal"
    ),
    "max_offset_loss": ConfigVar(
        key="max_offset_loss",
        prompt="Enter your maximum tolerated one-time percentage offset loss (0.01 for 1%) >>> ",
        prompt_on_new=True,
        type_str="decimal"
    ),
    "max_total_offset_loss": ConfigVar(
        key="max_total_offset_loss",
        prompt="Enter your maximum tolerated total offset loss (in quote currency) >>> ",
        prompt_on_new=True,
        type_str="decimal"
    ),
    "min_primary_amount": ConfigVar(
        key="min_primary_amount",
        prompt="Enter the min amount of base currency per trade on the primary exchange >>> ",
        prompt_on_new=True,
        type_str="decimal"
    ),
    "min_mirroring_amount": ConfigVar(
        key="min_mirroring_amount",
        prompt="Enter the min amount of base currency per trade on the mirrored exchange >>> ",
        prompt_on_new=True,
        type_str="decimal"
    ),
    "order_replacement_threshold": ConfigVar(
        key="order_replacement_threshold",
        prompt=("Enter the min percentage change in best bid/ask on taker exchange to trigger " 
          "an order replacment for best bid/ask on the maker exchange (1% = 0.01) >>> "),
        prompt_on_new=True,
        type_str="decimal"
    ),
    "bid_amount_ratio_type": ConfigVar(
        key="bid_amount_ratio_type",
        prompt="Enter the type of bid amount ratios you would like >>> ",
        prompt_on_new=True,
    ),
    "bid_amount_ratios": ConfigVar(
        key="bid_amount_ratios",
        prompt="Enter the bid amount ratios >>> ",
        required_if=lambda: (liquidity_mirroring_config_map.get("bid_amount_ratio_type").value == "manual"),
        type_str="list"
    ),
    "ask_amount_ratio_type": ConfigVar(
        key="ask_amount_ratio_type",
        prompt="Enter the type of ask amount ratios you would like >>> ",
        prompt_on_new=True,
    ),
    "fee_override": ConfigVar(
        key="fee_override",
        prompt="Enter the total fee percentage to use, inclusive of both exchanges (1% = 0.01) >>> ",
        prompt_on_new=False,
        default=None,
        required_if=lambda: False, 
        type_str="decimal"
    ),
    "slack_update_period": ConfigVar(
        key="slack_update_period",
        prompt="Enter the period of time between slack status updates (in hours) >>> ",
        prompt_on_new=True,
        default=1.0,
        type_str="decimal"
    ),
    "ask_amount_ratios": ConfigVar(
        key="ask_amount_ratios",
        prompt="Enter the ask amount ratios >>> ",
        required_if=lambda: (liquidity_mirroring_config_map.get("ask_amount_ratio_type").value == "manual"),
        type_str="list"
    ),
    "paper_trade_offset": ConfigVar(
        key="paper_trade_offset",
        prompt="Simulating the offsetting exchange on paper trade (True/False) >>> ",
        required_if=lambda: not global_config_map.get("paper_trade_enabled").value,
        default=False,
        type_str="bool",
        on_validated=lambda value: configure_offsetting_exchange(value)
    ),
    "post_only": ConfigVar(
        key="post_only",
        prompt="Use POST-only order types on primary exchange if supported (True/False) >>> ",
        default=False,
        type_str="bool"
    )
}

