from typing import (
    List,
    Tuple,
)
from decimal import Decimal
from hummingbot.client.config.global_config_map import global_config_map
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.connector.exchange.paper_trade import create_paper_trade_market
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.liquidity_mirroring.liquidity_mirroring import LiquidityMirroringStrategy
from hummingbot.strategy.liquidity_mirroring.liquidity_mirroring_config_map import liquidity_mirroring_config_map


def start(self):
    primary_market = liquidity_mirroring_config_map.get("primary_market").value.lower()
    mirrored_market = liquidity_mirroring_config_map.get("mirrored_market").value.lower()
    mirrored_trading_pair = liquidity_mirroring_config_map.get("market_trading_pair_to_mirror").value
    primary_trading_pair = liquidity_mirroring_config_map.get("primary_market_trading_pair").value
    two_sided_mirroring = liquidity_mirroring_config_map.get("two_sided_mirroring").value
    order_price_markup = liquidity_mirroring_config_map.get("order_price_markup").value
    max_exposure_base = liquidity_mirroring_config_map.get("max_exposure_base").value
    max_exposure_quote = liquidity_mirroring_config_map.get("max_exposure_quote").value
    max_offsetting_exposure = liquidity_mirroring_config_map.get("max_offsetting_exposure").value
    max_loss = liquidity_mirroring_config_map.get("max_offset_loss").value
    max_total_loss = liquidity_mirroring_config_map.get("max_total_offset_loss").value
    min_primary_amount = liquidity_mirroring_config_map.get("min_primary_amount").value
    min_mirroring_amount = liquidity_mirroring_config_map.get("min_mirroring_amount").value
    slack_hook = global_config_map.get("SLACK_HOOK").value
    paper_trade_offset = liquidity_mirroring_config_map.get("paper_trade_offset").value
    post_only = liquidity_mirroring_config_map.get("post_only").value
    slack_update_period = liquidity_mirroring_config_map.get("slack_update_period").value
    order_replacement_threshold = liquidity_mirroring_config_map.get("order_replacement_threshold").value
    fee_override = liquidity_mirroring_config_map.get("fee_override").value

    try:
        primary_market_trading_pair: str = primary_trading_pair
        mirrored_market_trading_pair: str = mirrored_trading_pair
        primary_assets: List[Tuple[str, str]] = self._initialize_market_assets(primary_market, [primary_market_trading_pair])[0]
        secondary_assets: List[Tuple[str, str]] = self._initialize_market_assets(mirrored_market,
                                                                                 [mirrored_market_trading_pair])[0]
    except ValueError as e:
        self._notify(str(e))
        return

    bid_ratios_type = liquidity_mirroring_config_map.get("bid_amount_ratio_type").value
    if bid_ratios_type == "manual":
        bid_ratios = []
        denominations = liquidity_mirroring_config_map.get("bid_amount_ratios").value
        denominator = Decimal(0)
        for summand in denominations:
            denominator += Decimal(summand)

        if denominator == 0:
            self.logger().warning("empty bid ratio list!")
            return
        else:
            for summand in denominations:
                bid_ratios.append(Decimal(summand) / denominator)
    else:
        bid_ratios = [Decimal(1 / 55), Decimal(2 / 55), Decimal(3 / 55), Decimal(4 / 55), Decimal(5 / 55), Decimal(6 / 55),
                      Decimal(7 / 55), Decimal(8 / 55), Decimal(9 / 55), Decimal(10 / 55)]

    ask_ratios_type = liquidity_mirroring_config_map.get("ask_amount_ratio_type").value
    if ask_ratios_type == "manual":
        ask_ratios = []
        denominations = liquidity_mirroring_config_map.get("ask_amount_ratios").value
        denominator = Decimal(0)
        for summand in denominations:
            denominator += Decimal(summand)

        if denominator == 0:
            self.logger().warning("empty ask ratio list!")
            return
        else:
            for summand in denominations:
                ask_ratios.append(Decimal(summand) / denominator)
    else:
        ask_ratios = [Decimal(1 / 55), Decimal(2 / 55), Decimal(3 / 55), Decimal(4 / 55), Decimal(5 / 55), Decimal(6 / 55),
                      Decimal(7 / 55), Decimal(8 / 55), Decimal(9 / 55), Decimal(10 / 55)]

    market_names: List[Tuple[str, List[str]]] = [(primary_market, [primary_market_trading_pair]),
                                                 (mirrored_market, [mirrored_market_trading_pair])]
    if not paper_trade_offset:
        self._initialize_wallet(token_trading_pairs=list(set(primary_assets + secondary_assets)))
        self._initialize_markets(market_names)
    else:
        self._initialize_wallet(token_trading_pairs=list(set(primary_assets)))
        self._initialize_markets([(primary_market, [primary_market_trading_pair])])
        try:
            market: ExchangeBase = create_paper_trade_market(mirrored_market, [mirrored_market_trading_pair])
        except Exception:
            raise
        paper_trade_account_balance = global_config_map.get("paper_trade_account_balance").value

        for asset in paper_trade_account_balance.keys():
            market.set_balance(asset, paper_trade_account_balance[asset])
        self.markets[mirrored_market]: ExchangeBase = market
        self.markets_recorder = MarketsRecorder(
            self.trade_fill_db,
            [self.markets[mirrored_market]],
            self.strategy_file_name,
            self.strategy_name,
        )
        self.markets_recorder.start()

    self.assets = set(primary_assets + secondary_assets)
    self.primary_market_trading_pair_tuples: MarketTradingPairTuple = MarketTradingPairTuple(self.markets[primary_market], primary_market_trading_pair, primary_assets[0], primary_assets[1])
    self.mirrored_market_trading_pair_tuples: MarketTradingPairTuple = MarketTradingPairTuple(self.markets[mirrored_market], mirrored_market_trading_pair, secondary_assets[0], secondary_assets[1])
    self.market_trading_pair_tuples = self.primary_market_trading_pair_tuples + self.mirrored_market_trading_pair_tuples
    self.strategy = LiquidityMirroringStrategy(primary_market_pair=self.primary_market_trading_pair_tuples,
                                               mirrored_market_pair=self.mirrored_market_trading_pair_tuples,
                                               two_sided_mirroring=two_sided_mirroring,
                                               order_price_markup=order_price_markup,
                                               max_exposure_base=max_exposure_base,
                                               max_exposure_quote=max_exposure_quote,
                                               max_offsetting_exposure=max_offsetting_exposure,
                                               max_loss=max_loss,
                                               max_total_loss=max_total_loss,
                                               bid_amount_percents=bid_ratios,
                                               ask_amount_percents=ask_ratios,
                                               min_primary_amount=min_primary_amount,
                                               min_mirroring_amount=min_mirroring_amount,
                                               order_replacement_threshold=order_replacement_threshold,
                                               post_only=post_only,
                                               slack_hook=slack_hook,
                                               slack_update_period=slack_update_period,
                                               fee_override=fee_override,
                                               logging_options=LiquidityMirroringStrategy.OPTION_LOG_ALL)
