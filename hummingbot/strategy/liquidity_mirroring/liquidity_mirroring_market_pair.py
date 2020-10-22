#!/usr/bin/env python

from typing import NamedTuple

from hummingbot.connector.exchange_base import ExchangeBase


class LiquidityMirroringMarketPair(NamedTuple):
    """
    Specifies a pair of exchanges for arbitrage
    """
    first: ExchangeBase
    second: ExchangeBase
