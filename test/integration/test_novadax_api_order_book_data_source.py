#!/usr/bin/env python
import aiohttp
from os.path import (
    join,
    realpath
)
import sys; sys.path.insert(0, realpath(join(__file__, "../../../")))

import asyncio
import logging
import unittest
from typing import List

from hummingbot.connector.exchange.novadax.novadax_api_order_book_data_source import (
    NovadaxAPIOrderBookDataSource,
)


class NovadaxAPIOrderBookDataSourceUnitTest(unittest.TestCase):
    trading_pairs: List[str] = [
        "BTC_USDT",
    ]

    @classmethod
    def setUpClass(cls):
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.data_source: NovadaxAPIOrderBookDataSource = NovadaxAPIOrderBookDataSource(
            trading_pairs=cls.trading_pairs
        )

    def test_get_trading_pairs(self):
        result: List[str] = self.ev_loop.run_until_complete(
            self.data_source.get_trading_pairs())

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        self.assertIsInstance(result[0], str)
        self.assertEqual(result[0], "BTC_USDT")

    def test_size_snapshot(self):
        async def run_session_for_fetch_snaphot():
            async with aiohttp.ClientSession() as client:
                result = await self.data_source.get_snapshot(client, "ETH_USDT")
                
                assert len(result["bids"]) > 5
                assert len(result["asks"]) > 5

        self.ev_loop.run_until_complete(run_session_for_fetch_snaphot())


def main():
    logging.basicConfig(level=logging.INFO)
    unittest.main()


if __name__ == "__main__":
    main()
