# distutils: language=c++

from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.strategy.strategy_base cimport StrategyBase
from libc.stdint cimport int64_t


cdef class LiquidityMirroringStrategy(StrategyBase):
    cdef:
        list mirrored_market_pairs
        list primary_market_pairs
        list bid_amounts
        list ask_amounts
        list previous_sells
        list previous_buys
        list equivalent_tokens
        list buys_to_replace
        list sells_to_replace
        list bid_replace_ranks
        list ask_replace_ranks
        dict marked_for_deletion
        str slack_url
        object performance_logger
        object best_bid_start
        object initial_base_amount
        object initial_quote_amount
        object order_replacement_threshold
        bint two_sided_mirroring
        bint funds_message_sent
        bint offset_beyond_threshold_message_sent
        bint fail_message_sent
        bint crossed_books
        object start_time
        object primary_best_bid
        object primary_best_ask
        object mirrored_best_bid
        object mirrored_best_ask
        object order_price_markup
        object max_exposure_base
        object max_exposure_quote
        object max_loss
        object max_total_loss
        object total_trading_volume
        int trades_executed
        object offset_base_exposure
        object offset_quote_exposure
        object max_offsetting_exposure
        object min_primary_amount
        object min_mirroring_amount
        object pm
        list bid_amount_percents
        list ask_amount_percents
        bint _all_markets_ready
        bint balances_set
        dict outstanding_offsets
        dict _order_id_to_market
        dict market_orderbook_heaps
        double _status_report_interval
        double _last_timestamp
        dict _last_trade_timestamps
        double _next_trade_delay
        set _sell_markets
        set _buy_markets
        int64_t _logging_options
        int _failed_order_tolerance
        bint _cool_off_logged
        int _failed_market_order_count
        object _last_failed_market_order_timestamp
        int cycle_number
        object slack_update_period

    cdef c_process_market_pair(self, object market_pair)
    cdef bint c_ready_for_new_orders(self, list market_trading_pairs)
    cdef object factor_in_fees(self, market_pair: MarketTradingPairTuple, price: object, amount: object, is_buy: bool, is_primary: bool)
    cdef bint is_maker_exchange(self, object market)
    cdef bint is_taker_exchange(self, object market)
    cdef bint _has_different_sign(self, object a, object b)
    cdef bint _has_reduced(self, object new, object old)
