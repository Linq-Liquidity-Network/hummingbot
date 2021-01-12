# distutils: language=c++

from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.strategy.strategy_base cimport StrategyBase
from libc.stdint cimport int64_t


cdef class LiquidityMirroringStrategy(StrategyBase):
    cdef:
        object mirrored_market_pair
        object primary_market_pair
        list bid_amounts
        list ask_amounts
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
        object order_price_markup
        object max_exposure_base
        object max_exposure_quote
        object max_loss
        object max_total_loss
        object total_trading_volume
        int trades_executed
        object max_offsetting_exposure
        object min_primary_amount
        object min_mirroring_amount
        object pm
        object offset_order_tracker
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
        int _failed_market_order_count
        object _last_failed_market_order_timestamp
        int cycle_number
        object slack_update_period
        object mm_order_type
        object fee_override

        object desired_book
        object current_book

    cdef c_check_balances(self)
    cdef c_process_market_pair(self, object market_pair)
    cdef bint is_maker_exchange(self, object market)
    cdef bint is_taker_exchange(self, object market)
    cdef _did_create_order(self, object order_created_event)
    cdef _did_complete_order(self, object completed_event)
