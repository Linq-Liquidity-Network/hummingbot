# distutils: language=c++

from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.strategy.strategy_base cimport StrategyBase
from libc.stdint cimport int64_t


cdef class ModelBook:
    cdef:
        public list bids
        public list asks
        object _original_best_bid
        object _original_best_ask
        object _min_amount

    cdef get_book_levels(self, list book, object side)
