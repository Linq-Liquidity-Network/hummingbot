# distutils: language=c++

from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.strategy.strategy_base cimport StrategyBase
from libc.stdint cimport int64_t


cdef class BookState:
    cdef:
        dict bids
        dict asks

    cdef get_book_levels(self, list book)
