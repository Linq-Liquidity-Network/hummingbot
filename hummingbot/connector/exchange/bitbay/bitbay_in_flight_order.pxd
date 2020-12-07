from hummingbot.connector.in_flight_order_base cimport InFlightOrderBase

cdef class BitbayInFlightOrder(InFlightOrderBase):
    cdef:
        public object market
        public object status
        public long long created_at
