from hummingbot.connector.in_flight_order_base cimport InFlightOrderBase

cdef class BlocktaneInFlightOrder(InFlightOrderBase):
    cdef:
        public object trade_id_set
