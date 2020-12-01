from hummingbot.connector.exchange_base cimport ExchangeBase
from hummingbot.core.data_type.transaction_tracker cimport TransactionTracker


cdef class NovadaxExchange(ExchangeBase):
    cdef:
        object _user_stream_tracker
        object _novadax_client
        object _ev_loop
        object _poll_notifier
        object _novadax_auth
        double _last_timestamp
        double _last_poll_timestamp
        dict _in_flight_orders
        dict _in_flight_orders_by_exchange_id
        dict _order_not_found_records
        TransactionTracker _tx_tracker
        dict _trading_rules
        public object _status_polling_task
        public object _user_stream_event_listener_task
        public object _user_stream_tracker_task
        public object _trading_rules_polling_task

    cdef c_did_timeout_tx(self, str tracking_id)
    cdef c_start_tracking_order(self,
                                str order_id,
                                str exchange_order_id,
                                str trading_pair,
                                object trade_type,
                                object price,
                                object amount,
                                object order_type)
