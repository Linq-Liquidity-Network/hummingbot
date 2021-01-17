from hummingbot.connector.exchange_base cimport ExchangeBase
from hummingbot.core.data_type.transaction_tracker cimport TransactionTracker

cdef class UpbitExchange(ExchangeBase):
    cdef:
        str API_REST_ENDPOINT
        str WS_ENDPOINT
        TransactionTracker _tx_tracker
        object _poll_notifier
        double _poll_interval
        double _last_timestamp
        object _shared_client
        object _upbit_auth
        int _upbit_accountid
        int _upbit_exchangeid
        str _upbit_private_key
        str _upbit_api_key
        object _order_sign_param

        object _user_stream_tracker
        object _user_stream_tracker_task
        object _user_stream_event_listener_task
        public object _polling_update_task
        public object _token_configuration

        dict _trading_rules
        object _lock
        object _exchange_rates
        object _pending_approval_tx_hashes
        dict _in_flight_orders
        dict _next_order_id
        object _order_id_lock
        dict _upbit_tokenids
        list _trading_pairs
        object _upbit_order_sign_param
