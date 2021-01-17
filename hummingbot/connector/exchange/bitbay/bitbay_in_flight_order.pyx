import copy
import json
import time
from typing import (Any, Dict, List, Tuple)
from decimal import Decimal
from hummingbot.connector.exchange.bitbay.bitbay_order_status import BitbayOrderStatus
from hummingbot.connector.in_flight_order_base cimport InFlightOrderBase
from hummingbot.connector.exchange.bitbay.bitbay_exchange cimport BitbayExchange
from hummingbot.core.event.events import (OrderFilledEvent, TradeType, OrderType, TradeFee, MarketEvent)

cdef class BitbayInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 market: BitbayExchange,
                 client_order_id: str,
                 exchange_order_id: str,
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 initial_state: BitbayOrderStatus,
                 filled_size: Decimal,
                 filled_volume: Decimal,
                 filled_fee: Decimal,
                 created_at: int):

        super().__init__(client_order_id=client_order_id,
                         exchange_order_id=exchange_order_id,
                         trading_pair=trading_pair,
                         order_type=order_type,
                         trade_type=trade_type,
                         price=price,
                         amount=amount,
                         initial_state = str(initial_state))
        self.market = market
        self.status = initial_state
        self.created_at = created_at
        self.executed_amount_base = filled_size
        self.executed_amount_quote = filled_volume
        self.fee_paid = filled_fee

        (base, quote) = self.market.split_trading_pair(trading_pair)
        self.fee_asset = base if trade_type is TradeType.BUY else quote

    @property
    def is_done(self) -> bool:
        return self.status >= BitbayOrderStatus.DONE

    @property
    def is_cancelled(self) -> bool:
        return self.status == BitbayOrderStatus.cancelled

    @property
    def is_failure(self) -> bool:
        return self.status >= BitbayOrderStatus.failed

    @property
    def is_expired(self) -> bool:
        return self.status == BitbayOrderStatus.expired

    @property
    def description(self):
        return f"{str(self.order_type).lower()} {str(self.trade_type).lower()}"

    def to_json(self):
        return json.dumps({
            "client_order_id": self.client_order_id,
            "exchange_order_id": self.exchange_order_id,
            "trading_pair": self.trading_pair,
            "order_type": self.order_type.name,
            "trade_type": self.trade_type.name,
            "price": str(self.price),
            "amount": str(self.amount),
            "status": self.status.name,
            "executed_amount_base": str(self.executed_amount_base),
            "executed_amount_quote": str(self.executed_amount_quote),
            "fee_paid": str(self.fee_paid),
            "created_at": self.created_at
        })

    @classmethod
    def from_json(cls, market, data: Dict[str, Any]) -> BitbayInFlightOrder:
        return BitbayInFlightOrder(
            market,
            data["client_order_id"],
            data["exchange_order_id"],
            data["trading_pair"],
            OrderType[data["order_type"]],
            TradeType[data["trade_type"]],
            Decimal(data["price"]),
            Decimal(data["amount"]),
            BitbayOrderStatus[data["status"]],
            Decimal(data["executed_amount_base"]),
            Decimal(data["executed_amount_quote"]),
            Decimal(data["fee_paid"]),
            data["created_at"]
        )

    @classmethod
    def from_bitbay_order(cls,
                            market: BitbayExchange,
                            side: TradeType,
                            client_order_id: str,
                            created_at: int,
                            hash: str,
                            trading_pair: str,
                            price: float,
                            amount: float) -> BitbayInFlightOrder:
        return BitbayInFlightOrder(
            market,
            client_order_id,
            hash,
            trading_pair,
            OrderType.LIMIT, # TODO: fix this to the actual type (ie. LIMIT_MAKER)
            side,
            Decimal(price),
            Decimal(amount),
            BitbayOrderStatus.waiting,
            Decimal(0),
            Decimal(0),
            Decimal(0),
            created_at
        )

    def update(self, data: Dict[str, Any]) -> List[Any]:
        events: List[Any] = []

        if "state" in data:
            details = data['state']
        else:
            details = data
        base: str
        quote: str
        trading_pair: str = data["market"]
        (base, quote) = self.market.split_trading_pair(trading_pair)

        start_amount_base: Decimal = Decimal(details["startAmount"])
        current_amount_base: Decimal = Decimal(details["currentAmount"])
        new_executed_amount_base: Decimal = start_amount_base - current_amount_base
        new_executed_amount_quote: Decimal = Decimal(details["rate"]) * new_executed_amount_base
        new_fee_rate: Decimal = self.market.get_fee(base, quote, self.order_type, self.trade_type, new_executed_amount_base, Decimal(data["rate"])).percent
        new_fee_paid = new_executed_amount_base * new_fee_rate

        if "action" in data:
            if data['action'] == 'update':
                new_status = BitbayOrderStatus['accepted']
                if current_amount_base == Decimal('0'):
                    new_status = BitbayOrderStatus['filled']
            elif data['action'] == 'remove':
                if current_amount_base > Decimal('0'):
                    new_status = BitbayOrderStatus['cancelled']
                else:
                    new_status = BitbayOrderStatus['filled']
        else:
            if current_amount_base == Decimal('0'):
                new_status = BitbayOrderStatus['filled']
            else:
                new_status = BitbayOrderStatus['accepted']
        
        if new_executed_amount_base > self.executed_amount_base or new_executed_amount_quote > self.executed_amount_quote:
            diff_base: Decimal = new_executed_amount_base - self.executed_amount_base
            diff_quote: Decimal = new_executed_amount_quote - self.executed_amount_quote
            diff_fee: Decimal = new_fee_paid - self.fee_paid
            if diff_quote > Decimal(0):
                price: Decimal = diff_quote / diff_base
            else:
                price: Decimal = self.executed_amount_quote / self.executed_amount_base

            events.append((MarketEvent.OrderFilled, diff_base, price, diff_fee))

        if not self.is_done and new_status == BitbayOrderStatus.cancelled:
            events.append((MarketEvent.OrderCancelled, None, None, None))

        if not self.is_done and new_status == BitbayOrderStatus.expired:
            events.append((MarketEvent.OrderExpired, None, None, None))

        if not self.is_done and new_status == BitbayOrderStatus.failed:
            events.append( (MarketEvent.OrderFailure, None, None, None) )

        self.status = new_status
        self.last_state = str(new_status)
        self.executed_amount_base = new_executed_amount_base
        self.executed_amount_quote = new_executed_amount_quote
        self.fee_paid = new_fee_paid

        if self.exchange_order_id is None:
            self.update_exchange_order_id(data.get('id', None))

        return events
