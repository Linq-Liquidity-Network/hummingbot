from decimal import Decimal


def get_available_balances(pm,
                           offset_order_tracker,
                           best_bid,
                           best_ask,
                           max_loss,
                           available_primary_base,
                           available_primary_quote,
                           available_mirrored_base,
                           available_mirrored_quote):
    """" Returns the availabale balance that we have for use in placing new primary market orders
    this includes all calculations related to ensuring that we have enough balance on the offsetting exchange
    to cover any fills that we might recieve
    """

    # Reduce our available assets on the mirrored exchange by the amount that we have pending to offset (not active on the mirrored exchange)
    active_offsetting_exposures = offset_order_tracker.get_active_exposures()
    pending_offset = pm.amount_to_offset
    if pending_offset < 0:
        pending_offset += max(active_offsetting_exposures.buys - active_offsetting_exposures.sells, Decimal(0))
        pending_offset_ab = abs(pending_offset)
        available_mirrored_quote -= pending_offset_ab * (pm.avg_price * (1 + max_loss))
        available_mirrored_quote = max(available_mirrored_quote, Decimal(0))
    elif pending_offset > 0:
        pending_offset += min(active_offsetting_exposures.buys - active_offsetting_exposures.sells, Decimal(0))
        available_mirrored_base -= pending_offset
        available_mirrored_base = max(available_mirrored_base, Decimal(0))

    # Limit our base balance based on our available base on the primary and the ability to buy back what we sell on the mirrored
    base_cap_from_mirrored = available_mirrored_quote / (best_ask * (1 + max_loss))
    available_base = min(available_primary_base, base_cap_from_mirrored)

    # Limit our quote balance based on our available quote on the primary and the ability to sell off any base that we buy on the mirrored
    quote_cap_from_mirrored = available_mirrored_base * (best_bid * (1 - max_loss))
    available_quote = min(available_primary_quote, quote_cap_from_mirrored)

    return available_base, available_quote
