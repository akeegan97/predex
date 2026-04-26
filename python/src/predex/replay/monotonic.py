from __future__ import annotations

import math
from typing import NamedTuple


PRICE_SCALE = 1000.0
CENT_SCALE = 100.0
TICKS_PER_CENT = 10
QTY_SCALE = 100
TAKER_FEE_RATE = 0.07
MIN_NET_EDGE_TICKS = 20
DEFAULT_ORDER_QTY_LOTS = QTY_SCALE
SIDE_BUY = 3
SIDE_SELL = 4


class PairState(NamedTuple):
    executable_qty_lots: int
    net_edge_ticks: int


def qty_to_contracts(qty_lots: int) -> float:
    return float(qty_lots) / float(QTY_SCALE)


def scale_ticks_by_qty_floor(ticks_per_contract: int, qty_lots: int) -> int:
    return (int(ticks_per_contract) * int(qty_lots)) // QTY_SCALE


def fee_ticks(price_ticks: int, qty_lots: int) -> int:
    if qty_lots <= 0 or price_ticks <= 0 or price_ticks >= int(PRICE_SCALE):
        return 0
    price_dollars = float(price_ticks) / PRICE_SCALE
    fee_dollars = TAKER_FEE_RATE * qty_to_contracts(qty_lots) * price_dollars * (1.0 - price_dollars)
    fee_cents = math.ceil(fee_dollars * CENT_SCALE)
    return fee_cents * TICKS_PER_CENT


def evaluate_pair_state(
    easier_ask_ticks: int | None,
    easier_ask_qty_lots: int | None,
    harder_bid_ticks: int | None,
    harder_bid_qty_lots: int | None,
    *,
    default_order_qty_lots: int = DEFAULT_ORDER_QTY_LOTS,
    min_net_edge_ticks: int = MIN_NET_EDGE_TICKS,
) -> PairState | None:
    if (
        easier_ask_ticks is None
        or easier_ask_qty_lots is None
        or harder_bid_ticks is None
        or harder_bid_qty_lots is None
    ):
        return None

    executable_qty_lots = min(
        default_order_qty_lots,
        easier_ask_qty_lots,
        harder_bid_qty_lots,
    )
    if executable_qty_lots <= 0:
        return None

    gross_edge_per_contract = harder_bid_ticks - easier_ask_ticks
    if gross_edge_per_contract <= 0:
        return None

    gross_edge_ticks = scale_ticks_by_qty_floor(gross_edge_per_contract, executable_qty_lots)
    net_edge_ticks = gross_edge_ticks - fee_ticks(easier_ask_ticks, executable_qty_lots) - fee_ticks(
        harder_bid_ticks,
        executable_qty_lots,
    )
    if net_edge_ticks < min_net_edge_ticks:
        return None
    return PairState(executable_qty_lots=executable_qty_lots, net_edge_ticks=net_edge_ticks)