from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Mapping


class OrderSide(StrEnum):
    BUY_YES = "buy_yes"
    SELL_YES = "sell_yes"
    BUY_NO = "buy_no"
    SELL_NO = "sell_no"


class TimeInForce(StrEnum):
    IOC = "ioc"
    GTC = "gtc"


@dataclass(frozen=True, slots=True)
class OrderIntent:
    strategy_id: str
    event_id: int
    market_id: int
    side: OrderSide
    limit_price_ticks: int
    qty_lots: int
    time_in_force: TimeInForce = TimeInForce.IOC
    reason: str = ""
    metadata: Mapping[str, object] | None = None
