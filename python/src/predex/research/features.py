from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True, slots=True)
class MarketFeatureSnapshot:
    market_id: int
    market_ticker: str
    event_market_index: int
    best_bid_ticks: int | None
    best_bid_qty_lots: int
    best_ask_ticks: int | None
    best_ask_qty_lots: int
    mid_ticks: float | None
    initialized: bool


@dataclass(frozen=True, slots=True)
class ChainFeatureSnapshot:
    event_id: int
    recv_ts_ns: int
    record_index: int
    updated_market_id: int
    markets: tuple[MarketFeatureSnapshot, ...]
    context: Mapping[str, object] | None = None
