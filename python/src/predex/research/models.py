from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypeAlias


BookSide: TypeAlias = Literal["bid", "ask"]


@dataclass(frozen=True, slots=True)
class ResearchMarketRoute:
    event_id: int
    market_id: int
    market_ticker: str
    event_market_index: int
    topology: str
    price_level_structure: str
    tradeable: bool = True
    strike_key: int | None = None


@dataclass(frozen=True, slots=True)
class BookLevel:
    price_ticks: int
    qty_lots: int


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    record_index: int
    recv_ts_ns: int
    sequence: int
    event_id: int
    market_id: int
    bid_levels: tuple[BookLevel, ...]
    ask_levels: tuple[BookLevel, ...]


@dataclass(frozen=True, slots=True)
class BookDelta:
    record_index: int
    recv_ts_ns: int
    sequence: int
    event_id: int
    market_id: int
    side: BookSide
    price_ticks: int
    delta_qty_lots: int


@dataclass(frozen=True, slots=True)
class PublicTrade:
    record_index: int
    recv_ts_ns: int
    sequence: int
    event_id: int
    market_id: int
    yes_price_ticks: int | None
    no_price_ticks: int | None
    qty_lots: int | None
    aggressor: str


@dataclass(frozen=True, slots=True)
class LifecycleEvent:
    record_index: int
    recv_ts_ns: int
    sequence: int
    event_id: int
    market_id: int
    msg_json: str


MarketUpdate: TypeAlias = MarketSnapshot | BookDelta | PublicTrade | LifecycleEvent
