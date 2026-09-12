from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class TopologyKind(StrEnum):
    UNKNOWN = "unknown"
    MONOTONIC_CHAIN = "monotonic_chain"
    MUTUALLY_EXCLUSIVE = "mutually_exclusive"
    UNORDERED_GROUP = "unordered_group"
    SINGLE_MARKET = "single_market"


@dataclass(slots=True)
class MarketRecord:
    ticker: str
    event_ticker: str
    title: str = ""
    subtitle: str = ""
    yes_sub_title: str = ""
    no_sub_title: str = ""
    strike_type: str = ""
    floor_strike: Any | None = None
    cap_strike: Any | None = None
    custom_strike: dict[str, Any] = field(default_factory=dict)
    close_time: str = ""
    expected_expiration_time: str = ""
    expiration_time: str = ""
    status: str = ""
    price_level_structure: str = "linear_cent"

    @classmethod
    def from_api(cls, payload: dict[str, Any]) -> "MarketRecord":
        return cls(
            ticker=str(payload.get("ticker", "")),
            event_ticker=str(payload.get("event_ticker", "")),
            title=str(payload.get("title", "")),
            subtitle=str(payload.get("subtitle", "")),
            yes_sub_title=str(payload.get("yes_sub_title", "")),
            no_sub_title=str(payload.get("no_sub_title", "")),
            strike_type=str(payload.get("strike_type", "")),
            floor_strike=payload.get("floor_strike"),
            cap_strike=payload.get("cap_strike"),
            custom_strike=dict(payload.get("custom_strike") or {}),
            close_time=str(payload.get("close_time", "")),
            expected_expiration_time=str(payload.get("expected_expiration_time", "")),
            expiration_time=str(payload.get("expiration_time", "")),
            status=str(payload.get("status", "")),
            price_level_structure=str(payload.get("price_level_structure", "linear_cent") or "linear_cent"),
        )

    def primary_time_reference(self) -> str:
        if self.close_time:
            return self.close_time
        if self.expected_expiration_time:
            return self.expected_expiration_time
        return self.expiration_time


@dataclass(slots=True)
class EventRecord:
    event_ticker: str
    series_ticker: str = ""
    title: str = ""
    sub_title: str = ""
    category: str = ""
    mutually_exclusive: bool = False
    markets: list[MarketRecord] = field(default_factory=list)

    @classmethod
    def from_api(cls, payload: dict[str, Any]) -> "EventRecord":
        markets_payload = payload.get("markets") or []
        markets = [MarketRecord.from_api(market) for market in markets_payload]
        event_ticker = str(payload.get("event_ticker", ""))
        for market in markets:
            if not market.event_ticker:
                market.event_ticker = event_ticker
        return cls(
            event_ticker=event_ticker,
            series_ticker=str(payload.get("series_ticker", "")),
            title=str(payload.get("title", "")),
            sub_title=str(payload.get("sub_title", "")),
            category=str(payload.get("category", "")),
            mutually_exclusive=bool(payload.get("mutually_exclusive", False)),
            markets=markets,
        )


@dataclass(frozen=True, slots=True)
class ClassifiedMarket:
    market: MarketRecord
    strike_key: int


@dataclass(frozen=True, slots=True)
class ClassifiedEvent:
    event: EventRecord
    topology_kind: TopologyKind
    markets: tuple[ClassifiedMarket, ...]
    reason: str
    synthetic_key: str = ""
