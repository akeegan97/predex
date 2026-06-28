from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


_TOPOLOGY_CODES = {
    "unknown": 0,
    "monotonic_chain": 1,
    "mutually_exclusive": 3,
    "unordered_group": 4,
    "single_market": 5,
}


@dataclass(frozen=True, slots=True)
class AppMarketRoute:
    market_ticker: str
    market_id: int
    event_id: int
    affinity_key: int
    topology: str
    topology_code: int
    shard_index: int
    shard_event_index: int
    event_market_index: int
    tradeable: bool
    price_level_structure: str


@dataclass(frozen=True, slots=True)
class AppEventRoute:
    event_id: int
    affinity_key: int
    topology: str
    topology_code: int
    shard_index: int
    shard_event_index: int
    markets: tuple[AppMarketRoute, ...]


@dataclass(frozen=True, slots=True)
class AppConfigIndex:
    shard_count: int
    events: tuple[AppEventRoute, ...]
    routes: tuple[AppMarketRoute, ...]
    markets_by_id: dict[int, AppMarketRoute]
    markets_by_ticker: dict[str, AppMarketRoute]
    events_by_id: dict[int, AppEventRoute]


def _parse_u32(value: Any, field_name: str) -> int:
    parsed = int(value)
    if parsed < 0 or parsed > 0xFFFFFFFF:
        raise ValueError(f"{field_name} outside uint32 range: {value!r}")
    return parsed


def _parse_u64(value: Any, field_name: str) -> int:
    parsed = int(value)
    if parsed < 0 or parsed > 0xFFFFFFFFFFFFFFFF:
        raise ValueError(f"{field_name} outside uint64 range: {value!r}")
    return parsed


def _topology_code(topology: str) -> int:
    try:
        return _TOPOLOGY_CODES[topology]
    except KeyError as exc:
        raise ValueError(f"unknown topology: {topology!r}") from exc


def load_app_config_index(path: str | Path) -> AppConfigIndex:
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    runtime = config.get("runtime") or {}
    shard_count = int(runtime.get("shard_count", 0))
    if shard_count <= 0:
        raise ValueError("runtime.shard_count must be positive")

    routes: list[AppMarketRoute] = []
    events: list[AppEventRoute] = []
    next_shard_event_index: dict[int, int] = defaultdict(int)

    for event_payload in (config.get("universe") or {}).get("events") or []:
        event_id = _parse_u32(event_payload["event_id"], "event_id")
        affinity_key = _parse_u64(event_payload["affinity_key"], "affinity_key")
        topology = str(event_payload["topology"])
        topology_code = _topology_code(topology)
        shard_index = affinity_key % shard_count
        shard_event_index = next_shard_event_index[shard_index]
        next_shard_event_index[shard_index] += 1

        event_markets: list[AppMarketRoute] = []
        for event_market_index, market_payload in enumerate(event_payload.get("markets") or []):
            route = AppMarketRoute(
                market_ticker=str(market_payload["kalshi_ticker"]),
                market_id=_parse_u32(market_payload["market_id"], "market_id"),
                event_id=event_id,
                affinity_key=affinity_key,
                topology=topology,
                topology_code=topology_code,
                shard_index=shard_index,
                shard_event_index=shard_event_index,
                event_market_index=event_market_index,
                tradeable=bool(market_payload.get("tradeable", False)),
                price_level_structure=str(market_payload.get("price_level_structure", "linear_cent")),
            )
            routes.append(route)
            event_markets.append(route)

        events.append(
            AppEventRoute(
                event_id=event_id,
                affinity_key=affinity_key,
                topology=topology,
                topology_code=topology_code,
                shard_index=shard_index,
                shard_event_index=shard_event_index,
                markets=tuple(event_markets),
            )
        )

    markets_by_id = {route.market_id: route for route in routes}
    markets_by_ticker = {route.market_ticker: route for route in routes}
    events_by_id = {event.event_id: event for event in events}
    if len(markets_by_id) != len(routes):
        raise ValueError("duplicate market_id values in app config")
    if len(markets_by_ticker) != len(routes):
        raise ValueError("duplicate kalshi_ticker values in app config")
    if len(events_by_id) != len(events):
        raise ValueError("duplicate event_id values in app config")

    return AppConfigIndex(
        shard_count=shard_count,
        events=tuple(events),
        routes=tuple(routes),
        markets_by_id=markets_by_id,
        markets_by_ticker=markets_by_ticker,
        events_by_id=events_by_id,
    )
