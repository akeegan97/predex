from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class MarketRoute:
    market_ticker: str
    market_id: int
    event_id: int
    affinity_key: int
    topology_kind: str
    strike_key: int


@dataclass(frozen=True, slots=True)
class EventRoute:
    event_id: int
    topology_kind: str
    affinity_key: int
    markets: tuple[MarketRoute, ...]


@dataclass(frozen=True, slots=True)
class ConfigIndex:
    routes: tuple[MarketRoute, ...]
    markets_by_id: dict[int, MarketRoute]
    markets_by_ticker: dict[str, MarketRoute]
    events_by_id: dict[int, EventRoute]


def _parse_market_route(payload: dict[str, Any]) -> MarketRoute:
    return MarketRoute(
        market_ticker=str(payload["market_ticker"]),
        market_id=int(payload["market_id"]),
        event_id=int(payload["event_id"]),
        affinity_key=int(payload["affinity_key"]),
        topology_kind=str(payload["topology_kind"]),
        strike_key=int(payload["strike_key"]),
    )


def load_config_index(path: str | Path) -> ConfigIndex:
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    routes = tuple(_parse_market_route(route) for route in config.get("market_routes") or [])
    markets_by_id = {route.market_id: route for route in routes}
    markets_by_ticker = {route.market_ticker: route for route in routes}

    grouped: dict[int, list[MarketRoute]] = defaultdict(list)
    for route in routes:
        grouped[route.event_id].append(route)

    events_by_id = {
        event_id: EventRoute(
            event_id=event_id,
            topology_kind=sorted_routes[0].topology_kind,
            affinity_key=sorted_routes[0].affinity_key,
            markets=tuple(sorted_routes),
        )
        for event_id, event_routes in grouped.items()
        for sorted_routes in [sorted(event_routes, key=lambda route: route.strike_key)]
    }
    return ConfigIndex(
        routes=routes,
        markets_by_id=markets_by_id,
        markets_by_ticker=markets_by_ticker,
        events_by_id=events_by_id,
    )
