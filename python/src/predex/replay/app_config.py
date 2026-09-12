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
    strike_key: int
    tradeable: bool
    price_level_structure: str
    event_ticker: str
    series_ticker: str
    event_category: str
    event_domain: str
    event_time_s: int
    event_close_time_s: int
    event_expected_expiration_time_s: int
    event_expiration_time_s: int
    market_time_s: int
    market_close_time_s: int
    market_expected_expiration_time_s: int
    market_expiration_time_s: int


@dataclass(frozen=True, slots=True)
class AppEventRoute:
    event_id: int
    affinity_key: int
    topology: str
    topology_code: int
    shard_index: int
    shard_event_index: int
    event_ticker: str
    series_ticker: str
    event_category: str
    event_domain: str
    event_time_s: int
    event_close_time_s: int
    event_expected_expiration_time_s: int
    event_expiration_time_s: int
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


def _infer_event_domain(*parts: str) -> str:
    searchable = " ".join(part.lower() for part in parts if part)
    if any(token in searchable for token in (
        "sports", "nba", "nfl", "mlb", "nhl", "soccer", "football", "tennis",
        "ufc", "golf", "world cup", "kxwc", "kxatp", "kxwta", "kxitf",
    )):
        return "sports"
    if any(token in searchable for token in ("crypto", "bitcoin", "ethereum", "kxbtc", "kxeth")):
        return "crypto"
    if any(token in searchable for token in (
        "financial", "finance", "economics", "economic", "fed", "inflation", "cpi",
        "gdp", "unemployment", "nasdaq", "s&p", "spx", "oil", "wti", "gas",
        "kxfed", "kxcpi", "kxin", "kxinx", "kxwti", "kxgas",
    )):
        return "financial_economic"
    if any(token in searchable for token in (
        "weather", "temperature", "rain", "snow", "hurricane", "climate",
    )):
        return "weather"
    if any(token in searchable for token in (
        "politics", "election", "president", "senate", "congress", "supreme court",
    )):
        return "politics"
    if any(token in searchable for token in (
        "entertainment", "culture", "celebrity", "movie", "tv", "oscars",
        "grammy", "love island",
    )):
        return "pop_culture"
    return "other"


def _load_report_event_metadata(config_path: Path, report_path: str | Path | None) -> dict[int, dict[str, str]]:
    path = Path(report_path) if report_path is not None else config_path.with_name("report.json")
    if not path.exists():
        return {}
    report = json.loads(path.read_text(encoding="utf-8"))
    metadata: dict[int, dict[str, str]] = {}
    for event in report.get("included_events") or []:
        try:
            event_id = _parse_u32(event["event_id"], "event_id")
        except (KeyError, TypeError, ValueError):
            continue
        event_ticker = str(event.get("event_ticker", ""))
        series_ticker = str(event.get("series_ticker", ""))
        metadata[event_id] = {
            "event_ticker": event_ticker,
            "series_ticker": series_ticker,
            "event_domain": _infer_event_domain(event_ticker, series_ticker),
        }
    return metadata


def _load_overlay_event_metadata(config_path: Path, overlay_path: str | Path | None) -> dict[str, dict[str, Any]]:
    path = Path(overlay_path) if overlay_path is not None else config_path.with_name("event_metadata.json")
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    events_payload = payload.get("events") or {}
    if isinstance(events_payload, list):
        return {
            str(event.get("event_ticker", "")): dict(event)
            for event in events_payload
            if event.get("event_ticker")
        }
    if isinstance(events_payload, dict):
        return {
            str(event_ticker): dict(metadata)
            for event_ticker, metadata in events_payload.items()
            if event_ticker and isinstance(metadata, dict)
        }
    return {}


def _parse_nonnegative_i64(value: Any, field_name: str) -> int:
    parsed = int(value or 0)
    if parsed < 0:
        raise ValueError(f"{field_name} must be non-negative: {value!r}")
    return parsed


def load_app_config_index(
    path: str | Path,
    report_path: str | Path | None = None,
    metadata_path: str | Path | None = None,
) -> AppConfigIndex:
    config_path = Path(path)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    report_metadata = _load_report_event_metadata(config_path, report_path)
    overlay_metadata = _load_overlay_event_metadata(config_path, metadata_path)
    runtime = config.get("runtime") or {}
    shard_count = int(runtime.get("shard_count", 0))
    if shard_count <= 0:
        raise ValueError("runtime.shard_count must be positive")

    routes: list[AppMarketRoute] = []
    events: list[AppEventRoute] = []
    next_shard_event_index: dict[int, int] = defaultdict(int)

    for event_payload in (config.get("universe") or {}).get("events") or []:
        event_id = _parse_u32(event_payload["event_id"], "event_id")
        fallback_metadata = report_metadata.get(event_id, {})
        affinity_key = _parse_u64(event_payload["affinity_key"], "affinity_key")
        topology = str(event_payload["topology"])
        topology_code = _topology_code(topology)
        shard_index = affinity_key % shard_count
        shard_event_index = next_shard_event_index[shard_index]
        next_shard_event_index[shard_index] += 1
        event_ticker = str(event_payload.get("event_ticker") or fallback_metadata.get("event_ticker", ""))
        series_ticker = str(event_payload.get("series_ticker") or fallback_metadata.get("series_ticker", ""))
        overlay_event = overlay_metadata.get(event_ticker, {})
        event_category = str(event_payload.get("event_category") or overlay_event.get("event_category", ""))
        event_domain = str(
            event_payload.get("event_domain") or
            overlay_event.get("event_domain") or
            fallback_metadata.get("event_domain") or
            _infer_event_domain(event_ticker, series_ticker, event_category)
        )
        event_time_s = _parse_nonnegative_i64(
            event_payload.get("event_time_s") or overlay_event.get("event_time_s", 0),
            "event_time_s",
        )
        event_close_time_s = _parse_nonnegative_i64(
            event_payload.get("event_close_time_s") or overlay_event.get("event_close_time_s", 0),
            "event_close_time_s",
        )
        event_expected_expiration_time_s = _parse_nonnegative_i64(
            event_payload.get("event_expected_expiration_time_s") or
            overlay_event.get("event_expected_expiration_time_s", 0),
            "event_expected_expiration_time_s",
        )
        event_expiration_time_s = _parse_nonnegative_i64(
            event_payload.get("event_expiration_time_s") or overlay_event.get("event_expiration_time_s", 0),
            "event_expiration_time_s",
        )
        market_metadata = overlay_event.get("markets") if isinstance(overlay_event.get("markets"), dict) else {}

        event_markets: list[AppMarketRoute] = []
        for event_market_index, market_payload in enumerate(event_payload.get("markets") or []):
            market_ticker = str(market_payload["kalshi_ticker"])
            overlay_market = market_metadata.get(market_ticker, {}) if isinstance(market_metadata, dict) else {}
            route = AppMarketRoute(
                market_ticker=market_ticker,
                market_id=_parse_u32(market_payload["market_id"], "market_id"),
                event_id=event_id,
                affinity_key=affinity_key,
                topology=topology,
                topology_code=topology_code,
                shard_index=shard_index,
                shard_event_index=shard_event_index,
                event_market_index=event_market_index,
                strike_key=int(market_payload.get("strike_key", event_market_index)),
                tradeable=bool(market_payload.get("tradeable", False)),
                price_level_structure=str(market_payload.get("price_level_structure", "linear_cent")),
                event_ticker=event_ticker,
                series_ticker=series_ticker,
                event_category=event_category,
                event_domain=event_domain,
                event_time_s=event_time_s,
                event_close_time_s=event_close_time_s,
                event_expected_expiration_time_s=event_expected_expiration_time_s,
                event_expiration_time_s=event_expiration_time_s,
                market_time_s=_parse_nonnegative_i64(
                    market_payload.get("market_time_s") or overlay_market.get("market_time_s", event_time_s),
                    "market_time_s",
                ),
                market_close_time_s=_parse_nonnegative_i64(
                    market_payload.get("market_close_time_s") or overlay_market.get("market_close_time_s", 0),
                    "market_close_time_s",
                ),
                market_expected_expiration_time_s=_parse_nonnegative_i64(
                    market_payload.get("market_expected_expiration_time_s") or
                    overlay_market.get("market_expected_expiration_time_s", 0),
                    "market_expected_expiration_time_s",
                ),
                market_expiration_time_s=_parse_nonnegative_i64(
                    market_payload.get("market_expiration_time_s") or overlay_market.get("market_expiration_time_s", 0),
                    "market_expiration_time_s",
                ),
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
                event_ticker=event_ticker,
                series_ticker=series_ticker,
                event_category=event_category,
                event_domain=event_domain,
                event_time_s=event_time_s,
                event_close_time_s=event_close_time_s,
                event_expected_expiration_time_s=event_expected_expiration_time_s,
                event_expiration_time_s=event_expiration_time_s,
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
