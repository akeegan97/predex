from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Callable, Iterable

from .affinity import stable_affinity_key, stable_event_id, stable_market_id
from .classifier import classify_config_events
from .config import (
    CredentialSettings,
    GeneratedEventConfig,
    SkippedEventConfig,
    _normalize_topology_set,
    _stable_config_event_id,
    _validate_classified_event,
)
from .models import ClassifiedEvent, EventRecord, MarketRecord, TopologyKind


@dataclass(slots=True)
class RuntimeSettings:
    shard_count: int = 4
    shard_queue_capacity: int = 8192
    router_queue_capacity: int = 8192
    frame_pool_capacity: int = 8192
    operator_queue_capacity: int = 64
    operator_socket_path: str = "/tmp/predex_operator.sock"
    market_data_tape_path: str = "logs/live/predex_tape.bin"

    def to_dict(self) -> dict[str, Any]:
        return {
            "shard_count": self.shard_count,
            "shard_queue_capacity": self.shard_queue_capacity,
            "router_queue_capacity": self.router_queue_capacity,
            "frame_pool_capacity": self.frame_pool_capacity,
            "operator_queue_capacity": self.operator_queue_capacity,
            "operator_socket_path": self.operator_socket_path,
            "market_data_tape_path": self.market_data_tape_path,
        }


@dataclass(slots=True)
class KalshiMarketDataSettings:
    enable_market_data: bool = False
    channels: tuple[str, ...] = ("orderbook_delta", "trade")

    def to_dict(self) -> dict[str, Any]:
        return {
            "enable_market_data": self.enable_market_data,
            "channels": list(self.channels),
        }


@dataclass(slots=True)
class KalshiSettings:
    credentials: CredentialSettings = field(default_factory=CredentialSettings)
    market_data: KalshiMarketDataSettings = field(default_factory=KalshiMarketDataSettings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "auth": self.credentials.to_dict(),
            "market_data": self.market_data.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class AppConfigBuildResult:
    config: dict[str, Any]
    included_events: tuple[GeneratedEventConfig, ...]
    skipped_events: tuple[SkippedEventConfig, ...]
    topology_counts: dict[str, int]

    def report(self) -> dict[str, Any]:
        return {
            "included_event_count": len(self.included_events),
            "included_market_count": sum(event.market_count for event in self.included_events),
            "skipped_event_count": len(self.skipped_events),
            "topology_counts": self.topology_counts,
            "included_events": [event.to_dict() for event in self.included_events],
            "skipped_events": [event.to_dict() for event in self.skipped_events],
        }


def _event_config_name(classified_event: ClassifiedEvent) -> str:
    if not classified_event.synthetic_key:
        return classified_event.event.event_ticker
    return f"{classified_event.event.event_ticker}::{classified_event.synthetic_key}"


def _parse_time_s(value: str) -> int:
    if not value:
        return 0
    text = value.strip()
    if not text:
        return 0
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return 0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.timestamp())


def _event_time_s(markets: Iterable[MarketRecord]) -> int:
    times = [
        parsed
        for market in markets
        if (parsed := _parse_time_s(market.primary_time_reference())) > 0
    ]
    return min(times) if times else 0


def _min_positive(values: Iterable[int]) -> int:
    positives = [value for value in values if value > 0]
    return min(positives) if positives else 0


def _event_time_fields(markets: Iterable[MarketRecord]) -> dict[str, int]:
    market_list = list(markets)
    return {
        "event_time_s": _event_time_s(market_list),
        "event_close_time_s": _min_positive(_parse_time_s(market.close_time) for market in market_list),
        "event_expected_expiration_time_s": _min_positive(
            _parse_time_s(market.expected_expiration_time) for market in market_list
        ),
        "event_expiration_time_s": _min_positive(_parse_time_s(market.expiration_time) for market in market_list),
    }


def _event_domain(event: EventRecord) -> str:
    category = event.category.strip().lower()
    searchable = " ".join(
        part.lower()
        for part in (event.event_ticker, event.series_ticker, event.title, event.sub_title, category)
        if part
    )

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
    if category:
        return category.replace(" ", "_")
    return "other"


def _market_config(market: MarketRecord, market_id: int, strike_key: int) -> dict[str, Any]:
    market_time_s = _parse_time_s(market.primary_time_reference())
    return {
        "market_id": str(market_id),
        "kalshi_ticker": market.ticker,
        "strike_key": strike_key,
        "tradeable": market.status == "active",
        "price_level_structure": market.price_level_structure or "linear_cent",
        "market_title": market.title,
        "market_subtitle": market.subtitle,
        "market_time_s": market_time_s,
        "market_close_time_s": _parse_time_s(market.close_time),
        "market_expected_expiration_time_s": _parse_time_s(market.expected_expiration_time),
        "market_expiration_time_s": _parse_time_s(market.expiration_time),
    }


def _resolve_u32_id(
    key: str,
    seen_ids: dict[int, str],
    *,
    id_kind: str,
    stable_id: Callable[[str], int],
) -> int:
    candidate = stable_id(key)
    existing_key = seen_ids.get(candidate)
    if existing_key is None or existing_key == key:
        seen_ids[candidate] = key
        return candidate

    attempt = 1
    while True:
        candidate = stable_id(f"{key}#{attempt}")
        existing_key = seen_ids.get(candidate)
        if existing_key is None or existing_key == key:
            seen_ids[candidate] = key
            return candidate
        attempt += 1
        if attempt > 1024:
            raise ValueError(f"unable to resolve {id_kind} collision for {key}")


def build_app_config_result(
    events: list[EventRecord],
    *,
    runtime: RuntimeSettings | None = None,
    kalshi: KalshiSettings | None = None,
    include_topologies: Iterable[TopologyKind | str] | None = None,
    exclude_topologies: Iterable[TopologyKind | str] | None = None,
    market_limit: int | None = None,
) -> AppConfigBuildResult:
    if not events:
        raise ValueError("at least one event is required to build an app config")
    if market_limit is not None and market_limit <= 0:
        raise ValueError("market_limit must be greater than zero when provided")

    runtime = runtime or RuntimeSettings()
    kalshi = kalshi or KalshiSettings()
    included_filter = _normalize_topology_set(include_topologies)
    excluded_filter = _normalize_topology_set(exclude_topologies) or set()

    universe_events: list[dict[str, Any]] = []
    included_events: list[GeneratedEventConfig] = []
    skipped_events: list[SkippedEventConfig] = []
    topology_counts: Counter[str] = Counter()

    seen_event_ids: dict[int, str] = {}
    seen_market_ids: dict[int, str] = {}
    seen_event_tickers: set[str] = set()
    seen_market_tickers: set[str] = set()
    included_market_count = 0

    for event in events:
        if event.event_ticker in seen_event_tickers:
            continue
        seen_event_tickers.add(event.event_ticker)

        for classified_event in classify_config_events(event):
            _validate_classified_event(classified_event)

            topology = classified_event.topology_kind
            if included_filter is not None and topology not in included_filter:
                skipped_events.append(
                    SkippedEventConfig(
                        event_ticker=event.event_ticker,
                        series_ticker=event.series_ticker,
                        topology_kind=topology,
                        market_count=len(classified_event.markets),
                        reason=f"excluded by include_topologies filter: {topology.value}",
                    )
                )
                continue
            if topology in excluded_filter:
                skipped_events.append(
                    SkippedEventConfig(
                        event_ticker=event.event_ticker,
                        series_ticker=event.series_ticker,
                        topology_kind=topology,
                        market_count=len(classified_event.markets),
                        reason=f"excluded by exclude_topologies filter: {topology.value}",
                    )
                )
                continue

            if market_limit is not None and included_market_count + len(classified_event.markets) > market_limit:
                skipped_events.append(
                    SkippedEventConfig(
                        event_ticker=event.event_ticker,
                        series_ticker=event.series_ticker,
                        topology_kind=topology,
                        market_count=len(classified_event.markets),
                        reason=(
                            f"excluded by market_limit: adding {len(classified_event.markets)} "
                            f"markets would exceed limit {market_limit}"
                        ),
                    )
                )
                continue

            base_event_id = _stable_config_event_id(classified_event)
            affinity_key = stable_affinity_key(classified_event.event.event_ticker)
            event_name = _event_config_name(classified_event)
            event_id = _resolve_u32_id(
                event_name,
                seen_event_ids,
                id_kind="event_id",
                stable_id=lambda key: base_event_id if key == event_name else stable_event_id(key),
            )
            event_time_fields = _event_time_fields(market.market for market in classified_event.markets)

            market_configs: list[dict[str, Any]] = []
            included_market_tickers: list[str] = []
            for classified_market in classified_event.markets:
                market = classified_market.market
                market_id = _resolve_u32_id(
                    market.ticker,
                    seen_market_ids,
                    id_kind="market_id",
                    stable_id=stable_market_id,
                )

                if market.ticker in seen_market_tickers:
                    raise ValueError(f"duplicate market ticker across included events: {market.ticker}")
                seen_market_tickers.add(market.ticker)

                included_market_tickers.append(market.ticker)
                market_configs.append(_market_config(market, market_id, classified_market.strike_key))

            universe_events.append(
                {
                    "event_id": str(event_id),
                    "affinity_key": str(affinity_key),
                    "topology": topology.value,
                    "event_ticker": classified_event.event.event_ticker,
                    "series_ticker": classified_event.event.series_ticker,
                    "event_title": classified_event.event.title,
                    "event_sub_title": classified_event.event.sub_title,
                    "event_category": classified_event.event.category,
                    "event_domain": _event_domain(classified_event.event),
                    **event_time_fields,
                    "markets": market_configs,
                }
            )
            included_market_count += len(classified_event.markets)
            topology_counts[topology.value] += 1
            included_events.append(
                GeneratedEventConfig(
                    event_ticker=classified_event.event.event_ticker,
                    series_ticker=classified_event.event.series_ticker,
                    topology_kind=topology,
                    market_count=len(classified_event.markets),
                    market_tickers=tuple(included_market_tickers),
                    event_id=event_id,
                    affinity_key=affinity_key,
                    reason=classified_event.reason,
                )
            )

    if not universe_events:
        raise ValueError("no events remained after classification and topology filtering")

    return AppConfigBuildResult(
        config={
            "runtime": runtime.to_dict(),
            "kalshi": kalshi.to_dict(),
            "universe": {"events": universe_events},
        },
        included_events=tuple(included_events),
        skipped_events=tuple(skipped_events),
        topology_counts=dict(topology_counts),
    )


def build_app_config(
    events: list[EventRecord],
    *,
    runtime: RuntimeSettings | None = None,
    kalshi: KalshiSettings | None = None,
    include_topologies: Iterable[TopologyKind | str] | None = None,
    exclude_topologies: Iterable[TopologyKind | str] | None = None,
    market_limit: int | None = None,
) -> dict[str, Any]:
    return build_app_config_result(
        events,
        runtime=runtime,
        kalshi=kalshi,
        include_topologies=include_topologies,
        exclude_topologies=exclude_topologies,
        market_limit=market_limit,
    ).config
