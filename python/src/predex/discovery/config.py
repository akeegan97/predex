from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

from .affinity import stable_affinity_key, stable_event_id, stable_market_id
from .classifier import classify_event
from .models import ClassifiedEvent, EventRecord, TopologyKind


@dataclass(slots=True)
class PipelineSettings:
    shard_count: int = 4
    frame_pool_capacity: int = 8192
    io_to_router_capacity: int = 4096
    router_to_logger_capacity: int = 4096
    shard_input_capacity: int = 1024
    shard_to_logger_capacity: int = 1024

    def to_dict(self) -> dict[str, int]:
        return {
            "frame_pool_capacity": self.frame_pool_capacity,
            "shard_count": self.shard_count,
            "io_to_router_capacity": self.io_to_router_capacity,
            "router_to_logger_capacity": self.router_to_logger_capacity,
            "shard_input_capacity": self.shard_input_capacity,
            "shard_to_logger_capacity": self.shard_to_logger_capacity,
        }


@dataclass(slots=True)
class CredentialSettings:
    key_id_env: str = "KALSHI_KEY_ID"
    private_key_pem_env: str = "KALSHI_PRIVATE_KEY_PEM"

    def to_dict(self) -> dict[str, str]:
        return {
            "key_id_env": self.key_id_env,
            "private_key_pem_env": self.private_key_pem_env,
        }


@dataclass(slots=True)
class DiscoverySettings:
    endpoint: str = "wss://api.elections.kalshi.com/trade-api/ws/v2"
    channels: tuple[str, ...] = ("trade", "orderbook_delta")
    lifecycle_channels: tuple[str, ...] = ("market_lifecycle_v2",)
    credentials: CredentialSettings = field(default_factory=CredentialSettings)

    def to_kalshi_dict(self, market_tickers: list[str]) -> dict[str, Any]:
        return {
            "endpoint": self.endpoint,
            "channels": list(self.channels),
            "lifecycle_channels": list(self.lifecycle_channels),
            "market_tickers": market_tickers,
            "credentials": self.credentials.to_dict(),
        }


@dataclass(slots=True)
class OmsTransportSettings:
    enabled: bool = False
    rest_endpoint: str = "https://api.elections.kalshi.com"
    private_ws_endpoint: str = "wss://api.elections.kalshi.com/trade-api/ws/v2"
    private_ws_channels: tuple[str, ...] = ("user_orders",)
    max_session_loss_ticks: int = 0
    available_capital_ticks: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "rest_endpoint": self.rest_endpoint,
            "private_ws_endpoint": self.private_ws_endpoint,
            "private_ws_channels": list(self.private_ws_channels),
            "max_session_loss_ticks": self.max_session_loss_ticks,
            "available_capital_ticks": self.available_capital_ticks,
        }


@dataclass(slots=True)
class LocalRiskSettings:
    # Maximum absolute net filled position (long or short) per market. 0 = disabled.
    max_net_position_lots_per_market: int = 0
    # Reject intents for markets closing within this many seconds. 0 = disabled.
    min_seconds_to_close: int = 0
    trading_enabled: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_net_position_lots_per_market": self.max_net_position_lots_per_market,
            "min_seconds_to_close": self.min_seconds_to_close,
            "trading_enabled": self.trading_enabled,
        }


@dataclass(frozen=True, slots=True)
class GeneratedEventConfig:
    event_ticker: str
    series_ticker: str
    topology_kind: TopologyKind
    market_count: int
    market_tickers: tuple[str, ...]
    event_id: int
    affinity_key: int
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_ticker": self.event_ticker,
            "series_ticker": self.series_ticker,
            "topology_kind": self.topology_kind.value,
            "market_count": self.market_count,
            "market_tickers": list(self.market_tickers),
            "event_id": self.event_id,
            "affinity_key": self.affinity_key,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class SkippedEventConfig:
    event_ticker: str
    series_ticker: str
    topology_kind: TopologyKind
    market_count: int
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_ticker": self.event_ticker,
            "series_ticker": self.series_ticker,
            "topology_kind": self.topology_kind.value,
            "market_count": self.market_count,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class TraderConfigBuildResult:
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


def _parse_iso_to_unix_seconds(time_str: str) -> int:
    if not time_str:
        return 0
    try:
        normalized = time_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except (ValueError, OSError):
        return 0


def _normalize_topology_set(
    values: Iterable[TopologyKind | str] | None,
) -> set[TopologyKind] | None:
    if values is None:
        return None
    normalized: set[TopologyKind] = set()
    for value in values:
        normalized.add(value if isinstance(value, TopologyKind) else TopologyKind(value))
    return normalized


def _validate_classified_event(classified_event: ClassifiedEvent) -> None:
    if not classified_event.markets:
        raise ValueError(f"event {classified_event.event.event_ticker} has no classified markets")

    market_tickers = [classified_market.market.ticker for classified_market in classified_event.markets]
    if len(set(market_tickers)) != len(market_tickers):
        raise ValueError(f"event {classified_event.event.event_ticker} contains duplicate market tickers")

    if classified_event.topology_kind == TopologyKind.MONOTONIC_CHAIN:
        if len(classified_event.markets) < 2:
            raise ValueError(
                f"event {classified_event.event.event_ticker} cannot be monotonic_chain with fewer than two markets"
            )
        strike_keys = [classified_market.strike_key for classified_market in classified_event.markets]
        if len(set(strike_keys)) != len(strike_keys):
            raise ValueError(
                f"event {classified_event.event.event_ticker} contains duplicate monotonic order keys"
            )


def build_trader_config_result(
    events: list[EventRecord],
    *,
    discovery: DiscoverySettings | None = None,
    pipeline: PipelineSettings | None = None,
    oms_transport: OmsTransportSettings | None = None,
    local_risk: LocalRiskSettings | None = None,
    tape_output_path: str = "predex_tape.bin",
    audit_output_path: str = "predex_audit.jsonl",
    include_topologies: Iterable[TopologyKind | str] | None = None,
    exclude_topologies: Iterable[TopologyKind | str] | None = None,
    market_limit: int | None = None,
) -> TraderConfigBuildResult:
    if not events:
        raise ValueError("at least one event is required to build a trader config")
    if market_limit is not None and market_limit <= 0:
        raise ValueError("market_limit must be greater than zero when provided")

    discovery = discovery or DiscoverySettings()
    pipeline = pipeline or PipelineSettings()
    oms_transport = oms_transport or OmsTransportSettings()
    local_risk = local_risk or LocalRiskSettings()
    included_filter = _normalize_topology_set(include_topologies)
    excluded_filter = _normalize_topology_set(exclude_topologies) or set()

    routes: list[dict[str, Any]] = []
    market_tickers: list[str] = []
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

        classified_event = classify_event(event)
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
                    reason=f"excluded by market_limit: adding {len(classified_event.markets)} markets would exceed limit {market_limit}",
                )
            )
            continue

        event_id = stable_event_id(classified_event.event.event_ticker)
        affinity_key = stable_affinity_key(classified_event.event.event_ticker)
        existing_event_ticker = seen_event_ids.get(event_id)
        if existing_event_ticker is not None and existing_event_ticker != classified_event.event.event_ticker:
            raise ValueError(
                f"event_id collision between {existing_event_ticker} and {classified_event.event.event_ticker}"
            )
        seen_event_ids[event_id] = classified_event.event.event_ticker

        included_market_tickers: list[str] = []
        for classified_market in classified_event.markets:
            market = classified_market.market
            market_id = stable_market_id(market.ticker)
            existing_market_ticker = seen_market_ids.get(market_id)
            if existing_market_ticker is not None and existing_market_ticker != market.ticker:
                raise ValueError(f"market_id collision between {existing_market_ticker} and {market.ticker}")
            seen_market_ids[market_id] = market.ticker

            if market.ticker in seen_market_tickers:
                raise ValueError(f"duplicate market ticker across included events: {market.ticker}")
            seen_market_tickers.add(market.ticker)

            market_tickers.append(market.ticker)
            included_market_tickers.append(market.ticker)
            routes.append(
                {
                    "market_ticker": market.ticker,
                    "market_id": market_id,
                    "event_id": event_id,
                    "affinity_key": affinity_key,
                    "topology_kind": topology.value,
                    "strike_key": classified_market.strike_key,
                    "close_time_s": _parse_iso_to_unix_seconds(market.primary_time_reference()),
                    "tradeable": market.status == "active",
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

    if not routes:
        raise ValueError("no events remained after classification and topology filtering")

    config = {
        "kalshi": discovery.to_kalshi_dict(market_tickers),
        "market_routes": routes,
        "pipeline": pipeline.to_dict(),
        "tape": {"output_path": tape_output_path},
        "audit": {"output_path": audit_output_path},
        "oms_transport": oms_transport.to_dict(),
        "local_risk": local_risk.to_dict(),
    }
    return TraderConfigBuildResult(
        config=config,
        included_events=tuple(included_events),
        skipped_events=tuple(skipped_events),
        topology_counts=dict(topology_counts),
    )


def build_trader_config(
    events: list[EventRecord],
    *,
    discovery: DiscoverySettings | None = None,
    pipeline: PipelineSettings | None = None,
    oms_transport: OmsTransportSettings | None = None,
    local_risk: LocalRiskSettings | None = None,
    tape_output_path: str = "predex_tape.bin",
    audit_output_path: str = "predex_audit.jsonl",
    include_topologies: Iterable[TopologyKind | str] | None = None,
    exclude_topologies: Iterable[TopologyKind | str] | None = None,
    market_limit: int | None = None,
) -> dict[str, Any]:
    return build_trader_config_result(
        events,
        discovery=discovery,
        pipeline=pipeline,
        oms_transport=oms_transport,
        local_risk=local_risk,
        tape_output_path=tape_output_path,
        audit_output_path=audit_output_path,
        include_topologies=include_topologies,
        exclude_topologies=exclude_topologies,
        market_limit=market_limit,
    ).config
