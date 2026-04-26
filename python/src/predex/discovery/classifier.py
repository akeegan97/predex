from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import re

from .models import ClassifiedEvent, ClassifiedMarket, EventRecord, MarketRecord, TopologyKind

_STRIKE_SCALE = Decimal("1000000")
_GREATER_TYPES = {"greater", "greater_or_equal", "greater_equal", "above", "over"}
_LESS_TYPES = {"less", "less_or_equal", "less_equal", "below", "under"}
_NUMERIC_CHAIN_TYPES = _GREATER_TYPES | _LESS_TYPES
_TIME_BEFORE_HINTS = (
    " before ",
    " by ",
    " no later than ",
    " on or before ",
)
_TIME_AFTER_HINTS = (
    " after ",
    " no earlier than ",
    " on or after ",
)
_TICKER_ENTITY_RE = re.compile(r"^(?P<entity>[A-Za-z]+)(?P<strike>-?\d+(?:\.\d+)?)$")


def _decimal_strike(value: object | None) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _scaled_key(value: Decimal) -> int:
    return int(value * _STRIKE_SCALE)


def _parse_timestamp_ns(value: str) -> int | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)

    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = parsed - epoch
    return (
        delta.days * 86_400 * 1_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _unique_keys(markets: list[ClassifiedMarket]) -> bool:
    return len({market.strike_key for market in markets}) == len(markets)


def _normalized_text(value: str) -> str:
    return f" {value.strip().lower()} " if value.strip() else ""


def _time_direction_hint(event: EventRecord) -> str | None:
    before_hits = 0
    after_hits = 0

    event_fragments = [
        event.event_ticker,
        event.title,
        event.sub_title,
    ]
    market_fragments = [
        fragment
        for market in event.markets
        for fragment in (
            market.ticker,
            market.title,
            market.subtitle,
            market.yes_sub_title,
            market.no_sub_title,
        )
    ]

    for fragment in (*event_fragments, *market_fragments):
        normalized = _normalized_text(fragment)
        if not normalized:
            continue
        before_hits += sum(hint in normalized for hint in _TIME_BEFORE_HINTS)
        after_hits += sum(hint in normalized for hint in _TIME_AFTER_HINTS)

    if before_hits and after_hits:
        return None
    if before_hits:
        return "before"
    if after_hits:
        return "after"
    return None


def _ordered_classification(
    event: EventRecord,
    markets: list[ClassifiedMarket],
    *,
    reason: str,
) -> ClassifiedEvent | None:
    if not markets or not _unique_keys(markets):
        return None
    ordered_markets = tuple(
        sorted(markets, key=lambda market: (market.strike_key, market.market.ticker))
    )
    return ClassifiedEvent(
        event=event,
        topology_kind=TopologyKind.MONOTONIC_CHAIN,
        markets=ordered_markets,
        reason=reason,
    )


def _numeric_monotonic_key(market: MarketRecord) -> int | None:
    strike_type = market.strike_type.strip().lower()
    floor_strike = _decimal_strike(market.floor_strike)
    cap_strike = _decimal_strike(market.cap_strike)

    if strike_type in _GREATER_TYPES:
        threshold = floor_strike if floor_strike is not None else cap_strike
        if threshold is None:
            return None
        return _scaled_key(threshold)

    if strike_type in _LESS_TYPES:
        threshold = cap_strike if cap_strike is not None else floor_strike
        if threshold is None:
            return None
        return -_scaled_key(threshold)

    return None


def _numeric_entity_key(market: MarketRecord) -> str | None:
    if market.custom_strike:
        return "|".join(
            f"{str(key).strip().lower()}={str(value).strip().lower()}"
            for key, value in sorted(market.custom_strike.items(), key=lambda item: str(item[0]).lower())
        )

    ticker_prefix = f"{market.event_ticker}-"
    if market.event_ticker and market.ticker.startswith(ticker_prefix):
        suffix = market.ticker[len(ticker_prefix):]
        match = _TICKER_ENTITY_RE.match(suffix)
        if match is not None:
            return match.group("entity").lower()
    return None


def _classify_numeric_monotonic_chain(event: EventRecord) -> ClassifiedEvent | None:
    strike_types = {market.strike_type.strip().lower() for market in event.markets if market.strike_type}
    if not strike_types or not strike_types.issubset(_NUMERIC_CHAIN_TYPES):
        return None

    classified_markets: list[ClassifiedMarket] = []
    for market in event.markets:
        strike_key = _numeric_monotonic_key(market)
        if strike_key is None:
            return None
        classified_markets.append(ClassifiedMarket(market=market, strike_key=strike_key))

    return _ordered_classification(
        event,
        classified_markets,
        reason="all markets expose comparable cumulative numeric thresholds",
    )


def _classify_event_without_numeric_split(event: EventRecord) -> ClassifiedEvent:
    if len(event.markets) == 1:
        return ClassifiedEvent(
            event=event,
            topology_kind=TopologyKind.SINGLE_MARKET,
            markets=(ClassifiedMarket(market=event.markets[0], strike_key=0),),
            reason="event only contains one market",
        )

    if event.mutually_exclusive:
        return ClassifiedEvent(
            event=event,
            topology_kind=TopologyKind.MUTUALLY_EXCLUSIVE,
            markets=tuple(
                ClassifiedMarket(market=market, strike_key=0) for market in event.markets
            ),
            reason="event is explicitly flagged as mutually_exclusive",
        )

    numeric_monotonic_chain = _classify_numeric_monotonic_chain(event)
    if numeric_monotonic_chain is not None:
        return numeric_monotonic_chain

    close_time_monotonic_chain = _classify_close_time_monotonic_chain(event)
    if close_time_monotonic_chain is not None:
        return close_time_monotonic_chain

    structural_mutex = _classify_structural_mutex(event)
    if structural_mutex is not None:
        return structural_mutex

    return ClassifiedEvent(
        event=event,
        topology_kind=TopologyKind.UNORDERED_GROUP,
        markets=tuple(ClassifiedMarket(market=market, strike_key=0) for market in event.markets),
        reason="event lacks a trustworthy monotonic order key and is not explicitly or structurally mutually exclusive",
    )


def classify_config_events(event: EventRecord) -> tuple[ClassifiedEvent, ...]:
    strike_types = {market.strike_type.strip().lower() for market in event.markets if market.strike_type}
    if not strike_types or not strike_types.issubset(_NUMERIC_CHAIN_TYPES):
        return (_classify_event_without_numeric_split(event),)

    markets_with_keys: list[tuple[ClassifiedMarket, str | None]] = []
    for market in event.markets:
        strike_key = _numeric_monotonic_key(market)
        if strike_key is None:
            return (_classify_event_without_numeric_split(event),)
        markets_with_keys.append(
            (ClassifiedMarket(market=market, strike_key=strike_key), _numeric_entity_key(market))
        )

    entity_keys = {entity_key for _, entity_key in markets_with_keys if entity_key is not None}
    if len(entity_keys) <= 1:
        return (_classify_event_without_numeric_split(event),)
    if any(entity_key is None for _, entity_key in markets_with_keys):
        return (_classify_event_without_numeric_split(event),)

    grouped_markets: dict[str, list[ClassifiedMarket]] = {}
    for classified_market, entity_key in markets_with_keys:
        assert entity_key is not None
        grouped_markets.setdefault(entity_key, []).append(classified_market)

    if any(len(group) < 2 for group in grouped_markets.values()):
        return (_classify_event_without_numeric_split(event),)

    classified_events: list[ClassifiedEvent] = []
    for entity_key, grouped in sorted(grouped_markets.items()):
        ordered = _ordered_classification(
            event,
            grouped,
            reason=(
                "all markets expose comparable cumulative numeric thresholds within entity "
                f"{entity_key}"
            ),
        )
        if ordered is None:
            return (_classify_event_without_numeric_split(event),)
        classified_events.append(
            ClassifiedEvent(
                event=ordered.event,
                topology_kind=ordered.topology_kind,
                markets=ordered.markets,
                reason=ordered.reason,
                synthetic_key=f"numeric:{entity_key}",
            )
        )

    return tuple(classified_events)


def _classify_close_time_monotonic_chain(event: EventRecord) -> ClassifiedEvent | None:
    reference_times = [market.primary_time_reference() for market in event.markets]
    if any(not reference_time for reference_time in reference_times):
        return None

    time_direction = _time_direction_hint(event)
    if time_direction is None:
        return None

    parsed_keys: list[ClassifiedMarket] = []
    for market in event.markets:
        timestamp_ns = _parse_timestamp_ns(market.primary_time_reference())
        if timestamp_ns is None:
            return None
        strike_key = timestamp_ns if time_direction == "after" else -timestamp_ns
        parsed_keys.append(ClassifiedMarket(market=market, strike_key=strike_key))

    custom_values = {
        tuple(sorted(market.custom_strike.items()))
        for market in event.markets
        if market.custom_strike
    }
    strike_types = {market.strike_type.strip().lower() for market in event.markets if market.strike_type}

    if not _unique_keys(parsed_keys):
        return None
    if not strike_types:
        return _ordered_classification(
            event,
            parsed_keys,
            reason=f"markets form a unique {time_direction}-style close-time ladder without conflicting strike metadata",
        )
    if strike_types == {"custom"} and len(custom_values) == 1:
        return _ordered_classification(
            event,
            parsed_keys,
            reason=f"markets share one custom entity and form a unique {time_direction}-style close-time ladder",
        )
    return None


def _classify_structural_mutex(event: EventRecord) -> ClassifiedEvent | None:
    close_times = {market.primary_time_reference() for market in event.markets}
    if len(close_times) != 1 or "" in close_times:
        return None

    strike_types = {market.strike_type.strip().lower() for market in event.markets if market.strike_type}
    custom_values = {
        tuple(sorted(market.custom_strike.items()))
        for market in event.markets
        if market.custom_strike
    }

    if strike_types in ({"custom"}, {"structured"}) and len(custom_values) == len(event.markets):
        return ClassifiedEvent(
            event=event,
            topology_kind=TopologyKind.MUTUALLY_EXCLUSIVE,
            markets=tuple(
                ClassifiedMarket(market=market, strike_key=0) for market in event.markets
            ),
            reason="markets share one decision horizon and each market has a distinct structured entity",
        )
    return None


def classify_event(event: EventRecord) -> ClassifiedEvent:
    if not event.event_ticker:
        raise ValueError("event_ticker is required for classification")
    if not event.markets:
        raise ValueError(f"event {event.event_ticker} does not contain any markets")

    numeric_config_events = classify_config_events(event)
    if len(numeric_config_events) > 1:
        return ClassifiedEvent(
            event=event,
            topology_kind=TopologyKind.UNORDERED_GROUP,
            markets=tuple(ClassifiedMarket(market=market, strike_key=0) for market in event.markets),
            reason="event contains multiple distinct numeric entities, so one global monotonic order is unsafe",
        )
    return _classify_event_without_numeric_split(event)
