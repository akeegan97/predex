from __future__ import annotations

import csv
import json
from collections import defaultdict
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .audit import SignalBundle
from .books import ReplayBookStore
from .config import ConfigIndex, EventRoute, MarketRoute
from .monotonic import SIDE_BUY, SIDE_SELL, evaluate_pair_state
from .tape import iter_market_events


@dataclass(frozen=True, slots=True)
class WindowSignal:
    window_id: int
    shard_id: int
    signal_id: int
    event_id: int
    easier_market_ticker: str
    harder_market_ticker: str
    signal_ts_ns: int
    edge_ticks: int
    accepted_leg_count: int
    rejected_leg_count: int
    transport_leg_count: int
    working_leg_count: int
    partial_fill_leg_count: int
    filled_leg_count: int
    canceled_leg_count: int
    uncertain_leg_count: int
    venue_rejected_leg_count: int
    any_fill: bool
    one_leg_fill: bool
    terminal_recv_ns: int | None
    terminal_after_window: bool


@dataclass(frozen=True, slots=True)
class SignalWindow:
    window_id: int
    event_id: int
    event_ticker: str
    easier_market_ticker: str
    harder_market_ticker: str
    start_record_index: int
    end_record_index: int
    start_recv_ts_ns: int | None
    end_recv_ts_ns: int | None
    duration_ms: float | None
    censored: bool
    start_edge_ticks: int
    peak_edge_ticks: int
    end_edge_ticks: int
    max_executable_qty_lots: int
    signal_count: int
    distinct_signal_edges: int
    accepted_signal_count: int
    rejected_signal_count: int
    transported_signal_count: int
    venue_rejected_signal_count: int
    any_fill_signal_count: int
    one_leg_fill_signal_count: int
    terminal_after_window_signal_count: int
    first_signal_ts_ns: int | None
    last_signal_ts_ns: int | None


@dataclass(frozen=True, slots=True)
class SignalWindows:
    event_id: int
    event_ticker: str
    market_tickers: tuple[str, ...]
    windows: tuple[SignalWindow, ...]
    signals: tuple[WindowSignal, ...]


@dataclass(frozen=True, slots=True)
class SignalEdgeLifetime:
    lifetime_id: int
    event_id: int
    event_ticker: str
    easier_market_ticker: str
    harder_market_ticker: str
    edge_ticks: int
    start_record_index: int
    end_record_index: int
    start_recv_ts_ns: int | None
    end_recv_ts_ns: int | None
    duration_ms: float | None
    censored: bool
    max_executable_qty_lots: int
    signal_count: int
    accepted_signal_count: int
    rejected_signal_count: int
    transported_signal_count: int
    venue_rejected_signal_count: int
    any_fill_signal_count: int
    one_leg_fill_signal_count: int
    terminal_after_lifetime_signal_count: int
    first_signal_ts_ns: int | None
    last_signal_ts_ns: int | None


@dataclass(frozen=True, slots=True)
class SignalEdgeLifetimes:
    event_id: int
    event_ticker: str
    market_tickers: tuple[str, ...]
    lifetimes: tuple[SignalEdgeLifetime, ...]


@dataclass(frozen=True, slots=True)
class AllSignalEdgeLifetimes:
    lifetimes: tuple[SignalEdgeLifetime, ...]


@dataclass(slots=True)
class _ActiveWindow:
    event_id: int
    event_ticker: str
    easier_market_ticker: str
    harder_market_ticker: str
    start_record_index: int
    end_record_index: int
    start_recv_ts_ns: int | None
    end_recv_ts_ns: int | None
    start_edge_ticks: int
    peak_edge_ticks: int
    end_edge_ticks: int
    max_executable_qty_lots: int


@dataclass(slots=True)
class _ActiveEdgeLifetime:
    event_id: int
    event_ticker: str
    easier_market_ticker: str
    harder_market_ticker: str
    edge_ticks: int
    start_record_index: int
    end_record_index: int
    start_recv_ts_ns: int | None
    end_recv_ts_ns: int | None
    max_executable_qty_lots: int


def _duration_ms(start_recv_ts_ns: int | None, end_recv_ts_ns: int | None) -> float | None:
    if start_recv_ts_ns is None or end_recv_ts_ns is None or end_recv_ts_ns < start_recv_ts_ns:
        return None
    return (end_recv_ts_ns - start_recv_ts_ns) / 1_000_000.0


def _event_ticker(route: EventRoute) -> str:
    return route.markets[0].market_ticker.rsplit("-", 1)[0] if route.markets else ""


def _resolve_event_route(
    *,
    config_index: ConfigIndex,
    event_id: int | None,
    market_ticker: str | None,
) -> EventRoute:
    if event_id is None and market_ticker is None:
        raise ValueError("event_id or market_ticker is required")

    if market_ticker is not None:
        market = config_index.markets_by_ticker.get(market_ticker)
        if market is None:
            raise ValueError(f"market_ticker not found in config: {market_ticker}")
        if event_id is None:
            event_id = market.event_id
        elif event_id != market.event_id:
            raise ValueError("market_ticker does not belong to the requested event_id")

    assert event_id is not None
    event_route = config_index.events_by_id.get(event_id)
    if event_route is None:
        raise ValueError(f"event_id not found in config: {event_id}")
    return event_route


def _candidate_pair(bundle: SignalBundle, *, config_index: ConfigIndex) -> tuple[MarketRoute, MarketRoute] | None:
    event_route = config_index.events_by_id.get(bundle.event_id)
    if event_route is None or event_route.topology_kind != "monotonic_chain":
        return None

    legs = bundle.legs()
    buy_leg = next((event for event in legs if event.side == SIDE_BUY), None)
    sell_leg = next((event for event in legs if event.side == SIDE_SELL), None)
    easier_market = config_index.markets_by_id.get(buy_leg.market_id) if buy_leg is not None else None
    harder_market = config_index.markets_by_id.get(sell_leg.market_id) if sell_leg is not None else None

    ordered_markets = list(event_route.markets)
    market_index_by_id = {market.market_id: index for index, market in enumerate(ordered_markets)}

    if easier_market is None and harder_market is None:
        return None
    if easier_market is None and harder_market is not None:
        harder_index = market_index_by_id.get(harder_market.market_id)
        if harder_index is None or harder_index == 0:
            return None
        easier_market = ordered_markets[harder_index - 1]
    if harder_market is None and easier_market is not None:
        easier_index = market_index_by_id.get(easier_market.market_id)
        if easier_index is None or easier_index + 1 >= len(ordered_markets):
            return None
        harder_market = ordered_markets[easier_index + 1]
    if easier_market is None or harder_market is None:
        return None
    if easier_market.strike_key >= harder_market.strike_key:
        return None
    return (easier_market, harder_market)


def _window_signal_from_bundle(
    bundle: SignalBundle,
    *,
    window_id: int,
    easier_market_ticker: str,
    harder_market_ticker: str,
    window_end_recv_ts_ns: int | None,
) -> WindowSignal:
    accepted_leg_count = sum(1 for event in bundle.oms_decisions if event.decision_code == 1)
    rejected_leg_count = sum(1 for event in bundle.oms_decisions if event.decision_code == 2)
    lifecycle_by_kind = defaultdict(int)
    filled_leg_indexes: set[int] = set()
    terminal_recv_candidates: list[int] = []
    for event in bundle.oms_lifecycles:
        lifecycle_by_kind[event.lifecycle_kind] += 1
        if event.lifecycle_kind in {2, 3}:
            filled_leg_indexes.add(event.leg_index)
    for event in bundle.shard_reconciles:
        if event.terminal_recv_ns > 0:
            terminal_recv_candidates.append(event.terminal_recv_ns)
    terminal_recv_ns = min(terminal_recv_candidates) if terminal_recv_candidates else None
    any_fill = len(filled_leg_indexes) > 0
    one_leg_fill = len(filled_leg_indexes) == 1
    return WindowSignal(
        window_id=window_id,
        shard_id=bundle.shard_id,
        signal_id=bundle.signal_id,
        event_id=bundle.event_id,
        easier_market_ticker=easier_market_ticker,
        harder_market_ticker=harder_market_ticker,
        signal_ts_ns=bundle.group_signal.ts_ns,
        edge_ticks=bundle.group_signal.edge_ticks,
        accepted_leg_count=accepted_leg_count,
        rejected_leg_count=rejected_leg_count,
        transport_leg_count=len(bundle.oms_transports),
        working_leg_count=lifecycle_by_kind[1],
        partial_fill_leg_count=lifecycle_by_kind[2],
        filled_leg_count=lifecycle_by_kind[3],
        canceled_leg_count=lifecycle_by_kind[4],
        uncertain_leg_count=lifecycle_by_kind[5],
        venue_rejected_leg_count=lifecycle_by_kind[6],
        any_fill=any_fill,
        one_leg_fill=one_leg_fill,
        terminal_recv_ns=terminal_recv_ns,
        terminal_after_window=(
            terminal_recv_ns is not None
            and window_end_recv_ts_ns is not None
            and terminal_recv_ns > window_end_recv_ts_ns
        ),
    )


def build_signal_windows(
    *,
    config_index: ConfigIndex,
    bundles: dict[tuple[int, int], SignalBundle],
    tape_path: str | Path,
    event_id: int | None = None,
    market_ticker: str | None = None,
) -> SignalWindows:
    event_route = _resolve_event_route(config_index=config_index, event_id=event_id, market_ticker=market_ticker)
    event_ticker = _event_ticker(event_route)
    selected_markets = (
        {market_ticker}
        if market_ticker is not None
        else {market.market_ticker for market in event_route.markets}
    )

    pair_routes: list[tuple[MarketRoute, MarketRoute]] = []
    pair_keys_by_market: dict[str, list[tuple[int, int]]] = defaultdict(list)
    for easier, harder in zip(event_route.markets, event_route.markets[1:]):
        if easier.market_ticker not in selected_markets and harder.market_ticker not in selected_markets:
            continue
        pair_routes.append((easier, harder))
        pair_key = (easier.market_id, harder.market_id)
        pair_keys_by_market[easier.market_ticker].append(pair_key)
        pair_keys_by_market[harder.market_ticker].append(pair_key)

    books = ReplayBookStore()
    active_windows: dict[tuple[int, int], _ActiveWindow] = {}
    closed_windows: list[tuple[tuple[int, int], _ActiveWindow, bool]] = []

    for market_event in iter_market_events(tape_path):
        if market_event.market_ticker not in pair_keys_by_market:
            continue
        books.apply(market_event)
        for pair_key in pair_keys_by_market[market_event.market_ticker]:
            easier = config_index.markets_by_id[pair_key[0]]
            harder = config_index.markets_by_id[pair_key[1]]
            easier_state = books.books.get(easier.market_ticker)
            harder_state = books.books.get(harder.market_ticker)
            easier_ask = easier_state.best_ask() if easier_state is not None else None
            harder_bid = harder_state.best_bid() if harder_state is not None else None
            pair_state = evaluate_pair_state(
                easier_ask.price_ticks if easier_ask is not None else None,
                easier_ask.qty_lots if easier_ask is not None else None,
                harder_bid.price_ticks if harder_bid is not None else None,
                harder_bid.qty_lots if harder_bid is not None else None,
            )

            active = active_windows.get(pair_key)
            if pair_state is None:
                if active is not None:
                    active.end_record_index = market_event.record_index
                    active.end_recv_ts_ns = market_event.recv_ts_ns
                    closed_windows.append((pair_key, active, False))
                    del active_windows[pair_key]
                continue

            if active is None:
                active_windows[pair_key] = _ActiveWindow(
                    event_id=event_route.event_id,
                    event_ticker=event_ticker,
                    easier_market_ticker=easier.market_ticker,
                    harder_market_ticker=harder.market_ticker,
                    start_record_index=market_event.record_index,
                    end_record_index=market_event.record_index,
                    start_recv_ts_ns=market_event.recv_ts_ns,
                    end_recv_ts_ns=market_event.recv_ts_ns,
                    start_edge_ticks=pair_state.net_edge_ticks,
                    peak_edge_ticks=pair_state.net_edge_ticks,
                    end_edge_ticks=pair_state.net_edge_ticks,
                    max_executable_qty_lots=pair_state.executable_qty_lots,
                )
                continue

            active.end_record_index = market_event.record_index
            active.end_recv_ts_ns = market_event.recv_ts_ns
            active.end_edge_ticks = pair_state.net_edge_ticks
            active.peak_edge_ticks = max(active.peak_edge_ticks, pair_state.net_edge_ticks)
            active.max_executable_qty_lots = max(active.max_executable_qty_lots, pair_state.executable_qty_lots)

    for pair_key, active in active_windows.items():
        closed_windows.append((pair_key, active, True))

    bundles_by_pair: dict[tuple[int, int], list[tuple[SignalBundle, MarketRoute, MarketRoute]]] = defaultdict(list)
    for bundle in bundles.values():
        if bundle.event_id != event_route.event_id:
            continue
        pair = _candidate_pair(bundle, config_index=config_index)
        if pair is None:
            continue
        easier, harder = pair
        if easier.market_ticker not in selected_markets and harder.market_ticker not in selected_markets:
            continue
        bundles_by_pair[(easier.market_id, harder.market_id)].append((bundle, easier, harder))

    windows: list[SignalWindow] = []
    window_signals: list[WindowSignal] = []
    for window_id, (pair_key, window, censored) in enumerate(
        sorted(closed_windows, key=lambda item: (item[1].start_record_index, item[0])),
        start=1,
    ):
        assigned_signals = []
        for bundle, easier, harder in bundles_by_pair.get(pair_key, []):
            signal_ts_ns = bundle.group_signal.ts_ns
            if window.start_recv_ts_ns is None or signal_ts_ns < window.start_recv_ts_ns:
                continue
            if window.end_recv_ts_ns is not None and signal_ts_ns > window.end_recv_ts_ns:
                continue
            assigned_signal = _window_signal_from_bundle(
                bundle,
                window_id=window_id,
                easier_market_ticker=easier.market_ticker,
                harder_market_ticker=harder.market_ticker,
                window_end_recv_ts_ns=window.end_recv_ts_ns,
            )
            assigned_signals.append(assigned_signal)
            window_signals.append(assigned_signal)

        windows.append(
            SignalWindow(
                window_id=window_id,
                event_id=window.event_id,
                event_ticker=window.event_ticker,
                easier_market_ticker=window.easier_market_ticker,
                harder_market_ticker=window.harder_market_ticker,
                start_record_index=window.start_record_index,
                end_record_index=window.end_record_index,
                start_recv_ts_ns=window.start_recv_ts_ns,
                end_recv_ts_ns=window.end_recv_ts_ns,
                duration_ms=_duration_ms(window.start_recv_ts_ns, window.end_recv_ts_ns),
                censored=censored,
                start_edge_ticks=window.start_edge_ticks,
                peak_edge_ticks=window.peak_edge_ticks,
                end_edge_ticks=window.end_edge_ticks,
                max_executable_qty_lots=window.max_executable_qty_lots,
                signal_count=len(assigned_signals),
                distinct_signal_edges=len({signal.edge_ticks for signal in assigned_signals}),
                accepted_signal_count=sum(1 for signal in assigned_signals if signal.accepted_leg_count > 0),
                rejected_signal_count=sum(1 for signal in assigned_signals if signal.rejected_leg_count > 0),
                transported_signal_count=sum(1 for signal in assigned_signals if signal.transport_leg_count > 0),
                venue_rejected_signal_count=sum(1 for signal in assigned_signals if signal.venue_rejected_leg_count > 0),
                any_fill_signal_count=sum(1 for signal in assigned_signals if signal.any_fill),
                one_leg_fill_signal_count=sum(1 for signal in assigned_signals if signal.one_leg_fill),
                terminal_after_window_signal_count=sum(
                    1 for signal in assigned_signals if signal.terminal_after_window
                ),
                first_signal_ts_ns=min((signal.signal_ts_ns for signal in assigned_signals), default=None),
                last_signal_ts_ns=max((signal.signal_ts_ns for signal in assigned_signals), default=None),
            )
        )

    return SignalWindows(
        event_id=event_route.event_id,
        event_ticker=event_ticker,
        market_tickers=tuple(sorted(selected_markets)),
        windows=tuple(windows),
        signals=tuple(window_signals),
    )


def build_signal_edge_lifetimes(
    *,
    config_index: ConfigIndex,
    bundles: dict[tuple[int, int], SignalBundle],
    tape_path: str | Path,
    event_id: int | None = None,
    market_ticker: str | None = None,
) -> SignalEdgeLifetimes:
    event_route = _resolve_event_route(config_index=config_index, event_id=event_id, market_ticker=market_ticker)
    event_ticker = _event_ticker(event_route)
    selected_markets = (
        {market_ticker}
        if market_ticker is not None
        else {market.market_ticker for market in event_route.markets}
    )

    pair_keys_by_market: dict[str, list[tuple[int, int]]] = defaultdict(list)
    for easier, harder in zip(event_route.markets, event_route.markets[1:]):
        if easier.market_ticker not in selected_markets and harder.market_ticker not in selected_markets:
            continue
        pair_key = (easier.market_id, harder.market_id)
        pair_keys_by_market[easier.market_ticker].append(pair_key)
        pair_keys_by_market[harder.market_ticker].append(pair_key)

    books = ReplayBookStore()
    active_lifetimes: dict[tuple[int, int], _ActiveEdgeLifetime] = {}
    closed_lifetimes: list[tuple[tuple[int, int], _ActiveEdgeLifetime, bool]] = []

    for market_event in iter_market_events(tape_path):
        if market_event.market_ticker not in pair_keys_by_market:
            continue
        books.apply(market_event)
        for pair_key in pair_keys_by_market[market_event.market_ticker]:
            easier = config_index.markets_by_id[pair_key[0]]
            harder = config_index.markets_by_id[pair_key[1]]
            easier_state = books.books.get(easier.market_ticker)
            harder_state = books.books.get(harder.market_ticker)
            easier_ask = easier_state.best_ask() if easier_state is not None else None
            harder_bid = harder_state.best_bid() if harder_state is not None else None
            pair_state = evaluate_pair_state(
                easier_ask.price_ticks if easier_ask is not None else None,
                easier_ask.qty_lots if easier_ask is not None else None,
                harder_bid.price_ticks if harder_bid is not None else None,
                harder_bid.qty_lots if harder_bid is not None else None,
            )

            active = active_lifetimes.get(pair_key)
            if pair_state is None:
                if active is not None:
                    active.end_record_index = market_event.record_index
                    active.end_recv_ts_ns = market_event.recv_ts_ns
                    closed_lifetimes.append((pair_key, active, False))
                    del active_lifetimes[pair_key]
                continue

            if active is None or active.edge_ticks != pair_state.net_edge_ticks:
                if active is not None:
                    closed_lifetimes.append((pair_key, active, False))
                active_lifetimes[pair_key] = _ActiveEdgeLifetime(
                    event_id=event_route.event_id,
                    event_ticker=event_ticker,
                    easier_market_ticker=easier.market_ticker,
                    harder_market_ticker=harder.market_ticker,
                    edge_ticks=pair_state.net_edge_ticks,
                    start_record_index=market_event.record_index,
                    end_record_index=market_event.record_index,
                    start_recv_ts_ns=market_event.recv_ts_ns,
                    end_recv_ts_ns=market_event.recv_ts_ns,
                    max_executable_qty_lots=pair_state.executable_qty_lots,
                )
                continue

            active.end_record_index = market_event.record_index
            active.end_recv_ts_ns = market_event.recv_ts_ns
            active.max_executable_qty_lots = max(active.max_executable_qty_lots, pair_state.executable_qty_lots)

    for pair_key, active in active_lifetimes.items():
        closed_lifetimes.append((pair_key, active, True))

    bundles_by_pair: dict[tuple[int, int], list[tuple[SignalBundle, MarketRoute, MarketRoute]]] = defaultdict(list)
    for bundle in bundles.values():
        if bundle.event_id != event_route.event_id:
            continue
        pair = _candidate_pair(bundle, config_index=config_index)
        if pair is None:
            continue
        easier, harder = pair
        if easier.market_ticker not in selected_markets and harder.market_ticker not in selected_markets:
            continue
        bundles_by_pair[(easier.market_id, harder.market_id)].append((bundle, easier, harder))

    lifetimes: list[SignalEdgeLifetime] = []
    for lifetime_id, (pair_key, lifetime, censored) in enumerate(
        sorted(closed_lifetimes, key=lambda item: (item[1].start_record_index, item[0], item[1].edge_ticks)),
        start=1,
    ):
        assigned_signals = []
        for bundle, easier, harder in bundles_by_pair.get(pair_key, []):
            signal_ts_ns = bundle.group_signal.ts_ns
            if bundle.group_signal.edge_ticks != lifetime.edge_ticks:
                continue
            if lifetime.start_recv_ts_ns is None or signal_ts_ns < lifetime.start_recv_ts_ns:
                continue
            if lifetime.end_recv_ts_ns is not None and signal_ts_ns > lifetime.end_recv_ts_ns:
                continue
            assigned_signals.append(
                _window_signal_from_bundle(
                    bundle,
                    window_id=lifetime_id,
                    easier_market_ticker=easier.market_ticker,
                    harder_market_ticker=harder.market_ticker,
                    window_end_recv_ts_ns=lifetime.end_recv_ts_ns,
                )
            )

        lifetimes.append(
            SignalEdgeLifetime(
                lifetime_id=lifetime_id,
                event_id=lifetime.event_id,
                event_ticker=lifetime.event_ticker,
                easier_market_ticker=lifetime.easier_market_ticker,
                harder_market_ticker=lifetime.harder_market_ticker,
                edge_ticks=lifetime.edge_ticks,
                start_record_index=lifetime.start_record_index,
                end_record_index=lifetime.end_record_index,
                start_recv_ts_ns=lifetime.start_recv_ts_ns,
                end_recv_ts_ns=lifetime.end_recv_ts_ns,
                duration_ms=_duration_ms(lifetime.start_recv_ts_ns, lifetime.end_recv_ts_ns),
                censored=censored,
                max_executable_qty_lots=lifetime.max_executable_qty_lots,
                signal_count=len(assigned_signals),
                accepted_signal_count=sum(1 for signal in assigned_signals if signal.accepted_leg_count > 0),
                rejected_signal_count=sum(1 for signal in assigned_signals if signal.rejected_leg_count > 0),
                transported_signal_count=sum(1 for signal in assigned_signals if signal.transport_leg_count > 0),
                venue_rejected_signal_count=sum(
                    1 for signal in assigned_signals if signal.venue_rejected_leg_count > 0
                ),
                any_fill_signal_count=sum(1 for signal in assigned_signals if signal.any_fill),
                one_leg_fill_signal_count=sum(1 for signal in assigned_signals if signal.one_leg_fill),
                terminal_after_lifetime_signal_count=sum(
                    1 for signal in assigned_signals if signal.terminal_after_window
                ),
                first_signal_ts_ns=min((signal.signal_ts_ns for signal in assigned_signals), default=None),
                last_signal_ts_ns=max((signal.signal_ts_ns for signal in assigned_signals), default=None),
            )
        )

    return SignalEdgeLifetimes(
        event_id=event_route.event_id,
        event_ticker=event_ticker,
        market_tickers=tuple(sorted(selected_markets)),
        lifetimes=tuple(lifetimes),
    )


def build_all_signal_edge_lifetimes(
    *,
    config_index: ConfigIndex,
    bundles: dict[tuple[int, int], SignalBundle],
    tape_path: str | Path,
) -> AllSignalEdgeLifetimes:
    bundles_by_pair: dict[tuple[int, int], list[tuple[SignalBundle, MarketRoute, MarketRoute]]] = defaultdict(list)
    for bundle in bundles.values():
        pair = _candidate_pair(bundle, config_index=config_index)
        if pair is None:
            continue
        easier, harder = pair
        bundles_by_pair[(easier.market_id, harder.market_id)].append((bundle, easier, harder))

    pair_route_by_key: dict[tuple[int, int], tuple[EventRoute, MarketRoute, MarketRoute]] = {}
    pair_keys_by_market: dict[str, list[tuple[int, int]]] = defaultdict(list)
    for pair_key, pair_bundles in bundles_by_pair.items():
        easier = pair_bundles[0][1]
        harder = pair_bundles[0][2]
        event_route = config_index.events_by_id[easier.event_id]
        pair_route_by_key[pair_key] = (event_route, easier, harder)
        pair_keys_by_market[easier.market_ticker].append(pair_key)
        pair_keys_by_market[harder.market_ticker].append(pair_key)

    books = ReplayBookStore()
    active_lifetimes: dict[tuple[int, int], _ActiveEdgeLifetime] = {}
    closed_lifetimes: list[tuple[tuple[int, int], _ActiveEdgeLifetime, bool]] = []

    for market_event in iter_market_events(tape_path):
        if market_event.market_ticker not in pair_keys_by_market:
            continue
        books.apply(market_event)
        for pair_key in pair_keys_by_market[market_event.market_ticker]:
            event_route, easier, harder = pair_route_by_key[pair_key]
            easier_state = books.books.get(easier.market_ticker)
            harder_state = books.books.get(harder.market_ticker)
            easier_ask = easier_state.best_ask() if easier_state is not None else None
            harder_bid = harder_state.best_bid() if harder_state is not None else None
            pair_state = evaluate_pair_state(
                easier_ask.price_ticks if easier_ask is not None else None,
                easier_ask.qty_lots if easier_ask is not None else None,
                harder_bid.price_ticks if harder_bid is not None else None,
                harder_bid.qty_lots if harder_bid is not None else None,
            )

            active = active_lifetimes.get(pair_key)
            if pair_state is None:
                if active is not None:
                    active.end_record_index = market_event.record_index
                    active.end_recv_ts_ns = market_event.recv_ts_ns
                    closed_lifetimes.append((pair_key, active, False))
                    del active_lifetimes[pair_key]
                continue

            if active is None or active.edge_ticks != pair_state.net_edge_ticks:
                if active is not None:
                    closed_lifetimes.append((pair_key, active, False))
                active_lifetimes[pair_key] = _ActiveEdgeLifetime(
                    event_id=event_route.event_id,
                    event_ticker=_event_ticker(event_route),
                    easier_market_ticker=easier.market_ticker,
                    harder_market_ticker=harder.market_ticker,
                    edge_ticks=pair_state.net_edge_ticks,
                    start_record_index=market_event.record_index,
                    end_record_index=market_event.record_index,
                    start_recv_ts_ns=market_event.recv_ts_ns,
                    end_recv_ts_ns=market_event.recv_ts_ns,
                    max_executable_qty_lots=pair_state.executable_qty_lots,
                )
                continue

            active.end_record_index = market_event.record_index
            active.end_recv_ts_ns = market_event.recv_ts_ns
            active.max_executable_qty_lots = max(active.max_executable_qty_lots, pair_state.executable_qty_lots)

    for pair_key, active in active_lifetimes.items():
        closed_lifetimes.append((pair_key, active, True))

    lifetimes: list[SignalEdgeLifetime] = []
    for lifetime_id, (pair_key, lifetime, censored) in enumerate(
        sorted(
            closed_lifetimes,
            key=lambda item: (item[1].event_id, item[1].start_record_index, item[0], item[1].edge_ticks),
        ),
        start=1,
    ):
        assigned_signals = []
        for bundle, easier, harder in bundles_by_pair.get(pair_key, []):
            signal_ts_ns = bundle.group_signal.ts_ns
            if bundle.group_signal.edge_ticks != lifetime.edge_ticks:
                continue
            if lifetime.start_recv_ts_ns is None or signal_ts_ns < lifetime.start_recv_ts_ns:
                continue
            if lifetime.end_recv_ts_ns is not None and signal_ts_ns > lifetime.end_recv_ts_ns:
                continue
            assigned_signals.append(
                _window_signal_from_bundle(
                    bundle,
                    window_id=lifetime_id,
                    easier_market_ticker=easier.market_ticker,
                    harder_market_ticker=harder.market_ticker,
                    window_end_recv_ts_ns=lifetime.end_recv_ts_ns,
                )
            )

        lifetimes.append(
            SignalEdgeLifetime(
                lifetime_id=lifetime_id,
                event_id=lifetime.event_id,
                event_ticker=lifetime.event_ticker,
                easier_market_ticker=lifetime.easier_market_ticker,
                harder_market_ticker=lifetime.harder_market_ticker,
                edge_ticks=lifetime.edge_ticks,
                start_record_index=lifetime.start_record_index,
                end_record_index=lifetime.end_record_index,
                start_recv_ts_ns=lifetime.start_recv_ts_ns,
                end_recv_ts_ns=lifetime.end_recv_ts_ns,
                duration_ms=_duration_ms(lifetime.start_recv_ts_ns, lifetime.end_recv_ts_ns),
                censored=censored,
                max_executable_qty_lots=lifetime.max_executable_qty_lots,
                signal_count=len(assigned_signals),
                accepted_signal_count=sum(1 for signal in assigned_signals if signal.accepted_leg_count > 0),
                rejected_signal_count=sum(1 for signal in assigned_signals if signal.rejected_leg_count > 0),
                transported_signal_count=sum(1 for signal in assigned_signals if signal.transport_leg_count > 0),
                venue_rejected_signal_count=sum(
                    1 for signal in assigned_signals if signal.venue_rejected_leg_count > 0
                ),
                any_fill_signal_count=sum(1 for signal in assigned_signals if signal.any_fill),
                one_leg_fill_signal_count=sum(1 for signal in assigned_signals if signal.one_leg_fill),
                terminal_after_lifetime_signal_count=sum(
                    1 for signal in assigned_signals if signal.terminal_after_window
                ),
                first_signal_ts_ns=min((signal.signal_ts_ns for signal in assigned_signals), default=None),
                last_signal_ts_ns=max((signal.signal_ts_ns for signal in assigned_signals), default=None),
            )
        )

    return AllSignalEdgeLifetimes(lifetimes=tuple(lifetimes))


def write_signal_windows_csv(path: str | Path, windows: Iterable[SignalWindow]) -> None:
    with Path(path).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "window_id",
                "event_id",
                "event_ticker",
                "easier_market_ticker",
                "harder_market_ticker",
                "start_record_index",
                "end_record_index",
                "start_recv_ts_ns",
                "end_recv_ts_ns",
                "duration_ms",
                "censored",
                "start_edge_ticks",
                "peak_edge_ticks",
                "end_edge_ticks",
                "max_executable_qty_lots",
                "signal_count",
                "distinct_signal_edges",
                "accepted_signal_count",
                "rejected_signal_count",
                "transported_signal_count",
                "venue_rejected_signal_count",
                "any_fill_signal_count",
                "one_leg_fill_signal_count",
                "terminal_after_window_signal_count",
                "first_signal_ts_ns",
                "last_signal_ts_ns",
            ]
        )
        for window in windows:
            writer.writerow(
                [
                    window.window_id,
                    window.event_id,
                    window.event_ticker,
                    window.easier_market_ticker,
                    window.harder_market_ticker,
                    window.start_record_index,
                    window.end_record_index,
                    window.start_recv_ts_ns,
                    window.end_recv_ts_ns,
                    window.duration_ms,
                    int(window.censored),
                    window.start_edge_ticks,
                    window.peak_edge_ticks,
                    window.end_edge_ticks,
                    window.max_executable_qty_lots,
                    window.signal_count,
                    window.distinct_signal_edges,
                    window.accepted_signal_count,
                    window.rejected_signal_count,
                    window.transported_signal_count,
                    window.venue_rejected_signal_count,
                    window.any_fill_signal_count,
                    window.one_leg_fill_signal_count,
                    window.terminal_after_window_signal_count,
                    window.first_signal_ts_ns,
                    window.last_signal_ts_ns,
                ]
            )


def write_window_signals_csv(path: str | Path, signals: Iterable[WindowSignal]) -> None:
    with Path(path).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "window_id",
                "shard_id",
                "signal_id",
                "event_id",
                "easier_market_ticker",
                "harder_market_ticker",
                "signal_ts_ns",
                "edge_ticks",
                "accepted_leg_count",
                "rejected_leg_count",
                "transport_leg_count",
                "working_leg_count",
                "partial_fill_leg_count",
                "filled_leg_count",
                "canceled_leg_count",
                "uncertain_leg_count",
                "venue_rejected_leg_count",
                "any_fill",
                "one_leg_fill",
                "terminal_recv_ns",
                "terminal_after_window",
            ]
        )
        for signal in sorted(signals, key=lambda item: (item.window_id, item.shard_id, item.signal_id)):
            writer.writerow(
                [
                    signal.window_id,
                    signal.shard_id,
                    signal.signal_id,
                    signal.event_id,
                    signal.easier_market_ticker,
                    signal.harder_market_ticker,
                    signal.signal_ts_ns,
                    signal.edge_ticks,
                    signal.accepted_leg_count,
                    signal.rejected_leg_count,
                    signal.transport_leg_count,
                    signal.working_leg_count,
                    signal.partial_fill_leg_count,
                    signal.filled_leg_count,
                    signal.canceled_leg_count,
                    signal.uncertain_leg_count,
                    signal.venue_rejected_leg_count,
                    int(signal.any_fill),
                    int(signal.one_leg_fill),
                    signal.terminal_recv_ns,
                    int(signal.terminal_after_window),
                ]
            )


def write_signal_edge_lifetimes_csv(path: str | Path, lifetimes: Iterable[SignalEdgeLifetime]) -> None:
    with Path(path).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "lifetime_id",
                "event_id",
                "event_ticker",
                "easier_market_ticker",
                "harder_market_ticker",
                "edge_ticks",
                "start_record_index",
                "end_record_index",
                "start_recv_ts_ns",
                "end_recv_ts_ns",
                "duration_ms",
                "censored",
                "max_executable_qty_lots",
                "signal_count",
                "accepted_signal_count",
                "rejected_signal_count",
                "transported_signal_count",
                "venue_rejected_signal_count",
                "any_fill_signal_count",
                "one_leg_fill_signal_count",
                "terminal_after_lifetime_signal_count",
                "first_signal_ts_ns",
                "last_signal_ts_ns",
            ]
        )
        for lifetime in lifetimes:
            writer.writerow(
                [
                    lifetime.lifetime_id,
                    lifetime.event_id,
                    lifetime.event_ticker,
                    lifetime.easier_market_ticker,
                    lifetime.harder_market_ticker,
                    lifetime.edge_ticks,
                    lifetime.start_record_index,
                    lifetime.end_record_index,
                    lifetime.start_recv_ts_ns,
                    lifetime.end_recv_ts_ns,
                    lifetime.duration_ms,
                    int(lifetime.censored),
                    lifetime.max_executable_qty_lots,
                    lifetime.signal_count,
                    lifetime.accepted_signal_count,
                    lifetime.rejected_signal_count,
                    lifetime.transported_signal_count,
                    lifetime.venue_rejected_signal_count,
                    lifetime.any_fill_signal_count,
                    lifetime.one_leg_fill_signal_count,
                    lifetime.terminal_after_lifetime_signal_count,
                    lifetime.first_signal_ts_ns,
                    lifetime.last_signal_ts_ns,
                ]
            )


def write_signal_windows_summary_json(path: str | Path, windows: SignalWindows) -> None:
    payload = {
        "event_id": windows.event_id,
        "event_ticker": windows.event_ticker,
        "market_tickers": list(windows.market_tickers),
        "window_count": len(windows.windows),
        "signal_count": len(windows.signals),
        "first_window_start_recv_ts_ns": min(
            (window.start_recv_ts_ns for window in windows.windows if window.start_recv_ts_ns is not None),
            default=None,
        ),
        "last_window_end_recv_ts_ns": max(
            (window.end_recv_ts_ns for window in windows.windows if window.end_recv_ts_ns is not None),
            default=None,
        ),
        "max_window_duration_ms": max(
            (window.duration_ms for window in windows.windows if window.duration_ms is not None),
            default=None,
        ),
    }
    Path(path).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")