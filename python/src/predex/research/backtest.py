from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable

from .chain_state import EventChainState
from .candidates import StrategyCandidate
from .event_replay import iter_updates_from_tables, load_routes_from_tables
from .features import ChainFeatureSnapshot
from .intents import OrderIntent
from .models import MarketUpdate, ResearchMarketRoute
from .outcomes import (
    CandidateDedupConfig,
    CandidateDeduper,
    CandidateOutcome,
    CandidateOutcomeConfig,
    CandidateOutcomeTracker,
)
from .strategies import Strategy, StrategyDecision


@dataclass(frozen=True, slots=True)
class BacktestResult:
    event_id: int
    updates_seen: int
    snapshots_seen: int
    candidates: tuple[StrategyCandidate, ...]
    intents: tuple[OrderIntent, ...]
    final_snapshot: ChainFeatureSnapshot | None
    outcomes: tuple[CandidateOutcome, ...] = field(default_factory=tuple)
    raw_candidate_count: int = 0
    deduped_candidate_count: int = 0
    errors: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class RunBacktestResult:
    run_dir: Path
    event_count: int
    active_event_count: int
    updates_seen: int
    snapshots_seen: int
    candidates: tuple[StrategyCandidate, ...]
    intents: tuple[OrderIntent, ...]
    outcomes: tuple[CandidateOutcome, ...] = field(default_factory=tuple)
    raw_candidate_count: int = 0
    deduped_candidate_count: int = 0
    errors: tuple[str, ...] = field(default_factory=tuple)
    final_snapshots: dict[int, ChainFeatureSnapshot] = field(default_factory=dict)


def run_strategy_on_event(
    *,
    event_id: int,
    routes: Iterable[ResearchMarketRoute],
    updates: Iterable[MarketUpdate],
    strategy: Strategy,
    dedup_config: CandidateDedupConfig | None = None,
    outcome_config: CandidateOutcomeConfig | None = None,
    stop_on_error: bool = True,
) -> BacktestResult:
    state = EventChainState(event_id=int(event_id), routes=tuple(routes))
    updates_seen = 0
    snapshots_seen = 0
    candidates: list[StrategyCandidate] = []
    intents: list[OrderIntent] = []
    outcomes: list[CandidateOutcome] = []
    errors: list[str] = []
    final_snapshot: ChainFeatureSnapshot | None = None
    deduper = CandidateDeduper(dedup_config) if dedup_config is not None else None
    outcome_tracker = CandidateOutcomeTracker(outcome_config) if outcome_config is not None else None
    raw_candidate_count = 0
    deduped_candidate_count = 0

    for update in updates:
        updates_seen += 1
        try:
            if outcome_tracker is not None and final_snapshot is not None:
                outcome_tracker.observe(final_snapshot, watermark_ts_ns=update.recv_ts_ns)
                outcomes = list(outcome_tracker.outcomes)
            snapshot = state.apply(update)
            if snapshot is None:
                continue
            snapshots_seen += 1
            final_snapshot = snapshot
            decision = _coerce_strategy_decision(strategy.on_chain_snapshot(snapshot))
            raw_candidate_count += len(decision.candidates)
            accepted_candidates = _dedup_candidates(decision.candidates, deduper)
            deduped_candidate_count += len(decision.candidates) - len(accepted_candidates)
            candidates.extend(accepted_candidates)
            if outcome_tracker is not None:
                for candidate in accepted_candidates:
                    outcome_tracker.add(candidate)
            intents.extend(decision.intents)
        except Exception as exc:
            message = f"record={getattr(update, 'record_index', '<unknown>')}: {exc}"
            errors.append(message)
            if stop_on_error:
                break

    return BacktestResult(
        event_id=int(event_id),
        updates_seen=updates_seen,
        snapshots_seen=snapshots_seen,
        candidates=tuple(candidates),
        intents=tuple(intents),
        outcomes=tuple(outcomes),
        raw_candidate_count=raw_candidate_count,
        deduped_candidate_count=deduped_candidate_count,
        final_snapshot=final_snapshot,
        errors=tuple(errors),
    )


def run_strategy_on_run(
    run_dir: str | Path,
    *,
    strategy_factory: Callable[[tuple[ResearchMarketRoute, ...]], Strategy],
    topology_filter: Iterable[str] | None = ("monotonic_chain",),
    include_trades: bool = True,
    include_lifecycle: bool = False,
    batch_size: int = 65_536,
    max_updates: int | None = None,
    dedup_config: CandidateDedupConfig | None = None,
    outcome_config: CandidateOutcomeConfig | None = None,
    stop_on_error: bool = True,
) -> RunBacktestResult:
    run_path = Path(run_dir)
    routes_by_event = load_routes_from_tables(run_path, topology_filter=topology_filter)
    states: dict[int, EventChainState] = {}
    strategies: dict[int, Strategy] = {}
    final_snapshots: dict[int, ChainFeatureSnapshot] = {}
    updates_seen = 0
    snapshots_seen = 0
    candidates: list[StrategyCandidate] = []
    intents: list[OrderIntent] = []
    outcomes: list[CandidateOutcome] = []
    errors: list[str] = []
    dedupers: dict[int, CandidateDeduper] = {}
    outcome_trackers: dict[int, CandidateOutcomeTracker] = {}
    raw_candidate_count = 0
    deduped_candidate_count = 0

    updates = iter_updates_from_tables(
        run_path,
        topology_filter=topology_filter,
        include_trades=include_trades,
        include_lifecycle=include_lifecycle,
        batch_size=batch_size,
    )
    for update in updates:
        if max_updates is not None and updates_seen >= max_updates:
            break
        updates_seen += 1
        event_id = int(update.event_id)
        routes = routes_by_event.get(event_id)
        if routes is None:
            continue

        state = states.get(event_id)
        if state is None:
            state = EventChainState(event_id=event_id, routes=routes)
            states[event_id] = state
            strategies[event_id] = strategy_factory(routes)
            if dedup_config is not None:
                dedupers[event_id] = CandidateDeduper(dedup_config)
            if outcome_config is not None:
                outcome_trackers[event_id] = CandidateOutcomeTracker(outcome_config)

        try:
            outcome_tracker = outcome_trackers.get(event_id)
            previous_snapshot = final_snapshots.get(event_id)
            if outcome_tracker is not None and previous_snapshot is not None:
                previous_outcome_count = len(outcome_tracker.outcomes)
                outcome_tracker.observe(previous_snapshot, watermark_ts_ns=update.recv_ts_ns)
                outcomes.extend(outcome_tracker.outcomes[previous_outcome_count:])
            snapshot = state.apply(update)
            if snapshot is None:
                continue
            snapshots_seen += 1
            final_snapshots[event_id] = snapshot
            decision = _coerce_strategy_decision(strategies[event_id].on_chain_snapshot(snapshot))
            raw_candidate_count += len(decision.candidates)
            accepted_candidates = _dedup_candidates(decision.candidates, dedupers.get(event_id))
            deduped_candidate_count += len(decision.candidates) - len(accepted_candidates)
            candidates.extend(accepted_candidates)
            if outcome_tracker is not None:
                for candidate in accepted_candidates:
                    outcome_tracker.add(candidate)
            intents.extend(decision.intents)
        except Exception as exc:
            message = f"event={event_id} record={getattr(update, 'record_index', '<unknown>')}: {exc}"
            errors.append(message)
            if stop_on_error:
                break

    return RunBacktestResult(
        run_dir=run_path,
        event_count=len(routes_by_event),
        active_event_count=len(states),
        updates_seen=updates_seen,
        snapshots_seen=snapshots_seen,
        candidates=tuple(candidates),
        intents=tuple(intents),
        outcomes=tuple(outcomes),
        raw_candidate_count=raw_candidate_count,
        deduped_candidate_count=deduped_candidate_count,
        errors=tuple(errors),
        final_snapshots=final_snapshots,
    )


def _coerce_strategy_decision(output: StrategyDecision | tuple[OrderIntent, ...]) -> StrategyDecision:
    if isinstance(output, StrategyDecision):
        return output
    return StrategyDecision(intents=tuple(output))


def _dedup_candidates(
    candidates: tuple[StrategyCandidate, ...],
    deduper: CandidateDeduper | None,
) -> tuple[StrategyCandidate, ...]:
    if deduper is None:
        return candidates
    return tuple(candidate for candidate in candidates if deduper.allow(candidate))
