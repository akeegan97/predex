from __future__ import annotations

import argparse
import heapq
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import pandas as pd

from predex.research import (
    CandidateDedupConfig,
    CandidateDeduper,
    CandidateOutcomeConfig,
    CandidateOutcomeTracker,
    ChainFeatureSnapshot,
    EventChainState,
    MonotonicResidualScanner,
    RunBacktestResult,
    StrategyCandidate,
    iter_updates_from_tables,
    load_routes_from_tables,
    run_strategy_on_run,
)
from predex.research.outcomes import CandidateOutcome


DEFAULT_RUN_DIR = Path("runs/predex-2026-06-30-230132-market-data-tuesday-night")
DEFAULT_HORIZONS_NS = (
    1_000_000_000,
    5_000_000_000,
    30_000_000_000,
    60_000_000_000,
)


@dataclass(frozen=True, slots=True)
class ResidualDiagnosticConfig:
    run_dir: Path = DEFAULT_RUN_DIR
    max_updates: int | None = 1_000_000
    min_abs_residual_ticks: float = 25.0
    scan_updated_neighborhood: bool = True
    cooldown_ns: int = 5_000_000_000
    decision_latency_ns: int = 1_000_000
    order_latency_ns: int = 5_000_000
    horizons_ns: tuple[int, ...] = DEFAULT_HORIZONS_NS
    include_trades: bool = False
    include_lifecycle: bool = False
    top_n: int = 20
    score_buckets: int = 10
    output_dir: Path | None = None


@dataclass(frozen=True, slots=True)
class ResidualDiagnosticResult:
    backtest: RunBacktestResult
    candidates: pd.DataFrame
    outcomes: pd.DataFrame


def run_residual_diagnostics(config: ResidualDiagnosticConfig) -> ResidualDiagnosticResult:
    if config.output_dir is not None:
        return run_streaming_residual_diagnostics(config)

    result = run_strategy_on_run(
        config.run_dir,
        strategy_factory=lambda routes: MonotonicResidualScanner(
            min_abs_residual_ticks=config.min_abs_residual_ticks,
            scan_updated_neighborhood=config.scan_updated_neighborhood,
            context={"route_count": len(routes)},
        ),
        include_trades=config.include_trades,
        include_lifecycle=config.include_lifecycle,
        max_updates=config.max_updates,
        dedup_config=CandidateDedupConfig(cooldown_ns=config.cooldown_ns),
        outcome_config=CandidateOutcomeConfig(
            horizons_ns=config.horizons_ns,
            decision_latency_ns=config.decision_latency_ns,
            order_latency_ns=config.order_latency_ns,
        ),
    )

    candidates = candidates_to_frame(result.candidates)
    outcomes = outcomes_to_frame(result.outcomes)
    diagnostic = ResidualDiagnosticResult(backtest=result, candidates=candidates, outcomes=outcomes)
    print_diagnostics(diagnostic, config)

    if config.output_dir is not None:
        write_diagnostics(diagnostic, config.output_dir)

    return diagnostic


def run_streaming_residual_diagnostics(config: ResidualDiagnosticConfig) -> ResidualDiagnosticResult:
    if config.output_dir is None:
        raise ValueError("--full requires --output-dir so diagnostics can stream to disk")

    routes_by_event = load_routes_from_tables(config.run_dir, topology_filter=("monotonic_chain",))
    states: dict[int, EventChainState] = {}
    strategies: dict[int, MonotonicResidualScanner] = {}
    dedupers: dict[int, CandidateDeduper] = {}
    outcome_trackers: dict[int, CandidateOutcomeTracker] = {}
    final_snapshots: dict[int, ChainFeatureSnapshot] = {}
    summary = _StreamingSummary(event_count=len(routes_by_event), top_n=config.top_n)
    candidates_writer = _PartitionedParquetWriter(config.output_dir / "research_candidates")
    outcomes_writer = _PartitionedParquetWriter(config.output_dir / "research_outcomes")

    updates = iter_updates_from_tables(
        config.run_dir,
        topology_filter=("monotonic_chain",),
        include_trades=config.include_trades,
        include_lifecycle=config.include_lifecycle,
    )

    try:
        for update in updates:
            if config.max_updates is not None and summary.updates_seen >= config.max_updates:
                break
            summary.updates_seen += 1
            event_id = int(update.event_id)
            routes = routes_by_event.get(event_id)
            if routes is None:
                continue

            state = states.get(event_id)
            if state is None:
                state = EventChainState(event_id=event_id, routes=routes)
                states[event_id] = state
                strategies[event_id] = MonotonicResidualScanner(
                    min_abs_residual_ticks=config.min_abs_residual_ticks,
                    scan_updated_neighborhood=config.scan_updated_neighborhood,
                    context={"route_count": len(routes)},
                )
                dedupers[event_id] = CandidateDeduper(CandidateDedupConfig(cooldown_ns=config.cooldown_ns))
                outcome_trackers[event_id] = CandidateOutcomeTracker(
                    CandidateOutcomeConfig(
                        horizons_ns=config.horizons_ns,
                        decision_latency_ns=config.decision_latency_ns,
                        order_latency_ns=config.order_latency_ns,
                    )
                )

            try:
                tracker = outcome_trackers[event_id]
                previous_snapshot = final_snapshots.get(event_id)
                if previous_snapshot is not None:
                    tracker.observe(previous_snapshot, watermark_ts_ns=update.recv_ts_ns)
                    if tracker.outcomes:
                        summary.add_outcomes(tracker.outcomes)
                        outcomes_writer.add(outcomes_to_frame(tracker.outcomes))
                        tracker.outcomes.clear()

                snapshot = state.apply(update)
                if snapshot is None:
                    continue
                summary.snapshots_seen += 1
                final_snapshots[event_id] = snapshot

                decision = strategies[event_id].on_chain_snapshot(snapshot)
                summary.raw_candidate_count += len(decision.candidates)
                accepted: list[StrategyCandidate] = []
                deduper = dedupers[event_id]
                for candidate in decision.candidates:
                    if deduper.allow(candidate):
                        accepted.append(candidate)
                    else:
                        summary.deduped_candidate_count += 1

                if accepted:
                    summary.add_candidates(accepted)
                    candidates_writer.add(candidates_to_frame(accepted))
                    for candidate in accepted:
                        tracker.add(candidate)
            except Exception as exc:
                message = f"event={event_id} record={getattr(update, 'record_index', '<unknown>')}: {exc}"
                summary.errors.append(message)
                if len(summary.errors) >= 5:
                    break
    finally:
        candidates_writer.close()
        outcomes_writer.close()

    summary.active_event_count = len(states)
    print_streaming_diagnostics(summary, config, candidates_writer.output_dir, outcomes_writer.output_dir)
    return ResidualDiagnosticResult(
        backtest=RunBacktestResult(
            run_dir=config.run_dir,
            event_count=summary.event_count,
            active_event_count=summary.active_event_count,
            updates_seen=summary.updates_seen,
            snapshots_seen=summary.snapshots_seen,
            candidates=(),
            intents=(),
            outcomes=(),
            raw_candidate_count=summary.raw_candidate_count,
            deduped_candidate_count=summary.deduped_candidate_count,
            errors=tuple(summary.errors),
        ),
        candidates=pd.DataFrame(),
        outcomes=pd.DataFrame(),
    )


@dataclass(slots=True)
class _StreamingSummary:
    event_count: int
    top_n: int
    active_event_count: int = 0
    updates_seen: int = 0
    snapshots_seen: int = 0
    raw_candidate_count: int = 0
    deduped_candidate_count: int = 0
    kept_candidate_count: int = 0
    outcome_count: int = 0
    missing_market_count: int = 0
    null_favorable_move_count: int = 0
    errors: list[str] = field(default_factory=list)
    score_sample: _ReservoirSample = field(default_factory=lambda: _ReservoirSample(limit=100_000))
    direction_counts: Counter[str] = field(default_factory=Counter)
    event_counts: Counter[int] = field(default_factory=Counter)
    market_counts: Counter[tuple[int, int, str]] = field(default_factory=Counter)
    quality_by_horizon: dict[tuple[object, ...], _OnlineQuality] = field(default_factory=lambda: defaultdict(_OnlineQuality))
    quality_by_horizon_direction: dict[tuple[object, ...], _OnlineQuality] = field(default_factory=lambda: defaultdict(_OnlineQuality))
    quality_by_score_band: dict[tuple[object, ...], _OnlineQuality] = field(default_factory=lambda: defaultdict(_OnlineQuality))
    quality_by_event: dict[tuple[object, ...], _OnlineQuality] = field(default_factory=lambda: defaultdict(_OnlineQuality))
    quality_by_route_count: dict[tuple[object, ...], _OnlineQuality] = field(default_factory=lambda: defaultdict(_OnlineQuality))
    quote_age_by_horizon: dict[tuple[object, ...], _OnlineLag] = field(default_factory=lambda: defaultdict(_OnlineLag))
    best_outcomes: list[tuple[float, int, dict[str, object]]] = field(default_factory=list)
    worst_outcomes: list[tuple[float, int, dict[str, object]]] = field(default_factory=list)
    _heap_sequence: int = 0

    def add_candidates(self, candidates: Iterable[StrategyCandidate]) -> None:
        for candidate in candidates:
            self.kept_candidate_count += 1
            self.score_sample.add(float(candidate.score))
            self.direction_counts[candidate.direction] += 1
            self.event_counts[int(candidate.event_id)] += 1
            self.market_counts[(int(candidate.event_id), int(candidate.market_id), candidate.direction)] += 1

    def add_outcomes(self, outcomes: Iterable[CandidateOutcome]) -> None:
        for outcome in outcomes:
            self.outcome_count += 1
            if outcome.missing_market:
                self.missing_market_count += 1
            if outcome.favorable_move_ticks is None:
                self.null_favorable_move_count += 1
                continue

            row = _outcome_summary_row(outcome)
            horizon_key = (row["horizon_label"],)
            horizon_direction_key = (row["horizon_label"], row["direction"])
            score_band_key = (row["horizon_label"], _score_band(float(row["score"])))
            event_key = (row["event_id"],)
            route_count = row.get("route_count")

            self.quality_by_horizon[horizon_key].add(row)
            self.quality_by_horizon_direction[horizon_direction_key].add(row)
            self.quality_by_score_band[score_band_key].add(row)
            self.quality_by_event[event_key].add(row)
            if route_count is not None:
                self.quality_by_route_count[(route_count,)].add(row)
            self.quote_age_by_horizon[horizon_key].add(int(row["quote_age_at_target_ns"]))
            self._push_best(row)
            self._push_worst(row)

    def _push_best(self, row: dict[str, object]) -> None:
        self._heap_sequence += 1
        score = float(row["favorable_move_ticks"])
        heapq.heappush(self.best_outcomes, (score, self._heap_sequence, row))
        if len(self.best_outcomes) > self.top_n:
            heapq.heappop(self.best_outcomes)

    def _push_worst(self, row: dict[str, object]) -> None:
        self._heap_sequence += 1
        score = -float(row["favorable_move_ticks"])
        heapq.heappush(self.worst_outcomes, (score, self._heap_sequence, row))
        if len(self.worst_outcomes) > self.top_n:
            heapq.heappop(self.worst_outcomes)


@dataclass(slots=True)
class _OnlineQuality:
    n: int = 0
    favorable_sum: float = 0.0
    favorable_zero_count: int = 0
    favorable_ge_25_count: int = 0
    favorable_ge_50_count: int = 0
    adverse_le_minus_25_count: int = 0
    adverse_le_minus_50_count: int = 0
    executable_n: int = 0
    executable_sum: float = 0.0
    executable_positive_count: int = 0
    executable_ge_25_count: int = 0
    executable_ge_50_count: int = 0
    reverted_count: int = 0
    continued_count: int = 0
    score_sum: float = 0.0

    def add(self, row: dict[str, object]) -> None:
        favorable = float(row["favorable_move_ticks"])
        self.n += 1
        self.favorable_sum += favorable
        self.favorable_zero_count += int(favorable == 0)
        self.favorable_ge_25_count += int(favorable >= 25)
        self.favorable_ge_50_count += int(favorable >= 50)
        self.adverse_le_minus_25_count += int(favorable <= -25)
        self.adverse_le_minus_50_count += int(favorable <= -50)
        self.reverted_count += int(bool(row["reverted"]))
        self.continued_count += int(bool(row["continued"]))
        self.score_sum += float(row["score"])
        executable = row.get("executable_favorable_ticks")
        if executable is not None:
            executable_value = float(executable)
            self.executable_n += 1
            self.executable_sum += executable_value
            self.executable_positive_count += int(executable_value > 0)
            self.executable_ge_25_count += int(executable_value >= 25)
            self.executable_ge_50_count += int(executable_value >= 50)

    def as_row(self, key: tuple[object, ...], columns: list[str]) -> dict[str, object]:
        row = {column: value for column, value in zip(columns, key)}
        row.update(
            {
                "n": self.n,
                "mean_favorable_ticks": self.favorable_sum / self.n if self.n else 0.0,
                "zero_rate": self.favorable_zero_count / self.n if self.n else 0.0,
                "fav_ge_25_rate": self.favorable_ge_25_count / self.n if self.n else 0.0,
                "fav_ge_50_rate": self.favorable_ge_50_count / self.n if self.n else 0.0,
                "adv_le_-25_rate": self.adverse_le_minus_25_count / self.n if self.n else 0.0,
                "adv_le_-50_rate": self.adverse_le_minus_50_count / self.n if self.n else 0.0,
                "revert_rate": self.reverted_count / self.n if self.n else 0.0,
                "continue_rate": self.continued_count / self.n if self.n else 0.0,
                "executable_n": self.executable_n,
                "mean_executable_ticks": (
                    self.executable_sum / self.executable_n if self.executable_n else None
                ),
                "exec_positive_rate": (
                    self.executable_positive_count / self.executable_n if self.executable_n else None
                ),
                "exec_ge_25_rate": (
                    self.executable_ge_25_count / self.executable_n if self.executable_n else None
                ),
                "exec_ge_50_rate": (
                    self.executable_ge_50_count / self.executable_n if self.executable_n else None
                ),
                "mean_abs_score": self.score_sum / self.n if self.n else 0.0,
            }
        )
        return row


@dataclass(slots=True)
class _OnlineLag:
    n: int = 0
    value_sum_ns: int = 0
    max_value_ns: int = 0

    def add(self, value_ns: int) -> None:
        self.n += 1
        self.value_sum_ns += value_ns
        self.max_value_ns = max(self.max_value_ns, value_ns)

    def as_row(self, key: tuple[object, ...], columns: list[str]) -> dict[str, object]:
        row = {column: value for column, value in zip(columns, key)}
        row.update(
            {
                "n": self.n,
                "mean_quote_age_ms": (self.value_sum_ns / self.n) / 1_000_000 if self.n else 0.0,
                "max_quote_age_ms": self.max_value_ns / 1_000_000,
            }
        )
        return row


@dataclass(slots=True)
class _ReservoirSample:
    limit: int
    values: list[float] = field(default_factory=list)
    seen: int = 0

    def add(self, value: float) -> None:
        self.seen += 1
        if len(self.values) < self.limit:
            self.values.append(value)
            return
        # Deterministic reservoir replacement keeps runs reproducible enough for diagnostics.
        index = self.seen % self.limit
        self.values[index] = value

    def describe(self) -> pd.Series:
        return pd.Series(self.values).describe(percentiles=[0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99])


@dataclass(slots=True)
class _PartitionedParquetWriter:
    output_dir: Path
    row_limit: int = 250_000
    _pending: list[pd.DataFrame] = field(default_factory=list)
    _pending_rows: int = 0
    _part_index: int = 0

    def __post_init__(self) -> None:
        if self.output_dir.exists() and any(self.output_dir.iterdir()):
            raise FileExistsError(
                f"{self.output_dir} already exists and is not empty; choose a fresh --output-dir"
            )

    def add(self, frame: pd.DataFrame) -> None:
        if frame.empty:
            return
        self._pending.append(frame)
        self._pending_rows += len(frame)
        if self._pending_rows >= self.row_limit:
            self.flush()

    def flush(self) -> None:
        if not self._pending:
            return
        self.output_dir.mkdir(parents=True, exist_ok=True)
        frame = pd.concat(self._pending, ignore_index=True)
        frame.to_parquet(self.output_dir / f"part-{self._part_index:06d}.parquet", index=False)
        self._part_index += 1
        self._pending.clear()
        self._pending_rows = 0

    def close(self) -> None:
        self.flush()


def candidates_to_frame(candidates: Iterable[StrategyCandidate]) -> pd.DataFrame:
    rows = []
    for candidate in candidates:
        metadata = candidate.metadata or {}
        rows.append(
            {
                "strategy_id": candidate.strategy_id,
                "event_id": candidate.event_id,
                "record_index": candidate.record_index,
                "recv_ts_ns": candidate.recv_ts_ns,
                "kind": candidate.kind,
                "market_id": candidate.market_id,
                "reference_market_id": candidate.reference_market_id,
                "direction": candidate.direction,
                "score": candidate.score,
                "observed_ticks": candidate.observed_ticks,
                "fair_ticks": candidate.fair_ticks,
                "edge_ticks": candidate.edge_ticks,
                "route_count": metadata.get("route_count"),
                "market_index": metadata.get("market_index"),
                "left_market_id": metadata.get("left_market_id"),
                "right_market_id": metadata.get("right_market_id"),
                "left_mid_ticks": metadata.get("left_mid_ticks"),
                "right_mid_ticks": metadata.get("right_mid_ticks"),
                "entry_bid_ticks": metadata.get("entry_bid_ticks"),
                "entry_ask_ticks": metadata.get("entry_ask_ticks"),
                "entry_bid_qty_lots": metadata.get("entry_bid_qty_lots"),
                "entry_ask_qty_lots": metadata.get("entry_ask_qty_lots"),
                "entry_spread_ticks": metadata.get("entry_spread_ticks"),
            }
        )
    return pd.DataFrame(rows)


def outcomes_to_frame(outcomes: Iterable[CandidateOutcome]) -> pd.DataFrame:
    rows = []
    for outcome in outcomes:
        candidate = outcome.candidate
        metadata = candidate.metadata or {}
        target_ts_ns = outcome.effective_start_ts_ns + outcome.horizon_ns
        quote_age_at_target_ns = target_ts_ns - outcome.evaluation_recv_ts_ns
        executable_favorable_ticks = _executable_favorable_ticks(
            direction=candidate.direction,
            entry_bid_ticks=_optional_float(metadata.get("entry_bid_ticks")),
            entry_ask_ticks=_optional_float(metadata.get("entry_ask_ticks")),
            future_bid_ticks=_optional_float(outcome.future_bid_ticks),
            future_ask_ticks=_optional_float(outcome.future_ask_ticks),
        )
        rows.append(
            {
                "strategy_id": candidate.strategy_id,
                "event_id": candidate.event_id,
                "market_id": candidate.market_id,
                "reference_market_id": candidate.reference_market_id,
                "record_index": candidate.record_index,
                "recv_ts_ns": candidate.recv_ts_ns,
                "kind": candidate.kind,
                "direction": candidate.direction,
                "score": candidate.score,
                "observed_ticks": candidate.observed_ticks,
                "fair_ticks": candidate.fair_ticks,
                "edge_ticks": candidate.edge_ticks,
                "route_count": metadata.get("route_count"),
                "market_index": metadata.get("market_index"),
                "entry_bid_ticks": metadata.get("entry_bid_ticks"),
                "entry_ask_ticks": metadata.get("entry_ask_ticks"),
                "entry_bid_qty_lots": metadata.get("entry_bid_qty_lots"),
                "entry_ask_qty_lots": metadata.get("entry_ask_qty_lots"),
                "entry_spread_ticks": metadata.get("entry_spread_ticks"),
                "horizon_ns": outcome.horizon_ns,
                "horizon_label": _format_duration_ns(outcome.horizon_ns),
                "latency_ns": outcome.latency_ns,
                "latency_label": _format_duration_ns(outcome.latency_ns),
                "effective_start_ts_ns": outcome.effective_start_ts_ns,
                "target_ts_ns": target_ts_ns,
                "evaluation_recv_ts_ns": outcome.evaluation_recv_ts_ns,
                "evaluation_record_index": outcome.evaluation_record_index,
                "evaluation_delay_ns": outcome.evaluation_recv_ts_ns - candidate.recv_ts_ns,
                "evaluation_delay_label": _format_duration_ns(outcome.evaluation_recv_ts_ns - candidate.recv_ts_ns),
                "target_lag_ns": outcome.evaluation_recv_ts_ns - target_ts_ns,
                "target_lag_label": _format_duration_ns(outcome.evaluation_recv_ts_ns - target_ts_ns),
                "quote_age_at_target_ns": quote_age_at_target_ns,
                "quote_age_at_target_label": _format_duration_ns(quote_age_at_target_ns),
                "future_observed_ticks": outcome.future_observed_ticks,
                "future_bid_ticks": outcome.future_bid_ticks,
                "future_ask_ticks": outcome.future_ask_ticks,
                "move_ticks": outcome.move_ticks,
                "favorable_move_ticks": outcome.favorable_move_ticks,
                "executable_favorable_ticks": executable_favorable_ticks,
                "executable_positive": (
                    executable_favorable_ticks > 0 if executable_favorable_ticks is not None else None
                ),
                "reverted": outcome.reverted,
                "continued": outcome.continued,
                "missing_market": outcome.missing_market,
            }
        )
    return pd.DataFrame(rows)


def print_diagnostics(diagnostic: ResidualDiagnosticResult, config: ResidualDiagnosticConfig) -> None:
    result = diagnostic.backtest
    candidates = diagnostic.candidates
    outcomes = diagnostic.outcomes

    print("\nResidual Scanner Diagnostics")
    print("============================")
    print(f"run_dir={config.run_dir}")
    print(f"max_updates={config.max_updates}")
    print(f"min_abs_residual_ticks={config.min_abs_residual_ticks}")
    print(f"scan_updated_neighborhood={config.scan_updated_neighborhood}")
    print(f"cooldown={_format_duration_ns(config.cooldown_ns)}")
    print(
        "latency="
        f"{_format_duration_ns(config.decision_latency_ns + config.order_latency_ns)} "
        f"(decision={_format_duration_ns(config.decision_latency_ns)}, "
        f"order={_format_duration_ns(config.order_latency_ns)})"
    )
    print(f"horizons={', '.join(_format_duration_ns(value) for value in config.horizons_ns)}")

    _print_section("Backtest Summary")
    print(f"events={result.event_count:,}")
    print(f"active_events={result.active_event_count:,}")
    print(f"updates={result.updates_seen:,}")
    print(f"snapshots={result.snapshots_seen:,}")
    print(f"raw_candidates={result.raw_candidate_count:,}")
    print(f"deduped_candidates={result.deduped_candidate_count:,}")
    print(f"kept_candidates={len(result.candidates):,}")
    print(f"outcomes={len(result.outcomes):,}")
    if result.raw_candidate_count:
        print(f"dedup_compression={result.deduped_candidate_count / result.raw_candidate_count:.2%}")
    if result.candidates:
        print(f"outcomes_per_kept_candidate={len(result.outcomes) / len(result.candidates):.2f}")
    print(f"errors={len(result.errors):,}")
    for error in result.errors[:5]:
        print(f"  {error}")

    if candidates.empty:
        print("\nNo candidates emitted.")
        return

    _print_section("Candidate Score Distribution")
    print(_describe(candidates["score"]))

    _print_section("Candidates By Direction")
    print(_value_counts(candidates, ["direction"], value_name="candidates"))

    _print_section("Candidates By Event")
    print(_top_counts(candidates, ["event_id"], "candidates", config.top_n))

    _print_section("Candidates By Market")
    print(_top_counts(candidates, ["event_id", "market_id", "direction"], "candidates", config.top_n))

    if outcomes.empty:
        print("\nNo outcomes labeled yet. Increase max_updates or shorten horizons.")
        return

    _print_section("Outcome Coverage")
    expected_outcomes = len(result.candidates) * len(config.horizons_ns)
    print(f"expected_outcomes_if_fully_labeled={expected_outcomes:,}")
    print(f"labeled_outcomes={len(outcomes):,}")
    if expected_outcomes:
        print(f"coverage={len(outcomes) / expected_outcomes:.2%}")
    print(f"missing_market={int(outcomes['missing_market'].sum()):,}")
    print(f"null_favorable_moves={int(outcomes['favorable_move_ticks'].isna().sum()):,}")

    valid = outcomes[outcomes["favorable_move_ticks"].notna()].copy()
    if valid.empty:
        print("\nNo directional outcomes with favorable_move_ticks.")
        return

    _print_section("Outcome Quality By Horizon")
    print(_quality_table(valid, ["horizon_label"]))

    _print_section("Outcome Quality By Horizon And Direction")
    print(_quality_table(valid, ["horizon_label", "direction"]))

    _print_section("Outcome Quality By Score Bucket")
    print(_score_bucket_quality(valid, config.score_buckets))

    _print_section("Outcome Quality By Event")
    print(_quality_table(valid, ["event_id"]).sort_values(["n", "mean_favorable_ticks"], ascending=[False, False]).head(config.top_n))

    _print_section("Outcome Quality By Route Count")
    if "route_count" in valid and valid["route_count"].notna().any():
        print(_quality_table(valid, ["route_count"]))
    else:
        print("route_count unavailable")

    _print_section("Quote Age At Target By Horizon")
    quote_age = (
        valid.groupby("horizon_label", observed=True)["quote_age_at_target_ns"]
        .agg(
            n="size",
            mean_quote_age_ms=lambda item: item.mean() / 1_000_000,
            median_quote_age_ms=lambda item: item.median() / 1_000_000,
            p95_quote_age_ms=lambda item: item.quantile(0.95) / 1_000_000,
            max_quote_age_ms=lambda item: item.max() / 1_000_000,
        )
        .reset_index()
    )
    print(quote_age)

    _print_section("Best Outcome Rows")
    print(
        valid.sort_values("favorable_move_ticks", ascending=False)
        .head(config.top_n)[
            [
                "event_id",
                "market_id",
                "direction",
                "score",
                "horizon_label",
                "favorable_move_ticks",
                "executable_favorable_ticks",
                "move_ticks",
                "entry_spread_ticks",
                "route_count",
                "market_index",
            ]
        ]
    )

    _print_section("Worst Outcome Rows")
    print(
        valid.sort_values("favorable_move_ticks", ascending=True)
        .head(config.top_n)[
            [
                "event_id",
                "market_id",
                "direction",
                "score",
                "horizon_label",
                "favorable_move_ticks",
                "executable_favorable_ticks",
                "move_ticks",
                "entry_spread_ticks",
                "route_count",
                "market_index",
            ]
        ]
    )


def print_streaming_diagnostics(
    summary: _StreamingSummary,
    config: ResidualDiagnosticConfig,
    candidates_dir: Path,
    outcomes_dir: Path,
) -> None:
    print("\nResidual Scanner Diagnostics")
    print("============================")
    print(f"run_dir={config.run_dir}")
    print("mode=streaming")
    print(f"max_updates={config.max_updates}")
    print(f"min_abs_residual_ticks={config.min_abs_residual_ticks}")
    print(f"scan_updated_neighborhood={config.scan_updated_neighborhood}")
    print(f"cooldown={_format_duration_ns(config.cooldown_ns)}")
    print(
        "latency="
        f"{_format_duration_ns(config.decision_latency_ns + config.order_latency_ns)} "
        f"(decision={_format_duration_ns(config.decision_latency_ns)}, "
        f"order={_format_duration_ns(config.order_latency_ns)})"
    )
    print(f"horizons={', '.join(_format_duration_ns(value) for value in config.horizons_ns)}")

    _print_section("Backtest Summary")
    print(f"events={summary.event_count:,}")
    print(f"active_events={summary.active_event_count:,}")
    print(f"updates={summary.updates_seen:,}")
    print(f"snapshots={summary.snapshots_seen:,}")
    print(f"raw_candidates={summary.raw_candidate_count:,}")
    print(f"deduped_candidates={summary.deduped_candidate_count:,}")
    print(f"kept_candidates={summary.kept_candidate_count:,}")
    print(f"outcomes={summary.outcome_count:,}")
    if summary.raw_candidate_count:
        print(f"dedup_compression={summary.deduped_candidate_count / summary.raw_candidate_count:.2%}")
    if summary.kept_candidate_count:
        print(f"outcomes_per_kept_candidate={summary.outcome_count / summary.kept_candidate_count:.2f}")
    print(f"errors={len(summary.errors):,}")
    for error in summary.errors[:5]:
        print(f"  {error}")

    if summary.kept_candidate_count == 0:
        print("\nNo candidates emitted.")
        return

    _print_section("Candidate Score Distribution")
    print(summary.score_sample.describe())

    _print_section("Candidates By Direction")
    print(_counter_to_frame(summary.direction_counts, ["direction"], "candidates"))

    _print_section("Candidates By Event")
    print(_counter_to_frame(summary.event_counts, ["event_id"], "candidates").head(config.top_n))

    _print_section("Candidates By Market")
    print(_counter_to_frame(summary.market_counts, ["event_id", "market_id", "direction"], "candidates").head(config.top_n))

    if summary.outcome_count == 0:
        print("\nNo outcomes labeled yet. Increase max_updates or shorten horizons.")
        print(f"\nwrote candidates to {candidates_dir}")
        return

    _print_section("Outcome Coverage")
    expected_outcomes = summary.kept_candidate_count * len(config.horizons_ns)
    print(f"expected_outcomes_if_fully_labeled={expected_outcomes:,}")
    print(f"labeled_outcomes={summary.outcome_count:,}")
    if expected_outcomes:
        print(f"coverage={summary.outcome_count / expected_outcomes:.2%}")
    print(f"missing_market={summary.missing_market_count:,}")
    print(f"null_favorable_moves={summary.null_favorable_move_count:,}")

    _print_section("Outcome Quality By Horizon")
    print(_online_quality_frame(summary.quality_by_horizon, ["horizon_label"]))

    _print_section("Outcome Quality By Horizon And Direction")
    print(_online_quality_frame(summary.quality_by_horizon_direction, ["horizon_label", "direction"]))

    _print_section("Outcome Quality By Score Band")
    print(_online_quality_frame(summary.quality_by_score_band, ["horizon_label", "score_band"]))

    _print_section("Outcome Quality By Event")
    event_quality = _online_quality_frame(summary.quality_by_event, ["event_id"])
    if not event_quality.empty:
        print(event_quality.sort_values(["n", "mean_favorable_ticks"], ascending=[False, False]).head(config.top_n))
    else:
        print(event_quality)

    _print_section("Outcome Quality By Route Count")
    print(_online_quality_frame(summary.quality_by_route_count, ["route_count"]))

    _print_section("Quote Age At Target By Horizon")
    print(_online_lag_frame(summary.quote_age_by_horizon, ["horizon_label"]))

    _print_section("Best Outcome Rows")
    print(_heap_rows(summary.best_outcomes, reverse=True))

    _print_section("Worst Outcome Rows")
    print(_heap_rows(summary.worst_outcomes, reverse=False))

    print(f"\nwrote candidates to {candidates_dir}")
    print(f"wrote outcomes to {outcomes_dir}")


def write_diagnostics(diagnostic: ResidualDiagnosticResult, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    if not diagnostic.candidates.empty:
        diagnostic.candidates.to_parquet(output_dir / "research_candidates.parquet", index=False)
    if not diagnostic.outcomes.empty:
        diagnostic.outcomes.to_parquet(output_dir / "research_outcomes.parquet", index=False)
    print(f"\nwrote diagnostics to {output_dir}")


def _quality_table(frame: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    aggregations = {
        "n": ("favorable_move_ticks", "size"),
        "mean_favorable_ticks": ("favorable_move_ticks", "mean"),
        "median_favorable_ticks": ("favorable_move_ticks", "median"),
        "p25_favorable_ticks": ("favorable_move_ticks", lambda item: item.quantile(0.25)),
        "p75_favorable_ticks": ("favorable_move_ticks", lambda item: item.quantile(0.75)),
        "zero_rate": ("favorable_move_ticks", lambda item: (item == 0).mean()),
        "fav_ge_25_rate": ("favorable_move_ticks", lambda item: (item >= 25).mean()),
        "fav_ge_50_rate": ("favorable_move_ticks", lambda item: (item >= 50).mean()),
        "adv_le_-25_rate": ("favorable_move_ticks", lambda item: (item <= -25).mean()),
        "adv_le_-50_rate": ("favorable_move_ticks", lambda item: (item <= -50).mean()),
        "revert_rate": ("reverted", "mean"),
        "continue_rate": ("continued", "mean"),
        "mean_abs_score": ("score", "mean"),
    }
    if "executable_favorable_ticks" in frame.columns:
        aggregations.update(
            {
                "executable_n": ("executable_favorable_ticks", "count"),
                "mean_executable_ticks": ("executable_favorable_ticks", "mean"),
                "exec_positive_rate": ("executable_positive", "mean"),
                "exec_ge_25_rate": ("executable_favorable_ticks", lambda item: (item >= 25).mean()),
                "exec_ge_50_rate": ("executable_favorable_ticks", lambda item: (item >= 50).mean()),
            }
        )
    return (
        frame.groupby(group_cols, observed=True)
        .agg(**aggregations)
        .reset_index()
    )


def _score_bucket_quality(frame: pd.DataFrame, bucket_count: int) -> pd.DataFrame:
    bucketed = frame.copy()
    bucketed["score_bucket"] = pd.qcut(bucketed["score"], q=bucket_count, duplicates="drop")
    return _quality_table(bucketed, ["horizon_label", "score_bucket"])


def _top_counts(frame: pd.DataFrame, group_cols: list[str], value_name: str, top_n: int) -> pd.DataFrame:
    return (
        frame.groupby(group_cols, observed=True)
        .size()
        .reset_index(name=value_name)
        .sort_values(value_name, ascending=False)
        .head(top_n)
    )


def _value_counts(frame: pd.DataFrame, group_cols: list[str], value_name: str) -> pd.DataFrame:
    return (
        frame.groupby(group_cols, observed=True)
        .size()
        .reset_index(name=value_name)
        .sort_values(value_name, ascending=False)
    )


def _describe(series: pd.Series) -> pd.Series:
    return series.describe(percentiles=[0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99])


def _online_quality_frame(items: dict[tuple[object, ...], _OnlineQuality], columns: list[str]) -> pd.DataFrame:
    rows = [quality.as_row(key, columns) for key, quality in items.items() if quality.n > 0]
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(columns)


def _online_lag_frame(items: dict[tuple[object, ...], _OnlineLag], columns: list[str]) -> pd.DataFrame:
    rows = [lag.as_row(key, columns) for key, lag in items.items() if lag.n > 0]
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(columns)


def _counter_to_frame(counter: Counter, columns: list[str], value_name: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for key, value in counter.most_common():
        key_tuple = key if isinstance(key, tuple) else (key,)
        row = {column: key_value for column, key_value in zip(columns, key_tuple)}
        row[value_name] = value
        rows.append(row)
    return pd.DataFrame(rows)


def _heap_rows(heap: list[tuple[float, int, dict[str, object]]], *, reverse: bool) -> pd.DataFrame:
    rows = [row for _score, _sequence, row in heap]
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values("favorable_move_ticks", ascending=not reverse)


def _outcome_summary_row(outcome: CandidateOutcome) -> dict[str, object]:
    candidate = outcome.candidate
    metadata = candidate.metadata or {}
    target_ts_ns = outcome.effective_start_ts_ns + outcome.horizon_ns
    quote_age_at_target_ns = target_ts_ns - outcome.evaluation_recv_ts_ns
    executable_favorable_ticks = _executable_favorable_ticks(
        direction=candidate.direction,
        entry_bid_ticks=_optional_float(metadata.get("entry_bid_ticks")),
        entry_ask_ticks=_optional_float(metadata.get("entry_ask_ticks")),
        future_bid_ticks=_optional_float(outcome.future_bid_ticks),
        future_ask_ticks=_optional_float(outcome.future_ask_ticks),
    )
    return {
        "event_id": candidate.event_id,
        "market_id": candidate.market_id,
        "direction": candidate.direction,
        "score": candidate.score,
        "horizon_label": _format_duration_ns(outcome.horizon_ns),
        "target_lag_ns": outcome.evaluation_recv_ts_ns - target_ts_ns,
        "quote_age_at_target_ns": quote_age_at_target_ns,
        "favorable_move_ticks": outcome.favorable_move_ticks,
        "executable_favorable_ticks": executable_favorable_ticks,
        "move_ticks": outcome.move_ticks,
        "entry_spread_ticks": metadata.get("entry_spread_ticks"),
        "reverted": outcome.reverted,
        "continued": outcome.continued,
        "route_count": metadata.get("route_count"),
        "market_index": metadata.get("market_index"),
    }


def _score_band(score: float) -> str:
    if score < 50:
        return "025-050"
    if score < 100:
        return "050-100"
    if score < 250:
        return "100-250"
    if score < 500:
        return "250-500"
    if score < 1_000:
        return "500-1000"
    return "1000+"


def _executable_favorable_ticks(
    *,
    direction: str,
    entry_bid_ticks: float | None,
    entry_ask_ticks: float | None,
    future_bid_ticks: float | None,
    future_ask_ticks: float | None,
) -> float | None:
    if direction == "buy_yes_fade_up":
        if entry_ask_ticks is None or future_bid_ticks is None:
            return None
        return future_bid_ticks - entry_ask_ticks
    if direction == "sell_yes_fade_down":
        if entry_bid_ticks is None or future_ask_ticks is None:
            return None
        return entry_bid_ticks - future_ask_ticks
    return None


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _print_section(title: str) -> None:
    print(f"\n{title}")
    print("-" * len(title))


def _format_duration_ns(value: int) -> str:
    sign = "-" if value < 0 else ""
    value = abs(value)
    if value >= 1_000_000_000:
        seconds = value / 1_000_000_000
        return f"{sign}{seconds:g}s"
    if value >= 1_000_000:
        milliseconds = value / 1_000_000
        return f"{sign}{milliseconds:g}ms"
    if value >= 1_000:
        microseconds = value / 1_000
        return f"{sign}{microseconds:g}us"
    return f"{sign}{value}ns"


def _parse_horizons(raw: str) -> tuple[int, ...]:
    return tuple(_parse_duration_ns(part.strip()) for part in raw.split(",") if part.strip())


def _parse_duration_ns(raw: str) -> int:
    value = raw.strip().lower()
    if value.endswith("ms"):
        return int(float(value[:-2]) * 1_000_000)
    if value.endswith("us"):
        return int(float(value[:-2]) * 1_000)
    if value.endswith("ns"):
        return int(float(value[:-2]))
    if value.endswith("s"):
        return int(float(value[:-1]) * 1_000_000_000)
    return int(float(value))


def _parse_args() -> ResidualDiagnosticConfig:
    parser = argparse.ArgumentParser(description="Run monotonic-chain residual scanner diagnostics.")
    parser.add_argument("--run-dir", type=Path, default=DEFAULT_RUN_DIR)
    parser.add_argument("--max-updates", type=int, default=1_000_000)
    parser.add_argument("--full", action="store_true", help="Scan the full run instead of max-updates.")
    parser.add_argument("--min-abs-residual-ticks", type=float, default=25.0)
    parser.add_argument(
        "--full-chain-scan",
        action="store_true",
        help="Recompute residual candidates for every initialized market on every update.",
    )
    parser.add_argument("--cooldown", default="5s")
    parser.add_argument("--decision-latency", default="1ms")
    parser.add_argument("--order-latency", default="5ms")
    parser.add_argument("--horizons", default="1s,5s,30s,60s")
    parser.add_argument("--include-trades", action="store_true")
    parser.add_argument("--include-lifecycle", action="store_true")
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--score-buckets", type=int, default=10)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()

    return ResidualDiagnosticConfig(
        run_dir=args.run_dir,
        max_updates=None if args.full else args.max_updates,
        min_abs_residual_ticks=args.min_abs_residual_ticks,
        scan_updated_neighborhood=not args.full_chain_scan,
        cooldown_ns=_parse_duration_ns(args.cooldown),
        decision_latency_ns=_parse_duration_ns(args.decision_latency),
        order_latency_ns=_parse_duration_ns(args.order_latency),
        horizons_ns=_parse_horizons(args.horizons),
        include_trades=args.include_trades,
        include_lifecycle=args.include_lifecycle,
        top_n=args.top_n,
        score_buckets=args.score_buckets,
        output_dir=args.output_dir,
    )


def main() -> int:
    run_residual_diagnostics(_parse_args())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
