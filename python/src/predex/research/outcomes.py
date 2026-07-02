from __future__ import annotations

from dataclasses import dataclass, field

from .candidates import StrategyCandidate
from .features import ChainFeatureSnapshot, MarketFeatureSnapshot


@dataclass(frozen=True, slots=True)
class CandidateDedupConfig:
    cooldown_ns: int = 0


@dataclass(frozen=True, slots=True)
class CandidateOutcomeConfig:
    horizons_ns: tuple[int, ...]
    decision_latency_ns: int = 0
    order_latency_ns: int = 0

    @property
    def total_latency_ns(self) -> int:
        return self.decision_latency_ns + self.order_latency_ns


@dataclass(frozen=True, slots=True)
class CandidateOutcome:
    candidate: StrategyCandidate
    horizon_ns: int
    latency_ns: int
    effective_start_ts_ns: int
    evaluation_recv_ts_ns: int
    evaluation_record_index: int
    future_observed_ticks: float | None
    future_bid_ticks: int | None
    future_ask_ticks: int | None
    move_ticks: float | None
    favorable_move_ticks: float | None
    reverted: bool
    continued: bool
    missing_market: bool = False


@dataclass(slots=True)
class CandidateDeduper:
    config: CandidateDedupConfig = field(default_factory=CandidateDedupConfig)
    _last_seen_by_key: dict[tuple[object, ...], int] = field(default_factory=dict)

    def allow(self, candidate: StrategyCandidate) -> bool:
        key = _candidate_dedup_key(candidate)
        last_seen = self._last_seen_by_key.get(key)
        if last_seen is not None and candidate.recv_ts_ns - last_seen < self.config.cooldown_ns:
            return False
        self._last_seen_by_key[key] = candidate.recv_ts_ns
        return True


@dataclass(slots=True)
class CandidateOutcomeTracker:
    config: CandidateOutcomeConfig
    outcomes: list[CandidateOutcome] = field(default_factory=list)
    _pending: list[_PendingOutcome] = field(default_factory=list)

    def add(self, candidate: StrategyCandidate) -> None:
        latency_ns = self.config.total_latency_ns
        effective_start_ts_ns = candidate.recv_ts_ns + latency_ns
        for horizon_ns in self.config.horizons_ns:
            self._pending.append(
                _PendingOutcome(
                    candidate=candidate,
                    horizon_ns=horizon_ns,
                    latency_ns=latency_ns,
                    effective_start_ts_ns=effective_start_ts_ns,
                    target_ts_ns=effective_start_ts_ns + horizon_ns,
                )
            )

    def observe(self, snapshot: ChainFeatureSnapshot, *, watermark_ts_ns: int | None = None) -> None:
        if not self._pending:
            return

        watermark = snapshot.recv_ts_ns if watermark_ts_ns is None else watermark_ts_ns
        still_pending: list[_PendingOutcome] = []
        markets_by_id = {market.market_id: market for market in snapshot.markets}
        for pending in self._pending:
            if watermark < pending.target_ts_ns:
                still_pending.append(pending)
                continue
            self.outcomes.append(_label_outcome(pending, snapshot, markets_by_id))
        self._pending = still_pending

    @property
    def pending_count(self) -> int:
        return len(self._pending)


@dataclass(frozen=True, slots=True)
class _PendingOutcome:
    candidate: StrategyCandidate
    horizon_ns: int
    latency_ns: int
    effective_start_ts_ns: int
    target_ts_ns: int


def _candidate_dedup_key(candidate: StrategyCandidate) -> tuple[object, ...]:
    return (
        candidate.strategy_id,
        candidate.event_id,
        candidate.market_id,
        candidate.kind,
        candidate.direction,
        candidate.reference_market_id,
    )


def _label_outcome(
    pending: _PendingOutcome,
    snapshot: ChainFeatureSnapshot,
    markets_by_id: dict[int, MarketFeatureSnapshot],
) -> CandidateOutcome:
    market = markets_by_id.get(pending.candidate.market_id)
    if market is None or market.mid_ticks is None or pending.candidate.observed_ticks is None:
        return CandidateOutcome(
            candidate=pending.candidate,
            horizon_ns=pending.horizon_ns,
            latency_ns=pending.latency_ns,
            effective_start_ts_ns=pending.effective_start_ts_ns,
            evaluation_recv_ts_ns=snapshot.recv_ts_ns,
            evaluation_record_index=snapshot.record_index,
            future_observed_ticks=market.mid_ticks if market is not None else None,
            future_bid_ticks=market.best_bid_ticks if market is not None else None,
            future_ask_ticks=market.best_ask_ticks if market is not None else None,
            move_ticks=None,
            favorable_move_ticks=None,
            reverted=False,
            continued=False,
            missing_market=market is None,
        )

    move_ticks = market.mid_ticks - pending.candidate.observed_ticks
    favorable_move_ticks = _favorable_move_ticks(pending.candidate.direction, move_ticks)
    return CandidateOutcome(
        candidate=pending.candidate,
        horizon_ns=pending.horizon_ns,
        latency_ns=pending.latency_ns,
        effective_start_ts_ns=pending.effective_start_ts_ns,
        evaluation_recv_ts_ns=snapshot.recv_ts_ns,
        evaluation_record_index=snapshot.record_index,
        future_observed_ticks=float(market.mid_ticks),
        future_bid_ticks=market.best_bid_ticks,
        future_ask_ticks=market.best_ask_ticks,
        move_ticks=float(move_ticks),
        favorable_move_ticks=favorable_move_ticks,
        reverted=favorable_move_ticks is not None and favorable_move_ticks > 0,
        continued=favorable_move_ticks is not None and favorable_move_ticks < 0,
        missing_market=False,
    )


def _favorable_move_ticks(direction: str, move_ticks: float) -> float | None:
    if direction == "buy_yes_fade_up":
        return float(move_ticks)
    if direction == "sell_yes_fade_down":
        return float(-move_ticks)
    return None
