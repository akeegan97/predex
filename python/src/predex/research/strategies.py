from __future__ import annotations

"""Research strategies for monotonic chains.

Thesis:
- Hard monotonic violations are the known structural edge: if a harder strike
  can be sold above the easier strike's buy price, the chain has leaked an
  executable relationship.
- The larger research question is statistical: individual strikes may be locally
  coherent, while the whole strike family reveals residuals versus a chain-level
  fair curve.
- The first pass deliberately stays model-agnostic. A simple local monotonic
  curve scanner emits candidates; later we can swap in splines, beta/log-normal
  CDFs, and family/time-to-expiry priors without changing the Strategy contract.
"""

from dataclasses import dataclass, field
from typing import Protocol

from .candidates import StrategyCandidate
from .features import ChainFeatureSnapshot, MarketFeatureSnapshot
from .intents import OrderIntent, OrderSide, TimeInForce


@dataclass(frozen=True, slots=True)
class StrategyDecision:
    candidates: tuple[StrategyCandidate, ...] = ()
    intents: tuple[OrderIntent, ...] = ()


class Strategy(Protocol):
    strategy_id: str

    def on_chain_snapshot(self, snapshot: ChainFeatureSnapshot) -> StrategyDecision:
        """Inspect an event-level chain snapshot and optionally emit candidates/intents."""


@dataclass(frozen=True, slots=True)
class NoopStrategy:
    strategy_id: str = "noop"

    def on_chain_snapshot(self, snapshot: ChainFeatureSnapshot) -> StrategyDecision:
        return StrategyDecision()


@dataclass(frozen=True, slots=True)
class MonotonicHardArbStrategy:
    strategy_id: str = "monotonic_hard_arb"
    min_edge_ticks: int = 1
    max_qty_lots: int = 1
    emit_intents: bool = True

    def on_chain_snapshot(self, snapshot: ChainFeatureSnapshot) -> StrategyDecision:
        markets = _initialized_markets(snapshot.markets)
        candidates: list[StrategyCandidate] = []
        intents: list[OrderIntent] = []

        for lower, higher in zip(markets, markets[1:]):
            if lower.best_ask_ticks is None or higher.best_bid_ticks is None:
                continue

            raw_edge = higher.best_bid_ticks - lower.best_ask_ticks
            edge_after_buffer = raw_edge - self.min_edge_ticks
            if edge_after_buffer < 0:
                continue

            qty_lots = min(
                self.max_qty_lots,
                lower.best_ask_qty_lots,
                higher.best_bid_qty_lots,
            )
            if qty_lots <= 0:
                continue

            candidate = StrategyCandidate(
                strategy_id=self.strategy_id,
                event_id=snapshot.event_id,
                record_index=snapshot.record_index,
                recv_ts_ns=snapshot.recv_ts_ns,
                kind="hard_monotonic_violation",
                market_id=higher.market_id,
                reference_market_id=lower.market_id,
                direction="buy_lower_sell_higher",
                score=float(edge_after_buffer),
                observed_ticks=float(higher.best_bid_ticks),
                fair_ticks=float(lower.best_ask_ticks),
                edge_ticks=float(edge_after_buffer),
                metadata={
                    "lower_market_id": lower.market_id,
                    "higher_market_id": higher.market_id,
                    "lower_ask_ticks": lower.best_ask_ticks,
                    "higher_bid_ticks": higher.best_bid_ticks,
                    "raw_edge_ticks": raw_edge,
                    "qty_lots": qty_lots,
                },
            )
            candidates.append(candidate)

            if self.emit_intents:
                intents.extend(
                    (
                        OrderIntent(
                            strategy_id=self.strategy_id,
                            event_id=snapshot.event_id,
                            market_id=lower.market_id,
                            side=OrderSide.BUY_YES,
                            limit_price_ticks=lower.best_ask_ticks,
                            qty_lots=qty_lots,
                            time_in_force=TimeInForce.IOC,
                            reason="hard monotonic violation: buy easier strike",
                            metadata={"paired_market_id": higher.market_id, "edge_ticks": edge_after_buffer},
                        ),
                        OrderIntent(
                            strategy_id=self.strategy_id,
                            event_id=snapshot.event_id,
                            market_id=higher.market_id,
                            side=OrderSide.SELL_YES,
                            limit_price_ticks=higher.best_bid_ticks,
                            qty_lots=qty_lots,
                            time_in_force=TimeInForce.IOC,
                            reason="hard monotonic violation: sell harder strike",
                            metadata={"paired_market_id": lower.market_id, "edge_ticks": edge_after_buffer},
                        ),
                    )
                )

        return StrategyDecision(candidates=tuple(candidates), intents=tuple(intents))


@dataclass(frozen=True, slots=True)
class MonotonicResidualScanner:
    strategy_id: str = "monotonic_residual_scanner"
    min_abs_residual_ticks: float = 5.0
    min_neighbor_count: int = 2
    scan_updated_neighborhood: bool = True
    emit_intents: bool = False
    context: dict[str, object] = field(default_factory=dict)

    def on_chain_snapshot(self, snapshot: ChainFeatureSnapshot) -> StrategyDecision:
        markets = _initialized_markets(snapshot.markets)
        if len(markets) < max(3, self.min_neighbor_count + 1):
            return StrategyDecision()

        candidates: list[StrategyCandidate] = []
        by_index = {market.event_market_index: market for market in markets if market.mid_ticks is not None}
        candidate_markets = self._candidate_markets(snapshot, markets, by_index)

        for market in candidate_markets:
            if market.mid_ticks is None:
                continue
            lower_neighbor = _nearest_initialized_neighbor(by_index, market.event_market_index, direction=-1)
            higher_neighbor = _nearest_initialized_neighbor(by_index, market.event_market_index, direction=1)
            if lower_neighbor is None or higher_neighbor is None:
                continue
            if lower_neighbor.mid_ticks is None or higher_neighbor.mid_ticks is None:
                continue

            fair_ticks = _linear_interpolated_mid(
                lower_neighbor,
                market.event_market_index,
                higher_neighbor,
            )
            residual_ticks = market.mid_ticks - fair_ticks
            if abs(residual_ticks) < self.min_abs_residual_ticks:
                continue

            direction = "sell_yes_fade_down" if residual_ticks > 0 else "buy_yes_fade_up"
            candidates.append(
                StrategyCandidate(
                    strategy_id=self.strategy_id,
                    event_id=snapshot.event_id,
                    record_index=snapshot.record_index,
                    recv_ts_ns=snapshot.recv_ts_ns,
                    kind="local_monotonic_residual",
                    market_id=market.market_id,
                    reference_market_id=None,
                    direction=direction,
                    score=abs(float(residual_ticks)),
                    observed_ticks=float(market.mid_ticks),
                    fair_ticks=float(fair_ticks),
                    edge_ticks=float(residual_ticks),
                    metadata={
                        "left_market_id": lower_neighbor.market_id,
                        "right_market_id": higher_neighbor.market_id,
                        "left_mid_ticks": lower_neighbor.mid_ticks,
                        "right_mid_ticks": higher_neighbor.mid_ticks,
                        "entry_bid_ticks": market.best_bid_ticks,
                        "entry_ask_ticks": market.best_ask_ticks,
                        "entry_bid_qty_lots": market.best_bid_qty_lots,
                        "entry_ask_qty_lots": market.best_ask_qty_lots,
                        "entry_spread_ticks": (
                            market.best_ask_ticks - market.best_bid_ticks
                            if market.best_bid_ticks is not None and market.best_ask_ticks is not None
                            else None
                        ),
                        "market_index": market.event_market_index,
                        **self.context,
                    },
                )
            )

        return StrategyDecision(candidates=tuple(candidates), intents=())

    def _candidate_markets(
        self,
        snapshot: ChainFeatureSnapshot,
        markets: tuple[MarketFeatureSnapshot, ...],
        by_index: dict[int, MarketFeatureSnapshot],
    ) -> tuple[MarketFeatureSnapshot, ...]:
        if not self.scan_updated_neighborhood:
            return markets

        by_market_id = {market.market_id: market for market in markets}
        updated = by_market_id.get(snapshot.updated_market_id)
        if updated is None:
            return ()

        affected_indices = {updated.event_market_index}
        lower_neighbor = _nearest_initialized_neighbor(by_index, updated.event_market_index, direction=-1)
        higher_neighbor = _nearest_initialized_neighbor(by_index, updated.event_market_index, direction=1)
        if lower_neighbor is not None:
            affected_indices.add(lower_neighbor.event_market_index)
        if higher_neighbor is not None:
            affected_indices.add(higher_neighbor.event_market_index)

        return tuple(
            by_index[index]
            for index in sorted(affected_indices)
            if index in by_index
        )


def _initialized_markets(markets: tuple[MarketFeatureSnapshot, ...]) -> tuple[MarketFeatureSnapshot, ...]:
    return tuple(
        sorted(
            (market for market in markets if market.initialized),
            key=lambda market: market.event_market_index,
        )
    )


def _nearest_initialized_neighbor(
    by_index: dict[int, MarketFeatureSnapshot],
    start_index: int,
    *,
    direction: int,
) -> MarketFeatureSnapshot | None:
    index = start_index + direction
    while 0 <= index <= max(by_index, default=-1):
        if index in by_index:
            return by_index[index]
        index += direction
    return None


def _linear_interpolated_mid(
    left: MarketFeatureSnapshot,
    target_index: int,
    right: MarketFeatureSnapshot,
) -> float:
    if left.mid_ticks is None or right.mid_ticks is None:
        raise ValueError("cannot interpolate with missing midpoint")
    width = right.event_market_index - left.event_market_index
    if width <= 0:
        return left.mid_ticks
    weight = (target_index - left.event_market_index) / width
    return left.mid_ticks + (right.mid_ticks - left.mid_ticks) * weight
