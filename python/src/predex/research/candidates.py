from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True, slots=True)
class StrategyCandidate:
    strategy_id: str
    event_id: int
    record_index: int
    recv_ts_ns: int
    kind: str
    market_id: int
    direction: str
    score: float
    reference_market_id: int | None = None
    observed_ticks: float | None = None
    fair_ticks: float | None = None
    edge_ticks: float | None = None
    metadata: Mapping[str, object] | None = None
