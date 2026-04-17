from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class AuditEvent:
    kind: str
    ts_ns: int
    shard_id: int
    signal_id: int
    group_id: int
    local_intent_id: int
    oms_request_id: int
    exchange: int
    event_id: int
    market_id: int
    side: int
    leg_index: int
    leg_count: int
    qty_lots: int
    aux_qty_lots: int
    price_ticks: int
    aux_price_ticks: int
    edge_ticks: int
    score: int
    decision_code: int
    reject_reason: int
    lifecycle_kind: int
    order_status: int
    event_exposure_lots: int
    market_exposure_lots: int

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "AuditEvent":
        return cls(
            kind=str(payload.get("kind", "")),
            ts_ns=int(payload.get("ts_ns", 0)),
            shard_id=int(payload.get("shard_id", 0)),
            signal_id=int(payload.get("signal_id", 0)),
            group_id=int(payload.get("group_id", 0)),
            local_intent_id=int(payload.get("local_intent_id", 0)),
            oms_request_id=int(payload.get("oms_request_id", 0)),
            exchange=int(payload.get("exchange", 0)),
            event_id=int(payload.get("event_id", 0)),
            market_id=int(payload.get("market_id", 0)),
            side=int(payload.get("side", 0)),
            leg_index=int(payload.get("leg_index", 0)),
            leg_count=int(payload.get("leg_count", 0)),
            qty_lots=int(payload.get("qty_lots", 0)),
            aux_qty_lots=int(payload.get("aux_qty_lots", 0)),
            price_ticks=int(payload.get("price_ticks", 0)),
            aux_price_ticks=int(payload.get("aux_price_ticks", 0)),
            edge_ticks=int(payload.get("edge_ticks", 0)),
            score=int(payload.get("score", 0)),
            decision_code=int(payload.get("decision_code", 0)),
            reject_reason=int(payload.get("reject_reason", 0)),
            lifecycle_kind=int(payload.get("lifecycle_kind", 0)),
            order_status=int(payload.get("order_status", 0)),
            event_exposure_lots=int(payload.get("event_exposure_lots", 0)),
            market_exposure_lots=int(payload.get("market_exposure_lots", 0)),
        )

    @property
    def key(self) -> tuple[int, int]:
        return (self.shard_id, self.signal_id)


@dataclass(frozen=True, slots=True)
class SignalBundle:
    shard_id: int
    signal_id: int
    group_signal: AuditEvent
    local_risk_events: tuple[AuditEvent, ...]
    submission_events: tuple[AuditEvent, ...]
    oms_decisions: tuple[AuditEvent, ...]
    oms_transports: tuple[AuditEvent, ...]
    oms_lifecycles: tuple[AuditEvent, ...]
    shard_reconciles: tuple[AuditEvent, ...]

    @property
    def event_id(self) -> int:
        return self.group_signal.event_id

    def legs(self) -> tuple[AuditEvent, ...]:
        source = self.submission_events or self.local_risk_events
        return tuple(sorted(source, key=lambda event: (event.leg_index, event.market_id, event.ts_ns)))


def load_audit_events(path: str | Path) -> list[AuditEvent]:
    events: list[AuditEvent] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            events.append(AuditEvent.from_json(json.loads(line)))
    return events


def build_signal_bundles(events: list[AuditEvent]) -> dict[tuple[int, int], SignalBundle]:
    grouped: dict[tuple[int, int], list[AuditEvent]] = {}
    for event in events:
        if event.signal_id == 0:
            continue
        grouped.setdefault(event.key, []).append(event)

    bundles: dict[tuple[int, int], SignalBundle] = {}
    for key, signal_events in grouped.items():
        group_signal = next((event for event in signal_events if event.kind == "group_signal"), None)
        if group_signal is None:
            continue
        bundles[key] = SignalBundle(
            shard_id=key[0],
            signal_id=key[1],
            group_signal=group_signal,
            local_risk_events=tuple(event for event in signal_events if event.kind == "local_risk"),
            submission_events=tuple(event for event in signal_events if event.kind == "submission"),
            oms_decisions=tuple(event for event in signal_events if event.kind == "oms_decision"),
            oms_transports=tuple(event for event in signal_events if event.kind == "oms_transport"),
            oms_lifecycles=tuple(event for event in signal_events if event.kind == "oms_lifecycle"),
            shard_reconciles=tuple(event for event in signal_events if event.kind == "shard_reconcile"),
        )
    return bundles
