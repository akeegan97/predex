# OMS Design (Draft)

## Scope
This doc is the implementation target for a production-grade OMS path after latency instrumentation lands.

## TODO(phase-1): Tick->Trade Latency
- Propagate timestamps across shard -> OMS -> transport -> lifecycle.
- Define canonical span formulas and missing-data behavior.
- Add p50/p95/p99 reporting pipeline from audit JSONL.

## TODO(phase-2): Real OMS State Machine
- Intent lifecycle: `Intent -> Accepted/Rejected -> Live -> PartialFill -> Filled/Canceled`.
- Replace/cancel transitions and idempotency semantics.
- Group-order policy handling (`AbortRemainingOnReject`, `BestEffort`).

## TODO(phase-3): Queue Contracts and Ownership
- Shard->OMS submission queue contract.
- OMS->transport command queue contract.
- Transport->OMS lifecycle queue contract.
- OMS->shard decision/lifecycle fanout guarantees.

## TODO(phase-4): Risk & Controls
- Global risk pre-trade checks.
- Shard-local risk/reentry policy integration.
- Backpressure and fail-safe behavior when downstream is unavailable.

## TODO(phase-5): Observability
- Required counters and latency histograms.
- Reconciliation invariants between shard and OMS state.
- Alert thresholds for queue lag, reject spikes, and stale lifecycle updates.
