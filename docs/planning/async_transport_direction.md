# Async Transport Direction

## Why Change

The current REST transport improved substantially once outbound traffic was split across four synchronous workers, but the remaining local send latency is still dominated by per-worker head-of-line blocking.

- Each worker processes one command at a time.
- After a worker writes a request, it blocks waiting for the HTTP response before it can service the next command on that lane.
- Independent orders that hash onto the same worker still queue behind the in-flight round trip.

Recent soak results show the global single-lane bottleneck is gone, but the healthy-path local latency floor is still much higher than the desired target because workers remain synchronous.

## Current State Summary

- OMS routes commands through `ExecutionTransportQueues` into a pool of worker-owned SPSC command queues.
- Each REST worker owns one blocking HTTP session.
- Reconciliation is pinned to worker 0.
- Venue responses are emitted back to OMS through one SPSC event queue per worker.
- Transport timing is measured at request-write time and response-read time.

Observed behavior from the latest soak:

- `tick_to_transport_submit` improved from the old ~218 ms p50 regime to ~23 ms p50.
- `decision_to_transport` mean improved from the old ~363 ms regime to ~26 ms.
- `transport_submit_to_response` remained ~53 ms p50, which is mostly venue/network time.
- The remaining local tail is caused by commands waiting behind an already in-flight request on the same worker.

## Target Direction

Replace the synchronous worker pool with a central async transport scheduler backed by a pool of persistent HTTPS connections.

The target model is:

- OMS remains the single owner of canonical order state.
- OMS emits outbound transport commands into one central transport ingress queue.
- A dedicated async transport scheduler owns an event loop / reactor and a pool of warm HTTPS sessions.
- Per-order sequencing is preserved, but unrelated orders can be dispatched onto any idle connection.
- Responses are matched back to the originating request and emitted to OMS as they arrive.
- Reconciliation traffic uses reserved capacity so it cannot starve hot submits.

## Expected Benefits

### What async should improve

- Remove per-worker head-of-line blocking for unrelated orders.
- Lower `decision_to_transport` from tens of milliseconds toward low single-digit milliseconds or low hundreds of microseconds on the healthy path.
- Reduce `tick_to_transport_submit` so the local transport path stops dominating the opportunity window.

### What async should not materially improve

- `transport_submit_to_response` venue RTT.
- Exchange-side authorization or reject behavior.
- Upstream outliers that occur before OMS hands work to transport.

## Blast Radius

### High-impact modules

- `cpp/include/predex/oms/execution_transport.hpp`
- `cpp/src/app.cpp`
- `cpp/include/predex/oms/transport/rest_worker.hpp`
- `cpp/src/oms/transport/rest_worker.cpp`
- `cpp/include/predex/oms/transport/persistent_http_session.hpp`
- `cpp/src/oms/transport/persistent_http_session.cpp`
- `cpp/src/oms/transport/kalshi_rest_adapter.cpp`
- `cpp/src/oms/oms.cpp`
- `cpp/src/oms/order_store.cpp`

### Moderate-impact areas

- audit emission and trace schema
- startup / shutdown / drain sequencing
- transport config surface in app config and trader config parsing

### Low-impact areas if OMS contract stays stable

- parser
- router
- shard strategy logic
- replay tooling beyond any schema additions

## Required Design Properties

### 1. Per-order sequencing

Submit, cancel, and modify for the same order must not overtake one another.

Implication:

- async dispatch cannot be purely opportunistic
- transport needs an order-keyed sequencing layer

### 2. Cross-order concurrency

Independent orders should not wait for one another if idle connection capacity exists.

Implication:

- routing by worker index should be replaced by routing by order-key readiness plus connection availability

### 3. In-flight risk accounting

With multiple requests live at once, risk must account for more than just acknowledged/open orders.

Implication:

- reserve risk at dispatch time
- release or reconcile on response / failure / timeout

### 4. Reconcile isolation

Reconcile snapshots should not consume the same capacity used by latency-sensitive submits.

Implication:

- dedicate one connection or a lower-priority scheduling class for reconcile

### 5. Precise observability

The transport must expose separate timestamps for:

- command accepted by scheduler
- bytes handed to socket
- response parsed
- timeout / retry / failure

Implication:

- audit must distinguish queueing delay from on-wire delay

## Candidate Architecture

### Transport scheduler

- single dedicated transport thread first
- owns async event loop
- owns pool of N persistent HTTPS sessions
- consumes one inbound command queue
- produces one outbound event queue

### Internal state

- map of in-flight requests by transport request id
- map of per-order pending operations
- pool of available / busy connections
- reconcile state separate from hot path

### Dispatch algorithm

1. Pop new command from ingress queue.
2. If the order key already has an in-flight command, enqueue behind that order key.
3. Otherwise dispatch immediately onto any idle connection.
4. Mark request as in flight and reserve risk.
5. On completion, emit normalized OMS event, release the connection, then dispatch the next ready command.

## Phased Migration Plan

### Phase 1: Instrumentation hardening

- keep existing sync pool
- add a scheduler-accepted timestamp if needed
- ensure audit clearly isolates upstream handoff, local queueing, and venue RTT

### Phase 2: Central scheduler skeleton

- preserve existing OMS command/event types
- collapse transport ingress/egress to central queues
- keep adapter/request-building code reusable

### Phase 3: Async connection pool

- move from blocking request/response to async session state machines
- allow concurrent in-flight requests across the connection pool
- preserve per-order sequencing

### Phase 4: Risk and shutdown hardening

- add explicit in-flight reservation accounting
- define timeout, retry, cancel-all, and shutdown drain behavior

### Phase 5: Latency tightening

- hybrid spin-then-yield policy on the transport scheduler
- connection pinning / warmup if justified
- reduce hot-path allocations and formatting overhead

## Internal Latency Follow-ups After Async Lands

These are separate from the transport redesign and should be treated as second-order work after the async pool is in place:

- compare latency on true optimized builds instead of Debug builds
- reduce client order id string allocation / formatting overhead
- avoid unnecessary JSON string construction on the hot path where practical
- cache top-of-book indices before considering any deeper book representation changes
- re-evaluate timestamp cost only after larger latency classes are removed

## Notes on Proposed Optimizations

### Build mode

The current `dev` preset is a Debug build. Any latency evaluation should also be repeated on a true Release build before treating the current internal numbers as a hard floor.

### Book data structure

The current book is already a dense fixed array indexed by price tick, not a tree-like structure. A “top N only” representation would be a semantics change, not a simple container swap. A safer first optimization is caching best-level indices while preserving the full fixed-depth source of truth.

### Timestamp source

Clock reads are worth revisiting only after the transport and build-mode issues are addressed. They are unlikely to be the dominant latency source today.

## Review Checklist

- Does the async scheduler preserve per-order ordering?
- Does the design keep OMS as the sole owner of canonical state?
- Is reconcile isolated from latency-sensitive submits?
- Are in-flight reservations explicit?
- Are submit-time and response-time audits both preserved?
- Can the transport shut down cleanly without orphaning state?