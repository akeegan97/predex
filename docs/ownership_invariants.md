# Ownership And Lifetime Invariants

This document captures the operational rules that keep the current runtime coherent.

## Runtime Ownership

`predex::App::Runtime` owns:
- websocket transport/session objects (public data WS; OMS private-WS transport is scaffolded but not constructed today)
- the frame pool
- all queues
- router, shards, logger, audit writer, OMS coordinator, OMS Gateway
- worker threads (IO, router, N shards, OMS coordinator, OMS Gateway, logger, audit). A private-WS worker thread will join this list when that transport is wired.

Other components mostly hold references or non-owning pointers into runtime-owned state.

## Thread Ownership

The runtime is designed around single-owner stage execution:

- The public websocket transport/session is owned by the IO thread during steady-state operation
- The router is owned by the router thread
- Shard `i` is owned by shard thread `i`
- The OMS coordinator (including `OrderStore` and `GlobalRisk`) is owned by the OMS coordinator thread
- The OMS Gateway pipeline (including `AsyncRestConnection` pool) is owned by the OMS Gateway thread
- The logger is owned by the logger thread
- The audit writer is owned by the audit thread
- Each `BookStore` is shard-local and only written by its shard thread

This is the core concurrency assumption behind the current design. Crossing these ownership boundaries requires going through the designated queues.

## Queue Invariants

Each queue has exactly one producer and one consumer (SPSC), except where noted.

**Public data pipeline:**

- `io_to_router_queue`
  - producer: IO thread
  - consumer: router thread

- `router_to_logger_queue`
  - producer: router thread
  - consumer: logger thread

- `shard_input_queue[i]`
  - producer: router thread
  - consumer: shard thread `i`

- `shard_to_logger_queue[i]`
  - producer: shard thread `i`
  - consumer: logger thread

**Frame recycle (per-producer SPSC + consumer fan-in):**

Every frame-handle producer owns its own recycle SPSC; `IOWriter` (running on the IO thread) is the sole consumer and round-robins across all producer queues. This is the correct way to preserve SPSC discipline when multiple producers need to return frames — adding more producers to a single shared recycle queue would violate the strict one-writer rule (see [[feedback_spsc_producers]]).

- `recycle_from_logger` — producer: logger thread; consumer: IO thread
- `recycle_from_router` — producer: router thread; consumer: IO thread
- `recycle_from_shards[i]` — producer: shard thread `i`; consumer: IO thread

**Shard → OMS:**

- `shard_to_oms_intent_queue[i]`
  - producer: shard thread `i`
  - consumer: OMS coordinator thread (polls all, round-robin)

**OMS → shard:**

- `oms_to_shard_decision_queue[i]`
  - producer: OMS coordinator thread
  - consumer: shard thread `i`

- `oms_to_shard_lifecycle_queue[i]`
  - producer: OMS coordinator thread
  - consumer: shard thread `i`

**OMS coordinator → Gateway thread:**

- `oms_command_queue`
  - producer: OMS coordinator thread
  - consumer: OMS Gateway thread (`CommandIngress` stage)
  - carries: `OmsToKalshiCommand` variant (`SubmitOrderCmd | CancelOrderCmd | ModifyOrderCmd`)

**Gateway thread → OMS coordinator:**

- `oms_rest_event_queue`
  - producer: OMS Gateway thread (`AsyncRestConnection` via `SessionPool`)
  - consumer: OMS coordinator thread

- `ws_event_queue` (reserved; pointer is `nullptr` today, will be wired when private-WS transport lands)
  - producer: private-WS worker thread (not constructed today)
  - consumer: OMS coordinator thread

`ExecutionTransport::try_pop_event()` checks `ws_event_queue` first (skipped if `nullptr`), then round-robins across `rest_event_queues`. All queues are strict SPSC.

**Audit:**

- `shard_audit_queue[i]`
  - producer: shard thread `i`
  - consumer: audit thread (polls all)

- `oms_audit_queue`
  - producer: OMS coordinator thread
  - consumer: audit thread

## Frame Pool Invariants

`FramePool` holds the backing storage for all live frames in the public data pipeline.

The rules are:
- `IOWriter` is the only stage that acquires fresh frame slots
- `IOWriter` is also the only stage that calls `FramePool::recycle(...)`
- Router, shards, and logger only read frame contents through a handle
- A `FrameHandle` remains valid only while its generation matches the pool slot generation

This keeps pool mutation centralized on the IO thread and avoids any cross-thread synchronization on the pool itself.

## Handle Lifecycle Invariants

For each inbound websocket message:

1. A frame slot is acquired by `IOWriter`
2. The payload is copied once into the pool
3. The same handle moves through router → shard → logger (or short-circuits to a producer-side recycle on router-drop / shard-drop paths)
4. Whichever stage terminates the handle's journey pushes it to that stage's recycle SPSC (`recycle_from_logger` / `recycle_from_router` / `recycle_from_shards[i]`)
5. The IO thread (`IOWriter`) must eventually drain all recycle SPSCs and call `FramePool::recycle()`

Every producer must recycle even after a write or apply failure so the pool does not silently lose capacity.

## Message Classification Invariants

The router classifies frames into one of three buckets:
- shard-bound market data (trade, orderbook_delta, snapshot)
- logger-only control-plane data (subscribed acknowledgements, lifecycle messages)
- drop (unrecognized or filtered messages)

Logger-only frames must not force shard work. The shard should never receive a frame it cannot parse into a `NormalizedEvent`.

## Shard Invariants

Each shard owns:
- one `shard_input_queue`
- one `shard_to_logger_queue`
- one `shard_to_oms_intent_queue`
- one `oms_to_shard_decision_queue`
- one `oms_to_shard_lifecycle_queue`
- one `shard_audit_queue`
- one parser instance
- one `EventStore` (containing `BookStore` instances and `EventDerivedState` per event)
- one `ShardPipeline` (LocalRiskManager + strategies)
- one `LocalRiskState`

The shard does not own the frame pool, logger queue backing storage, or OMS coordinator state.

The shard is responsible for:
- Parsing `FrameHandle` into `NormalizedEvent`
- Applying events to `EventStore` (books + derived topology state)
- Running `ShardPipeline` on each applied event
- Draining `oms_to_shard_decision_queue` and `oms_to_shard_lifecycle_queue` to update `LocalRiskState`
- Forwarding the `FrameHandle` to `shard_to_logger_queue` after applying

## OMS Invariants

- `OrderStore` and `GlobalRisk` are single-writer: only the OMS coordinator thread modifies them during normal operation
- `Oms::seed_reconciled_order()` is the designated pre-thread-start adoption path. It is currently uncalled — the future `adopt` mode of `oms_transport.startup_open_orders_policy` will call it before the OMS thread starts (see [[project_operational_modes]]).
- `halt_mode_` is an `std::atomic<uint8_t>` — safe to read from any thread, written by `request_soft_halt()` and `request_hard_halt()`
- A cancel-all-on-hard-halt sweep on the OMS thread is designed (guarded by `hard_halt_cancel_triggered_`) but not yet implemented. Today's discrete-session strategies leave no resting orders for it to clean up; the invariant is reserved for the long-range strategy landing.

## Logger Invariants

The logger is the terminal sink for raw payload persistence.

It is responsible for:
- Polling `router_to_logger_queue` and all `shard_to_logger_queue[i]`
- Writing length-prefixed payloads to the binary tape file
- Pushing `FrameHandle` instances to `recycle_from_logger` after writing

The logger must not retain ownership of a handle after it finishes with the frame.

## Audit Invariants

The audit thread is the terminal sink for structured audit events. It polls `shard_audit_queue[i]` and `oms_audit_queue`, then writes `AuditEvent` records as JSONL. Audit writes are best-effort; audit queue overflow does not affect the market data pipeline.

## Error And Shutdown Invariants

Runtime shutdown is ordered to preserve data integrity:

1. `request_hard_halt()` on OMS — blocks new submissions
2. `running = false` — signals IO and pipeline threads
3. IO thread join (closes public WS)
4. Router thread join + drain
5. Shard thread joins + drain
6. OMS coordinator join (pumps until idle)
7. OMS Gateway thread join
8. Logger thread drain + join
9. Audit thread drain + join

When the cancel-all-on-hard-halt sweep lands (currently designed-not-wired), it will run on the OMS coordinator thread in response to `halt_mode_ == kHard` detected at the top of `pump()`. `App::Runtime::stop()` will continue to be its only trigger via `request_hard_halt()`; nothing else may invoke that path. A private-WS worker thread join will be added once that transport is wired.

## Documentation Rule

When the implementation changes, update this file and:
- [Architecture](architecture.md)
- [Data Contract](data_contract.md)
- [OMS Design](oms_design.md)

These docs are meant to stay aligned with the real runtime, not an aspirational design.
