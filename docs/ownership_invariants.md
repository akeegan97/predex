# Ownership And Lifetime Invariants

This document captures the operational rules that keep the current runtime coherent.

## Runtime Ownership

`predex::App::Runtime` owns:
- websocket transport/session objects (public data WS and OMS private WS)
- the frame pool
- all queues
- router, shards, logger, audit writer, OMS coordinator
- worker threads (IO, router, N shards, OMS coordinator, OMS REST, OMS private WS, logger, audit)

Other components mostly hold references or non-owning pointers into runtime-owned state.

## Thread Ownership

The runtime is designed around single-owner stage execution:

- The public websocket transport/session is owned by the IO thread during steady-state operation
- The router is owned by the router thread
- Shard `i` is owned by shard thread `i`
- The OMS coordinator (including `OrderStore` and `RiskEngine`) is owned by the OMS coordinator thread
- The OMS REST client is owned by the OMS REST thread
- The OMS private websocket is owned by the OMS private WS thread
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

- `recycle_queue`
  - producer: logger thread
  - consumer: IO thread

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

**OMS coordinator → REST thread:**

- `oms_submit_queue`
  - producer: OMS coordinator thread
  - consumer: OMS REST thread

- `oms_cancel_queue`
  - producer: OMS coordinator thread
  - consumer: OMS REST thread

- `oms_modify_queue`
  - producer: OMS coordinator thread
  - consumer: OMS REST thread

**Transport → OMS coordinator:**

- `oms_transport_update_queue` — **MPSC deviation**: two producers
  - producers: OMS REST thread AND OMS private WS thread
  - consumer: OMS coordinator thread

This is the only queue that deviates from strict SPSC. The two transport threads write independently; the OMS coordinator drains both through the same queue.

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
3. The same handle moves through router → shard → logger
4. The logger must eventually push the handle to the recycle queue
5. The IO thread must eventually call `FramePool::recycle()` via the recycle queue

The logger must recycle even after a write failure so the pool does not silently lose capacity.

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

- `OrderStore` and `RiskEngine` are single-writer: only the OMS coordinator thread modifies them during normal operation
- `seed_orphaned_order()` is the only exception: it writes `OrderStore` and `RiskEngine` before the OMS thread starts
- `halt_mode_` is an `std::atomic<uint8_t>` — safe to read from any thread, written by `request_soft_halt()` and `request_hard_halt()`
- `cancel_all_live_orders()` is called only on the OMS thread (guarded by `hard_halt_cancel_triggered_`)

## Logger Invariants

The logger is the terminal sink for raw payload persistence.

It is responsible for:
- Polling `router_to_logger_queue` and all `shard_to_logger_queue[i]`
- Writing length-prefixed payloads to the binary tape file
- Pushing `FrameHandle` instances to `recycle_queue` after writing

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
6. OMS coordinator join (tail drain: `cancel_all_live_orders()` + pump until idle)
7. Directly flush remaining cancel commands via REST client
8. OMS REST and private WS thread joins
9. Logger thread drain + join
10. Audit thread drain + join

Shutdown must not race the OMS thread with `cancel_all_live_orders()`. The OMS thread is the only caller of that function during normal operation; `stop()` drives it via `request_hard_halt()` and then lets `pump()` detect the hard halt.

## Documentation Rule

When the implementation changes, update this file and:
- [Architecture](architecture.md)
- [Data Contract](data_contract.md)
- [OMS Design](oms_design.md)

These docs are meant to stay aligned with the real runtime, not an aspirational design.
