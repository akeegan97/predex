# Architecture

This document describes the current runtime architecture.

## Scope

The runtime is a full trading pipeline:

- connect to the Kalshi public websocket and ingest live market data
- maintain shard-local order books via a zero-copy frame pipeline
- run strategy and local-risk evaluation per shard
- route order intents to a central OMS coordinator
- submit, cancel, and modify orders via the Kalshi REST API
- receive fills and lifecycle events from the Kalshi private websocket
- persist raw inbound data to a binary tape
- persist audit records (OMS decisions, fills, latency spans) to JSONL

## Main Components

### `predex_websocket`

Defined in `cpp/include/predex/websocket/`.

Responsibilities:
- websocket transport mechanics (Boost.Beast)
- Kalshi-specific connect, subscribe, and auth request building
- REST client for order submission, cancellation, and modification
- raw payload delivery to the IO thread and OMS private WS thread

### `predex_core_pipeline`

Defined across `ingest/`, `routing/`, `shards/`, `tape/`, `parsers/`, `audit/`, `oms/`.

Responsibilities:
- frame allocation and zero-copy reuse
- bounded SPSC queue handoff between pipeline stages
- market-data routing and shard fanout
- shard-local book maintenance and event store
- strategy signal evaluation and local risk gating
- OMS intent coordination, order state tracking, risk controls
- binary tape persistence and audit JSONL persistence

### `predex_app`

Defined in `cpp/include/predex/app.hpp` and `cpp/src/app.cpp`.

Responsibilities:
- construct the complete runtime object graph
- own all queues, thread instances, and stage objects
- lifecycle: `start()` / `run()` / `stop()`
- WS reconnect with exponential backoff on both data and OMS channels
- periodic health status dump (30-second interval)

## Thread Topology

The runtime runs seven classes of worker thread:

1. **IO thread** — owns public WS receive; calls `IOWriter::on_wire_message`; drains frame recycle queue; handles reconnect with backoff.

2. **Router thread** — drains `io_to_router_queue`; classifies messages; looks up `market_id` and `affinity_key`; enforces sequence checks; fans out to shard input queues or the logger queue.

3. **N shard threads** — each shard owns one input queue; parses frames into `NormalizedEvent`; applies events to `EventStore` (books + derived topology state); runs `ShardPipeline` (local risk + strategy evaluation); drains OMS decision and lifecycle queues; forwards handles to the logger.

4. **OMS coordinator thread** — drains all shard intent queues (round-robin); runs `GlobalRiskManager` pre-trade checks; maintains `OrderStore`; pushes `OmsToKalshiCommand` variants (Submit / Cancel / Modify) to `oms_command_queue`; drains `oms_rest_event_queue` (and optionally `ws_event_queue`) for venue lifecycle events via `ExecutionTransport::try_pop_event()`; fans lifecycle events back to the originating shard.

5. **OMS Gateway thread** — 5-stage pipeline (`CommandIngress → OrderSequencer → BatchPlanner → RateLimiter → SessionPool`) that pops typed command variants from `oms_command_queue`, enforces per-lineage ordering and rate limits, and executes orders asynchronously via persistent HTTPS connections (`AsyncRestConnection`); pushes `KalshiToOmsEvent` results to `oms_rest_event_queue`.

6. **Logger thread** — drains router and all shard logger queues; writes frames to the binary tape file; recycles frame handles back to the IO thread.

7. **Audit thread** — drains all shard and OMS audit queues; writes `AuditEvent` records as JSONL.

## Queue Graph

All queues are SPSC unless otherwise noted.

```
IO thread
  -> io_to_router_queue

Router thread
  -> router_to_logger_queue
  -> shard_input_queue[i]     (one per shard)

Shard thread i
  -> shard_to_logger_queue[i]
  -> shard_to_oms_intent_queue[i]
  -> shard_audit_queue[i]

OMS coordinator thread
  <- shard_to_oms_intent_queue[i]   (polls all, round-robin)
  -> oms_to_shard_decision_queue[i]
  -> oms_to_shard_lifecycle_queue[i]
  -> oms_command_queue              (variant: SubmitOrderCmd | CancelOrderCmd | ModifyOrderCmd)
  -> oms_audit_queue
  <- oms_rest_event_queue           (Gateway thread only)
  <- ws_event_queue                 (private WS, when wired)

OMS Gateway thread
  <- oms_command_queue
    [CommandIngress -> OrderSequencer -> BatchPlanner -> RateLimiter -> SessionPool]
    -> AsyncRestConnection -> Kalshi REST API
  -> oms_rest_event_queue

Logger thread
  <- router_to_logger_queue
  <- shard_to_logger_queue[i]       (polls all)
  -> recycle_queue

Audit thread
  <- shard_audit_queue[i]           (polls all)
  <- oms_audit_queue

IO thread
  <- recycle_queue
```

All queues are strict SPSC. The OMS coordinator drains `oms_rest_event_queue` (and optionally `ws_event_queue`) via `ExecutionTransport::try_pop_event()`.

## Frame Lifecycle

The hot-path object is `predex::core::ingest::kalshi::FrameHandle`.

1. `IOWriter` acquires a slot from `FramePool` and copies the websocket payload once.
2. The handle is pushed to the router.
3. Router forwards to a shard queue or directly to the logger queue.
4. Shard parses/applies and forwards the same handle to the logger.
5. Logger writes the payload and pushes the handle to the recycle queue.
6. `IOWriter` drains recycle handles and returns slots to `FramePool`.

No payload copies occur between steps 2–6.

## Routing Model

The router:
1. Classifies messages: shard-bound market data, direct-to-logger control plane, or drop.
2. Looks up `market_id` and `affinity_key` from `MarketRegistry`.
3. Enforces session sequence checks; calls `reset_sequence_state()` after WS reconnect.

Shard assignment is `affinity_key % shard_count`. All markets in the same Kalshi event share an affinity key so they land on the same shard.

## Event Store and Book Model

Each shard owns an `EventStore` that holds one `Event` per Kalshi event (group of markets).

Each `Event` contains:
- a `BookStore` with per-market bid/ask levels, sequence state, pending-delta buffer, and trade state
- an `EventDerivedState` that mirrors the book into a topology-specific view:
  - `MonotonicChainState` — markets ordered by strike key; used by monotonic arb
  - `MutuallyExclusiveState` — unordered set of markets summing to 1
  - `UnorderedGroupState` — unordered set of independent markets
  - `SingleMarketState` — single-market event

On WS reconnect, `EventStore::reset_all_books()` clears `has_snapshot` so the next snapshot from the new session is accepted cleanly.

### Kalshi Reciprocal Pricing

Kalshi's wire format has no explicit Ask book. The ask side is derived from the No-bid: a No-bid at tick `p` implies an Ask at `1000 - p`. The parser handles this at both snapshot and delta level. Everything downstream sees a standard two-sided book.

## Strategy Pipeline

Each shard runs a `ShardPipeline` that fires on every applied market event:

1. All active strategies fan out and emit signals into a shared buffer (up to `kMaxSignalsPerEvent = 16` single-leg signals and a separate group-signal buffer):
   - `MonotonicArbStrategy` — detects probability-monotonicity violations across a chain event; emits IOC leg pairs as `GroupSignal`.
   - `CdfViolationStrategy` — detects CDF-level mispricing (stub).
   - `MarketMakingStrategy` — quotes bid/ask around fair value (stub).
   - `MeanReversionStrategy` — mean-reversion signal (stub).

2. The pipeline then iterates every collected signal and runs **`LocalRiskManager::evaluate`** independently per signal — checks close-time gating, net position limits, open intent counts, and event/market exposure limits. Each risk-approved signal is submitted as an `OmsSubmission`; rejected signals are skipped and counted.

3. All accepted intents (potentially multiple per event) are pushed to `shard_to_oms_intent_queue[i]`.

4. OMS decisions and fill lifecycle events are drained from `oms_to_shard_decision_queue[i]` and `oms_to_shard_lifecycle_queue[i]` to update `LocalRiskState` (open exposure, net filled position).

## OMS and Execution

The OMS coordinator is the single writer to all order state. See [`oms_design.md`](oms_design.md) for the full design.

Key properties:
- `GlobalRiskManager` pre-trade check before every intent is accepted.
- `OrderStore` tracks live orders in three lookup indices: by `oms_request_id`, `client_order_id`, and `exchange_order_id`.
- Drawdown circuit breaker: if `session_net_ticks < -max_session_loss_ticks`, fires a **soft halt** (blocks new submissions; existing orders survive to settle).
- Controlled shutdown fires a **hard halt** (blocks new submissions + cancel-all on the OMS thread).

## Startup Reconciliation

At startup, `reconcile_open_orders_from_rest(is_startup=true)` fetches open orders from the REST API and adopts any orders from a previous session into `OrderStore` with synthetic request IDs. This ensures the kill switch covers orphaned orders and fills are accounted for in session P&L.

After OMS private WS reconnect, `reconcile_open_orders_from_rest(is_startup=false)` pushes lifecycle ACK events for current-session orders so the OMS can reconcile state via `client_order_id`.

## Reconnect Behavior

Both the public data WS and the OMS private WS have independent reconnect loops with exponential backoff (100ms base, 5s cap, max 5 doublings).

On public WS reconnect:
- Re-subscribes to configured channels.
- Calls `router->reset_sequence_state()` so fresh SIDs from the new session are accepted.
- Calls `shard->request_reset()` on each shard (atomic flag checked at top of `pump()`) which calls `EventStore::reset_all_books()` on the shard thread.

## Observability

`App::run()` prints a health line to stdout every 30 seconds:

```
[timestamp UTC] STATUS | halted=false | pnl_ticks=+0 | live_orders=0 | intents=0 | rejected=0 | transport_updates=0 | router_frames=0 | router_drops=0 | desynced_events=0
```

## Tape Model

The logger is the terminal sink for raw feed capture. Tape format (PDT2):

```
File header:
  magic[4]    = 'P','D','T','2'
  version     = 2  (uint16_t, little-endian)
  flags       = 0  (uint16_t, little-endian)

Per record (repeated):
  recv_ts_ns  (uint64_t, little-endian)
  len         (uint32_t, little-endian)
  payload     (len bytes — raw websocket text)
```

The payload is the raw websocket text, not the normalized event.

## Shutdown Sequence

`stop()` drains the pipeline in dependency order:

1. `request_hard_halt()` on OMS (blocks new submissions; schedules cancel-all on next OMS pump).
2. `running = false`.
3. Join IO thread (closes WS).
4. Join + drain router thread.
5. Join + drain shard threads.
6. Join OMS coordinator thread (tail drain: `cancel_all_live_orders()` + pump until idle).
7. Join OMS Gateway thread.
8. Drain + join logger thread.
9. Drain + join audit thread.
