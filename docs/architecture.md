# Architecture

This document describes the current runtime architecture.

## Scope

The runtime is a full trading pipeline:

- connect to the Kalshi public websocket and ingest live market data
- maintain shard-local order books via a zero-copy frame pipeline
- run strategy and local-risk evaluation per shard
- route order intents to a central OMS coordinator
- submit, cancel, and modify orders via the Kalshi REST API
- surface fill and lifecycle events on the REST response path (Kalshi private-WS transport is scaffolded but not yet wired)
- persist raw inbound data to a binary tape
- persist audit records (OMS decisions, fills, latency spans) to JSONL

## Main Components

### `predex_websocket`

Defined in `cpp/include/predex/websocket/`.

Responsibilities:
- websocket transport mechanics (Boost.Beast)
- Kalshi-specific connect, subscribe, and auth request building
- REST client for order submission, cancellation, and modification
- raw payload delivery to the IO thread (private-WS adapter/worker are scaffolded but not currently constructed by `App`)

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
- public-WS reconnect with exponential backoff (100ms base → 5s cap, 5 doublings)
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

All queues are strict SPSC. Where multiple producers need to feed one consumer (notably frame recycling), each producer owns its own SPSC and the consumer fan-in reads round-robin across them.

```
IO thread
  -> io_to_router_queue

Router thread
  -> router_to_logger_queue
  -> shard_input_queue[i]     (one per shard)
  -> recycle_from_router      (producer side of recycle fan-in)

Shard thread i
  -> shard_to_logger_queue[i]
  -> shard_to_oms_intent_queue[i]
  -> shard_audit_queue[i]
  -> recycle_from_shards[i]   (producer side of recycle fan-in)

OMS coordinator thread
  <- shard_to_oms_intent_queue[i]   (polls all, round-robin)
  -> oms_to_shard_decision_queue[i]
  -> oms_to_shard_lifecycle_queue[i]
  -> oms_command_queue              (variant: SubmitOrderCmd | CancelOrderCmd | ModifyOrderCmd)
  -> oms_audit_queue
  <- oms_rest_event_queue           (Gateway thread only)
  <- ws_event_queue                 (always nullptr today; populated when private WS is wired)

OMS Gateway thread
  <- oms_command_queue
    [CommandIngress -> OrderSequencer -> BatchPlanner -> RateLimiter -> SessionPool]
    -> AsyncRestConnection -> Kalshi REST API
  -> oms_rest_event_queue

Logger thread
  <- router_to_logger_queue
  <- shard_to_logger_queue[i]       (polls all)
  -> recycle_from_logger            (producer side of recycle fan-in)

Audit thread
  <- shard_audit_queue[i]           (polls all)
  <- oms_audit_queue

IO thread (IOWriter consumer)
  <- recycle_from_logger
  <- recycle_from_router
  <- recycle_from_shards[i]         (round-robin across all producer queues)
```

The recycle topology is **per-producer SPSC + consumer fan-in**, not a single shared queue. Every producer thread (logger, router, each shard) owns its own recycle SPSC; `IOWriter` is the sole consumer across all of them. The OMS coordinator drains `oms_rest_event_queue` (and `ws_event_queue` when populated) via `ExecutionTransport::try_pop_event()`.

## Frame Lifecycle

The hot-path object is `predex::core::ingest::kalshi::FrameHandle`.

1. `IOWriter` acquires a slot from `FramePool` and copies the websocket payload once.
2. The handle is pushed to the router.
3. Router forwards to a shard queue or directly to the logger queue.
4. Shard parses/applies and forwards the same handle to the logger.
5. Logger writes the payload and pushes the handle to *its* recycle SPSC (`recycle_from_logger`).
6. Frames that bypass the logger (router-side drops, shard-side drops) are pushed to the producer's own recycle SPSC (`recycle_from_router`, `recycle_from_shards[i]`).
7. `IOWriter` drains all recycle producers round-robin and returns slots to `FramePool`.

No payload copies occur between steps 2 and the recycle return.

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
- `GlobalRisk` pre-trade check before every intent is accepted; capital reservation runs against `oms_transport.available_capital_ticks`.
- `OrderStore` tracks live orders in three lookup indices: by `oms_request_id`, `client_order_id`, and `exchange_order_id`.
- Two-level halt mode is **designed** with explicit `kSoft` / `kHard` separation, but neither trigger is fully wired today:
  - **Soft halt** is intended for the drawdown circuit breaker (`session_net_ticks < -max_session_loss_ticks` blocks new submissions; existing orders survive to settle). `Oms::request_soft_halt()` exists but has no caller; session-level fill P&L accumulation is not yet implemented.
  - **Hard halt** is intended for controlled shutdown (blocks new submissions + cancel-all on the OMS thread). `Oms::request_hard_halt()` flips the mode flag, but the cancel-all sweep does not yet run from the OMS pump.

## Startup Reconciliation

At startup, when `oms_transport.enabled` is true, `App::Runtime::reconcile_open_orders_from_rest()` consults the configured `oms_transport.startup_open_orders_policy` and acts on whatever Kalshi REST reports as open orders. The four policy modes are:

- **`refuse_if_present`** (default, live) — paginated `KalshiRestAdapter::fetch_open_orders`; if any open order is found, log every order (ticker / status / side / action / qty / price / exchange_order_id) and abort startup. Preserves the kill-switch invariant: every order that exists at the venue must either be in `OrderStore` or not exist at all when the live loop begins.
- **`ignore`** (live, dev/loose) — skip the REST fetch entirely. Useful for dev iteration when prior-session state is known clean and the round trip is unwanted.
- **`cancel_all`** (scaffolded, deferred) — intended to fetch + cancel everything via REST before going live. Not yet implemented; selecting this mode aborts startup with a clear deferral message.
- **`adopt`** (scaffolded, deferred) — intended to seed configured-market open orders into `OrderStore` via `Oms::seed_reconciled_order`. The `OpenOrderSnapshot → OrderState` mapping requires per-order `IntentContext` (strategy / shard / event) that is not recoverable from a Kalshi REST snapshot alone; pending `OrderStore` persistence across sessions or a strategy hint encoded into `client_order_id`. Selecting this mode aborts startup with a clear deferral message.

The current operational reality (discrete-session, IOC-only monotonic arb) means the policy fires zero times in practice: there are never resting orders left over. The policy is the defensive gate for the long-range strategy landing.

## Reconnect Behavior

The public data WS reconnects with exponential backoff (100ms base, 5s cap, max 5 doublings). There is currently no OMS private-WS connection, so no separate reconnect loop exists for it.

On public WS reconnect:
- Re-subscribes to configured channels.
- Calls `router->reset_sequence_state()` so fresh SIDs from the new session are accepted.
- Calls `shard->request_reset()` on each shard (atomic flag checked at top of `pump()`) which calls `EventStore::reset_all_books()` on the shard thread.

Deeper reconnect hardening (sequence-gap recovery during the reconnect window, post-reconnect snapshot reconciliation against pre-disconnect book state) is pending.

## Observability

`App::run()` prints a health line to stdout every 30 seconds. The schema is wide (~35 fields including gateway latency stages — `gw_i2s_ms`, `gw_s2p_ms`, `gw_q2p_ms`, `gw_p2a_ms`, `gw_a2s_ms`, `gw_s2w_ms`, `gw_wire_ms`, `gw_cold_ms` — plus session-pool counters, recovery telemetry, and audit drop counts). See `App::Runtime::print_health_status` in `cpp/src/app.cpp` for the canonical format; this doc deliberately does not duplicate it to keep them from drifting. Truncated example:

```
[timestamp UTC] STATUS | halted=false | live_orders=0 | oms_shard_requests=0 | ...
```

`pnl_ticks` is not currently emitted — it depends on the drawdown breaker landing first (see OMS section above).

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

1. `request_hard_halt()` on OMS (flips the halt flag; blocks new submissions).
2. `running = false`.
3. Join IO thread (closes WS).
4. Join + drain router thread.
5. Join + drain shard threads.
6. Join OMS coordinator thread (pumps until idle).
7. Join OMS Gateway thread.
8. Drain + join logger thread.
9. Drain + join audit thread.

A cancel-all sweep at hard halt is **designed** but not yet implemented (see OMS section). Today's shutdown does not cancel resting venue orders — the discrete-session monotonic-arb strategy never leaves any, but this is a known gap for the long-range strategy landing.
