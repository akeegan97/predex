# Design Decisions

This document records deliberate architectural choices and the reasoning behind them. The intent is to answer "why this way and not that way" for decisions that are not self-evident from reading the code.

## SPSC Queues Between Every Stage

Each stage boundary uses a single-producer single-consumer lock-free queue rather than a mutex-protected shared queue or a general-purpose MPSC queue.

The SPSC constraint is strict: exactly one thread writes and one thread reads each queue during steady-state operation. Given that invariant, SPSC queues require no compare-and-swap, no contention, and no memory barriers beyond the acquire/release pair at head and tail. A mutex adds at minimum a kernel syscall on contention and a cache-line ownership transfer on every acquire. A general-purpose concurrent queue adds overhead even when there is provably only one producer and one consumer.

The queue topology in `ownership_invariants.md` is designed so the SPSC invariant is always satisfied everywhere. The OMS transport path is designed to support two producers (the OMS Gateway thread and a future private-WS worker thread) feeding `KalshiToOmsEvent` back to the OMS coordinator. Rather than share one queue and break the SPSC invariant — which would introduce a data race on `tail_` — these are split into two distinct queues: `oms_rest_event_queue` (written only by the Gateway thread) and `ws_event_queue` (reserved for the private-WS worker). `ExecutionTransport::try_pop_event()` checks `ws_event_queue` first then round-robins across REST event queues, so neither starves the other when both are live. Today `ws_event_queue` is `nullptr` and only the REST path is wired, but the topology is already correct for the eventual second producer — adding it doesn't require a queue redesign.

The same principle drives the recycle topology: rather than have logger, router, and each shard share one recycle queue (which would make it MPSC and require CAS or locks), each producer owns its own SPSC and the IO thread fans them in via round-robin. See [[feedback_spsc_producers]] for the rule: never add a second producer to an existing SPSC.

## Router as a Separate Thread

`IOWriter` could own SPSC queues to each shard directly and dispatch frames without a Router thread in the middle. That would remove one thread hop and one queue.

The Router stage exists to keep the IO thread as lean as possible. The IO thread's only job is to drain the websocket as fast as the network delivers frames. Putting registry lookup, minimal JSON classification, and sequence checking on the IO thread adds variable-cost work to the most latency-sensitive stage in the pipeline. Any stall on the IO thread directly delays frame receipt.

The Router absorbs that classification work without touching the receive path. It can stall on a registry miss or a JSON parse without affecting when the next frame comes off the wire.

A secondary reason: without a Router, `IOWriter` would need direct access to `MarketRegistry`, all shard input queues, and the logger queue simultaneously. The Router contains that coupling. `IOWriter` knows only about the frame pool and one outbound queue.

## simdjson On-Demand at the Router, Full Parse at the Shard

The primary reason for deferring full parsing to shards is parallelism. The Router is a single thread. If it did full deserialization, it would parse one frame at a time regardless of how many shards exist downstream. By doing only the minimum classification work at the Router and deferring full parse to shards, N shard threads parse N frames in parallel. In the wall-clock time the Router would spend fully parsing one frame, N shards can each be fully parsing their own frame simultaneously.

The Router therefore does the minimum work needed to make a routing decision: extract the market ticker, session and sequence identifiers, and a coarse event type. simdjson's on-demand API stops parsing as soon as the query is satisfied, which keeps the Router's per-frame cost as low as possible so it can hand off to shards quickly and move to the next frame.

A secondary benefit: keeping full parse out of the Router avoids coupling the routing stage to the complete Kalshi event schema. The Router only needs to know enough to classify and dispatch. Schema changes in the full event payload don't touch the Router.

## Zero-Copy Frame Pool

Inbound websocket payloads are copied exactly once: from the websocket receive buffer into a pre-allocated slot in the frame pool. No downstream stage copies the payload again. Router, shard, and logger all read from the same pool slot through a `FrameHandle`.

The alternative is heap-allocating a buffer per message and passing ownership downstream. That model allocates and frees on every message, which adds allocator pressure and latency spikes proportional to message rate.

The frame pool pre-allocates all slots at startup. A generation counter on each slot prevents use-after-recycle without requiring locks. Pool capacity is a configured bound on how many frames can be in-flight simultaneously. If the logger falls behind, the pool exhausts and `IOWriter` drops incoming frames rather than growing unbounded — a deliberate backpressure decision that keeps memory bounded at the cost of data loss under sustained overload.

## Shard Affinity Key

The `affinity_key` in each `MarketRouteConfig` controls which shard a market's frames land on. It is an explicit config field rather than a value derived automatically from the market ticker hash.

The separation exists to allow co-location of related markets on the same shard. Kalshi event groups — where multiple sub-markets represent different strike levels of the same underlying outcome — should land on the same shard so a strategy can observe the full probability space of the event without cross-shard coordination. If affinity were derived per market ticker, related sub-markets could scatter across shards with no mechanism to group them.

The intended invariant: all sub-markets of the same Kalshi event share an affinity key derived from the event ticker. The Python discovery tooling (`stable_affinity_key`) derives this from the event ticker so all markets in an event hash to the same key. Shard assignment is `affinity_key % shard_count`.

## Soft vs. Hard Halt

The halt mechanism uses two distinct levels (`HaltMode::kSoft` and `HaltMode::kHard`) rather than a single boolean kill switch.

A single kill switch that immediately cancels all open orders would be harmful for strategies that hold complementary positions to settlement. For example, `MonotonicArbStrategy` may hold two opposite-side legs that are individually loss-making but net profitable at settlement. Canceling both legs on a drawdown threshold would crystallize the loss rather than letting the arb settle.

`kSoft` halt is designed for the drawdown circuit breaker: block new submissions but leave existing orders alive to fill or settle. The breaker condition (`session_net_ticks_ < -max_session_loss_ticks_`) and the per-fill P&L accumulation aren't yet implemented; today `request_soft_halt()` exists but has no caller. The mechanism is in place for the long-range strategy landing.

`kHard` halt is designed for controlled shutdown via `App::stop()`: block new submissions and cancel all live orders before the process terminates. The mode flag is wired (and prevents new submissions during the shutdown drain) but the cancel-all sweep on the OMS thread is **not yet implemented** — `hard_halt_cancel_triggered_` reserves the trigger but no code path in `pump()` enqueues `CancelOrderCmd` for live orders on hard halt. Today's monotonic-arb runs leave no resting orders, so the gap is silent; it must close before any long-range strategy ships.

`halt_mode_` is an `std::atomic<uint8_t>` so `is_halted()` is safe to query from the health-dump path without acquiring a lock.

## Startup Reconciliation Policy

Strategy lifetimes drive how the system should treat venue state across session boundaries:

- **Session-contained strategies** (today's monotonic arb, future hard CDF arb): trades resolve within the session, IOC-only, no resting orders by design. Config can be regenerated freely.
- **Long-range strategies** (future MM, soft-monotonic): orders are expected to rest across sessions. Config stays static for long-range markets; only the session-contained universe regenerates.

Rather than picking a single startup behavior that suits both, the OMS exposes `oms_transport.startup_open_orders_policy` with four named modes (`ignore`, `refuse_if_present`, `cancel_all`, `adopt`). The operator selects what to do with prior-session orders based on which mode they're running this session — see [[project_operational_modes]] for the framing.

The default is **`refuse_if_present`**, which is consistent with the rest of the safe-by-default surface (`oms_transport.enabled=false`, `local_risk.trading_enabled=false`): if any open order is found at the venue at startup, abort and surface every order to the operator. Three reasons this is the right default:

1. **Preserves the kill-switch invariant.** Every order that exists at the venue must be in `OrderStore` or not exist at all when the live loop begins. Refusing to start is the cheapest way to enforce that without trying to infer per-order intent from a REST snapshot.
2. **Surfaces config drift.** A venue order whose market isn't in the current config means either the config is wrong for this session or there's a config-generation bug. Both deserve an abort, not a silent reconciliation.
3. **Operational reality today.** Monotonic arb is IOC-only, so the abort branch never fires in practice. The strictness costs nothing while the system is session-loop-only, and the gate is already in place when long-range strategies start needing it.

The `adopt` mode (seed prior-session orders into `OrderStore` via `Oms::seed_reconciled_order`) is scaffolded — the enum value is selectable, but selecting it aborts with a deferral message. The reason it's deferred rather than implemented: the `OpenOrderSnapshot → OrderState` mapping requires per-order `IntentContext` (strategy id, shard id, event id) that **cannot be recovered from a Kalshi REST snapshot alone**. Closing the gap requires either persisting `OrderStore` across sessions or encoding a strategy hint into `client_order_id`, both of which are bigger plumbing changes that should land alongside the first long-range strategy that actually needs them. Until then, attempting to adopt would mean populating context with placeholders and pretending we know the strategy provenance — which is exactly the kind of "shadow state" the project avoids.

The escape hatch from "the operator can't restart cleanly because of the strict policy" is intended to be an out-of-band reconcile CLI (`predex-reconcile` or `trader_app --reconcile-only`), not a more permissive runtime mode. Strictness in the live loop plus tooling for operator recovery, rather than lenience in the live loop.

## OMS Coordinator as a Single Writer

All order state mutations — insert, apply lifecycle, erase — go through the OMS coordinator thread. `OrderStore` and `GlobalRisk` have no internal synchronization because they are single-writer by design.

The alternative (locking `OrderStore` so multiple threads can update it) would add contention on every fill event and every new order, which are already the latency-critical events the OMS is designed to process efficiently.

The consequence is that shard threads cannot read order state directly. They receive decisions and lifecycle events through queue messages. This is acceptable because shards only need to know: was my intent accepted, and what is my current filled position? Both arrive via `oms_to_shard_decision_queue` and `oms_to_shard_lifecycle_queue`.
