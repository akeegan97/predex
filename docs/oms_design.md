# OMS Design

This document describes the implemented OMS coordinator: its components, lifecycle, queues, risk controls, and halt behavior.

## Scope

The OMS coordinator is the single writer to all order state. It runs on its own thread and is responsible for:

- Draining shard intent queues (round-robin across shards)
- Running global pre-trade risk checks on each intent
- Maintaining `OrderStore` (all live and terminal order state)
- Issuing submit, cancel, and modify commands to the OMS REST thread
- Processing lifecycle events from `oms_transport_update_queue` (two producers: REST thread and private WS thread)
- Fanning lifecycle events back to the originating shard
- Tracking session P&L (net ticks) and enforcing the drawdown circuit breaker
- At hard halt: cancelling all live orders via `cancel_all_live_orders()`

## Key Types

### `OrderIntent`

A request from a shard strategy to submit a new order. Fields:

- `origin` — `IntentOrigin`: shard_id, affinity_key, group_id, local_intent_id, leg_index, leg_count, signal_id, event_id, market_id, latency timestamps
- `exchange` — currently always `kKalshi`
- `side` — `kBuy` or `kSell`
- `qty_lots`
- `limit_price_ticks` — optional; absent means market order
- `time_in_force` — `kGtc`, `kIoc`, or `kFok`
- `intent_ts_ns`

### `GroupOrderIntent`

A multi-leg atomic or best-effort group of up to 4 `OrderIntent` legs. Used by `MonotonicArbStrategy` to submit both legs of an arb simultaneously.

- `execution_policy` — `kAbortRemainingOnReject` or `kBestEffort`

### `OmsSubmission`

A `std::variant<OrderIntent, GroupOrderIntent, CancelIntent, ModifyIntent>` pushed to the shard intent queue.

### `IntentDecision`

The OMS coordinator's response to a shard: `kAccepted`, `kRejected`, or `kModified`. The inner data variant holds an `AcceptedIntent`, `RejectedIntent`, or `ModifiedIntent`. Pushed to `oms_to_shard_decision_queue[i]`.

### `OrderLifecycleEvent`

A transport-level event arriving from the exchange: ack, reject, partial fill, fill, cancel ack, cancel reject, replace ack, replace reject, or canceled. Produced by both the REST thread and the private WS thread and pushed to `oms_transport_update_queue`.

### `OrderState`

The per-order mutable state owned by `OrderStore`:

- `oms_request_id`, `client_order_id`, `exchange_order_id`
- `status` (`kPendingSubmit` → `kLive` → `kPartiallyFilled` / `kFilled` / `kCanceled` etc.)
- `original_qty_lots`, `live_qty_lots`, `cum_fill_qty_lots`
- `live_limit_price_ticks`
- Latency timestamps: `oms_decision_ts_ns`, `transport_submit_ts_ns`, `first_fill_recv_ns`, `terminal_recv_ns`

## OrderStore

`OrderStore` owns all live order state and three lookup indices:

| Index | Key | Value |
|---|---|---|
| `orders_by_request_id_` | `OmsRequestId` | `OrderState` |
| `request_by_client_order_id_` | `ClientOrderId` | `OmsRequestId` |
| `request_by_exchange_order_id_` | `ExchangeOrderId` | `OmsRequestId` |

The three indices allow matching incoming lifecycle events that may arrive with only a `client_order_id` (REST response) or only an `exchange_order_id` (private WS).

`adopt_orphaned(OrderState)` inserts into all three indices without assigning a new request ID. Used exclusively by `seed_orphaned_order()` before the OMS thread starts.

## RiskEngine

`RiskEngine` wraps `GlobalRiskManager` and owns mutable per-event risk state. Single writer: OMS coordinator thread.

Per-event state:

- `open_orders` — count of non-terminal orders for the event
- `exposure_lots` — total open qty across all live orders for the event

`evaluate(intent, oms_request_id, client_order_id, decision_ts_ns)` assembles the combined `GlobalRiskState` for `intent.origin.event_id` and delegates to `GlobalRiskManager::evaluate()`.

`on_intent_accepted(AcceptedIntent)` increments open order count and exposure.
`on_fill(event_id, fill_qty_lots)` decrements exposure.
`on_order_terminal(event_id, remaining_open_qty_lots)` decrements open order count and clears the remaining exposure.

## Halt Modes

```cpp
enum class HaltMode : std::uint8_t {
    kNone = 0,
    kSoft = 1,  // block new submissions; existing orders survive to fill/settle
    kHard = 2,  // block new submissions + cancel-all (processed on OMS thread)
};
```

`halt_mode_` is an `std::atomic<std::uint8_t>` readable from any thread.

### Soft Halt

`request_soft_halt()` transitions `kNone → kSoft` via `compare_exchange_strong` (no-op if already soft or hard halted). New intent submissions are rejected with `IntentRejectReason::kHalted`. Existing orders are left alive to fill or settle — correct behavior for arb strategies that must hold legs to settlement.

**Trigger**: drawdown circuit breaker — `session_net_ticks_ < -max_session_loss_ticks_` when `max_session_loss_ticks_ > 0`.

### Hard Halt

`request_hard_halt()` sets `halt_mode_` to `kHard` unconditionally. At the top of the next `pump()` call on the OMS thread, `cancel_all_live_orders()` is called exactly once (guarded by `hard_halt_cancel_triggered_`). This enqueues a `CancelOrderCmd` for every order in `OrderStore`.

**Trigger**: controlled shutdown via `App::stop()` → `oms->request_hard_halt()`.

## Startup Reconciliation

`reconcile_open_orders_from_rest(is_startup=true)` is called before the OMS thread starts. It fetches open orders from the Kalshi REST API and for each:

1. Looks up the `market_ticker` in the market registry to resolve `market_id`, `event_id`, `shard_id`, `affinity_key`.
2. Builds an `OrderState` with status `kLive`, `original_qty_lots` and `live_qty_lots` from `remaining_count`, `exchange_order_id` and `client_order_id` from the REST response.
3. Calls `Oms::seed_orphaned_order(state, side, outcome)` which:
   - Assigns a synthetic `oms_request_id` from `next_oms_request_id_++`
   - Calls `risk_engine_.on_intent_accepted(...)` with `live_qty_lots` so event exposure counts are accurate
   - Calls `order_store_.adopt_orphaned(state)`

Adopted orders are fully tracked by the kill switch and their fills are counted in `session_net_ticks_`.

## OMS WS Reconnect Reconciliation

`reconcile_open_orders_from_rest(is_startup=false)` is called after the private WS reconnects. It fetches current-session open orders by `client_order_id` and pushes a synthetic `kAck` `OrderLifecycleEvent` for each so the OMS can reconcile state without double-inserting.

## Session P&L and Drawdown

`session_net_ticks_` accumulates signed tick P&L on every fill event:

- `kBuy` fill: `session_net_ticks_ -= fill_qty_lots * fill_price_ticks`
- `kSell` fill: `session_net_ticks_ += fill_qty_lots * fill_price_ticks`

After each transport update, if `max_session_loss_ticks_ > 0` and `session_net_ticks_ < -max_session_loss_ticks_` and `halt_mode_ == kNone`, `request_soft_halt()` is called.

## Transport Commands

The OMS coordinator enqueues commands to three separate queues consumed by the OMS REST thread:

| Command | Queue | Trigger |
|---|---|---|
| `SubmitOrderCmd` | `oms_submit_queue` | accepted intent |
| `CancelOrderCmd` | `oms_cancel_queue` | cancel intent or `cancel_all_live_orders()` |
| `ModifyOrderCmd` | `oms_modify_queue` | modify intent |

The REST thread executes blocking HTTP calls and pushes the resulting `OrderLifecycleEvent` to `oms_rest_update_queue`. The private WS thread pushes fill and lifecycle events to `oms_ws_update_queue`. The OMS coordinator drains both round-robin via `ExecutionTransport::try_pop_lifecycle_event()`.

## Pump Loop

Each call to `pump(max_transport_updates, max_shard_intents)`:

1. If `halt_mode_ >= kHard` and `!hard_halt_cancel_triggered_`: call `cancel_all_live_orders()`, set `hard_halt_cancel_triggered_ = true`.
2. Drain up to `max_transport_updates` events from `oms_transport_update_queue`.
3. Drain up to `max_shard_intents` submissions from `shard_intent_queues_` (round-robin starting from `next_shard_index_`).

Round-robin intent draining ensures no shard starves another under load.

## Lifecycle State Machine

```
PendingSubmit
  -> Live             (kAck from REST/WS)
  -> Rejected         (kReject from REST/WS)

Live
  -> PartiallyFilled  (kPartialFill)
  -> Filled           (kFill — terminal, erased from OrderStore)
  -> PendingCancel    (CancelOrderCmd enqueued)
  -> PendingModify    (ModifyOrderCmd enqueued)
  -> Canceled         (kCanceled — terminal, erased from OrderStore)

PartiallyFilled
  -> Filled           (kFill — terminal)
  -> Canceled         (kCanceled — terminal)

PendingCancel
  -> Canceled         (kCancelAck — terminal)
  -> Live             (kCancelReject — reverts to Live)

PendingModify
  -> Live (replaced)  (kReplaceAck)
  -> Live (original)  (kReplaceReject)
```

Terminal states (Filled, Rejected, Canceled) cause the order to be erased from `OrderStore`. The OMS calls `risk_engine_.on_order_terminal()` and, on fills, `risk_engine_.on_fill()`.

## Observability

The OMS exposes the following counters via `App::run()` health dump:

- `live_orders` — `order_store_.live_order_count()`
- `intents` — `processed_intent_count_`
- `rejected` — `rejected_intent_count_`
- `transport_updates` — `processed_transport_update_count_`
- `pnl_ticks` — `session_net_ticks_`
- `halted` — `is_halted()`
