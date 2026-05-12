# OMS Design

This document describes the OMS coordinator: its components, lifecycle, queues, risk controls, and halt behavior. Several behaviors below are **designed-not-wired** and are called out inline — the document distinguishes "live" from "scaffolded but deferred" to keep the reader aligned with the current discrete-session runtime.

## Scope

The OMS coordinator is the single writer to all order state. It runs on its own thread and is responsible for:

- Draining shard intent queues (round-robin across shards)
- Running global pre-trade risk checks on each intent (`GlobalRisk`, capital-reservation model)
- Maintaining `OrderStore` (all live and terminal order state)
- Issuing submit, cancel, and modify commands to the OMS Gateway thread via `oms_command_queue`
- Processing lifecycle events from `oms_rest_event_queue` (the only producer today; `ws_event_queue` is reserved for a future Kalshi private-WS transport)
- Fanning lifecycle events back to the originating shard
- *(Designed, not wired)* Tracking session P&L and enforcing the drawdown soft-halt against `oms_transport.max_session_loss_ticks`
- *(Designed, not wired)* At hard halt: cancelling all live orders before exit

## Key Types

Canonical definitions live in `cpp/include/predex/oms/oms_types.hpp` and `cpp/include/predex/oms/order_store.hpp`; this section summarises the shapes rather than restating every field, so the doc and code don't drift on minor renames.

### `NewOrderIntent`

A request from a shard strategy to submit a new order. Carries an `IntentContext` (strategy id, shard id, event id, market id, signal id, intent latency timestamps), exchange selector, side / outcome, quantity in lots, optional limit price in ticks, time-in-force (`kGtc` / `kIoc` / `kFok`), and liquidity / order-type intent hints.

### `GroupOrderIntent`

A multi-leg group of up to `kMaxGroupOrderLegs` `NewOrderIntent` legs. Used by `MonotonicArbStrategy` to submit both legs of an arb simultaneously. Carries a `GroupExecutionPolicy` (`kAbortRemainingOnReject` or `kBestEffort`).

### `ShardOmsRequest`

A variant of `NewOrderIntent` / `GroupOrderIntent` / `CancelOrderIntent` / `ModifyOrderIntent`, pushed to the per-shard request queue.

### `OmsToShardDecision`

The OMS coordinator's response to a shard intent: accepted / rejected / modified, with the appropriate inner data carrier. Pushed to `oms_to_shard_decision_queue[i]`.

### `KalshiToOmsEvent`

A transport-level event arriving from the exchange: submit ack/reject, fill (partial or final), cancel ack/reject, replace ack/reject, or order-uncertain. Produced today by the OMS Gateway thread and pushed to `oms_rest_event_queue`. The `ws_event_queue` variant is reserved for the future private-WS transport.

### `OrderState`

The per-order mutable state owned by `OrderStore`. Key fields (see `order_store.hpp` for the canonical list):

- Identifiers: `oms_request_id`, `client_order_id`, `exchange_order_id`
- Status: `OrderStatus` enum — `kPendingSubmit`, `kWorking`, `kPartiallyFilled`, `kFilled`, `kPendingCancel`, `kCanceled`, `kPendingReplace`, `kRejected`, `kUncertain`
- Quantities: `initial_qty_lots`, `working_qty_lots`, `cumulative_filled_qty_lots`
- Prices: `initial_limit_price_ticks`, `working_limit_price_ticks` (both optional)
- TIF / liquidity / order type intents
- Latency timestamps: `intent_ts_ns`, `oms_decision_ts_ns`, `venue_submit_ts_ns`, `venue_ack_ts_ns`, `first_fill_ts_ns`, `last_fill_ts_ns`, `terminal_ts_ns`, `last_update_ts_ns`

## OrderStore

`OrderStore` owns all live order state and three lookup indices keyed by `OmsRequestId`, `ClientOrderId`, and `ExchangeOrderId`. The three indices allow matching incoming lifecycle events that may arrive with only a `client_order_id` (REST response) or only an `exchange_order_id` (future private-WS lifecycle events). The OMS coordinator is the single writer.

`OrderStore::adopt_reconciled_order(state)` inserts into all three indices without assigning a new request ID. It is currently reachable only from `Oms::seed_reconciled_order(state)`, which is itself uncalled today — it is the future call site for the `adopt` mode of `oms_transport.startup_open_orders_policy` (see Startup Reconciliation below).

## GlobalRisk

`GlobalRisk` is the global pre-trade gate. Single writer: OMS coordinator thread. Today it implements a **capital-reservation model** rather than per-event open-order / exposure counting:

- A budget of available capital is configured via `oms_transport.available_capital_ticks`.
- On `on_new_order_accepted(intent, decision_ts)`, the cost (price × qty) of the order is reserved from the available budget.
- Reservation is released on terminal lifecycle events (fill, cancel, reject) — partial fills convert the reserved capital into realised exposure.
- If a new intent would exceed the available budget, it is rejected pre-submit.

Per-event open-order and exposure counters are *not* tracked here. Local risk (`LocalRiskManager`) handles per-market net-position bookkeeping on the shard thread; `GlobalRisk` is concerned only with global capital headroom. Earlier doc revisions described a `RiskEngine` wrapper with per-event state — that abstraction does not exist in code today and the `GlobalRisk` capital-reservation model is what actually runs.

## Halt Modes

```cpp
enum class HaltMode : std::uint8_t {
    kNone = 0,
    kSoft = 1,  // designed: block new submissions; existing orders survive to fill/settle
    kHard = 2,  // designed: block new submissions + cancel-all (processed on OMS thread)
};
```

`halt_mode_` is `std::atomic<std::uint8_t>` readable from any thread. The two-level split is deliberate (see `design_decisions.md`): drawdown signals should not cancel locked-in arb legs, while operator/shutdown signals should sweep everything.

### Soft Halt (designed, not yet wired)

`Oms::request_soft_halt()` sets `halt_mode_` to `kSoft`. New intent submissions are rejected by the pump's halt check; existing orders are left alive to fill or settle. Intended trigger is the drawdown breaker (`session_net_ticks_ < -max_session_loss_ticks_`), but neither `session_net_ticks_` nor the breaker condition is implemented today — `request_soft_halt()` has no caller in the codebase. The mechanism is in place; the trigger is the gap.

### Hard Halt (partially wired)

`Oms::request_hard_halt()` sets `halt_mode_` to `kHard`. It is called by `App::Runtime::stop()` during shutdown. The mode flag prevents new submissions for the brief tail-drain window.

What is **not** yet wired: the cancel-all sweep on the OMS thread. The `hard_halt_cancel_triggered_` flag exists, but no code path in the current OMS pump enqueues `CancelOrderCmd` for every live order on hard halt. Today's monotonic-arb runs never leave resting orders, so the gap is silent in practice; it must close before any long-range strategy can run safely. See [[project_halt_modes]] for the design rationale.

## Startup Reconciliation

When `oms_transport.enabled` is true, `App::Runtime::reconcile_open_orders_from_rest()` runs before any worker threads start and dispatches on `oms_transport.startup_open_orders_policy`:

- **`refuse_if_present`** (default, live) — paginates `KalshiRestAdapter::fetch_open_orders`; on non-empty result, logs every order (ticker / status / side / action / qty / price / exchange_order_id) and aborts startup. Preserves the kill-switch invariant for the live loop.
- **`ignore`** (live) — skips the REST fetch entirely. Intended for dev iteration when prior-session state is known clean.
- **`cancel_all`** (scaffolded, deferred) — intended to fetch + REST-cancel everything before going live; selecting it currently aborts startup with a clear deferral message. Implementation is deferred until the operator interface (a `predex-reconcile` CLI or `--reconcile-only` flag) is firmed up alongside the first long-range strategy.
- **`adopt`** (scaffolded, deferred) — intended to seed configured-market open orders into `OrderStore` via `Oms::seed_reconciled_order`. The `OpenOrderSnapshot → OrderState` mapping requires per-order `IntentContext` (strategy id, shard id, event id) that **cannot be recovered from a Kalshi REST snapshot alone**. Closing this gap requires either persisting `OrderStore` across sessions or encoding a strategy hint into `client_order_id`; both are out of scope until a strategy actually needs to rest orders across sessions.

The current operational reality is that no strategy rests orders today, so `refuse_if_present` passes with zero orders on every startup. The policy is the defensive gate for the long-range strategy landing, not a feature exercised by today's runs. See [[project_operational_modes]] for the operational-mode framing.

(There is no separate "WS reconnect reconciliation" path today because the private WS isn't wired. Once it is, post-disconnect reconciliation will need to REST-fetch fill history to recover events missed during the gap — see `project_kalshi_user_orders_protocol` in memory for the protocol detail.)

## Session P&L and Drawdown (designed, not yet wired)

The intended drawdown mechanism is: accumulate signed tick P&L on every fill (`session_net_ticks_`), and after each transport update check whether `max_session_loss_ticks > 0 && session_net_ticks < -max_session_loss_ticks && halt_mode == kNone`; if so, call `request_soft_halt()`.

Today neither `session_net_ticks_` nor the per-fill accumulation exists in the OMS, and `oms_transport.max_session_loss_ticks` is parsed from config but never read by any consumer. The mechanism is unblocked the moment a strategy starts producing fills worth bounding — `MonotonicArbStrategy`'s paired-IOC pattern makes the breaker irrelevant today (a single arb either lands flat or doesn't land at all).

## Transport Commands

The OMS coordinator pushes typed command variants to a single `oms_command_queue` consumed by the OMS Gateway thread:

| Command | Queue | Trigger |
|---|---|---|
| `SubmitOrderCmd` | `oms_command_queue` | accepted intent |
| `CancelOrderCmd` | `oms_command_queue` | cancel intent (cancel-all-on-hard-halt is designed but not yet wired) |
| `ModifyOrderCmd` | `oms_command_queue` | modify intent |

`oms_command_queue` carries `OmsToKalshiCommand = std::variant<SubmitOrderCmd, CancelOrderCmd, ModifyOrderCmd>`. The Gateway thread routes commands through the 5-stage pipeline (`CommandIngress → OrderSequencer → BatchPlanner → RateLimiter → SessionPool`) and executes them via `AsyncRestConnection`. Results are pushed to `oms_rest_event_queue`; the `ws_event_queue` pointer is `nullptr` today and will carry private-WS events once that transport is wired. The OMS coordinator drains both via `ExecutionTransport::try_pop_event()`.

## Pump Loop

Each call to `pump(max_kalshi_events, max_shard_requests)`:

1. Drain up to `max_kalshi_events` events from `oms_rest_event_queue` (and `ws_event_queue` once wired) via `ExecutionTransport::try_pop_event()`.
2. Drain up to `max_shard_requests` submissions from the per-shard request queues (round-robin starting from `next_shard_index_`).

Round-robin intent draining ensures no shard starves another under load. The cancel-all-on-hard-halt step is reserved in the design — `hard_halt_cancel_triggered_` exists for this purpose — but is not yet implemented inside `pump()`.

## Lifecycle State Machine

```
PendingSubmit
  -> Working          (submit ack from venue)
  -> Rejected         (submit reject from venue)
  -> Uncertain        (transport-level uncertainty — e.g. write succeeded but ack lost)

Working
  -> PartiallyFilled  (partial fill)
  -> Filled           (terminal — erased from OrderStore)
  -> PendingCancel    (CancelOrderCmd enqueued)
  -> PendingReplace   (ModifyOrderCmd enqueued)
  -> Canceled         (terminal — erased from OrderStore)

PartiallyFilled
  -> Filled           (terminal)
  -> Canceled         (terminal)

PendingCancel
  -> Canceled         (cancel ack — terminal)
  -> Working          (cancel reject — reverts to Working)

PendingReplace
  -> Working          (replace ack — new working price/qty)
  -> Working          (replace reject — original survives)
```

Terminal states (`Filled`, `Rejected`, `Canceled`) cause the order to be erased from `OrderStore`. The OMS releases the corresponding capital reservation in `GlobalRisk` on terminal events and converts reservation → realised on fills. `Uncertain` is a non-terminal limbo used when the transport layer cannot confirm whether a submit reached the venue; resolution comes from a post-write recovery REST fetch handled by the Gateway.

## Observability

The OMS exposes the following counters via `App::Runtime::print_health_status` (see `cpp/src/app.cpp` for the canonical schema; the doc lists them by intent, not as a complete inventory):

- `halted` — `Oms::is_halted()`
- `live_orders` — `order_store_.live_order_count()`
- `oms_shard_requests` — processed shard request count
- `oms_kalshi_events` — processed transport event count
- `oms_emitted_decisions` / `oms_emitted_transport` / `oms_emitted_lifecycle` — outbound counters per direction
- `oms_rejected_decisions` — pre-trade reject count

`pnl_ticks` is reserved in the health-line vocabulary but not emitted today; it lands together with the drawdown breaker (see Session P&L section).
