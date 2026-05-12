# Data Contract

This document describes the data that moves through the current runtime.

## 1. Inbound Websocket Payload

Source:
- `predex::websocket::WsSession`

Shape:
- Raw websocket text payload from Kalshi public feed

At this boundary the data is still exchange-native JSON text. No routing metadata has been attached yet.

## 2. Frame Pool Representation

Types:
- [`predex::core::ingest::kalshi::KalshiFrame`](../cpp/include/predex/ingest/frame_pool.hpp)
- [`predex::core::ingest::kalshi::FrameHandle`](../cpp/include/predex/ingest/frame_pool.hpp)

`IOWriter` copies the inbound payload into `KalshiFrame` and pushes a `FrameHandle` downstream.

`KalshiFrame` contains:
- `recv_ts_ns_`
- `len_`
- `flags_`
- `payload[...]`

`FrameHandle` contains:
- Sequencing and session metadata
- Slot index and generation
- Routing metadata:
  - `market_id_`
  - `affinity_key_`
- Coarse message type:
  - `event_type_`

Contract:
- Payload bytes live in the frame pool
- Downstream stages pass handles, not copied payloads

## 3. Router Contract

Type in flight:
- `FrameHandle`

Router responsibilities:
- Inspect the referenced `KalshiFrame`
- Classify the message (shard-bound, logger-only, drop)
- Look up `market_id` and `affinity_key` from `MarketRegistry`
- Enforce session sequence checks
- Forward the handle to either:
  - A shard input queue (`shard_input_queue[i]`)
  - The logger queue (`router_to_logger_queue`)

Control-plane messages (e.g., `subscribed` acknowledgements) go directly to the logger. Shard-bound messages are market data events that require book application.

## 4. Parser Contract

Type:
- [`predex::parsers::ParseResult<predex::internal::NormalizedEvent>`](../cpp/include/predex/parsers/parse_result.hpp)

Parser inputs:
- `FrameHandle`
- `KalshiFrame`

Parser output:
- `NormalizedEvent`

Current normalized event fields:
- `type`
- `meta`
  - `exchange`
  - `affinity_key`
  - `market_id`
  - `sequence_id`
  - `recv_ns`
  - `exchange_ts_ns`
- `raw_sequence_id`
- `data`
  - `SnapshotData`
  - `DeltaData`
  - `TradeData`

This is the first stage where exchange-native JSON is converted into a stable internal event model.

**Kalshi ask derivation**: Kalshi's wire format has no explicit ask book. The ask side is derived from the No-bid: a No-bid at tick `p` implies an Ask at `1000 - p`. The parser handles this at both snapshot and delta level. Everything downstream sees a standard two-sided book.

## 5. Book Application Contract

Type owner:
- [`predex::core::shards::kalshi::EventStore`](../cpp/include/predex/shards/event_store.hpp)

Each shard owns an `EventStore` that holds one `Event` per Kalshi event group.

Per-event state:
- `BookStore` — per-market bid/ask levels, sequence state, pending-delta buffer, trade state, apply/desync counters
- `EventDerivedState` — topology-specific mirror of the books:
  - `MonotonicChainState` — markets ordered by strike key (monotonic arb)
  - `MutuallyExclusiveState` — unordered markets summing to 1
  - `UnorderedGroupState` — unordered independent markets
  - `SingleMarketState` — single-market event

Application rules:
- Snapshots establish a baseline book (`has_snapshot = true`)
- Deltas update one side/price level
- Out-of-sequence deltas are buffered and replayed when possible
- Invalid or stale sequence events are counted; a persistent desync increments `desynced_events`
- On WS reconnect, `EventStore::reset_all_books()` clears `has_snapshot` so the next snapshot from the new session is accepted cleanly

## 6. Strategy Pipeline Contract

After each book application, the shard runs `ShardPipeline::on_event(NormalizedEvent, EventStore)`:

1. `LocalRiskManager::evaluate(intent)` — pre-strategy gate. Checks:
   - `trading_enabled`
   - `min_seconds_to_close` — reject if `close_time_s - now_s < limit`
   - `max_net_position_lots_per_market` — reject if net filled position exceeds limit
   - Open intent count limit per event
   - Event and market exposure limits

2. All active strategies fan out and emit signals into a shared buffer (up to `kMaxSignalsPerEvent = 16` single-leg signals plus a group-signal buffer):
   - `MonotonicArbStrategy` — detects probability-monotonicity violations across a chain event; emits IOC leg pairs as `GroupSignal`
   - `CdfViolationStrategy` — stub
   - `MarketMakingStrategy` — stub
   - `MeanReversionStrategy` — stub

3. The pipeline iterates every collected signal and runs `LocalRiskManager::evaluate` independently per signal. Each risk-approved signal generates an `OmsSubmission`; rejected signals are skipped. Multiple submissions may be pushed per event.

4. Accepted `OmsSubmission`s are pushed to `shard_to_oms_intent_queue[i]`.

## 7. OMS Intent Contract

Types pushed to `shard_to_oms_intent_queue[i]`:
- `ShardOmsRequest` = `std::variant<NewOrderIntent, GroupOrderIntent, CancelOrderIntent, ModifyOrderIntent>`

The OMS coordinator drains these round-robin across shards. Each submission is passed through `GlobalRisk` (capital-reservation pre-trade check) before being accepted or rejected.

## 8. OMS Decision Contract

`OmsToShardDecision` is pushed to `oms_to_shard_decision_queue[i]`:

- `kAccepted` + accepted-intent data (`oms_request_id`, `client_order_id`, originating intent)
- `kRejected` + reject reason
- `kModified` + modification metadata

See `cpp/include/predex/oms/oms_types.hpp` for the canonical field layout. The originating shard uses decisions to update `LocalRiskState` (open intent counts).

## 9. Order Lifecycle Contract

`KalshiToOmsEvent` records are pushed to two SPSC queues, one per producer:
- `oms_rest_event_queue` — written by the OMS Gateway thread (REST API responses from `AsyncRestConnection`)
- `ws_event_queue` — reserved for the future private-WS worker; pointer is `nullptr` today and `ExecutionTransport::try_pop_event()` skips it

The OMS coordinator drains both via `ExecutionTransport::try_pop_event()`, applies each event to `OrderStore`, releases the `GlobalRisk` capital reservation on terminal events (converting it to realised exposure on fills), and fans the event to `oms_to_shard_lifecycle_queue[i]` for the originating shard.

The shard uses lifecycle events to update `LocalRiskState` (net filled position).

Lifecycle event kinds cover: submit ack/reject, partial fill, fill, cancel ack/reject, replace ack/reject, and a transport-level `Uncertain` state used when a write reached the wire but the response was lost. See `cpp/include/predex/oms/oms_types.hpp` for the canonical variant.

## 10. Transport Command Contract

Commands pushed by the OMS coordinator to the Gateway thread via a single unified queue:

| Queue | Type (variant) | Key Fields |
|---|---|---|
| `oms_command_queue` | `SubmitOrderCmd` | `oms_request_id`, `intent`, `client_order_id` |
| `oms_command_queue` | `CancelOrderCmd` | `oms_request_id`, `origin`, `client_order_id`, `exchange_order_id`, `cmd_ts_ns` |
| `oms_command_queue` | `ModifyOrderCmd` | `oms_request_id`, `replacement_intent`, `client_order_id`, `exchange_order_id` |

`oms_command_queue` carries `OmsToKalshiCommand = std::variant<SubmitOrderCmd, CancelOrderCmd, ModifyOrderCmd>`. The Gateway's `CommandIngress` stage pops from this queue and routes commands through the 5-stage pipeline (`CommandIngress → OrderSequencer → BatchPlanner → RateLimiter → SessionPool`) before dispatching to `AsyncRestConnection`.

## 11. Audit Contract

`AuditEvent` records are pushed to `shard_audit_queue[i]` (by shards) and `oms_audit_queue` (by the OMS coordinator). The audit thread drains both and writes JSONL records to the configured audit output path (for example `logs/live/predex_audit.jsonl`).

Audit events capture: OMS decisions, fills, latency spans, halt transitions.

## 12. Tape Contract

Terminal sink:
- [`predex::core::tape::kalshi::Logger`](../cpp/include/predex/tape/logger.hpp)

Tape record format (PDT2):

```text
File header:
  magic[4]    = 'P','D','T','2'
  version     = 2  (uint16_t, little-endian)
  flags       = 0  (uint16_t, little-endian)

Per record (repeated):
  recv_ts_ns  (uint64_t, little-endian)
  len         (uint32_t, little-endian)
  payload     (len bytes — raw websocket text)
```

The payload written to tape is the raw inbound websocket text, not the normalized event.

## 13. Ownership Contract

Stage ownership over the market data message lifecycle:

1. Websocket session receives raw text
2. `IOWriter` copies it into the frame pool and pushes a handle
3. Router classifies and forwards the handle (or recycles directly on router-side drops via `recycle_from_router`)
4. Shard (or logger directly) consumes the handle (shard-side drops recycle via `recycle_from_shards[i]`)
5. Shard applies the event and forwards the handle to the logger
6. Logger persists the raw payload
7. Logger pushes the handle to `recycle_from_logger`
8. `IOWriter` drains all recycle SPSCs round-robin and returns the slot to `FramePool`

Frame recycling uses **per-producer SPSC + consumer fan-in** rather than a single shared recycle queue — every producer thread owns its own recycle SPSC. This ownership flow is the main runtime data contract. If it changes, the surrounding docs should change with it.
