# Runtime Data Contract

This document lists the value messages crossing PredEx thread boundaries. The
C++ types are canonical; this file explains their purpose, identity, and
failure semantics.

## Contract rules

All cross-thread messages follow these rules:

1. They are bounded values suitable for an SPSC queue.
2. Mutable ownership never crosses the queue.
3. Universe-sensitive work carries a universe version.
4. Market work carries stable numeric event/market/shard identity after the
   wire session resolves the ticker.
5. A successful enqueue transfers responsibility for the message or frame
   handle to the consumer.
6. A failed enqueue must have an explicit terminal path; order-book loss also
   produces an integrity fact.
7. Timestamps used for latency are steady-clock nanoseconds from the same
   process.

## Universe contracts

`ControlPlane` builds and distributes immutable universe views.

### `UniverseSnapshot`

Used by public market data and recovery. It contains:

- universe version;
- ticker-to-numeric route metadata;
- event/market/shard indices and affinity;
- the configured market-data subscription set.

### Shard universe commands

Each shard receives only the events assigned to it. Install, quiesce, resume,
and related lifecycle commands include the target shard and universe version.

### `OrderRouteUniverse`

Used by OMS, order REST, and private order feed. It maps numeric market/event
identity to Kalshi tickers and tradeability. The snapshot is immutable and
shared by value through `shared_ptr<const ...>`.

## Public market-data contract

### `FrameHandle`

The handle identifies one immutable payload in `FramePool`. Important fields
include:

- pool index and generation;
- universe version;
- frame kind;
- SID and sequence;
- market, event, affinity, and shard identity;
- event and market indices inside the shard;
- optional recovery incident identity;
- ingress, wire-publish, router-publish, and shard timing fields.

The payload bytes remain in the pool. Copying a handle does not copy the JSON
frame or duplicate ownership of the slot.

### `MarketDataPathMessage`

The wire-to-router and router-to-shard queues carry:

```cpp
std::variant<
    FrameHandle,
    MarketInvalidationBarrier,
    OrderBookSubscriptionInvalidationBarrier
>
```

Using one ordered variant prevents a barrier from overtaking the data loss it
describes.

### `MarketInvalidationBarrier`

Describes market-local order-book loss. It carries the exact universe, incident
origin/ID, SID/sequence, event/market, target shard, target indices, and
`BookInvalidationReason`.

Current causes include wire pool exhaustion, wire-to-router loss, and
router-to-shard loss.

### `OrderBookSubscriptionInvalidationBarrier`

Describes a subscription-wide order-book SID sequence gap. It carries universe,
incident, SID, expected sequence, observed sequence, and reason. The router
fans it to every shard.

## Shard/control recovery contract

After applying a barrier, the shard reports the resulting book transition. A
newly invalidated market generates `ShardMarketRecoveryRequired`; repeated
facts for a market already awaiting recovery remain distinguishable and are
deduplicated by the coordinator.

`RecoveryCoordinator` correlates:

- shard invalidation observation;
- `RecoverMarketIo` command and request attempt;
- `IoRecoveryRequestAccepted` or `IoRecoveryRequestFailed`;
- `ShardRecoverySnapshotApplied`;
- acknowledgement and snapshot timeouts.

Recovery is complete only at the final shard-applied event. A websocket command
acknowledgement means the request was accepted, not that book state is usable.

## Shard/strategy contract

`ShardToStrategyMessage` is a variant of:

- `MonotonicPairObservation`
- `StrategyEventUnavailable`
- `StrategyShardUnavailable`

An observation is an immutable projection of the event state required by the
strategy. It includes source shard, universe, event revision, event/market
identity, ordered pair metadata, bounded book views, availability, and timing.

Unavailable messages revoke eligibility. Strategy must not keep evaluating a
cached observation after receiving the corresponding invalidation.

## Strategy/OMS contract

`StrategyIntent` is a variant of new, cancel, modify, and group intents.

### `IntentContext`

Context follows every intent and carries:

- strategy index and strategy-defined IDs;
- event, market, signal, group, and leg identity;
- universe version, event revision, and source shard;
- ingress, book-apply, observation, dequeue, evaluation, and intent-publish
  timestamps.

This lets OMS validate provenance and attribute latency without reading shard
or strategy state.

### `GroupOrderIntent`

A group contains a fixed-capacity array plus an explicit `leg_count`. It also
carries admission policy and expected gross edge, estimated fee, and expected
net edge. Unused array elements are not orders.

`kALL_OR_NONE` means all legs must pass local OMS admission together. It does
not imply atomic venue fills.

## OMS/order-REST contract

`OmsToKalshiCommand` includes single-order submit/cancel/modify commands,
batched submission, and portfolio reconciliation requests. Each command has an
OMS identity and the ticker-resolvable order context needed by the adapter.

`KalshiToOmsEvent` includes:

- REST order and batch responses;
- private-websocket order/fill facts;
- reconciled open-order facts;
- venue portfolio/position snapshots;
- an order-REST egress-drained marker.

The REST adapter owns HTTP serialization and venue-field conversion. OMS owns
the meaning of the resulting lifecycle transition.

## OMS/strategy contract

`OmsToStrategyMessage` includes:

- admission or rejection;
- per-order lifecycle state;
- group admission and execution state;
- sequenced strategy portfolio state;
- sequenced per-market position state.

Portfolio messages are authoritative snapshots from OMS. Strategy-local
estimates may be used for decision timing but cannot replace a newer OMS
sequence.

## Operator contract

The operator protocol is one newline-delimited JSON request and one
newline-delimited JSON response per Unix-domain connection.

Requests identify one command and request ID. Responses contain:

- `ok`;
- the echoed request ID;
- a response `type`;
- an acknowledgement, error, status, or counter snapshot payload.

`status` is the compact lifecycle view. `counterstats` is the complete raw
snapshot and may be large. `stats` is currently an alias for `counterstats`.

The client preserves the JSON body rather than interpreting server acceptance.
Automation must inspect `ok`, not only the `predexctl` exit code.

## Tape contract

`MarketDataLogger` writes the versioned `PDT2` binary format. The file begins
with magic, format version, and flags. Each little-endian record contains a
fixed header followed by the raw websocket payload:

```text
[universe_version, recv_ts_ns, sequence, affinity_key]
[sid, market_id, event_id, shard/event/market indices]
[payload_length, frame_kind, topology, flags]
[payload bytes]
```

The tape preserves received JSON together with the numeric routing and ingress
metadata needed for deterministic downstream materialization. The Python
reader rejects unknown magic/version values and truncated record headers or
payloads.

## Numeric units

- Money and price use `$0.0001` ticks (`10,000` ticks per dollar).
- Quantities use fixed-point lots defined by the strategy/OMS contract.
- Internal durations use nanoseconds.
- Wire portfolio values with more precision are conservatively rounded at the
  Kalshi adapter boundary.

Unit names should remain explicit in field names. A change to scale is a data
contract migration, not a local formatting edit.

## Telemetry contract

The full counter snapshot separates:

- exchange sequence gaps, duplicates, and stale frames;
- intentional filtering and logger-only frames;
- downstream delivery losses;
- book invalidation and recovery;
- queue/pool high-water marks;
- logger write/recycle failures;
- OMS admission, reconciliation, execution, and repair;
- stage latency histograms.

The snapshot is assembled asynchronously. Differences between adjacent stage
counts can represent in-flight queue contents and should be interpreted with
the high-water and failure counters, not assumed to be loss by subtraction.
