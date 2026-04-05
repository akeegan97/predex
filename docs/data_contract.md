# Data Contract

This document describes the data that moves through the current runtime.

## 1. Inbound Websocket Payload

Source:
- `predex::websocket::WsSession`

Shape:
- raw websocket text payload from Kalshi

At this boundary the data is still just exchange-native JSON text. No routing metadata has been attached yet.

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
- sequencing and session metadata
- slot index and generation
- routing metadata:
  - `market_id_`
  - `affinity_key_`
- coarse message type:
  - `event_type_`

Contract:
- payload bytes live in the frame pool
- downstream stages pass handles, not copied payloads

## 3. Router Contract

Type in flight:
- `FrameHandle`

Router responsibilities:
- inspect the referenced `KalshiFrame`
- classify the message
- look up `market_id` and `affinity_key`
- perform sequence checks
- forward the handle to either:
  - a shard input queue
  - the logger queue

Direct-to-logger messages are part of the contract. Not every inbound message should reach a shard.

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

## 5. Book Application Contract

Type owner:
- [`predex::core::shards::kalshi::BookStore`](../cpp/include/predex/shards/book_store.hpp)

Per-market state:
- bid levels
- ask levels
- last sequence id
- pending delta buffer
- last trade
- counters for apply, replay, stale sequence, and desync events

Application rules today:
- snapshots establish a baseline book
- deltas update one side/price level
- trades update last-trade state
- invalid or stale sequence behavior is tracked explicitly in book state counters

## 6. Tape Contract

Terminal sink:
- [`predex::core::tape::kalshi::Logger`](../cpp/include/predex/tape/logger.hpp)

Tape record format:

```text
[u32 payload_len_le][payload bytes]
```

Repeated for every message that reaches the logger.

The payload written to tape is the raw inbound websocket payload, not the normalized event.

## 7. Ownership Contract

Stage ownership over the message lifecycle:

1. websocket session receives raw text
2. `IOWriter` copies it into the frame pool and pushes a handle
3. Router forwards the handle
4. shard or logger consumes the handle
5. logger persists the raw payload
6. logger pushes the handle to recycle
7. `IOWriter` drains recycle and returns the slot to `FramePool`

This ownership flow is the main runtime contract. If it changes, the surrounding docs should change with it.
