# Architecture

This document describes the currently supported runtime architecture in the C++ codebase.

## Scope

The runtime that exists today is a market-data capture and book-maintenance engine:
- connect to Kalshi websocket
- copy inbound frames into a bounded frame pool
- route frames to either shard processing or direct tape logging
- parse and apply market-data events in shard-local book stores
- persist the raw inbound feed to a tape file

The runtime does not yet include a production OMS, risk engine, replay executable, or strategy runtime beyond the shard event-handler seam.

## Main Components

### `predex_websocket`

Defined in:
- [`cpp/include/predex/websocket/client.hpp`](../cpp/include/predex/websocket/client.hpp)
- [`cpp/include/predex/websocket/session.hpp`](../cpp/include/predex/websocket/session.hpp)
- [`cpp/include/predex/websocket/kalshi/ws_adapter.hpp`](../cpp/include/predex/websocket/kalshi/ws_adapter.hpp)

Responsibilities:
- own websocket transport mechanics
- build Kalshi-specific connect and subscribe requests
- return raw websocket payloads to the IO thread

The transport/session boundary is intentionally generic. The Kalshi adapter is exchange-specific.

### `predex_core_pipeline`

Defined across:
- ingest
- routing
- shards
- tape
- parser

Responsibilities:
- frame allocation and reuse
- bounded queue handoff between stages
- routing and shard fanout
- parser output normalization
- shard-local book application
- binary tape persistence

### `predex_app`

Defined in:
- [`cpp/include/predex/app.hpp`](../cpp/include/predex/app.hpp)
- [`cpp/src/app.cpp`](../cpp/src/app.cpp)

Responsibilities:
- construct the runtime object graph
- own all queues, workers, and stage instances
- spawn and stop threads
- expose a small lifecycle:
  - `start()`
  - `run()`
  - `stop()`

## Thread Topology

The runtime is built around four classes of worker:

1. IO thread
   - owns websocket receive
   - calls `IOWriter::on_wire_message(...)`
   - drains recycled frame handles

2. Router thread
   - drains the IO-to-router queue
   - classifies messages
   - forwards to shard queues or directly to the logger

3. N shard threads
   - each shard owns one input queue
   - parse frame payloads into `NormalizedEvent`
   - apply to a shard-local `BookStore`
   - forward processed frame handles to the logger

4. Logger thread
   - drains router and shard logger queues
   - writes length-prefixed payloads to tape
   - recycles frame handles back to ingest

## Queue Graph

```text
IO thread
  -> io_to_router_queue
Router thread
  -> router_to_logger_queue
  -> shard_input_queue[i]
Shard thread i
  -> shard_to_logger_queue[i]
Logger thread
  -> recycle_queue
IO thread
  -> drain recycle_queue
```

All queues in the current design are SPSC queues. Multi-source fan-in is handled by the logger thread polling multiple SPSC inputs.

## Frame Lifecycle

The hot-path object moving between stages is `predex::core::ingest::kalshi::FrameHandle`.

The lifecycle is:

1. `IOWriter` acquires a slot from `FramePool`
2. raw websocket bytes are copied into the corresponding `KalshiFrame`
3. the handle is pushed to the router
4. Router either:
   - forwards the handle to a shard, or
   - forwards the handle directly to the logger
5. Shard parses/applies and forwards the same handle to the logger
6. Logger writes the payload and pushes the handle to the recycle queue
7. `IOWriter` drains recycle handles and returns them to `FramePool`

The payload itself is not copied between internal stages after the initial IO copy into the frame pool.

## Routing Model

The router does three things:

1. classify the message
   - shard-bound market data
   - direct-to-logger control plane messages
   - drop if unsupported or invalid

2. route market messages through `MarketRegistry`
   - set `market_id`
   - set `affinity_key`

3. enforce sequence checks where applicable

The shard choice is derived from the configured affinity key.

## Parser And Book Model

The current parser interface:
- takes a `FrameHandle`
- takes the corresponding `KalshiFrame`
- returns `ParseResult<NormalizedEvent>`

The shard applies parser output into a shard-local `BookStore`, which owns:
- per-market bid/ask levels
- optional sequence state
- pending delta buffering
- trade metadata

## Tape Model

The logger is the terminal sink for raw feed capture.

Current tape format:
- `u32` little-endian payload length
- raw websocket payload bytes

This keeps the live runtime simple and makes it easy to build a replay/inspection tool outside the hot path.

## Current Boundaries Versus Planned Work

Current supported boundaries:
- Kalshi websocket ingest
- frame-pool based routing
- shard-local book maintenance
- tape recording

Planned boundaries:
- replay from tape
- strategy and risk hooks per shard
- OMS and order transport
- Python discovery/backtesting toolchain
