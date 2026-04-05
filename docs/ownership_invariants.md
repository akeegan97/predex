# Ownership And Lifetime Invariants

This document captures the operational rules that keep the current runtime coherent.

## Runtime Ownership

`predex::App::Runtime` owns:
- websocket transport/session objects
- the frame pool
- all queues
- router, shards, and logger
- worker threads

Other components mostly hold references or non-owning pointers into runtime-owned state.

## Thread Ownership

The runtime is designed around single-owner stage execution:

- websocket transport/session is owned by the IO thread during steady-state operation
- router is owned by the router thread
- shard `i` is owned by shard thread `i`
- logger is owned by the logger thread
- each `BookStore` is shard-local and only written by its shard thread

This is the core concurrency assumption behind the current design.

## Queue Invariants

Each queue has exactly one producer and one consumer:

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

The logger is the only stage that polls multiple input queues, but each individual input queue remains SPSC.

## Frame Pool Invariants

`FramePool` holds the backing storage for all live frames.

The rules are:
- `IOWriter` is the only stage that acquires fresh frame slots
- `IOWriter` is also the only stage that calls `FramePool::recycle(...)`
- Router, shards, and logger only read frame contents through a handle
- a `FrameHandle` remains valid only while its generation matches the pool slot generation

This is the current ownership model that keeps pool mutation centralized.

## Handle Lifecycle Invariants

For each inbound websocket message:

1. a frame slot is acquired
2. the payload is copied once into the pool
3. the same handle moves through router, shard or logger, and tape logger
4. the logger must eventually return the handle to the recycle queue
5. the IO thread must eventually recycle the handle back into the frame pool

The logger should recycle even after write failure so the pool does not silently lose capacity.

## Message Classification Invariants

The router classifies frames into one of three buckets:
- shard-bound market data
- logger-only control-plane data
- drop

Examples:
- `trade`, `orderbook_delta`, and snapshots are shard-bound
- `subscribed` acknowledgements are logger-only

This distinction matters because a logger-only frame should not force shard work.

## Shard Invariants

Each shard owns:
- one input queue
- one logger output queue
- one parser instance
- one `BookStore`
- one optional event-handler pointer

The shard does not own the frame pool or the logger queue backing storage.

The shard is responsible for:
- reading the frame referenced by a handle
- parsing it into `NormalizedEvent`
- applying the event to its `BookStore`
- forwarding the handle to the logger afterward

## Logger Invariants

The logger is the terminal sink for raw payload persistence.

It is responsible for:
- polling router and shard logger inputs
- writing length-prefixed payloads to tape
- forwarding handles to the recycle queue

The logger should not retain ownership of a handle after it finishes with the corresponding frame.

## Error And Shutdown Invariants

Runtime shutdown is driven by the shared `running` flag plus thread stop requests.

The intended behavior is:
- a fatal worker error stores an error message and flips `running` false
- `App::run()` observes `running == false` and exits
- `stop()` requests stop on all worker threads and joins them
- the IO loop closes the websocket session during its own cleanup path

The runtime should not rely on another thread racing the websocket transport in order to stop it.

## Documentation Rule

When the implementation changes, update this file and:
- [Architecture](architecture.md)
- [Data Contract](data_contract.md)

These three docs are meant to stay aligned with the real runtime, not an aspirational design.
