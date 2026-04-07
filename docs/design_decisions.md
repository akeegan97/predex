# Design Decisions

This document records deliberate architectural choices and the reasoning behind them. The intent is to answer "why this way and not that way" for decisions that are not self-evident from reading the code.

## SPSC Queues Between Every Stage

Each stage boundary uses a single-producer single-consumer lock-free queue rather than a mutex-protected shared queue or a general-purpose MPSC queue.

The SPSC constraint is strict: exactly one thread writes and one thread reads each queue during steady-state operation. Given that invariant, SPSC queues require no compare-and-swap, no contention, and no memory barriers beyond the acquire/release pair at head and tail. A mutex adds at minimum a kernel syscall on contention and a cache-line ownership transfer on every acquire. A general-purpose concurrent queue adds overhead even when there is provably only one producer and one consumer.

The queue topology in `ownership_invariants.md` is designed so the SPSC invariant is always satisfied. Adding a second producer to any queue would break a design assumption, not just a performance assumption.

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

The intended invariant: all sub-markets of the same Kalshi event should share an affinity key derived from the event ticker, not from the individual market ticker. The current default (affinity by market index) is a placeholder that works for single-market configurations but should be replaced with event-level grouping as the strategy layer develops.
