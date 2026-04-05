# Performance Backlog

This file tracks performance work that still matters, but is deliberately deferred while the runtime shape settles.

## Principles

- keep the current pipeline correct and observable first
- optimize one bottleneck family at a time
- measure before and after each change
- avoid speculative micro-optimization when the architecture is still moving

## Near-Term Hot Path Work

1. Remove or reduce parser-side payload copies.
   - Current state: inbound payload is copied once into the frame pool, but parser-side work can still allocate or copy more than necessary.
   - Goal: keep parsing as close to frame-pool-backed memory as possible.

2. Reuse parser state per thread.
   - Current state: parser construction and transient parse state may still be more expensive than necessary.
   - Goal: make parser state thread-owned and reusable.

3. Revisit `FramePool` slot lifecycle overhead.
   - Current state: the pool is correct and centralized around `IOWriter`, but we have not tuned it for maximum throughput yet.
   - Goal: preserve safety while reducing lifecycle overhead where profiling shows it matters.

4. Make idle behavior configurable.
   - Current state: router, shard, and logger loops use `yield()` when idle.
   - Goal: support policy choices like sleep, yield, spin, or spin-then-yield depending on deployment environment.

## Routing And Sharding Work

1. Validate the classification fast path.
   - Goal: keep control-plane messages off shard threads as cheaply as possible.

2. Reduce per-message dynamic allocation in routing and parse output.
   - Goal: avoid accidental allocator pressure in steady-state market-data flow.

3. Revisit shard-affinity policy once discovery/config tooling exists.
   - Goal: make shard assignment intentional and stable under larger market sets.

## Logging And Tape Work

1. Add rotation and retention policy around tape files.
   - Current state: one output path, no runtime rotation policy.

2. Add lightweight tape inspection and replay tools.
   - Current state: tape format is simple, but first-party tooling is still missing.

3. Measure logger throughput under sustained shard fan-in.
   - Goal: understand when one logger thread remains sufficient and when partitioned logging becomes necessary.

## Future Runtime Work

These are real future items, but they are not implemented today:

1. Strategy and risk hooks colocated with shards
2. OMS and order transport
3. replay executable
4. Python discovery, config synthesis, and backtesting tooling

## Benchmark Discipline

For each serious performance PR:

1. capture baseline throughput and latency
2. change one subsystem at a time
3. rerun the same workload
4. keep correctness checks green
