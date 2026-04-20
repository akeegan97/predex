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
   - Current state: router, shard, OMS, logger, and audit loops use a spin-then-yield-then-sleep idle policy with configurable thresholds.
   - Goal: expose per-thread policy tuning in the trader config for deployment-specific environments.

## Routing And Sharding Work

1. Validate the classification fast path.
   - Goal: keep control-plane messages off shard threads as cheaply as possible.

2. Reduce per-message dynamic allocation in routing and parse output.
   - Goal: avoid accidental allocator pressure in steady-state market-data flow.

## OMS Work

1. REST rate limiting.
   - Current state: no throttle on submit/cancel/modify calls to `oms_rest_client`.
   - Goal: add a per-second rate limiter so a burst of strategy signals does not blow through exchange rate limits.

2. Soft halt logging.
   - Current state: `request_soft_halt()` fires silently.
   - Goal: emit an audit event and a stderr line when the drawdown circuit breaker trips so operators can observe the trigger.

3. Fills during OMS WS outage.
   - Current state: fills that arrive on the exchange while the private WS is down are not recovered in `reconcile_open_orders_from_rest(is_startup=false)` because the REST snapshot only reflects remaining open qty, not completed fills.
   - Goal: query fill history after reconnect and apply missing fills to `session_net_ticks_`. Deferred as a rare edge case.

## Logging And Tape Work

1. Add rotation and retention policy around tape files.
   - Current state: one output path, no runtime rotation policy.

2. Add lightweight tape inspection and replay tools.
   - Current state: tape format is simple; Python tooling for decode and replay is planned.

3. Measure logger throughput under sustained shard fan-in.
   - Goal: understand when one logger thread remains sufficient and when partitioned logging becomes necessary.

## Replay Tooling

1. Build a Python tape decoder.
2. Add a Python replay driver that can emit JSONL or feed synthetic consumers.
3. Introduce a dedicated replay executable if the C++ side needs one.

## Benchmark Discipline

For each serious performance PR:

1. Capture baseline throughput and latency.
2. Change one subsystem at a time.
3. Rerun the same workload.
4. Keep correctness checks green.
