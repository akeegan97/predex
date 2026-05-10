# Open Backlog

Current list of still-open bugs, cleanup items, and performance follow-ups that
remain relevant after the `feat/live_oms` work landed on `main`.

This file is intentionally focused on **current** work. Historical branch-era
transport/gateway planning has been removed now that that architecture exists in
the repo.

Format: `### <title>` → **Where / Impact / Next step / Status**

---

## Correctness / Resilience

### Router sequence-gap should trip an explicit soft halt

- **Where**: `cpp/src/router/router.cpp` sequence checking and reconnect/reset
  path in `cpp/src/app.cpp`
- **Impact**: a market-data sequence gap currently freezes shard-side book
  application implicitly instead of transitioning the runtime into an explicit
  degraded mode. That is safer than trading on bad state, but too silent and too
  load-bearing on current strategy triggering behavior.
- **Next step**: have Router signal a soft halt when a sequence gap is detected
  so new intent generation stops explicitly while logging/tape capture can
  continue for postmortem analysis.
- **Status**: open

### Frame pool handle can leak under triple-backpressure

- **Where**: `cpp/src/router/router.cpp` recycle fallback path
- **Impact**: if shard, logger, and recycle queues are all backpressured at once
  and the recycle queue also rejects the handle, the pool slot can be lost for
  the lifetime of the process.
- **Next step**: add a safe last-resort recycle path or a bounded retry so drop
  handling cannot permanently bleed pool capacity.
- **Status**: open

### Fills during private OMS WS outage are still not fully recovered

- **Where**: `reconcile_open_orders_from_rest(is_startup=false)` path in
  `cpp/src/app.cpp`
- **Impact**: current reconnect repair can re-adopt open orders, but completed
  fills that happened while the private WS was down are still a blind spot
  because the open-orders snapshot only reflects remaining working quantity.
- **Next step**: add post-reconnect fill-history recovery and apply missing fill
  updates into session PnL / state.
- **Status**: open

---

## Performance / Latency

### Switch hot-path telemetry increments to relaxed atomics

- **Where**: `cpp/src/ingest/io_writer.cpp`
- **Impact**: some per-frame telemetry increments still use the default
  `seq_cst` atomic operators when relaxed ordering is sufficient.
- **Next step**: replace `++atomic_counter` style hot-path increments with
  `fetch_add(1, std::memory_order_relaxed)`.
- **Status**: open

### Revisit `FramePool` lifecycle overhead with measurement

- **Where**: `cpp/include/predex/ingest/frame_pool.hpp`,
  `cpp/src/ingest/frame_pool.cpp`, surrounding ingest/router/logger flow
- **Impact**: the pool model is correct and centralized, but not yet tuned as
  aggressively as the rest of the pipeline.
- **Next step**: profile the remaining cost of acquire/recycle and only optimize
  after measuring that it matters relative to parser, routing, and OMS work.
- **Status**: open

### Reuse parser state per thread where it pays off

- **Where**: parser construction / per-message transient state
- **Impact**: parser-local scratch/state may still be more expensive than
  necessary under sustained load.
- **Next step**: evaluate thread-owned reusable parser state after capturing a
  stable baseline.
- **Status**: open

### Validate and tighten routing/shard allocation behavior

- **Where**: router, shard dispatch, parse output shaping
- **Impact**: control-plane classification and routing are materially cleaner
  now, but still worth checking for accidental per-message dynamic allocation or
  avoidable churn on the steady-state path.
- **Next step**: profile routing / parse output under replay and trim remaining
  allocator pressure where it shows up.
- **Status**: open

### Book-state memory layout still has room to improve

- **Where**: `cpp/include/predex/shards/book_store.hpp`
- **Impact**: the dense book representation is much smaller than before, but it
  still leaves performance on the table via wide fixed arrays, queueing of
  pending deltas, and heap-scattered per-market book storage.
- **Next step**:
  - consider narrower qty storage where safe
  - add cached best-level indices
  - consider fixed-size pending-delta rings
  - consider more contiguous book storage if market indexing is stable enough
- **Status**: open

### Add in-process latency histograms for stage-level tuning

- **Where**: runtime instrumentation, likely alongside existing audit latency
  fields
- **Impact**: Python replay and audit summaries are good for postmortems, but
  they are not the lowest-overhead source of truth for tuning p99 behavior.
- **Next step**: add a lightweight in-process histogram utility for stage-level
  latency recording and keep replay analysis as the offline forensic layer.
- **Status**: open

---

## Operational / Observability

### Soft-halt events should be more visible

- **Where**: OMS halt path
- **Impact**: the drawdown-triggered soft halt exists, but operator-facing
  visibility can still be clearer.
- **Next step**: emit a stronger audit/log signal when the runtime transitions
  into soft halt so operators do not have to infer it indirectly.
- **Status**: open

### Tape rotation / retention policy is still ad hoc

- **Where**: live artifact output under `logs/live/`
- **Impact**: the runtime now writes to a better location, but long-running
  operation still lacks a formal rotation and retention policy.
- **Next step**: define rotation behavior and retention defaults for tape, audit,
  and trace outputs.
- **Status**: open

### Measure logger throughput under sustained fan-in

- **Where**: tape/logger path
- **Impact**: one logger thread is currently the right simplicity/performance
  tradeoff, but its throughput ceiling under heavier shard fan-in is still not
  characterized.
- **Next step**: benchmark sustained logging load and only split logging if data
  shows it is needed.
- **Status**: open

---

## Documentation / Clarity

### Document shard reset asymmetry more explicitly

- **Where**: shard reset / book-resync path in `cpp/include/predex/shards/shard.hpp`
- **Impact**: the current behavior appears intentional: book state is reset on
  public-feed reconnect while OMS tracking and strategy/intent state are not.
  That asymmetry is important and easy to accidentally “simplify” later.
- **Next step**: add clearer documentation or naming so future changes do not
  collapse book resync into a broader state reset by accident.
- **Status**: open

---

## Working Principles

For serious performance or cleanup work:

1. Measure before and after.
2. Change one subsystem at a time.
3. Prefer correctness and observability over speculative micro-optimization.
4. Keep replay/audit evidence attached to meaningful runtime changes where
   possible.
