# Cleanup / Bug Tracker

Running list of known issues, smells, and half-finished work surfaced during reviews. Each entry is self-contained so it can be picked up in any order.

Format: `### <short title>` → **Where / Impact / Fix sketch / Status**

---

### OMS telemetry fields read from main thread without synchronization

- **Where**: `cpp/include/predex/oms/oms.hpp:112` (`session_net_ticks_` plain `std::int64_t`), `cpp/src/oms/oms.cpp:202-204` (getter), `cpp/src/oms/oms.cpp:261,263` (mutated on fill); `cpp/include/predex/oms/order_store.hpp:52` (`live_order_count()`), `cpp/src/oms/order_store.cpp:207-209` (reads `std::unordered_map::size()`); consumed by main thread at `cpp/src/app.cpp:1336-1337`.
- **Impact**: Data race between main (`print_health_status`) and OMS worker thread. Scalar reads are likely fine on x86 (aligned 64-bit loads don't tear) but are UB per the C++ memory model. `unordered_map::size()` during concurrent insert/rehash is genuinely dangerous — can return garbage or touch a mid-rehash internal structure.
- **Fix sketch**: Make scalar counters `std::atomic<std::int64_t>` with relaxed ordering; for `live_order_count` either (a) maintain an `std::atomic<std::size_t>` alongside map mutations, or (b) snapshot into a plain struct on a periodic tick owned by the OMS thread and have main read the snapshot.
- **Status**: open.

---

### Router seq-gap should trip explicit soft-halt, not rely on implicit freeze

- **Where**: `cpp/src/router/router.cpp:135-151` (`check_sequence`); classify consults it at `:112-114` for non-lifecycle frames; reset only called from `cpp/src/app.cpp:664` on WS reconnect.
- **Impact**: `check_sequence` only advances `last_seq_by_sid_[sid]` on the strict `seq == last+1` path. Any gap (dropped frame upstream, IOWriter backpressure, router queue full at IOWriter) returns false without advancing, so every subsequent frame on that sid routes `kToLogger` and never reaches the shard. **Kalshi uses one sid per subscription, so all `orderbook_delta`/`orderbook_snapshot` traffic shares a single sid — a gap there freezes the entire book layer app-wide, not just one market.** Today this is *implicitly* safe because strategies only fire from `AppliedEventUpdate`, which requires the book to apply, and no applies means no strategy invocations. But that invariant is load-bearing and fragile: any future code path that drives strategies from a timer, a trade-sid update, or a lifecycle event would silently run against a frozen book. Additionally even the "safe" implicit behavior means the app sits silently doing nothing, which is worse than explicitly halting with a known cause.
- **Fix sketch**: On `check_sequence` returning false, Router should trip **soft-halt** (no cancel of resting orders — we've just lost the ability to re-price, but existing orders are still valid trades). Concretely: give Router a way to signal halt (either a pointer to the OMS `halt_mode_` atomic it already has, or a dedicated halt-signal SPSC the main/oms loop observes), and flip it to kSoft on any gap. Operator choice at that point: (a) terminate the process, or (b) keep running so the logger continues to tape frames for post-mortem. Either way, new intents stop flowing. Out of scope here: hard-halt / cancel-all, which depends on REST and isn't the right match for a market-data desync.
- **Status**: open. Reframe from "add desynced flag" to "explicit soft-halt trip" per discussion — make the fail path concrete instead of `should`-based.

---

### Frame pool bleeds on triple-backpressure (shard + logger + recycle all full)

- **Where**: `cpp/src/router/router.cpp:185,194,200` (drop paths `(void)recycle_queue_.try_push(handle)`).
- **Impact**: If the recycle SPSC itself is full when Router needs to drop a frame, the handle is discarded without recycling — the underlying pool slot is leaked for the life of the process. Requires shard, logger, AND recycle all full simultaneously, so it's a cascading-failure scenario. Given pool size is finite, repeated triple-stalls eventually starve IOWriter.
- **Fix sketch**: Router is the single producer into the recycle queue on its side, so as a last resort it can call `frame_pool_.recycle(handle)` directly (Router is on a different thread than IOWriter, which is the pool's consumer-side user — need to confirm pool is safe for multi-thread recycle, otherwise a small bounded retry on the SPSC is enough).
- **Status**: open. Low-likelihood but unbounded-impact.

---

### IOWriter atomic telemetry counters default to seq_cst on hot path

- **Where**: `cpp/src/ingest/io_writer.cpp:14,16,25,38,41,48,50` — every `++received_count_` / `++dropped_count_` / `++oversized_count_` / `++recycle_failed_count_`.
- **Impact**: `std::atomic<T>::operator++` defaults to `memory_order_seq_cst` which inserts full memory barriers. These are pure telemetry counters with no ordering dependency on surrounding state; relaxed ordering is correct and avoids the per-frame barrier. Small nit but directly on the ingest hot path.
- **Fix sketch**: Replace `++counter_` with `counter_.fetch_add(1, std::memory_order_relaxed)`.
- **Status**: open. Perf polish.

---

### BookState dense array is ~160× oversized for current market classes

- **Where**: `cpp/include/predex/shards/book_store.hpp:15` (`kMaxPriceTicks = 1000`), `:17-18` (BidLevels/AskLevels use `internal::QtyLots` = `std::int64_t`), `:25-26` (two full-range arrays per book), `:84` (`unordered_map<MarketId, BookState> books_`).
- **Impact**: Each side is `1001 × 8B ≈ 8KB`, each book `~16KB`. At 1000 markets that's `~16MB` of book state — materially better than the prior `~160MB`, but still sparse for binary markets and still large enough to pressure caches once you multiply by active events and ancillary state. Compounded by `std::unordered_map<MarketId, BookState>` scattering those blocks across the heap as separately-allocated nodes — no spatial locality between sibling markets on the same event.
- **Fix sketch**: Template the tick range: `BookState<TickRange>` and `BookStore<TickRange>`, default `<1000>` for the current interim market class. If/when finer-grained markets come back into scope, route them separately with a wider specialization instead of paying that memory cost everywhere. Also switch `QtyLots` storage in book arrays to `std::uint32_t` (2× additional shrink; Kalshi per-level qty fits comfortably in 32 bits). Add cached `best_bid_tick` / `best_ask_tick` fields on BookState so ToB queries don't scan the dense array.
- **Status**: open. High-leverage — touches hot path cache behavior, blocks naturally on the tick-range template refactor.

---

### BookState ancillary allocation patterns

- **Where**: `cpp/include/predex/shards/book_store.hpp:27` (`std::deque<NormalizedEvent> pending_deltas`), `:84` (`unordered_map<MarketId, BookState> books_`).
- **Impact**: `std::deque` chunks on the heap; bounded at 512 but each push/pop may touch a different chunk and NormalizedEvent copies aren't trivial. `unordered_map` stores BookState nodes as separate heap allocations, so even after the tick-range shrink, sibling market books won't sit contiguously — iteration is pointer-chasing. Cache-hostile for any shard-wide sweep (telemetry, reset, re-snapshot on reconnect).
- **Fix sketch**: Replace `pending_deltas` with a fixed-size ring buffer sized to `kMaxPendingDeltas` — contiguous and allocation-free. Since markets are enumerable from MarketRegistry at startup, replace the map with `std::vector<BookState>` indexed by a compact per-shard market index (registry hands out the index alongside the route). Keeps all books for a shard contiguous.
- **Status**: open. Pairs naturally with the tick-range template refactor (same file, same cache story).

---

### Shard reset resets books but not pipeline state — verify intent

- **Where**: `cpp/include/predex/shards/shard.hpp:55-57` — `reset_requested_.exchange(false)` triggers only `event_store_.reset_all_books()`, leaving `bundle_`'s `tracked_intents_`, `local_intent_id_by_request_id_`, and `risk_state_` untouched.
- **Impact**: Probably intentional — on a market-data WS reconnect we shouldn't drop tracking of orders we've actually submitted, since they're real on Kalshi's side regardless of our WS session. But the asymmetry (wipe book state, keep OMS-side state) is load-bearing and undocumented. Future refactor could accidentally "unify" the reset and cause real in-flight orders to be forgotten.
- **Fix sketch**: Add a comment on `request_reset()` explaining the asymmetry. Optional: pull the book reset into a differently-named method (e.g. `request_book_resync`) so the name reflects that OMS tracking is intentionally preserved.
- **Status**: open. Documentation / clarity only.

---

## Phase 2: performance tuning & latency instrumentation

Deferred until E2E live-trading is working. Sequencing: land the pipeline, prove it runs, capture a clean baseline, then attack latency incrementally — so every optimization has a before/after number attached rather than being a stab in the dark.

Current baseline (measured via Python replay over `audit.jsonl`): **p50 ingest→strategy-return ≈ 75μs; p99 into single-digit ms.** The spread is the story — 100× between p50 and p99 is almost always one of a few known causes.

### Work item: in-process latency histogram + per-stage instrumentation

Graduate measurement from "Python consumer of audit jsonl" (retrospective, I/O- and JSON-parse-bound) into an in-process C++ histogram with sub-μs recording overhead. Python tool stays useful for post-hoc forensic work; in-process becomes the canonical source of truth for tuning.

- **Shape**: `predex::utils::LatencyHistogram` with pre-allocated log-scaled buckets (e.g. 100ns–100ms). `record(ns)` does a `fetch_add` on the right bucket, no allocation, no I/O. `dump()` computes percentiles on demand. One histogram per stage, per thread (or shard) to avoid contention.
- **Per-stage timestamps**: `rdtsc` (or `QueryPerformanceCounter` on Windows, `clock_gettime(CLOCK_MONOTONIC)` on Linux) at stage boundaries — `tick_recv`, `parse_done`, `book_apply_done`, `strategy_eval_done`, `intent_enqueue`, `transport_submit`, `first_fill_recv`. Latency fields already thread through `IntentOrigin` / `OrderState`; this just routes them into the histogram instead of only audit events.
- **Why it matters for the project framing**: answers "which stage stalled?" on a p99 outlier instead of "the whole pipeline was slow." Single biggest artifact to put in front of a reviewer.

### Tail-driver suspects (bet-ordered, to attack after baseline)

1. **Scheduler preemption on Windows**. No thread pinning, default 15.6ms timer tick. Histogram bumps near ~1ms and ~15ms are the fingerprint. Mitigated by moving hot threads to Linux with `isolcpus` + `SCHED_FIFO` (`chrt -f`). Operational fix, typically an order-of-magnitude p99 reduction. The highest-leverage tail fix in this codebase.
2. **Hot-path allocations**. `beast::buffers_to_string()` on every WS recv (`cpp/src/websocket/client.cpp:312`); `std::string` in `ClientOrderId` / `OrderFill::raw_action` / `OrderFill::raw_side`; `std::deque` chunk allocs in `BookState::pending_deltas`; `std::unordered_map` rehashes (tracked_intents_, market_index_by_id, net_position_lots_by_market, last_seq_by_sid_). Each is a candidate for 100μs–ms stalls when the allocator hits a slow path or a rehash triggers.
3. **Cold-book DRAM + TLB miss cascade**. 160KB-per-book × 1000 markets far exceeds L3; first delta on an idle market pulls dozens of cache lines from DRAM plus a TLB walk. Collapses naturally when the tick-range template refactor (see "BookState dense array" finding above) lands — ~1KB/book fits entire working set in L2.
4. **Audit / logger backpressure**. `fprintf(stderr, ...)` sprinkled in unusual paths (e.g. `cpp/src/oms/oms.cpp:286-293` on unknown fill side). These are synchronous and, under actual log volume, can block the hot thread. Consider gating all stderr prints through the logger thread or behind a compile-time flag.
5. **false sharing on adjacent atomics / SPSC head/tail**. Auditable once the histogram is in place — if a particular stage's p99 is stable in isolation but inflates under load, that's a false-sharing tell. Fix with `alignas(std::hardware_destructive_interference_size)` on the relevant fields.

### Measurement hygiene

- Lock baseline runs to a quiet machine config (no background builds, browser closed, etc.) before attributing results.
- Capture the same tape of WS frames once and replay through the in-process pipeline repeatedly for stable numbers — the deterministic replay harness under `logs/replay/` is the right driver.
- Record not just percentiles but full histograms; the *shape* of the tail (long-smooth vs bimodal) tells you which category of cause is responsible.
- Before/after numbers on every optimization. One-line changelog entries per tuning pass build the interview story as a side effect.

