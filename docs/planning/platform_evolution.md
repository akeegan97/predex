# Platform Evolution: Session Runner → Always-On Service

This is the planning doc for evolving Predex from a discrete-session trading runner into an always-on service. It captures the target architecture, the phased work to get there, and the load-bearing design decisions made along the way.

Format: phased plan. Each phase is intentionally PR-sized rather than epic-sized so progress is measurable and partial landings are useful on their own.

This is forward-looking. None of what's described below is implemented today unless explicitly noted.

---

## Target Shape

The always-on service is a long-lived process that:

- runs **continuously** across days and weeks rather than terminating at end-of-session
- regenerates its trading universe via a **synthetic-day refresh** rather than full restart
- **persists state** across crashes / planned restarts / venue disconnects without losing track of resting orders or open positions
- exposes a **control surface** (HTTP) for operator-driven actions: pause, refresh, kill switch, status
- enforces **clean lifecycle modes** so tear-down behavior is deterministic and matches operator intent

Today's shape is the opposite of all of these: discrete sessions, regenerate-via-restart, no state persistence, no control surface, and a tear-down path that doesn't yet do everything the docs claim. The phasing below closes those gaps in dependency order.

---

## Load-Bearing Design Decisions

These are not negotiable inputs to the phasing; they're the answers we've already settled on after design conversations and are recorded here so they don't get relitigated mid-implementation.

### Synthetic-day refresh, not continuous-across-days

At refresh time, the system **flattens venue exposure** (cancel-all, verify flat), then swaps the universe config, then resumes. The alternative — "let orders ride across refresh" — was rejected because it produces an indefinite-duration window where the system has no live observation of resting orders. Flatten-and-reload provides a deterministic clean boundary, which dramatically simplifies persistence design.

The cost ("continuous market making across midnight rollover") is largely theoretical: today's strategies are IOC-only and rest no orders, and even when MM ships the 00:00-01:00 window is generally desirable to skip for capital recycling and fair-value baseline recompute.

### Three configs, three lifecycles

- **Infra config**: shard count, frame pool capacity, queue capacities, idle policy, endpoints, output paths. Read once at startup, immutable for process lifetime. Changes require full restart with hard tear-down.
- **Universe config**: market routes — tickers, market_ids, event_ids, topologies, strike keys. What Python discovery produces today. Swapped at refresh boundaries.
- **Operational state**: `trading_enabled`, per-strategy enables, risk limits, halt mode, OrderStore checkpoint, open positions. Mutates constantly; owned by the running process; persisted via audit-log-replay (not by humans editing JSON).

### Audit-log-replay for persistence, not a database

The audit JSONL already captures every state transition. Adding periodic checkpoint records gives event-sourced persistence with no new runtime dependency, no schema migrations, and no hot-path query cost. SQLite as a read-replica may earn its keep later for ad-hoc operator queries (Grafana, position views), but not before Phase 3.

### `affinity_key` deterministic-derivation is load-bearing

`stable_affinity_key(event_ticker)` is what makes regenerated universes route to the same shards. If this ever changes to content-based or load-balanced affinity, a regenerated universe could move an open order's shard mid-flight. Don't change it without explicit reason and a migration plan.

### `refuse_if_present` philosophy extends to refresh

At every boundary where venue state and our state might diverge (startup reconcile, refresh verify-flat), the policy is: **abort loudly rather than silently absorb the divergence.** Same default as `oms_transport.startup_open_orders_policy=refuse_if_present` today. A single transient venue error blocking trading until an operator intervenes is recoverable; entering a live loop with phantom exposure is not.

---

## Phasing

### Phase 1 — Persistence Foundation

Goal: state is durable across crashes, restarts, and venue disconnects.

**Phase 1a — Audit-log checkpointing.** Add a `kCheckpoint` audit record kind emitted from the OMS pump path every N events or N seconds, carrying serialized OrderStore + GlobalRisk + relevant counters. Self-contained PR; no behavior change beyond writing additional audit lines.

**Phase 1b — Startup recovery.** On startup, find the last checkpoint in the audit log, restore OMS state from it, replay events after it. Same code path runs for both planned restart and crash recovery.

**Phase 1c — Three-way reconcile.** At startup, after audit replay, REST `fetch_open_orders` against the venue and compare against the just-replayed state:
- in replayed ∩ venue → adopt cleanly into OrderStore (wire `Oms::seed_reconciled_order`, finally)
- in replayed − venue → market said order is gone; REST fill-history fetch to recover missed fills (blocks on private-WS being wired, see `open_backlog.md`)
- in venue − replayed → loud abort; operator must reconcile out-of-band

A subtle Phase-4-driven requirement on Phase 1c: if a one-sided arb fill happened pre-crash but the residual handler hadn't acted yet, restoring OrderStore alone isn't sufficient — the residual handler needs to be *told* "you have a stranded position, react." Phase 1c should emit "stranded position discovered" lifecycle events on completion of reconcile so Phase 4b's residual handler picks up unmanaged positions automatically. Designing this in now is cheap; retrofitting later is annoying.

**Phase 1d — Four lifecycle modes.** Make `App::Runtime::stop()` and signal handling explicit about which mode is firing:

| Mode | Trigger | Behavior |
|---|---|---|
| WS reconnect | venue disconnect | reset books, resubscribe, orders preserved at venue (already partly there) |
| Graceful tear-down | SIGTERM, deploy | drain in-flight, checkpoint, exit; don't touch venue orders |
| Hard tear-down | SIGINT, `--emergency-stop` | REST cancel-all with timeout, checkpoint, exit |
| Crash recovery | unexpected exit | next startup runs Phase 1b+1c |

Cancel-all-on-hard-halt is currently described in docs but not wired in code (see `oms_design.md`). This phase closes that gap.

---

### Phase 2 — Config Split and Control Surface

Goal: configs match their actual lifecycles, and operators can drive the system without restarting it.

**Phase 2a — Config split (mechanical).** Three files, three loaders, no functional changes. Infra and universe stay file-based; operational state stays in-memory with a clean serialization boundary so it can be checkpointed alongside the audit log.

**Phase 2b — Universe ↔ operational-state interlock.** Discovery CLI accepts `--current-state <file>` and the regeneration ensures markets with open positions are preserved in the new universe. (Note: with the synthetic-day refresh model from Phase 3 this set is empty at refresh time, but the plumbing must still exist for unhappy-path regeneration scenarios — e.g., post-crash startup with non-empty state.)

**Phase 2c — Control surface (HTTP).** A small HTTP server thread exposing:
- `GET /status` — health summary, current state machine position, counters
- `POST /pause` — transition to soft halt
- `POST /resume` — leave soft halt
- `POST /trading-enabled` — flip the master switch without restart
- `POST /refresh` — trigger synthetic-day refresh (Phase 3 prerequisite)
- `POST /emergency-stop` — hard tear-down

**Phase 2d — Hot-reload operational subset.** SIGHUP rereads the operational config file (risk limits, strategy params, kill switch). Atomic pointer swap to a hot-config struct; cold-config (infra + universe) stays immutable for process lifetime. Unsafe changes (shard count, market routes, frame pool size) reject with a clear error.

---

### Phase 3 — Synthetic-Day Refresh

Goal: regenerate the universe without restarting the process.

The state machine adds a `kRefreshing` state with internal sub-steps. The refresh handler runs the following bounded-timeout sequence (each step has its own timeout; cancel-all timeout is the only real risk surface):

1. Transition to `kRefreshPending`; LocalRiskManager rejects new intents; audit "refresh begin".
2. Drain pending: shard→OMS, OMS→Gateway, Gateway in-flight queues to empty.
3. Cancel-all: REST cancel every order in OrderStore; drain lifecycle queue until `live_order_count == 0`.
4. Verify flat: REST `fetch_open_orders`; non-empty → HARD ABORT (`refuse_if_present` philosophy).
5. Unsubscribe; clear MarketRegistry, EventStores, router sequence state.
6. Regenerate universe config via Python discovery; atomic file swap.
7. Re-init runtime state from new universe (same threads, fresh state); do not restart the process.
8. Subscribe to new channels.
9. Transition to `kRunning`; audit "refresh complete".

**Cancel-all timeout policy: retry-with-backoff, then strict.** N retries on transient venue errors with exponential backoff, then refuse to enter the new universe and hold in `kRefreshFailed` until operator intervention. Lenient mode (carry un-cancellable orders into the new session) is explicitly rejected — defeats the point of flatten-and-reload.

**Trigger: operator-controlled (`POST /refresh`) for v1.** Scheduled cron is deferred to Phase 4 polish. Automated cron introduces a class of "refresh fired at the wrong moment" failure modes that the operator-triggered model doesn't.

---

### Phase 4 — Resting-Order Strategy Landing

Goal: prove the platform with a strategy that actually rests orders across sessions. Pure continuous MM is explicitly out of scope — Kalshi's 10 req/sec REST rate limit makes pure MM uncompetitive on quote freshness. Target strategies are slow-cycle resting orders: soft-monotonic, mean-reversion fading, event-driven CDF arb.

Likely sub-pieces:

- Per-strategy capital allocation (today `GlobalRisk` is a single capital pool)
- Per-strategy adoption policy on startup (resting strategies want `adopt` mode; IOC arb stays `refuse_if_present`)
- Strategy identity encoded into `client_order_id` so adopted orders carry provenance (resolves the deferred gap in `oms_design.md::Startup Reconciliation`)
- Strategy-aware reconcile semantics during Phase 1c three-way compare
- Partial-fill semantics in `apply_venue_event` — current code treats every fill as terminal; GTC resting orders require leaving `kPartiallyFilled` orders working until they fully resolve

This phase is where the deferred startup policy modes (`cancel_all`, `adopt`) finally get wired beyond their current "fail-loud deferred" scaffolding.

**Phase 4b — Residual-fill handler.** A separate but Phase 4-adjacent capability: live-trading data shows one-sided fills (cancel/fill or fill/cancel outcomes from non-atomic batched submits) account for ~10% of signals but ~50% of PnL drawdown, with a microstructural adverse-selection mechanism — the move that breaks the arb leaves you positioned against it. Kalshi has no group-atomic execution, so statistical mitigation (the frontier-qty and edge-cushion knobs in `MonotonicArbStrategy`) plus *behavioral* mitigation (a residual handler) are the two viable defenses.

Architecture (Shape C): per-shard residual handler running on the shard thread (only the shard has book state for price-aware exit timing), emitting `ShardOmsRequest` variants on a dedicated `shard_to_oms_residual_queue[i]` that OMS drains *before* the normal intent queue every pump. Priority separation solves the "exits compete with entries for queue space" concern at the cost of one additional SPSC per shard. Residual policy is per-strategy (arb wants fast exit, soft-arb may want bounded wait).

Load-bearing dependencies:
- `OmsToShardLifecycleEvent` must carry group-level resolution (which legs filled/canceled, with qty and price) so the handler can decide per-group, not just per-order
- Phase 1 (persistence) must surface stranded positions on startup reconcile — after a crash with an unmanaged one-sided fill, the residual handler needs to be told "react to this stranded position" via a lifecycle event, not silently restored to OrderStore and forgotten

Even an imperfect handler caps tail risk: status-quo stranded legs have unbounded loss in principle; handler-managed stranded legs are bounded to "current spread + a few ticks slippage." Projected impact: 50% → 20-30% drawdown contribution from one-sided fills.

---

### Phase 5 — Operator Polish

Parallelizable with later phases. Not blocking, but compounding:

- Prometheus metrics endpoint (alongside the health-line stdout dump)
- Structured JSON logging with levels (alongside current stdout printf logging)
- Liveness / readiness HTTP endpoints suitable for orchestration
- Scheduled cron refresh (now that operator-triggered refresh has been exercised in production)

---

### Phase 6 — Reliability and Deploy

Depends on Phase 1 being solid:

- systemd unit (or equivalent) with restart-on-failure and crash-loop detection
- Watchdog sidecar process for liveness pinging
- Crash-recovery test harness — kill -9 mid-session, restart, verify state recovery
- Containerization for portable deploy
- Optional: SQLite read-replica for external operator queries (if and only if Phase 5 metrics aren't enough)

---

## Sequencing Notes

- **Phase 1 is the keystone.** Everything else depends on persistence existing. Start here.
- **Phase 1a is the smallest viable first PR** — just emit checkpoint records, don't yet consume them. Two-three days. Validates the serialization format before recovery logic depends on it.
- **Phase 2 and Phase 3 can run partially in parallel** — Phase 2a (config split) and Phase 2c (control surface) don't strictly block each other.
- **Phase 4 cannot start before Phase 3 finishes.** Resting orders without refresh = continuous-across-days, which we've explicitly rejected.
- **Phase 5 can land any time after Phase 2c.** Phase 6 should land before Phase 4 goes live with real capital.

## What This Doesn't Cover

Not in scope here, but worth knowing exists:

- Backtesting / strategy replay layer — covered in `replay_matrix.md`.
- Per-stage observability counters for the shard pipeline — the absence of these came up during a recent zero-signals diagnosis; it's a smaller scope than this doc but a real visibility gap (`open_backlog.md` should reference it as a separate item).
- Multi-process / multi-host coordination. Single-process design is sufficient through Phase 6; multi-process is a different epic if and when it earns its keep.

---

## Status

Forward-looking. No phase has started. This doc captures the agreed direction so future work has a stable reference.
