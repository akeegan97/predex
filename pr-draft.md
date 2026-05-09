# PR Draft: Promote `feat/live_oms` to `main`

## Summary

This PR promotes the `feat/live_oms` branch to `main` as the new canonical codebase.

The branch replaces the older synchronous / partial OMS pathway with the current live OMS runtime, async REST execution pool, replay and soak-analysis tooling, and the newer monotonic-arbitrage safety instrumentation that came out of recent live-trading investigation.

This is a large integration PR. It is best reviewed as a branch transition rather than as a narrow feature diff.

## What Changed

### OMS / execution runtime

- Added the async OMS gateway stack:
  - `CommandIngress`
  - `OrderSequencer`
  - `BatchPlanner`
  - `RateLimiter`
  - `SessionPool`
  - `AsyncRestConnection`
- Added persistent async REST execution via:
  - `KalshiRestAdapter`
  - `PersistentHttpSession`
  - `KalshiPrivateWsAdapter`
- Reworked the trader app wiring around the new OMS runtime and transport flow.
- Added clearer separation between intake, sequencing, dispatch, transport, completion, and recovery.

### Strategy / shard safety work

- Extended monotonic arb order selection to use more conservative paired-leg execution logic.
- Added book-quality gating for monotonic arb:
  - top-gap continuity checks
  - near-top multilevel support checks
  - paired frontier support tracking
- Added audit telemetry for:
  - `reference_depth_levels`
  - `aux_reference_depth_levels`
  - `reference_depth_qty_lots`
  - `aux_reference_depth_qty_lots`
  - `paired_frontier_qty_lots`
- Refactored hot-path candidate selection to avoid `std::vector` allocation in the strategy path.

### Replay / soak analysis / Python tooling

- Added replay app and supporting replay modules for:
  - ingest normalization
  - latency review
  - soak summaries
  - monotonic signal verification
  - time-window analysis
- Expanded Python discovery/config generation tooling.
- Fixed numeric subchain handling so valid monotonic subchains are preserved while singleton leftovers are dropped instead of merged into invalid chains.

### Observability / artifacts / operations

- Expanded audit and transport timing output.
- Added richer REST trace output for async execution.
- Moved default generated runtime artifacts out of repo root and into `logs/live/`.
- Added `IngestedRun` support and better run metadata handling.

### Docs / repo cleanup

- Reworked `README.md` to reflect the current architecture and operational posture.
- Added `docs/README.md` as a docs index.
- Added `docs/results.md` for lightweight runtime/latency reporting.
- Reorganized planning notes under `docs/planning/`.
- Updated config examples and docs to match the current runtime layout.

### Code quality / CI hygiene

- Ran repo-wide `clang-format` to bring C++ sources in line with CI formatting checks.
- Burned down current `clang-tidy` findings on the active code path.
- Relaxed one low-signal tidy rule:
  - removed `readability-convert-member-functions-to-static`
- Cleaned up several moved-from / optional access / nested conditional / ownership-contract issues uncovered during tidy.
- Converted OMS client/exchange order IDs to fixed-capacity value types instead of heap-backed `std::string` storage.

## Why

`main` is currently behind the implementation that is actually being used and iterated on.

This branch contains:

- the current live OMS runtime
- the replay/soak review toolchain used to analyze behavior
- the current monotonic arb safety instrumentation
- the updated docs and repo layout that match how the project is actually operated

Keeping that work on a long-lived side branch now creates more confusion than safety.

## Validation

Validated locally:

- `cmake --build --preset build-perf --target trader_app`
- repo C++ formatting sweep completed and the `clang-format --dry-run --Werror` pass is clean
- `clang-tidy` cleanup completed for the current branch state
- Python discovery tests pass:
  - `python3 -m unittest python.tests.test_discovery`

Recent live soak / replay work on this branch also informed the monotonic-arb safety changes and the added audit telemetry, though those results are documented separately in repo artifacts and docs rather than treated as CI evidence.

## Review Notes

- This PR is large. Recommended review order:
  1. `README.md`
  2. `docs/README.md`
  3. OMS gateway / transport architecture:
     - `cpp/include/predex/oms/gateway/*`
     - `cpp/src/oms/gateway/async_rest_connection.cpp`
     - `cpp/src/oms/transport/*`
  4. shard / monotonic arb updates:
     - `cpp/include/predex/shards/shard_pipeline.hpp`
     - `cpp/include/predex/shards/strategies/monotonic_arb.hpp`
  5. replay / Python tooling

- The diff includes both architectural work and repo hygiene work. Some files are noisy because of the repo-wide format normalization.

## Known Follow-Ups

- Revisit websocket read-error reconnect handling for transient disconnects such as `connection reset by peer`.
- Use the new depth telemetry to make a deliberate decision on dynamic sizing.
- Continue post-merge cleanup / polish now that the branch can become trunk.

## Merge Intent

This PR is intended to make `main` reflect the current working system, not just cherry-pick one feature.

After merge, future work should branch from `main` rather than continue treating `feat/live_oms` as the long-lived source of truth.
