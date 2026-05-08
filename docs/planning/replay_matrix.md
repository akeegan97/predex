# Replay Roadmap

Replay tooling now has the core readers and analyzers in Python, but it is still awkward because the workflows are command-centric instead of dataset-centric. The next step should be a first-class ingest layer that turns one live run into a reusable analysis dataset for research, backtesting, and postmortems.

## What Exists Today

- Live `trader_app` writes raw inbound websocket payloads to a binary tape.
- The tape format is stable and versioned. Current v2 records are:

```text
magic=PDT2 | version=u16 | flags=u16 | repeated[recv_ts_ns=u64, payload_len=u32, payload bytes]
```

- Python replay code already exists under `python/src/predex/replay/`:
  - `tape.py` decodes binary tape records into normalized market events
  - `audit.py` loads audit JSONL and bundles per-signal lifecycle state
  - `timeline.py` / `windows.py` export event timelines and opportunity windows
  - `latency.py` / `soak.py` summarize latency and run-level execution outcomes
  - `dashboard.py` visualizes generated replay artifacts
- Python discovery and config generation tooling exists under `python/src/predex/discovery/`.

## Current Gap

The missing piece is not another narrow analyzer. The missing piece is a canonical run dataset.

Today most replay commands do some version of this:

1. Re-read raw tape
2. Re-read full audit JSONL
3. Reconstruct a task-specific in-memory view
4. Export one-off CSV/HTML/JSON artifacts

That makes the tooling slower than it should be, harder to compose, and awkward for anything beyond verification or one-off forensic analysis.

## Recommended Direction

Build the Python tooling around a one-time ingest step that materializes a run into a columnar dataset with a small manifest.

Suggested command shape:

1. `predex-replay ingest-run`
2. `predex-replay analyze-run`
3. `predex-replay backtest-run`
4. `predex-replay dashboard`

The first command should do the expensive work once. Everything else should read prebuilt artifacts.

## Canonical Run Dataset

Suggested output layout:

```text
runs/
  <run_id>/
    manifest.json
    config.snapshot.json
    frames.parquet
    market_events.parquet
    audit_events.parquet
    signals.parquet
    orders.parquet
    fills.parquet
    windows.parquet
    latencies.parquet
```

Suggested table purposes:

- `frames.parquet`: raw frame metadata, sequence numbers, recv timestamps, payload type, optional raw payload bytes or offsets
- `market_events.parquet`: normalized order book snapshots, deltas, and trades keyed by market and sequence
- `audit_events.parquet`: flattened audit rows with stable ids and normalized enums
- `signals.parquet`: one row per logical group signal with attached route, edge, score, and risk/OMS summary
- `orders.parquet`: one row per submitted leg with local intent, request, response, and lifecycle state
- `fills.parquet`: normalized fill/cancel/terminal state derived from audit and traces
- `windows.parquet`: deduplicated opportunity windows for event-level strategy studies
- `latencies.parquet`: precomputed latency spans so research queries do not have to re-derive them

The manifest should record:

- source files and sizes
- content hashes
- config path and digest
- ingest version
- schema version
- start and end timestamps
- row counts per table

## Why This Is Better

This changes replay from “run a bespoke script for each question” into “query a stable run model.”

That unlocks:

- fast postmortems with DuckDB, Polars, or pandas against parquet instead of re-parsing JSONL and tape each time
- strategy studies that join market state, signals, submissions, and fills in one place
- backtests that start from normalized market events instead of bespoke audit-only logic
- easier regression comparisons across runs because datasets share schemas and ids
- a cleaner dashboard because the UI can read prepared tables instead of recomputing analysis views

## Proposed Python Stack

- PyArrow for parquet writing and schema control
- DuckDB or Polars for fast local analysis on large runs
- pandas remains acceptable at the UI edge, but not as the primary ingestion engine

This is still a Python-first problem. There is no need to move the ingest pipeline into C++ unless replay throughput becomes a measured bottleneck.

## Backtesting Shape

Once the canonical dataset exists, backtesting can be split into two layers:

1. Market-state replay layer
   - iterate normalized `market_events`
   - reconstruct per-market books deterministically
   - expose a stable callback interface for strategies
2. Strategy evaluation layer
   - run a strategy implementation against replayed state
   - emit simulated signals, orders, fills, and exposure
   - compare simulated outcomes against live audit outcomes when desired

That gives you both historical strategy testing and “what would the current selector have done?” style counterfactual analysis using the same substrate.

## Concrete Next Steps

1. Add `predex-replay ingest-run` that builds a manifest plus parquet tables for tape, audit, and trace sources.
2. Normalize stable ids across tables: `run_id`, `event_id`, `market_id`, `(shard_id, signal_id)`, `oms_request_id`, and leg keys.
3. Refactor existing timeline/window/soak commands to read ingested parquet tables first and fall back to raw inputs only when needed.
4. Point `dashboard.py` at the canonical run dataset instead of scanning ad hoc replay exports.
5. Add a strategy replay API that consumes normalized market events and config snapshots.

## Recommended Division of Labor

- C++ for live ingest, hot-path runtime logic, and OMS execution
- Python for offline ingest, analytics, strategy replay, research, and backtesting
