# Replay Roadmap

Replay tooling has a working ingest layer that materializes one live run into a reusable analysis dataset. The remaining open work is around dataset-aware analyzers and a strategy replay/backtest layer that consumes the ingested dataset.

## What Exists Today

- Live `trader_app` writes raw inbound websocket payloads to a binary tape (PDT2):

```text
magic=PDT2 | version=u16 | flags=u16 | repeated[recv_ts_ns=u64, payload_len=u32, payload bytes]
```

- Python replay code under `python/src/predex/replay/`:
  - `tape.py` — decodes binary tape into normalized market events
  - `audit.py` — loads audit JSONL and bundles per-signal lifecycle state
  - `timeline.py` / `windows.py` — export event timelines and opportunity windows
  - `latency.py` / `soak.py` — summarize latency and run-level execution outcomes
  - `dashboard.py` — visualization layer
- **`predex-replay ingest-run`** materializes a run into the canonical dataset shape below (shipped).
- **`predex-replay export-event-timeline`** and **`audit-summary`** are the working command-centric analyzers.
- **`predex-replay-dashboard`** is the visualization entry point.
- Python discovery and config generation tooling lives under `python/src/predex/discovery/`.

## Canonical Run Dataset (shipped)

`ingest-run` writes the following under `logs/runs/<run_id>/`:

```text
runs/
  <run_id>/
    manifest.json
    market_routes.parquet
    frames.parquet
    market_events.parquet
    audit_events.parquet
    signals.parquet
    legs.parquet
    latencies.parquet
    trace_requests.parquet
    trace_orders.parquet
```

The manifest records source file paths and sizes, content hashes, config digest, ingest version, schema version, time bounds, and row counts per table. If no `--trace` arguments are passed to `ingest-run`, `predex_rest_trace*.jsonl` files beside the audit log are auto-detected.

## Why This Shape

The dataset turns replay from "run a bespoke script for each question" into "query a stable run model."

- Fast postmortems with DuckDB, Polars, or pandas against parquet instead of re-parsing JSONL and tape each time
- Strategy studies that join market state, signals, submissions, and fills in one place
- Easier regression comparisons across runs because schemas and ids are shared
- A dashboard that reads prepared tables instead of recomputing analysis views

## Open Work

### Refactor command-centric analyzers to be dataset-first

- **What**: `timeline`, `windows`, `soak`, `latency-histograms` mostly re-parse raw tape and audit JSONL. They should prefer the ingested parquet tables and fall back to raw inputs only when needed.
- **Why**: avoid the duplicated parse cost and keep the analysis surface coherent.

### Backtesting / strategy replay layer

The dataset unlocks a two-layer backtest design that does not exist yet:

1. **Market-state replay layer** — iterate normalized `market_events`, reconstruct per-market books deterministically, expose a stable callback interface for strategies.
2. **Strategy evaluation layer** — run a strategy implementation against replayed state, emit simulated signals / orders / fills / exposure, optionally compare against live audit outcomes for the same run.

This gives both historical strategy testing and "what would the current selector have done?" counterfactual analysis on the same substrate. Worth landing when the next strategy candidate (CDF arb, soft-monotonic, MM) needs to be validated against history before going live.

### Dashboard against canonical dataset

`dashboard.py` exists. Confirm it reads canonical parquet outputs (not ad hoc replay exports) and remove any remaining bespoke loading paths.

## Stack

- PyArrow for parquet writing and schema control
- DuckDB or Polars for fast local analysis on large runs
- pandas acceptable at the UI edge, not the primary ingestion engine

Still a Python-first problem. No need to move ingest into C++ unless replay throughput becomes a measured bottleneck.

## Division of Labor

- C++ for live ingest, hot-path runtime logic, and OMS execution
- Python for offline ingest, analytics, strategy replay, research, and backtesting
