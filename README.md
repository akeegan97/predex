# Predex

Predex is an event-driven trading runtime for Kalshi prediction markets. It ingests live websocket market data, maintains shard-local order books, evaluates strategy signals, routes order intents through a central OMS, executes through Kalshi's REST/private-websocket interfaces, and records both raw feed data and structured audit trails for replay.

This repository is not trying to pretend Kalshi WebSocket JSON is a colocated binary feed. The point is different: build and validate the architecture around it well. Predex is a systems project about bounded message handoff, ownership discipline, observability, replayability, and execution correctness under real market conditions.

## What This Repo Is

- A C++20 trading runtime with a thread-per-stage pipeline and strict ownership boundaries
- A live OMS path with global and local risk controls, order lifecycle tracking, and kill-switch behavior
- A Python discovery/config toolchain that synthesizes event-centric trader configs from Kalshi metadata
- A replay and ingest workflow that turns raw tape, audit logs, and REST traces into inspectable datasets
- An experimental strategy environment where live soak testing is used to validate execution mechanics before adding more features

## What This Repo Is Not

- A high-frequency, sub-microsecond, colocated feed handler
- A finished multi-strategy production trading system
- A claim that all current strategy ideas are mature enough for sizing up aggressively

The runtime is real. Some strategy logic is still experimental by design.

## Current Status

Predex today is a discrete-session runtime: configs are typically regenerated per run, and the only live strategy (`MonotonicArbStrategy`) trades IOC-only with no orders resting between sessions. The OMS and reconciliation paths are designed for the eventual long-range strategy landing (MM, soft-monotonic) but several of those paths are intentionally scaffolded rather than wired.

Working today:

- `trader_app` connects to Kalshi, subscribes to configured public channels, maintains shard-local books, evaluates strategy signals, routes order intents through the OMS, and records raw inbound payloads to a binary tape
- The OMS tracks full order lifecycle across request, client-order-id, and exchange-order-id lookups; capital reservation is enforced by `GlobalRisk` against `oms_transport.available_capital_ticks`
- Live order execution uses Kalshi REST for submit/cancel/modify, routed through a Gateway pipeline (command ingress → sequencer → batch planner → token-bucket rate limiter → session pool). Fill and lifecycle events currently come back on the REST response path
- Startup runs a configurable `oms_transport.startup_open_orders_policy` against Kalshi REST. The default `refuse_if_present` aborts startup if any prior-session orders are present, preserving the kill-switch invariant. With today's IOC-only strategy this path fires zero times in practice but is the defensive gate for future long-range strategies
- `MonotonicArbStrategy` uses paired IOC limit selection with book-quality gating, continuity checks, and audit telemetry around visible frontier depth
- The Python discovery pipeline generates event-centric configs and handles safe subchain splitting for multi-entity numeric events
- Replay tooling can summarize audit logs, export event timelines, and ingest a run into parquet datasets for offline analysis

Scaffolded but not yet wired:

- Kalshi private-websocket transport for fills/lifecycle events: adapter and worker exist, but the OMS `ws_event_queue` is always `nullptr` today
- Startup `cancel_all` and `adopt` reconciliation policies: enum values defined and config-selectable, but the code paths fail-loud with a clear deferral message
- Drawdown soft-halt on `oms_transport.max_session_loss_ticks`: `Oms::request_soft_halt()` exists but has no caller; session-level fill P&L accumulation is not yet implemented
- Cancel-all-on-hard-halt: `Oms::request_hard_halt()` flips the mode flag but no cancel sweep runs from the OMS pump

Still intentionally incomplete:

- `CdfViolationStrategy`, `MarketMakingStrategy`, and `MeanReversionStrategy` are wired into the shard pipeline but emit no signals
- Dynamic sizing is not yet enabled; the current focus is validating execution quality and reducing one-sided fills
- Public-websocket reconnect uses exponential backoff today; deeper resilience hardening (sequence-gap recovery, post-reconnect snapshot reconciliation) is pending

## Architecture At A Glance

```text
Kalshi public WS -> IO thread -> Router -> Shards -> OMS -> REST (private WS scaffolded)
                                      |         |       |
                                      |         |       +-> Audit JSONL
                                      |         +----------> Order intents / lifecycle
                                      +--------------------> Binary tape logger
```

Key runtime properties:

- **Single-owner threads** across IO, routing, shards, OMS, logger, and audit
- **Bounded SPSC queues** between hot-path stages
- **Zero-copy frame handoff** after the initial websocket payload copy into the frame pool
- **Shard-local event stores** that maintain books and derived event topology state
- **Central OMS coordinator** as the single writer to order state
- **Raw tape + structured audit** so strategy and execution behavior can be inspected after live runs

Supporting docs:

- [Docs Guide](docs/README.md)
- [Architecture](docs/architecture.md)
- [OMS Design](docs/oms_design.md)
- [Ownership and Invariants](docs/ownership_invariants.md)
- [Design Decisions](docs/design_decisions.md)
- [Data Contract](docs/data_contract.md)
- [Python Toolchain](docs/predex-python.md)
- [Results](docs/results.md)

Planning/backlog docs that are useful but not front-door material:

- [Open Backlog](docs/planning/open_backlog.md)
- [Replay Roadmap](docs/planning/replay_matrix.md)

Generated examples and working artifacts live in `docs/`, but they are outputs, not canonical documentation:

- `docs/generated_config*.json`
- `docs/generated_config.report.json`
- `docs/generated_discovery_report.json`

## Safe Live-Trading Defaults

The repo is intentionally safe by default.

- `oms_transport.enabled` defaults to `false`
- `local_risk.trading_enabled` defaults to `false`

That means you can generate configs, run the pipeline, inspect signals, and record audit/tape data without accidentally submitting live orders unless you explicitly opt in.

## Observed Runtime Characteristics

Recent soak and replay artifacts show a consistent pattern:

- shard-local signal generation is very fast relative to the full execution loop
- the healthy warm local path to `request_sent` is typically low-single-digit milliseconds
- venue/network round trip dominates end-to-end order latency
- recent strategy improvements have therefore focused more on execution quality and book durability than on pretending the venue wire path can be optimized away

Representative latency snapshots and supporting artifacts live in [docs/results.md](docs/results.md).

## Build Targets

Current CMake targets in [`cpp/CMakeLists.txt`](cpp/CMakeLists.txt):

- `predex_websocket`
- `predex_core_pipeline`
- `predex_app`
- `trader_app`
- `replay_app`
- `parser_regression_test`, `kalshi_rest_adapter_boundary_test` (test executables)

## Quickstart

### Prerequisites

- CMake 3.24+
- Ninja
- A C++20 compiler
- `vcpkg` or equivalent system packages for Boost.System, OpenSSL, `nlohmann_json`, and `simdjson`

### Configure and Build

```bash
cmake --preset dev-vcpkg
cmake --build --preset build-dev-vcpkg
```

### Configure Credentials

```bash
export KALSHI_KEY_ID=...
export KALSHI_PRIVATE_KEY_PEM='-----BEGIN PRIVATE KEY-----...'
```

If you keep credentials in the repo-root [`.env`](.env), the Python CLI and repo wrappers will load them automatically.

Example `.env` keys:

```bash
KALSHI_KEY_ID=...
KALSHI_PRIVATE_KEY_PEM='-----BEGIN PRIVATE KEY-----...'
```

### Run The Trader

```bash
./build/dev/cpp/trader_app --config docs/trader_config.example.json
```

The runtime prints a periodic health line to stdout. The schema is wide (~35 fields including gateway latency stages, session-pool counters, and audit drop counts); see `App::Runtime::print_health_status` in `cpp/src/app.cpp` for the canonical format. Truncated example:

```text
[timestamp UTC] STATUS | halted=false | live_orders=0 | oms_shard_requests=0 | ...
```

The repo wrapper below auto-loads the root `.env` before launching the binary:

```bash
./scripts/trader_app --config docs/trader_config.example.json
```

## Config Generation With Python

The Python discovery toolchain builds event-centric trader configs from Kalshi metadata.

Generate config for explicit event tickers:

```bash
PYTHONPATH=python/src python3 -m predex.discovery \
  --event-ticker KXPGATOUR-VATO26 \
  --event-ticker KXWMARMAD-26 \
  --shard-count 4 \
  --output docs/generated_config.json
```

Or discover by series:

```bash
PYTHONPATH=python/src python3 -m predex.discovery \
  --series-ticker KXPGATOUR \
  --limit 20 \
  --output docs/generated_config.json
```

If you use a repo-root virtualenv, the wrapper below will load `.env` and prefer `.venv/bin/predex` automatically:

```bash
./scripts/predex \
  --include-topology monotonic_chain \
  --event-limit 20 \
  --market-limit 60 \
  --output docs/generated_config.json \
  --report-output docs/generated_config.report.json
```

To page through every matching event instead of stopping at the default cap:

```bash
./scripts/predex \
  --include-topology monotonic_chain \
  --all-events \
  --output docs/generated_config.json \
  --report-output docs/generated_config.report.json
```

The same CLI exposes the main runtime capacity knobs, including:

- `--shard-count`
- `--frame-pool-capacity`
- `--io-to-router-capacity`
- `--router-to-logger-capacity`
- `--shard-input-capacity`
- `--shard-to-logger-capacity`
- `--oms-rest-worker-count`

## Tape, Replay, And Run Ingest

### Tape Format

```text
[u32 payload_len_le][payload bytes][u32 payload_len_le][payload bytes]...
```

Each payload is the raw websocket text frame received from Kalshi.

### Replay Summary

```bash
PYTHONPATH=python/src python3 -m predex.replay audit-summary \
  --config docs/generated_config.json \
  --audit logs/live/predex_audit.jsonl
```

### Export Event Timeline

```bash
./scripts/predex-replay export-event-timeline \
  --config docs/generated_config.json \
  --audit logs/live/predex_audit.jsonl \
  --tape logs/live/predex_tape.bin \
  --event-id 1344469444 \
  --output-dir logs/replay \
  --prefix eggs_event
```

This writes:

- timeline CSV (`*.csv`) with top-of-book progression
- signal-hit CSV (`*.signals.csv`)
- summary JSON (`*.summary.json`)
- standalone visualization HTML (`*.html`)

Add `--parquet` to also emit parquet files. This requires `pyarrow` in your venv:

```bash
.venv/bin/pip install pyarrow
```

### Ingest A Live Run

```bash
./scripts/predex-replay ingest-run \
  --config docs/generated_config.json \
  --audit logs/live/predex_audit.jsonl \
  --tape logs/live/predex_tape.bin \
  --run-id live_2026_05_04
```

This writes a reusable dataset under `logs/runs/<run_id>/`, including:

- `market_routes.parquet`
- `frames.parquet`
- `market_events.parquet`
- `audit_events.parquet`
- `signals.parquet`
- `legs.parquet`
- `latencies.parquet`
- `trace_requests.parquet`
- `trace_orders.parquet`
- `manifest.json`

If no `--trace` arguments are provided, `ingest-run` auto-detects `predex_rest_trace*.jsonl` files beside the audit log.

### Replay Dashboard

Install the visualization dependencies:

```bash
.venv/bin/pip install '.[replay-viz]'
```

Launch:

```bash
./scripts/predex-replay-dashboard
```

## Runtime Config

The config has seven top-level sections:

- `kalshi`
- `market_routes`
- `pipeline`
- `tape`
- `audit`
- `oms_transport`
- `local_risk`

Example: [`docs/trader_config.example.json`](docs/trader_config.example.json)

Key fields:

- `kalshi.endpoint` — websocket URL
- `kalshi.channels` — channel subscriptions such as `orderbook_delta` and `trade`
- `kalshi.lifecycle_channels` — market lifecycle channels such as `market_lifecycle_v2`
- `kalshi.market_tickers` — markets to subscribe to
- `kalshi.credentials.key_id_env` / `private_key_pem_env` — environment variable names for credentials
- `market_routes[*].event_id` — stable local event id used to group markets in shard-local event stores
- `market_routes[*].affinity_key` — per-event shard affinity; shard choice is `affinity_key % shard_count`
- `market_routes[*].topology_kind` — one of `single_market`, `mutually_exclusive`, `monotonic_chain`, `unordered_group`
- `market_routes[*].strike_key` — integer ordering key for topology-specific ordering
- `market_routes[*].close_time_s` — Unix timestamp used by close-time gating
- `market_routes[*].tradeable` — whether the market accepts new orders
- `pipeline.frame_pool_capacity` — frame pool slot count
- `pipeline.shard_count` — number of shard threads
- `tape.output_path` — binary tape output path
- `audit.output_path` — JSONL audit log output path
- `oms_transport.enabled` — enable live order submission
- `oms_transport.rest_endpoint` — Kalshi REST API base URL
- `oms_transport.private_ws_endpoint` — Kalshi private websocket URL for fills *(reserved; private-WS transport is not yet wired into the OMS)*
- `oms_transport.max_session_loss_ticks` — drawdown limit in ticks *(reserved; soft-halt breaker is not yet wired)*
- `oms_transport.available_capital_ticks` — capital reservation budget enforced by `GlobalRisk`; `0` disables capital gating
- `oms_transport.rest_worker_count` — size of the async REST worker pool
- `oms_transport.startup_open_orders_policy` — what to do with prior-session orders found at the venue at startup. One of `"ignore"`, `"refuse_if_present"` (default), `"cancel_all"`, `"adopt"`. Today only `"ignore"` and `"refuse_if_present"` are implemented; `"cancel_all"` and `"adopt"` are scaffolded and abort startup with a clear deferral message
- `local_risk.max_net_position_lots_per_market` — max absolute net filled position per market; `0` disables
- `local_risk.min_seconds_to_close` — reject intents for markets closing within this many seconds; `0` disables
- `local_risk.trading_enabled` — master switch for strategy intent generation

`market_routes` is required by `trader_app`. The Python discovery CLI is the intended way to synthesize it.

## Repository Layout

- `cpp/apps` — executable entrypoints
- `cpp/include/predex` — public headers for websocket, pipeline, and app wiring
- `cpp/src` — implementations
- `cpp/tests` — C++ tests and scaffolding
- `python/src/predex` — Python discovery, replay, and config synthesis tooling
- `python/tests` — Python unit tests
- `scripts` — repo wrappers for trader, replay, and discovery flows
- `docs` — canonical docs, planning notes, schema captures, and generated examples
- `logs` — replay outputs and normalized run datasets

## How To Read This Repo

If you are new here, the fastest path is:

1. Read this README for scope and current status.
2. Read [docs/README.md](docs/README.md) for the docs map.
3. Read [docs/architecture.md](docs/architecture.md) for the runtime topology.
4. Read [docs/oms_design.md](docs/oms_design.md) for order-state ownership and execution flow.
5. Skim [docs/predex-python.md](docs/predex-python.md) for the discovery/config toolchain.
6. Use `docs/trader_config.example.json` and the `scripts/` wrappers to run the pipeline safely with trading disabled by default.

## Near-Term Priorities

- Validate whether dynamic sizing is justified after enough depth-aware soak data is collected
- Deepen public-websocket reconnect hardening: sequence-gap recovery and post-reconnect snapshot reconciliation
- Wire the drawdown soft-halt (`max_session_loss_ticks`) once a strategy begins accumulating session P&L worth bounding
- Continue repo cleanup so `main` can serve as the canonical branch and project front door

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
