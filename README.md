# Predex

Predex is a market data pipeline for capturing live Kalshi websocket traffic, routing it through shard-local book processing, and persisting the raw feed to a binary tape.

## What This Is

The exchange target is [Kalshi](https://kalshi.com), a regulated prediction market with a free WebSocket API. Individual access to real binary exchange feeds (ITCH, OUCH, SBE, proprietary UDP multicast) is prohibitively expensive — Kalshi is the accessible substrate for building and validating real pipeline architecture.

The WebSocket transport is the ceiling on wire latency. The internal pipeline after it is not. The goal of this project is to get the internal architecture right: bounded-latency message handoff, correct memory ordering, single-owner thread design, and a shard-local book that can serve as the foundation for a strategy and execution layer.

What the internals do:

- **Lock-free SPSC queues** with cache-line-padded indices and acquire/release memory ordering for each stage handoff
- **Frame pool** with generation-tracked handles for zero-copy payload passing from IO through the full pipeline and back to recycle
- **Thread-per-stage** with single ownership per component — no locks on the hot path
- **Router** using simdjson on-demand parsing to classify and dispatch frames without full deserialization
- **Shard-local order book** per market with snapshot-first state machine, pending delta buffering, sequence validation, and desync detection
- **Binary tape** for replay, with a simple length-prefixed format

What this is not: a sub-microsecond co-located feed handler. The WebSocket transport and JSON wire format preclude that. The architecture and the code patterns are the point.

## Current Status

Working today:

- `trader_app` connects to Kalshi, subscribes to configured channels, and records inbound payloads to a tape file
- Core runtime is split into focused libraries: `predex_websocket`, `predex_core_pipeline`, `predex_app`
- Tape format is stable for downstream replay tooling: 4-byte little-endian payload length followed by raw websocket payload bytes

Intentionally incomplete:

- Strategy and per-shard event handling beyond the interface seam
- Local and global risk checks
- OMS and order transport
- Replay executable
- Python tooling for tape decode and backtesting

## Architecture

```text
                    +----------------------------------------------+
                    |              predex::App::Runtime            |
                    +----------------------------------------------+

Kalshi WS
    |
    v
+-------------------------+      +---------------------------+
| websocket::WsSession    | ---> | ingest::IOWriter         |
| single owner: IO thread |      | acquire frame + copy     |
+-------------------------+      +-------------+-------------+
                                              |
                                              v
                                   +---------------------------+
                                   | io_to_router SPSC queue   |
                                   +-------------+-------------+
                                                 |
                                                 v
                                   +---------------------------+
                                   | routing::Router           |
                                   | single owner: router thread|
                                   +------+--------------------+
                                          |
                     +--------------------+--------------------+
                     |                                         |
                     v                                         v
        +---------------------------+             +---------------------------+
        | shard_input[i] SPSC queue |             | router_to_logger queue    |
        +-------------+-------------+             +-------------+-------------+
                      |                                           |
                      v                                           |
        +---------------------------+                             |
        | shards::Shard             |                             |
        | single owner: shard i     |                             |
        +-------------+-------------+                             |
                      |                                           |
                      v                                           v
        +---------------------------+             +---------------------------+
        | shard_to_logger[i] queue  | ----------> | tape::Logger             |
        +---------------------------+             | single owner: logger      |
                                                  +-------------+-------------+
                                                                |
                                                                v
                                                  +---------------------------+
                                                  | recycle queue             |
                                                  +-------------+-------------+
                                                                |
                                                                v
                                                  +---------------------------+
                                                  | ingest::IOWriter          |
                                                  | recycle handles           |
                                                  +---------------------------+
```

Supporting docs:

- [Architecture](docs/architecture.md)
- [Design Decisions](docs/design_decisions.md)
- [Ownership and Invariants](docs/ownership_invariants.md)
- [Data Contract](docs/data_contract.md)
- [Performance Backlog](docs/perf_backlog.md)
- [Replay Roadmap](docs/replay_matrix.md)

## Build Targets

Current CMake targets in [`cpp/CMakeLists.txt`](cpp/CMakeLists.txt):

- `predex_websocket`
- `predex_core_pipeline`
- `predex_app`
- `trader_app`

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

### Run

```bash
./build/dev/cpp/trader_app --config docs/trader_config.example.json
```

The runtime is quiet on success. The main observable artifact is the configured tape file, default `predex_tape.bin`.

The repo wrapper below auto-loads the root `.env` before launching the binary:

```bash
./scripts/trader_app --config docs/trader_config.example.json
```

### Generate Configs With Python

The repo now includes a small Python discovery toolchain for building event-centric trader configs from Kalshi metadata.

```bash
PYTHONPATH=python/src python3 -m predex.discovery \
  --event-ticker KXPGATOUR-VATO26 \
  --event-ticker KXWMARMAD-26 \
  --shard-count 4 \
  --output docs/generated_config.json
```

You can also discover by series ticker instead of enumerating events:

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

To page through every matching event instead of stopping at the default discovery cap:

```bash
./scripts/predex \
  --include-topology monotonic_chain \
  --all-events \
  --output docs/generated_config.json \
  --report-output docs/generated_config.report.json
```

The same CLI also exposes the main C++ runtime pipeline knobs, such as:
- `--shard-count`
- `--frame-pool-capacity`
- `--io-to-router-capacity`
- `--router-to-logger-capacity`
- `--shard-input-capacity`
- `--shard-to-logger-capacity`

## Tape Format

```text
[u32 payload_len_le][payload bytes][u32 payload_len_le][payload bytes]...
```

Each payload is the raw websocket text frame received from Kalshi.

## Replay Verification and Timeline Export

Use the Python replay CLI to inspect audit output and generate event timeline artifacts from tape:

```bash
PYTHONPATH=python/src python3 -m predex.replay audit-summary \
  --config docs/generated_config.json \
  --audit predex_audit.jsonl
```

Export a timeline for one event (or pass `--market-ticker` for a single-market focus):

```bash
./scripts/predex-replay export-event-timeline \
  --config docs/generated_config.json \
  --audit predex_audit.jsonl \
  --tape predex_tape.bin \
  --event-id 1344469444 \
  --output-dir docs/replay \
  --prefix eggs_event
```

This writes:
- timeline CSV (`*.csv`) with top-of-book progression
- signal-hit CSV (`*.signals.csv`)
- summary JSON (`*.summary.json`)
- standalone visualization HTML (`*.html`)

Add `--parquet` to also emit parquet files (`*.parquet`, `*.signals.parquet`). This requires `pyarrow` in your venv:

```bash
.venv/bin/pip install pyarrow
```

### Replay Dashboard (Streamlit)

Install viz dependencies in your venv:

```bash
.venv/bin/pip install '.[replay-viz]'
```

Launch:

```bash
./scripts/predex-replay-dashboard
```

The dashboard shows a run-wide view from config+audit (all events, submarkets, and signals), then lets you drill into per-market timeline charts from `docs/replay/*.summary.json` datasets when available.

## Runtime Config

The config has five top-level sections: `kalshi`, `market_routes`, `pipeline`, `tape`, and `audit`.

Example: [`docs/trader_config.example.json`](docs/trader_config.example.json)

Key fields:

- `kalshi.endpoint` — websocket URL
- `kalshi.channels` — channel subscriptions (e.g. `orderbook_delta`, `trade`)
- `kalshi.market_tickers` — markets to subscribe to
- `kalshi.credentials.key_id_env` / `private_key_pem_env` — environment variable names for credentials
- `market_routes[*].event_id` — stable local event id used to group markets in shard-local event stores
- `market_routes[*].affinity_key` — stable per-event shard affinity; shard choice is `affinity_key % shard_count`
- `market_routes[*].topology_kind` — one of `single_market`, `mutually_exclusive`, `monotonic_chain`, `unordered_group`
- `market_routes[*].strike_key` — integer ordering key for topology-specific market ordering
- `pipeline.frame_pool_capacity` — frame pool slot count
- `pipeline.shard_count` — number of shard threads
- `audit.output_path` — JSONL audit log output path
- `tape.output_path` — binary tape output path

`market_routes` is required by `trader_app`. The Python discovery CLI is the intended way to synthesize it programmatically.

## Repository Layout

- `cpp/apps` — executable entrypoints
- `cpp/include/predex` — public headers for websocket, pipeline, and app wiring
- `cpp/src` — implementations
- `python/src/predex` — Python discovery and config synthesis tooling
- `python/tests` — Python unit tests
- `cpp/tests` — tests and scaffolding
- `docs` — architecture, contracts, config examples, and planning notes

## Near-Term Roadmap

- Replay executable and Python tape reader
- Market discovery and config generation tooling
- Shard-local strategy and risk hooks
- OMS and paper/live execution plumbing
- Backtesting framework on top of tape replay

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
