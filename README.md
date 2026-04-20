# Predex

Predex is a full trading pipeline for Kalshi prediction markets: live websocket ingest, shard-local order book maintenance, strategy signal evaluation, OMS coordination, REST order execution, and binary tape persistence.

## What This Is

The exchange target is [Kalshi](https://kalshi.com), a regulated prediction market with a free WebSocket API. Individual access to real binary exchange feeds (ITCH, OUCH, SBE, proprietary UDP multicast) is prohibitively expensive — Kalshi is the accessible substrate for building and validating real pipeline architecture.

The WebSocket transport is the ceiling on wire latency. The internal pipeline after it is not. The goal of this project is to get the internal architecture right: bounded-latency message handoff, correct memory ordering, single-owner thread design, and a full pipeline from feed to execution.

What the internals do:

- **Lock-free SPSC queues** with cache-line-padded indices and acquire/release memory ordering for each stage handoff
- **Frame pool** with generation-tracked handles for zero-copy payload passing from IO through the full pipeline and back to recycle
- **Thread-per-stage** with single ownership per component — no locks on the hot path
- **Router** using simdjson on-demand parsing to classify and dispatch frames without full deserialization
- **Shard-local order book** per market with snapshot-first state machine, pending delta buffering, sequence validation, and desync detection
- **Strategy pipeline** with `LocalRiskManager` pre-gate and strategy hooks per shard (`MonotonicArbStrategy`, stub strategies for CDF violation, market making, mean reversion)
- **OMS coordinator** with `GlobalRiskManager` pre-trade checks, full order lifecycle tracking, drawdown circuit breaker, and soft/hard halt modes
- **Order transport** via Kalshi REST API (submit, cancel, modify) and private WebSocket (fills, lifecycle events)
- **Startup reconciliation** — open orders from prior sessions are adopted into `OrderStore` so the kill switch covers them and fills count against session P&L
- **Binary tape** for replay, with a simple length-prefixed format
- **Audit log** — OMS decisions, fills, and latency spans persisted as JSONL

What this is not: a sub-microsecond co-located feed handler. The WebSocket transport and JSON wire format preclude that. The architecture and the code patterns are the point.

## Current Status

Working today:

- `trader_app` connects to Kalshi, subscribes to configured channels, maintains shard-local books, runs strategy evaluation, routes order intents through the OMS, submits/cancels/modifies orders via REST, and records inbound payloads to a tape file
- Core runtime is split into focused libraries: `predex_websocket`, `predex_core_pipeline`, `predex_app`
- `MonotonicArbStrategy` detects probability-monotonicity violations across chain events and submits IOC leg pairs
- OMS coordinator tracks full order lifecycle across three lookup indices; kill switch covers current and prior-session orders
- Python discovery toolchain generates event-centric trader configs from Kalshi metadata
- Tape format is stable for downstream replay tooling: 4-byte little-endian payload length followed by raw websocket payload bytes

Intentionally incomplete:

- `CdfViolationStrategy`, `MarketMakingStrategy`, `MeanReversionStrategy` — stubs
- Python tape decoder and replay driver
- REST rate limiting on OMS transport

## Architecture

```text
                    +--------------------------------------------------------------+
                    |                  predex::App::Runtime                        |
                    +--------------------------------------------------------------+

Kalshi public WS                                      Kalshi private WS
    |                                                         |
    v                                                         v
+------------------+    +------------------+    +---------------------------+
| WsSession        |--->| ingest::IOWriter |    | OMS private WS thread     |
| IO thread        |    | acquire + copy   |    | fills + lifecycle events  |
+------------------+    +--------+---------+    +-------------+-------------+
                                  |                            |
                                  v                            |
                         io_to_router_queue                    |
                                  |                            |
                                  v                            v
                         +------------------+      oms_transport_update_queue (MPSC)
                         | routing::Router  |                  |
                         | router thread    |                  |
                         +---+----------+---+                  |
                             |          |                      |
              shard_input[i] |          | router_to_logger     |
                             v          v                      |
                    +---------------+  +--------+              |
                    | shards::Shard |  | logger |              |
                    | EventStore    |  | queue  |              |
                    | ShardPipeline |  +--------+              |
                    +---+---+---+---+       |                  |
                        |   |   |           v              +---+-------------------+
          intent queue  |   |   |    +----------+          | oms::Oms coordinator  |
                        |   |   |    |  Logger  |          | OrderStore            |
                        v   |   |    |  thread  |          | RiskEngine            |
               +--------+   |   |    +----+-----+          | HaltMode              |
               | oms::Oms|   |   |         |               +---+---+---+-----------+
               | intents |   |   |    recycle_queue            |   |   |
               +---------+   |   |         |               submit cancel modify
                        ^    |   |         v                   |   |   |
          decision      |    |   |    IOWriter recycle         v   v   v
          + lifecycle   |    |   |                        +------------------+
                        +----+---+                        | OMS REST thread  |
                  audit queues   |                        | Kalshi REST API  |
                                 v                        +------------------+
                          +------------+
                          | Audit      |
                          | thread     |
                          | JSONL      |
                          +------------+
```

Supporting docs:

- [Architecture](docs/architecture.md)
- [OMS Design](docs/oms_design.md)
- [Design Decisions](docs/design_decisions.md)
- [Ownership and Invariants](docs/ownership_invariants.md)
- [Data Contract](docs/data_contract.md)
- [Python Toolchain](docs/predex-python.md)
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

The runtime prints a health status line to stdout every 30 seconds:

```
[timestamp UTC] STATUS | halted=false | pnl_ticks=+0 | live_orders=0 | intents=0 | rejected=0 | transport_updates=0 | router_frames=0 | router_drops=0 | desynced_events=0
```

The repo wrapper below auto-loads the root `.env` before launching the binary:

```bash
./scripts/trader_app --config docs/trader_config.example.json
```

### Generate Configs With Python

The repo includes a Python discovery toolchain for building event-centric trader configs from Kalshi metadata.

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

The config has seven top-level sections: `kalshi`, `market_routes`, `pipeline`, `tape`, `audit`, `oms_transport`, and `local_risk`.

Example: [`docs/trader_config.example.json`](docs/trader_config.example.json)

Key fields:

- `kalshi.endpoint` — websocket URL
- `kalshi.channels` — channel subscriptions (e.g. `orderbook_delta`, `trade`)
- `kalshi.lifecycle_channels` — lifecycle channel subscriptions (e.g. `market_lifecycle_v2`)
- `kalshi.market_tickers` — markets to subscribe to
- `kalshi.credentials.key_id_env` / `private_key_pem_env` — environment variable names for credentials
- `market_routes[*].event_id` — stable local event id used to group markets in shard-local event stores
- `market_routes[*].affinity_key` — stable per-event shard affinity; shard choice is `affinity_key % shard_count`
- `market_routes[*].topology_kind` — one of `single_market`, `mutually_exclusive`, `monotonic_chain`, `unordered_group`
- `market_routes[*].strike_key` — integer ordering key for topology-specific market ordering
- `market_routes[*].close_time_s` — Unix timestamp of market close; used by `min_seconds_to_close` gating
- `market_routes[*].tradeable` — whether the market accepts new orders
- `pipeline.frame_pool_capacity` — frame pool slot count
- `pipeline.shard_count` — number of shard threads
- `tape.output_path` — binary tape output path
- `audit.output_path` — JSONL audit log output path
- `oms_transport.enabled` — enable live order submission (default `false`)
- `oms_transport.rest_endpoint` — Kalshi REST API base URL
- `oms_transport.private_ws_endpoint` — Kalshi private websocket URL for fills
- `oms_transport.max_session_loss_ticks` — drawdown limit in ticks; `0` disables the circuit breaker
- `local_risk.max_net_position_lots_per_market` — max absolute net filled position per market; `0` disables
- `local_risk.min_seconds_to_close` — reject intents for markets closing within this many seconds; `0` disables
- `local_risk.trading_enabled` — master switch for strategy intent generation

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

- Python tape decoder and replay driver
- REST rate limiting on OMS transport
- Soft halt audit event and stderr log on drawdown trigger
- Fill history recovery after OMS private WS reconnect
- Stub strategy implementations (CDF violation, market making, mean reversion)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
