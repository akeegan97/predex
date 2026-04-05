# Predex

Predex is a low-latency market data pipeline for capturing live Kalshi websocket traffic, routing it through shard-local book processing, and persisting the raw feed to a binary tape.

The current C++ codebase is intentionally narrow:
- live Kalshi websocket ingest
- bounded frame-pool based message handoff
- single router thread plus N shard threads
- shard-local parsing and book maintenance
- terminal tape logging with recycle back to ingest

Strategy, risk, OMS, replay, and Python research tooling are planned next steps, but they are not the primary supported surface of this repository today.

## Current Status

What works today:
- `trader_app` can connect to Kalshi, subscribe to channels, and record inbound payloads to a tape file.
- The core runtime is split into focused libraries: `predex_websocket`, `predex_core_pipeline`, and `predex_app`.
- The tape format is stable enough for downstream replay tooling: 4-byte little-endian payload length followed by raw websocket payload bytes.

What is still intentionally incomplete:
- strategy and per-shard event handling beyond the interface seam
- local/global risk checks
- OMS and order transport
- replay executable
- Python tooling for tape decode, discovery, config generation, and backtesting

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
- [Ownership And Invariants](docs/ownership_invariants.md)
- [Data Contract](docs/data_contract.md)
- [Performance Backlog](docs/perf_backlog.md)
- [Replay Roadmap](docs/replay_matrix.md)

## Build Targets

Current CMake targets in [`cpp/CMakeLists.txt`](cpp/CMakeLists.txt):
- `predex_websocket`
- `predex_core_pipeline`
- `predex_app`
- `trader_app`

The repository still contains a few placeholder or legacy-adjacent files, but the targets above are the supported C++ surface right now.

## Quickstart

### Prerequisites

- CMake 3.24+
- Ninja
- a C++20 compiler
- `vcpkg` or equivalent system packages for Boost.System, OpenSSL, `nlohmann_json`, and `simdjson`

### Configure And Build

```bash
cmake --preset dev-vcpkg
cmake --build --preset build-dev-vcpkg
```

### Configure Credentials

The example config uses environment-backed credentials:

```bash
export KALSHI_KEY_ID=...
export KALSHI_PRIVATE_KEY_PEM='-----BEGIN PRIVATE KEY-----...'
```

### Run The Live Trader Bootstrap

```bash
./build/dev/cpp/trader_app --config docs/trader_config.example.json
```

The current runtime is intentionally quiet on success. The main observable artifact is the configured tape file, typically `predex_tape.bin`.

## Tape Format

The tape is a binary stream of repeated records:

```text
[u32 payload_len_le][payload bytes][u32 payload_len_le][payload bytes]...
```

Each payload is the raw websocket text frame received from Kalshi.

## Runtime Config Shape

The live bootstrap consumes a small JSON config with these top-level sections:
- `kalshi`
- `market_routes`
- `pipeline`
- `tape`

Example file:
- [docs/trader_config.example.json](docs/trader_config.example.json)

Important fields:
- `kalshi.endpoint`
- `kalshi.channels`
- `kalshi.market_tickers`
- `kalshi.credentials.key_id` or `key_id_env`
- `kalshi.credentials.private_key_pem` or `private_key_pem_env`
- `pipeline.frame_pool_capacity`
- `pipeline.shard_count`
- `pipeline.frame_queue_capacity` or explicit queue capacities
- `tape.output_path`

If `market_routes` is omitted, `trader_app` can derive simple routes from the configured market ticker list.

## Repository Layout

- `cpp/apps`: executable entrypoints
- `cpp/include/predex`: public headers for websocket, pipeline, and app wiring
- `cpp/src`: implementations
- `cpp/tests`: older tests and scaffolding, not yet fully migrated to the current `predex` namespace layout
- `docs`: architecture, contracts, config examples, and planning notes
- `scripts`: helper scripts and future tooling entrypoints

## Near-Term Roadmap

- Python tape reader and replay tooling
- market discovery and config generation tooling
- shard-local strategy and risk hooks
- OMS and paper/live execution plumbing
- backtesting framework on top of tape replay

## Contributing

Contribution guidelines live in [CONTRIBUTING.md](CONTRIBUTING.md).
