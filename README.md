# PredEx

PredEx is a C++20 event-driven trading runtime for Kalshi prediction markets. It
ingests public market data, maintains shard-owned order books, publishes
event-level observations to strategy, coordinates multi-leg order execution,
reconciles venue state, and records the raw feed for offline research.

The project is primarily about systems engineering: explicit ownership,
bounded queues, integrity-preserving failure behavior, operational control, and
reproducible research. It is not presented as a colocated HFT stack or a
finished production trading platform.

## System at a glance

```text
                                 +--------------------+
operator Unix socket <---------> | control plane      |
                                 +----+----+----+------+
                                      |    |    |
Kalshi public WS -> wire session -> router -> N shards -> strategy
                         |            |         |           |
                         |            |         +---------> logger -> tape.bin
                         |            |                     |
                         |            +---------------------+
                         |
                         +-- sequence/integrity facts -> control plane

strategy -> OMS -> persistent HTTP/2 order REST -> Kalshi
             ^                     |
             |                     +-> acknowledgements/reconciliation
             +---- private order websocket <---- Kalshi
```

The main process is composed from single-owner threads joined by bounded SPSC
queues:

- `ControlPlane` owns lifecycle, readiness, trading authorization, and recovery
  coordination.
- `KalshiWireSession` owns the public websocket, envelope parsing, subscription
  sequence observation, frame-pool acquisition, and snapshot requests.
- `Router` performs numeric dispatch to the owning shard and propagates
  integrity barriers when delivery fails.
- Each `Shard` owns its event store and order books. Related markets remain on
  one shard through an event affinity key.
- `MonotonicArbStrategy` consumes immutable event observations and emits typed
  group intents.
- `Oms` is the single writer for group/order execution state and strategy
  portfolio state.
- `OrderRestSession` owns persistent HTTP/2 order and portfolio requests.
- `KalshiOrderSession` owns the private order websocket.
- `MarketDataLogger` is the terminal sink for raw market-data frames.
- `UnixCommandServer` exposes a per-config local operator endpoint used by
  `predexctl`.

See [Architecture](docs/architecture.md) and
[Ownership and Invariants](docs/ownership_invariants.md) for the detailed
contracts.

## Correctness model

PredEx treats loss detection and capacity as separate concerns. Larger queues
absorb normal startup bursts; they are not the correctness mechanism.

- Public websocket sequence numbers are observed in the wire session before
  market filtering.
- A subscription sequence gap invalidates every affected order book.
- A market-local delivery failure invalidates only the target market.
- An invalidated book rejects deltas until a replacement snapshot is applied.
- Recovery uses Kalshi's `get_snapshot` subscription action without changing
  the existing delta subscription.
- Transport loss, intentional filtering, downstream delivery loss, and
  logger-only traffic have separate counters.
- Recovery incidents are latched and deduplicated instead of producing an
  unbounded stream of repeated failures.
- Frame slots are generation-checked and returned through one SPSC recycle
  queue per producer.
- OMS execution state, capital reservations, fills, and repair decisions have
  one writer: the OMS thread.

These rules make degraded state explicit. A stale or partially updated book
must not silently remain tradeable.

## Current boundary

Implemented and exercised:

- Public order-book, trade, and lifecycle ingestion
- Sharded event stores and replacement-snapshot recovery
- Bounded frame and message ownership across the ingestion pipeline
- Monotonic-arbitrage observation and intent generation
- Multi-leg OMS admission, reservation, lifecycle tracking, and repair state
- Persistent REST connections and periodic venue portfolio reconciliation
- Private order-feed parsing for orders, fills, and market positions
- Runtime trading authorization and operator kill controls
- Per-stage steady-clock latency histograms and component counters
- Binary raw-feed capture plus Python replay and materialization tooling

Known limitations:

- Public and private websocket reconnect/resubscription is not yet automatic;
  a transport disconnect currently faults readiness and requires operator
  intervention or process restart.
- Live multi-leg execution has deliberately small operating limits and still
  needs more observed fill/repair incidents before sizing can be justified.
- Strategy candidate and gate-rejection telemetry is not yet present in the
  operator statistics snapshot, so zero OMS intents does not distinguish “no
  raw violation” from “candidate rejected before publication.”
- Research results authorize only the exact experiments whose frozen gates
  passed. They do not implicitly authorize a live controller or broader
  deployment.

## Repository layout

```text
cpp/apps/predex/       runtime composition root
cpp/apps/predexctl/    local operator client
cpp/include/predex/    runtime interfaces and message contracts
cpp/src/               runtime implementations
cpp/tests/             C++ unit and component tests
python/src/predex/     discovery, config generation, and replay tooling
python/tests/tooling/  tests for the supported Python surface
scripts/ops/           supported operator and replay wrappers
docs/                  canonical design docs and historical artifacts
```

## Build and test

Prerequisites:

- CMake 3.24+
- Ninja
- Clang with C++20 support
- vcpkg

```bash
export VCPKG_ROOT=/path/to/vcpkg

cmake --preset dev-vcpkg
cmake --build --preset build-dev-vcpkg --parallel 2
ctest --preset test-dev-vcpkg
```

The performance preset enables `-O3`, native CPU tuning, and `NDEBUG`:

```bash
cmake --preset perf-vcpkg
cmake --build --preset build-perf-vcpkg --parallel 2
ctest --preset test-perf-vcpkg
```

The ordinary Python operator/config surface has no third-party runtime
dependencies:

```bash
PYTHONPATH=python/src python3 -m unittest \
  python.tests.tooling.test_env \
  python.tests.tooling.test_discovery \
  python.tests.tooling.test_replay
```

Install the optional Arrow dependency for materialization and route-table
rewrites with:

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[replay]'
```

## Generate a run configuration

The C++ runtime consumes the `app` configuration schema. The generator can
discover open events, classify event topology, build stable IDs and shard
affinity, and create a run directory containing the config and report.

```bash
./scripts/ops/predex \
  --config-format app \
  --all-events \
  --include-topology monotonic_chain \
  --run-label market-data \
  --frame-pool-capacity 65536 \
  --router-queue-capacity 32768 \
  --shard-input-capacity 32768
```

Market data is enabled automatically for generated run directories. OMS and
strategy wiring remain disabled unless their explicit flags are supplied.
Credentials are read by environment-variable name from the generated config:

```bash
export KALSHI_KEY_ID=...
export KALSHI_PRIVATE_KEY_PEM='-----BEGIN PRIVATE KEY-----...'
```

Never commit credentials or a populated `.env` file.

## Run and operate PredEx

Start the process with its generated config:

```bash
./build/perf/cpp/predex runs/<run-name>/config.json
```

Each file-backed config receives a deterministic Unix socket path. Source the
activation helper once so subsequent commands address the correct process:

```bash
source scripts/ops/predex-use runs/<run-name>

./build/perf/cpp/predexctl status
./build/perf/cpp/predexctl stats
```

Available operator commands:

```text
status
counterstats              (alias: stats)
allow-trading
disable-trading
cancel-all-orders
shutdown-graceful
shutdown-forceful
```

`predexctl` uses `--socket` first and `PREDEX_SOCKET_PATH` second. A successful
client exit means a complete request/response transport exchange occurred; the
JSON response still determines whether the server accepted the command.

Trading requires both configured order components and an explicit runtime
authorization:

```bash
./build/perf/cpp/predexctl allow-trading
```

Before a routine shutdown:

```bash
./build/perf/cpp/predexctl disable-trading
./build/perf/cpp/predexctl stats
./build/perf/cpp/predexctl shutdown-graceful
```

Confirm that `live_orders`, `pending_submit_orders`, and `uncertain_orders` are
zero before terminating the process.

## Observability

`predexctl status` exposes the lifecycle, trading-session phase, authorization,
and shutdown state. `predexctl counterstats` returns the full machine-readable
snapshot, including:

- Per-channel sequence gaps, duplicates, stale frames, intentional filtering,
  logger-only frames, and downstream loss
- Frame-pool and queue high-water marks
- Shard apply/reject/desync and book-invalidation counters
- Snapshot requests and recovery duration
- Logger records, bytes, and failures
- OMS reconciliation, execution, repair, and live-order state
- Public-to-book and public-to-logger latency distributions

Latency timestamps use `std::chrono::steady_clock`. They are suitable for
durations inside one process and are intentionally not wall-clock timestamps.

## Research boundary

The public Python surface ends at discovery, configuration, tape inspection,
metadata enrichment, and materialization. Experimental models, cohorts, and
counterfactual research remain in a local workspace and are intentionally not
distributed by this repository. Their results do not silently promote a live
controller or change runtime authorization.

See [Data Contract](docs/data_contract.md) for the boundary between recorded
data and downstream analysis.

## Further documentation

- [Documentation guide](docs/README.md)
- [Architecture](docs/architecture.md)
- [Ownership and invariants](docs/ownership_invariants.md)
- [OMS design](docs/oms_design.md)
- [Data contract](docs/data_contract.md)
- [Design decisions](docs/design_decisions.md)
- [Python toolchain](docs/predex-python.md)
- [Measured results](docs/results.md)
- [Main takeover checklist](docs/planning/main_takeover.md)
- [Open backlog](docs/planning/open_backlog.md)
