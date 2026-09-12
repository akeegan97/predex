# Python Toolchain

The supported Python package provides two operator-facing entry points:

- `predex` / `predex-discovery`: discover Kalshi events, classify topology,
  generate run configs, and materialize completed runs;
- `predex-replay`: inspect a current tape/config, summarize a config, enrich
  historical run metadata, or materialize a completed run.

The live process itself is C++. Python is not imported into the runtime hot
path.

## Installation

The discovery and basic replay/config tools use the standard library:

```bash
export PYTHONPATH="$PWD/python/src"
python3 -m predex.discovery --help
python3 -m predex.replay --help
```

For installed commands with the optional Arrow materialization dependency:

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[replay]'
```

`scripts/ops/predex` loads the repository `.env` when present, prefers the
installed `.venv/bin/predex`, and otherwise runs the source package with
`PYTHONPATH`.

## Generate the current C++ schema

The production binary consumes the `app` schema. Always pass
`--config-format app`; the `trader` format remains only for older artifacts.

Generate an explicit small universe:

```bash
./scripts/ops/predex \
  --config-format app \
  --event-ticker KXEXAMPLE-26 \
  --enable-market-data \
  --output runs/example/config.json \
  --report-output runs/example/report.json
```

Generate a complete run bundle from open events:

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

`--run-label` creates a timestamped directory under `runs/` and defaults the
config, report, tape, and materialization paths into that directory. Existing
run directories fail closed unless `--overwrite-run-dir` is explicit.

The generator uses `https://external-api.kalshi.com/trade-api/v2` by default.
It pages `GET /events`, fetches nested event details concurrently, and reuses a
persistent HTTP connection per worker. Retryable HTTP/network failures use
bounded exponential backoff and honor `Retry-After` when supplied.

## Discovery and topology

The generator converts Kalshi metadata into stable runtime identity:

- stable event ID;
- stable market ID;
- event-derived affinity key;
- event topology;
- ordered `strike_key` for monotonic chains;
- tradeability and price-level structure;
- market and event time metadata.

Supported topology classes include:

- `monotonic_chain`
- `mutually_exclusive`
- `unordered_group`
- `single_market`

Classification fails closed when a set cannot safely be ordered. Event filters
apply after classification:

```bash
--include-topology monotonic_chain
--exclude-topology unordered_group
--market-limit 5000
```

`--market-limit` keeps events whole; it does not truncate one event into an
invalid partial topology.

## Runtime settings

The app generator writes these current runtime groups:

- `runtime`: shard/queue/pool capacities, per-config operator socket, tape path,
  polling policy, and optional synthetic session cutoffs;
- `kalshi.auth`: names of credential environment variables;
- `kalshi.market_data`: public channels and enablement;
- `kalshi.order_rest`: persistent REST endpoint and concurrency;
- `kalshi.private_order_feed`: private websocket channels and enablement;
- `oms`: allocation, venue reserve, group limits/repair, intent age, and
  reconciliation cadence;
- `strategy`: monotonic-arbitrage enablement and gates;
- `universe`: classified events and markets.

The generator never embeds credential values. It writes only environment
variable names, normally `KALSHI_KEY_ID` and `KALSHI_PRIVATE_KEY_PEM`.

### Polling profiles

```bash
--thread-polling-profile harvest
--thread-spin-iterations 64
--thread-yield-iterations 64
--thread-min-sleep-us 50
--thread-max-sleep-us 1000
```

Use `harvest` for long captures where thermal/power behavior matters. Use
`low_latency` for short latency-sensitive sessions after measuring the host.

### OMS and live strategy

OMS/private-order components remain disabled unless `--oms-enabled` is
provided. The strategy additionally requires
`--enable-monotonic-arb-strategy`.

```bash
./scripts/ops/predex \
  --config-format app \
  --all-events \
  --include-topology monotonic_chain \
  --run-label monotonic-live \
  --oms-enabled \
  --enable-monotonic-arb-strategy \
  --oms-available-capital-ticks 50000 \
  --oms-maximum-group-reservation-ticks 20000 \
  --monotonic-arb-order-quantity-lots 100
```

Generation validates that allocation and group reservation can cover one
configured two-leg group. It does not authorize live trading. The C++ process
still starts with trading disabled until the operator sends `allow-trading`.

PredEx money uses `$0.0001` ticks. For example, `50000` ticks is `$5.00`.

## Per-config operator targeting

For a file-backed config without an explicit socket, the generator derives a
stable `/tmp/predex-operator-<hash>.sock` path from the absolute config path.
Stdout-only generation uses a random path because no file identity exists.

After the process starts, source the helper once:

```bash
source scripts/ops/predex-use runs/<run-name>
```

It resolves the config, validates `runtime.operator_socket_path`, and exports:

```text
PREDEX_CONFIG
PREDEX_SOCKET_PATH
```

The helper must be sourced; executing it in a child shell cannot modify the
current terminal environment.

## Inspect a config or tape

Summarize event and market distribution:

```bash
PYTHONPATH=python/src python3 -m predex.replay config-summary \
  --config runs/<run-name>/config.json
```

Emit JSON for automation:

```bash
PYTHONPATH=python/src python3 -m predex.replay config-summary \
  --config runs/<run-name>/config.json \
  --json
```

Inspect the current `PDT2` tape header and sample records:

```bash
PYTHONPATH=python/src python3 -m predex.replay inspect-tape \
  --config runs/<run-name>/config.json \
  --tape runs/<run-name>/tape.bin \
  --limit 20
```

The reader rejects unknown magic/version values and truncated records.

## Materialize a completed run

Materialization requires `pyarrow` from the optional `replay` dependencies:

```bash
./scripts/ops/predex --materialize --path runs/<run-name>
```

The streaming writer produces:

```text
tables/frames.parquet
tables/deltas.parquet
tables/trades.parquet
tables/snapshots.parquet
tables/snapshot_levels.parquet
tables/lifecycles.parquet
tables/event_routes.parquet
tables/market_routes.parquet
tables/manifest.json
```

The manifest records hashes, row counts, expected-table checks, and whether the
materialization verified. Compression and raw-tape removal are explicitly
gated:

```bash
./scripts/ops/predex \
  --materialize \
  --path runs/<run-name> \
  --compress-if-verified \
  --remove-if-verified
```

`--remove-if-verified` removes `tape.bin` only after the expected Parquet tables
and compressed source artifact have been verified.

## Enrich older runs

Historical configs may not contain the event-family and time metadata needed
by newer research. Enrichment writes an overlay and can refresh route tables:

```bash
PYTHONPATH=python/src python3 -m predex.replay enrich-metadata \
  --run-dir runs/<run-name>
```

Use `--runs-root runs` for multiple directories or
`--no-rewrite-route-tables` to write only the metadata overlay.

## Tests

The dependency-free operator/config suite is:

```bash
PYTHONPATH=python/src python3 -m unittest \
  python.tests.tooling.test_env \
  python.tests.tooling.test_discovery \
  python.tests.tooling.test_replay
```

The CI job installs this supported package from a clean checkout before running
the same tests and smoke-checking both console entry points.

## Research boundary

Materialization produces offline input tables; it does not authorize a model or
strategy. Experimental model, cohort, and counterfactual code is kept in a
separate local workspace and is not part of the distributed Python package.
Research outputs must still preserve holdouts and report mechanical
reproduction separately from economic or deployment conclusions.
