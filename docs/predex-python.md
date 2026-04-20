# Predex Python Toolchain

The Python toolchain lives in `python/src/predex/` and provides two CLI tools:

- **`predex`** (`predex.discovery`) — discover Kalshi events via the REST API and generate a trader config JSON for `trader_app`
- **`predex-replay`** (`predex.replay`) — analyze audit logs and tape files produced by a live run

Both CLIs are wrapped by scripts in `scripts/` that source `.env` and set `PYTHONPATH` automatically.

---

## Credentials and Environment

Credentials are loaded from the environment. The toolchain searches for a `.env` file starting from the current directory and walking up to ancestors:

```bash
KALSHI_KEY_ID=your-key-id
KALSHI_PRIVATE_KEY_PEM='-----BEGIN PRIVATE KEY-----
...
-----END PRIVATE KEY-----'
```

You can also point to a specific env file:

```bash
PREDEX_ENV_FILE=/path/to/.env ./scripts/predex ...
```

The credential variable names written into generated configs (`key_id_env`, `private_key_pem_env`) default to `KALSHI_KEY_ID` and `KALSHI_PRIVATE_KEY_PEM` and can be overridden with `--key-id-env` / `--private-key-env`.

---

## `predex` — Event Discovery and Config Generation

### What it does

Calls the Kalshi REST API to fetch events and their markets, classifies each event into a topology kind, and emits a `trader_app`-compatible JSON config with stable `market_id`, `event_id`, and `affinity_key` values.

### Quick examples

Fetch specific events by ticker:
```bash
./scripts/predex \
  --event-ticker KXPGATOUR-VATO26 \
  --event-ticker KXWMARMAD-26 \
  --output docs/generated_config.json
```

Discover all open events under a series:
```bash
./scripts/predex \
  --series-ticker KXPGATOUR \
  --all-events \
  --include-topology monotonic_chain \
  --output docs/generated_config.json \
  --report-output docs/generated_config.report.json
```

Limit total markets and enable live trading:
```bash
./scripts/predex \
  --series-ticker KXPGATOUR \
  --market-limit 60 \
  --oms-enabled \
  --output docs/generated_config.json
```

### All arguments

#### Event selection

| Argument | Default | Description |
|---|---|---|
| `--event-ticker TICKER` | — | Fetch a specific event ticker. Repeatable. |
| `--series-ticker TICKER` | — | Discover events under this series (used when explicit tickers not given). |
| `--status STATUS` | `open` | Event status filter for series discovery. |
| `--event-limit N` / `--limit N` | `50` | Max events fetched from the API. |
| `--all-events` | off | Page through all matching events (ignores `--event-limit`). |
| `--market-limit N` | — | Max total included markets. Events are kept whole; an event is skipped if adding it would exceed the limit. |

#### Topology filtering

| Argument | Description |
|---|---|
| `--include-topology TOPOLOGY` | Only include events with this topology. Repeatable. |
| `--exclude-topology TOPOLOGY` | Exclude events with this topology. Repeatable. |

Valid topology values: `monotonic_chain`, `mutually_exclusive`, `unordered_group`, `single_market`, `unknown`.

#### Pipeline config

| Argument | Default | Description |
|---|---|---|
| `--shard-count N` | `4` | Number of shard threads. |
| `--frame-pool-capacity N` | `8192` | Frame pool slot count. |
| `--io-to-router-capacity N` | `4096` | IO-to-router queue capacity. |
| `--router-to-logger-capacity N` | `4096` | Router-to-logger queue capacity. |
| `--shard-input-capacity N` | `1024` | Per-shard input queue capacity. |
| `--shard-to-logger-capacity N` | `1024` | Per-shard logger queue capacity. |

#### Subscription channels

| Argument | Default | Description |
|---|---|---|
| `--channel CHANNEL` | `trade`, `orderbook_delta` | Market data subscription channels. Repeatable. |
| `--lifecycle-channel CHANNEL` | `market_lifecycle_v2` | Lifecycle channels (no per-market filter). Repeatable. |

#### OMS and risk

| Argument | Default | Description |
|---|---|---|
| `--oms-enabled` | off | Enable live OMS transport in generated config. |
| `--oms-rest-endpoint URL` | Kalshi prod | OMS REST endpoint. |
| `--oms-private-ws-endpoint URL` | Kalshi prod | OMS private WS endpoint. |
| `--oms-private-ws-channel CHANNEL` | `user_orders` | OMS private WS channels. Repeatable. |

#### Output and endpoints

| Argument | Default | Description |
|---|---|---|
| `--output PATH` | stdout | Write config JSON to file. |
| `--report-output PATH` | — | Write a build report (included/skipped events, topology counts). |
| `--tape-output PATH` | `predex_tape.bin` | Tape output path embedded in config. |
| `--audit-output PATH` | `predex_audit.jsonl` | Audit output path embedded in config. |
| `--api-base-url URL` | Kalshi prod | Kalshi REST API base URL for discovery. |
| `--ws-endpoint URL` | Kalshi prod | WebSocket endpoint embedded in config. |
| `--key-id-env VAR` | `KALSHI_KEY_ID` | Env var name for key ID embedded in credentials block. |
| `--private-key-env VAR` | `KALSHI_PRIVATE_KEY_PEM` | Env var name for private key PEM. |

### Build report

With `--report-output`, a JSON file is written alongside the config:

```json
{
  "included_event_count": 12,
  "included_market_count": 48,
  "skipped_event_count": 3,
  "topology_counts": { "monotonic_chain": 10, "mutually_exclusive": 2 },
  "included_events": [...],
  "skipped_events": [
    {
      "event_ticker": "KXFOO-26",
      "topology_kind": "unordered_group",
      "reason": "excluded by include_topologies filter: unordered_group"
    }
  ]
}
```

---

## `predex-replay` — Post-Run Analysis

### What it needs

All subcommands require at minimum a `--config` (generated trader config) and an `--audit` (JSONL audit log from `trader_app`). Some subcommands also require a `--tape` (binary tape file).

### Subcommands

#### `audit-summary`

Quick statistical overview of a run.

```bash
./scripts/predex-replay audit-summary \
  --config docs/generated_config.json \
  --audit predex_audit.jsonl \
  --limit 10
```

| Argument | Default | Description |
|---|---|---|
| `--config PATH` | required | Trader config JSON. |
| `--audit PATH` | required | Audit JSONL from `trader_app`. |
| `--limit N` | `10` | Number of highest-edge group signals to display. |

Output: JSON with `audit_kind_counts`, `group_signal_count`, and a ranked `top_group_signals` list.

---

#### `inspect-signal`

Verify a single signal against the order book tape.

```bash
./scripts/predex-replay inspect-signal \
  --config docs/generated_config.json \
  --audit predex_audit.jsonl \
  --tape predex_tape.bin \
  --signal-id 42 \
  --shard-id 0
```

| Argument | Required | Description |
|---|---|---|
| `--config PATH` | yes | Trader config JSON. |
| `--audit PATH` | yes | Audit JSONL. |
| `--tape PATH` | yes | Binary tape. |
| `--signal-id ID` | yes | Signal ID from the audit log. |
| `--shard-id ID` | yes | Shard ID that generated the signal. |

Output: JSON with signal details, leg prices, recomputed edge vs audited edge, fee impact, and OMS decision summary.

---

#### `export-event-timeline`

Replay the order book for a full event and export a time-series of top-of-book progression alongside signal hits.

```bash
./scripts/predex-replay export-event-timeline \
  --config docs/generated_config.json \
  --audit predex_audit.jsonl \
  --tape predex_tape.bin \
  --event-id 88422102 \
  --output-dir docs/replay \
  --prefix pgatour_event
```

With a single-market focus:
```bash
./scripts/predex-replay export-event-timeline \
  --config docs/generated_config.json \
  --audit predex_audit.jsonl \
  --tape predex_tape.bin \
  --market-ticker KXPGATOUR-VATO26-JSPA \
  --output-dir docs/replay \
  --prefix jspa_market \
  --parquet
```

| Argument | Default | Description |
|---|---|---|
| `--config PATH` | required | Trader config JSON. |
| `--audit PATH` | required | Audit JSONL. |
| `--tape PATH` | required | Binary tape. |
| `--event-id ID` | — | Event ID to export. |
| `--market-ticker TICKER` | — | Restrict to a single market within the event. |
| `--output-dir DIR` | `docs/replay` | Output directory. |
| `--prefix NAME` | `event_timeline` | Filename prefix for all outputs. |
| `--parquet` | off | Also write `.parquet` files (requires `pyarrow`). |

Outputs written to `output-dir/prefix.*`:

| File | Contents |
|---|---|
| `*.csv` | Top-of-book progression for all markets in the event |
| `*.signals.csv` | Signal hit rows with recomputed edge vs audited edge |
| `*.summary.json` | Machine-readable summary; used by the replay dashboard |
| `*.html` | Standalone interactive chart (no server required) |
| `*.parquet` / `*.signals.parquet` | Parquet equivalents (with `--parquet`) |

---

#### `latency-histograms`

Analyse latency span distributions from the audit log.

```bash
./scripts/predex-replay latency-histograms \
  --audit predex_audit.jsonl \
  --backend plotly \
  --output-html docs/replay/latency.html \
  --output-csv docs/replay/latency.csv
```

| Argument | Default | Description |
|---|---|---|
| `--audit PATH` | required | Audit JSONL. |
| `--backend` | `plotly` | Plot backend: `plotly`, `matplotlib`, or `both`. |
| `--output-html PATH` | `docs/replay/latency_histograms.html` | HTML output path. |
| `--output-png-prefix PATH` | — | PNG prefix; writes `<prefix>.hist.png` and `<prefix>.trend.png`. |
| `--output-csv PATH` | `docs/replay/latency_histograms.csv` | CSV of raw latency rows. |
| `--output-json PATH` | — | Optional JSON summary. |
| `--event-id ID` | — | Filter to a single event. |
| `--market-id ID` | — | Filter to a single market. |
| `--kinds KINDS_CSV` | all default kinds | Comma-separated audit event kinds to include. |
| `--spans SPANS_CSV` | all default spans | Comma-separated span field names to plot. |
| `--bins N` | `80` | Histogram bin count. |
| `--max-ms FLOAT` | — | Upper cap in ms; drops values above this. |
| `--time-bucket-ms N` | `1000` | Bucket size for time-trend chart. |
| `--histogram-every-seconds N` | — | Emit time-sliced histogram panels every N runtime seconds (matplotlib/both). |
| `--max-bucket-plots N` | `24` | Max time-slice panels per span. |

Default latency span fields analysed:

| Field | Description |
|---|---|
| `tick_to_signal_ns` | Tick received → strategy signal |
| `signal_to_submission_ns` | Signal → intent enqueued to OMS |
| `submission_to_decision_ns` | Intent enqueued → OMS decision |
| `decision_to_transport_ns` | OMS decision → REST submit dispatched |
| `transport_to_first_fill_ns` | REST submit → first fill received |
| `tick_to_first_fill_ns` | End-to-end: tick received → first fill |
| `tick_to_terminal_ns` | End-to-end: tick received → order terminal |

---

### Replay Dashboard (Streamlit)

```bash
.venv/bin/pip install '.[replay-viz]'
./scripts/predex-replay-dashboard
```

Shows a run-wide view from config + audit (all events, sub-markets, signals) and lets you drill into per-market timeline charts when `export-event-timeline` summary files are present in `docs/replay/`.

---

## Config Generation Flow

```
KalshiPublicClient.discover_events()
        │
        ▼
list[EventRecord]           (event_ticker, series_ticker, markets[])
        │
        ▼
classifier.classify_event()
        │
        ▼
list[ClassifiedEvent]       (topology_kind, markets with strike_keys)
        │
        ▼
build_trader_config_result()
        │
        ├── topology filtering (include/exclude)
        ├── market_limit gating (whole-event granularity)
        ├── stable ID generation
        │     stable_event_id(event_ticker)   → event_id   [1, 2^32-2]
        │     stable_affinity_key(event_ticker) → affinity_key [0, 2^16-1]
        │     stable_market_id(market_ticker) → market_id  [1, 2^32-2]
        │     (all Blake2B-based; deterministic across runs)
        ├── market route construction per market
        │     { market_ticker, market_id, event_id, affinity_key,
        │       topology_kind, strike_key, close_time_s, tradeable }
        └── final config dict
```

### Topology classification

| Topology | Detection rule |
|---|---|
| `single_market` | Event has exactly one market |
| `monotonic_chain` | All markets have comparable numeric strike types (greater/less) — ordered by threshold; or all markets have distinct close-times with a direction hint (before/after) in titles |
| `mutually_exclusive` | Exchange `mutually_exclusive` flag is set; or all markets share the same close-time and have structurally distinct entities |
| `unordered_group` | Fallback |

### Strike keys

| Pattern | Key value |
|---|---|
| Numeric threshold (greater) | `int(threshold * 1_000_000)` |
| Numeric threshold (less) | `int(threshold * 1_000_000)` negated |
| Time-based (after) | Nanoseconds since epoch |
| Time-based (before) | Nanoseconds since epoch, negated |
| Mutually exclusive / unknown | `0` |

The shard affinity assignment is `affinity_key % shard_count`. All markets in the same event share an affinity key so they land on the same shard.

---

## Programmatic API

The discovery and config modules can be used directly from Python:

```python
from predex.discovery.kalshi import KalshiPublicClient
from predex.discovery.config import (
    build_trader_config_result,
    DiscoverySettings,
    PipelineSettings,
    OmsTransportSettings,
    LocalRiskSettings,
)

client = KalshiPublicClient()
events = client.discover_events(series_ticker="KXPGATOUR", limit=50)

result = build_trader_config_result(
    events,
    pipeline=PipelineSettings(shard_count=4),
    oms_transport=OmsTransportSettings(enabled=False, max_session_loss_ticks=500),
    local_risk=LocalRiskSettings(
        max_net_position_lots_per_market=10,
        min_seconds_to_close=60,
        trading_enabled=True,
    ),
    include_topologies=["monotonic_chain"],
    market_limit=80,
)

import json
with open("config.json", "w") as f:
    json.dump(result.config, f, indent=2)

print(result.report())
```

---

## Module Layout

```
python/src/predex/
├── discovery/
│   ├── __main__.py          # Entry point: python -m predex.discovery
│   ├── cli.py               # Argument parsing and CLI wiring
│   ├── models.py            # MarketRecord, EventRecord, ClassifiedEvent, TopologyKind
│   ├── classifier.py        # classify_event() — topology detection and strike key assignment
│   ├── affinity.py          # stable_event_id / stable_market_id / stable_affinity_key
│   ├── kalshi.py            # KalshiPublicClient — REST API with retry and pagination
│   └── config.py            # build_trader_config_result / build_trader_config
└── replay/
    ├── __main__.py          # Entry point: python -m predex.replay
    ├── cli.py               # Subcommand routing
    ├── audit.py             # AuditEvent, SignalBundle, load_audit_events
    ├── config.py            # ConfigIndex, load_config_index
    ├── tape.py              # iter_tape_payloads, iter_market_events (binary tape reader)
    ├── books.py             # ReplayBookState — order book replay from tape
    ├── timeline.py          # build_event_timeline, export functions
    ├── verify.py            # verify_signal_bundle — recompute edge vs audit
    ├── latency.py           # export_latency_histograms
    └── dashboard.py         # Streamlit replay dashboard
```
