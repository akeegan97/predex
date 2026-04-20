# Replay Roadmap

Replay tooling is partially implemented in Python but a dedicated replay executable does not yet exist.

## What Exists Today

- Live `trader_app` writes raw inbound websocket payloads to a binary tape.
- The tape format is stable:

```text
[u32 payload_len_le][payload bytes]...
```

- Sample replay-oriented payload files live under [`docs/replay`](replay).
- Python discovery and config generation tooling exists at `python/src/predex/discovery/`:
  - `config.py` — `build_trader_config` / `build_trader_config_result` generate trader JSON configs from event records
  - `classifier.py` — classifies events into topology kinds
  - `models.py` — `EventRecord`, `ClassifiedEvent`, `TopologyKind`
  - `affinity.py` — `stable_affinity_key`, `stable_event_id`, `stable_market_id`

## What Does Not Exist Yet

- A first-party `replay_app` C++ executable
- A Python tape decoder
- A maintained replay matrix runner
- A paper OMS evaluation harness
- Config-profile override tooling for replay

## Intended Next Steps

1. Build a Python tape decoder that reads the binary tape and emits JSONL or `NormalizedEvent` objects.
2. Add a Python replay driver that can feed synthetic consumers or drive backtesting.
3. Integrate the Python config generator with replay so historical data can be re-run against a generated config.
4. Later, introduce a dedicated C++ replay executable if the runtime side needs direct feed injection.

## Recommended Division of Labor

- C++ for live ingest and low-latency runtime components
- Python for tape decode, replay, discovery, research, and backtesting
