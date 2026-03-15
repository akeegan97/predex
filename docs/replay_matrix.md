# Replay Matrix Workflow

This repo now includes a deterministic replay path to compare paper OMS behavior across three profiles:

- `baseline`
- `latency`
- `stress`

## Inputs

- Base runtime config: `docs/trader_config.example.json`
- Profile overrides:
  - `docs/profiles/paper_oms.baseline.override.json`
  - `docs/profiles/paper_oms.latency.override.json`
  - `docs/profiles/paper_oms.stress.override.json`
- Fixed replay payload set: `docs/replay/top3_markets_fixed.jsonl`

## Record Live Tape (Optional)

You can record real inbound WS payloads from `trader_app` into JSONL:

```bash
timeout --signal=INT 900s ./build/dev/cpp/trader_app \
  --config docs/trader_config.example.json \
  --record-jsonl /tmp/kalshi_tape.jsonl
```

Each inbound WS payload is written as one JSON line.

Then replay that tape:

```bash
REPEAT=1 PUSH_BATCH=16 MAX_DRAIN=50000 \
  scripts/run_replay_matrix.sh \
  docs/trader_config.example.json \
  /tmp/kalshi_tape.jsonl
```

## Run Matrix

```bash
REPEAT=300 PUSH_BATCH=16 MAX_DRAIN=50000 \
  scripts/run_replay_matrix.sh \
  docs/trader_config.example.json \
  docs/replay/top3_markets_fixed.jsonl
```

Each profile run emits one `replay.summary` line with comparable counters/latencies.

To push faster than receive-time, increase:

- `REPEAT` (replay tape multiple times)
- `PUSH_BATCH` (inject larger bursts per pump step)

## Useful Fields

- `parser_rejects`, `book_apply_rejects`, `parse_errors_total`
- `ingest_q_hwm`, `shard_q_hwm_total`, `shard_q_hwm_max`
- `strategy_intents_emitted`, `strategy_intents_submitted`, `strategy_risk_rejects`
- `oms_submitted`, `oms_sent`, `oms_rejects`, `oms_pending_q_hwm`
- `oms_latency_p95_ns`, `oms_latency_p99_ns`
