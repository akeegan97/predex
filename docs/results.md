# Results

This document keeps measured runtime characteristics out of the main project README while still giving a concrete picture of how the system behaves.

These numbers are not universal guarantees or synthetic benchmarks. They are representative results from live soak and replay analysis artifacts in this repository.

## Representative Latency Snapshot

Source:

- [../logs/runs/live_2026_05_07/manifest.json](../logs/runs/live_2026_05_07/manifest.json)
- [../logs/runs/live_2026_05_07/latencies.parquet](../logs/runs/live_2026_05_07/latencies.parquet)
- [../logs/runs/live_2026_05_07/audit_events.parquet](../logs/runs/live_2026_05_07/audit_events.parquet)

This run recorded `502,482` pipeline probes and `24` transport submissions before warm-state trimming.

Warm-state trim used for the table below:

- drop the first `128` pipeline probes to remove early shard wake-up and allocation noise
- drop the first `4` order-path rows per relevant audit kind, which corresponds to the first two two-leg transport attempts on this run

### Steady-State By Stage

| Stage | Samples | P50 (ms) | P95 (ms) | P99 (ms) | Mean (ms) | Max (ms) | Notes |
|---|---:|---:|---:|---:|---:|---:|---|
| `tick_to_signal` | `502,354` | `0.005653` | `0.011279` | `0.019131` | `0.006575` | `6.900106` | Market-data event applied on shard to strategy signal creation |
| `signal_to_submission` | `20` | `0.018530` | `0.025629` | `0.025629` | `0.016588` | `0.025629` | Shard-local signal accepted into the submit path |
| `submission_to_decision` | `20` | `0.005879` | `0.019056` | `0.021341` | `0.007297` | `0.021912` | OMS decision latency after the shard enqueues work |
| `decision_to_transport` | `20` | `0.720151` | `19.381644` | `19.387957` | `7.475000` | `19.389535` | Local OMS-to-transport dispatch; tails show connection/session wake-up cost |
| `tick_to_transport_submit` | `20` | `0.741410` | `19.416481` | `19.416481` | `7.498884` | `19.416481` | End-to-end local path before bytes are on the wire |
| `transport_submit_to_response` | `20` | `53.525742` | `63.400955` | `63.400955` | `54.193251` | `63.400955` | Venue/network round trip after request write |
| `tick_to_transport_response` | `20` | `55.627107` | `80.091110` | `80.091110` | `61.692136` | `80.091110` | End-to-end from inbound market event to HTTP response |

The main publication number for this run is therefore steady-state `tick_to_transport_response` at about `55.6 ms` p50, with venue/network RTT still dominating the full path.

## Interpretation

The broad pattern from recent runs has been:

- shard-local strategy evaluation is very fast relative to the full order loop
- the local path to "request written to the wire" is sub-millisecond at p50 on a healthy warm path, with a remaining long tail when transport-side wake-up shows through
- venue/network round trip dominates end-to-end order latency
- most recent strategy work has therefore focused more on execution quality and book durability than on squeezing tiny amounts out of the local compute path

## Why These Numbers Matter

Predex is not trying to claim exchange-like microsecond wire performance on a websocket + JSON venue. The useful engineering questions here are:

- is the internal runtime architecture bounded and observable?
- can order intent timing be decomposed stage by stage?
- can failed executions be diagnosed from tape, audit, and REST traces?
- can strategy behavior be improved empirically from soak results?

These measurements are intended to answer those questions, not to act as a marketing benchmark.

## Additional Result Artifacts

Historical and supporting artifacts live in:

- `docs/replay/` — soak summaries, edge-lifetime analysis, exported timelines, charts
- `logs/runs/` — normalized run datasets produced by `predex-replay ingest-run`

Useful examples:

- [../logs/runs/live_2026_05_07/manifest.json](../logs/runs/live_2026_05_07/manifest.json)
- [replay/soak_analysis.json](replay/soak_analysis.json)
- [replay/soak_analysis_2026-05-03.json](replay/soak_analysis_2026-05-03.json)
- [replay/soak_analysis_overnight_2026-05-03.json](replay/soak_analysis_overnight_2026-05-03.json)
- [replay/soak_live_2026-05-04.json](replay/soak_live_2026-05-04.json)

## Future Expansion

If this document grows, the next additions should probably be:

- one small table for execution outcomes from representative soaks
- one section on one-sided fill mitigation before and after depth-aware IOC gating
- one section on when dynamic sizing became justified, if it does

That keeps `README.md` lightweight while preserving the evidence trail for people who want to go deeper.
