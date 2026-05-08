# Results

This document keeps measured runtime characteristics out of the main project README while still giving a concrete picture of how the system behaves.

These numbers are not universal guarantees or synthetic benchmarks. They are representative results from live soak and replay analysis artifacts in this repository.

## Representative Latency Snapshot

Source:

- [replay/soak_live_2026-05-04.json](replay/soak_live_2026-05-04.json)

This run recorded `270,615` pipeline probes and `24` transport submissions.

### Median / Mean By Stage

| Stage | Median (ms) | Mean (ms) | Notes |
|---|---:|---:|---|
| `tick_to_signal` | `0.008` | `0.010` | Market-data event applied on shard to strategy signal creation |
| `signal_to_submission` | `0.016` | `0.022` | Signal accepted into shard submit path |
| `submission_to_decision` | `0.010` | `0.030` | OMS decision latency |
| `decision_to_transport` | `0.908` | `2.417` | Local OMS-to-transport dispatch |
| `tick_to_transport_submit` | `0.930` | `2.469` | End-to-end local path before bytes are on the wire |
| `transport_submit_to_response` | `52.666` | `63.263` | Venue/network round trip after request write |
| `tick_to_transport_response` | `53.503` | `65.731` | End-to-end from inbound market event to HTTP response |

## Interpretation

The broad pattern from recent runs has been:

- shard-local strategy evaluation is very fast relative to the full order loop
- the local path to "request written to the wire" is low-single-digit milliseconds on a healthy warm path
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
