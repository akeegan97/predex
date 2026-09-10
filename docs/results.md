# Validation Results

This document records bounded evidence for the current runtime. It separates
what a run exercised from what it did not exercise; a healthy capture is not
automatically an execution or strategy validation.

## 2026-09-08 live-system soak

Runtime commit:

```text
491aba15e19fd32b157e6c5d7f1b08fa224715f3
```

Config SHA-256:

```text
9bf9afac744dad09a6d091f2f9309b0f3e9c8f47b5c3724ab2b99aacbad1ad15
```

The raw tape is intentionally not committed. The final operator snapshot was
taken before graceful shutdown of a four-shard, low-latency-profile session
with:

- frame pool: `65,536` slots;
- router and shard queues: `32,768` entries;
- public order-book, trade, and lifecycle channels enabled;
- order REST and private order feed enabled;
- monotonic-arbitrage strategy enabled;
- strategy allocation: `50,000` money ticks (`$5.00`).

### Throughput and persistence

| Metric | Final value |
|---|---:|
| Frames received | `119,483,082` |
| Frames published | `119,408,477` |
| Order-book frames observed | `119,142,101` |
| Trade frames observed | `252,372` |
| Lifecycle frames observed | `88,609` |
| Tape records written | `119,408,073` |
| Tape bytes written | `40,215,055,365` |
| Logger write failures | `0` |
| Logger recycle failures | `0` |

`74,605` frames were classified as dropped at the wire boundary. The total was
fully accounted for by `72,231` lifecycle messages for markets outside the
configured universe and `2,374` unsupported envelope types. Pool exhaustion,
router enqueue failure, and logger fallback failure were all zero.

### Integrity and capacity

| Metric | Final value |
|---|---:|
| Sequence gaps | `0` |
| Duplicate/stale sequences | `0` |
| Downstream delivery losses | `0` |
| Market/subscription barriers | `0` |
| Shard parse/event rejects | `0` |
| Shard desyncs | `0` |
| Recovery incidents | `0` |
| Leaked handles | `0` |
| Frame-pool high-water | `22,228 / 65,536` |
| Router-queue high-water | `74 / 32,768` |
| Maximum shard-queue high-water | `6,275 / 32,768` |

The capacity peaks left meaningful headroom. This run did not deliberately
inject loss, so it validates that recovery remained dormant during a healthy
session, not that every recovery branch works against the live venue.

### Order-book ingress latency

`ingress_to_book_apply` for order-book traffic:

| Statistic | Duration |
|---|---:|
| p50 | `5 us` |
| p95 | `10 us` |
| p99 | `25 us` |
| p99.9 | `250 us` |
| mean | `34.6 us` |
| maximum | `184.6 ms` |

The isolated maximum is consistent with a scheduler/system pause. The stable
p99.9 is the useful evidence that it was not a sustained latency regime.

### OMS and venue connectivity

| Metric | Final value |
|---|---:|
| Portfolio reconciliations requested/completed | `6,608 / 6,608` |
| Portfolio reconciliation failures | `0` |
| REST requests/responses | `13,216 / 13,216` |
| REST retries/failures | `0 / 0` |
| Private-feed messages received | `3` |
| Private-feed drops/parse failures | `0 / 0` |
| Strategy intents | `0` |
| Live/pending/uncertain orders | `0 / 0 / 0` |
| Execution incidents | `0` |

The venue available balance was `298,800` money ticks (`$29.88`) and the
configured strategy allocation remained `$5.00`.

### What this run establishes

- The public ingestion, router, shard, and tape path sustained more than 119
  million frames without detected integrity or capacity loss.
- Persistent order REST supported thousands of successful reconciliation
  cycles without reconnect or retry.
- The private order feed remained connected for the session.
- Trading authorization and the complete component-readiness graph remained
  live.

### What this run does not establish

- No strategy intent was emitted, so submit, fill, partial-group, and repair
  mechanics were not exercised by this session.
- Current operator telemetry does not expose raw strategy candidates and gate
  rejections, so zero intents cannot yet be decomposed further.
- Neither public nor private websocket disconnected, so automatic reconnect is
  still an unimplemented and untested operational gap.
- These results do not justify increasing trading limits.

## Local merge-gate rehearsal

On the documentation/merge-preparation branch, an index-only clean checkout
configured and built the complete public C++ graph. CTest then passed:

```text
predex_tests  passed
1/1 distributed test executable passed
```

The supported Python tooling suite passed `50` unit tests with the optional
Arrow dependency installed. In the dependency-free CI environment, the one
materialization test is skipped and the remaining `49` pass:

```text
python.tests.tooling.test_env
python.tests.tooling.test_discovery
python.tests.tooling.test_replay
```

The ASan/UBSan preset built and the distributed runtime tests passed with local
leak detection disabled. LeakSanitizer could not initialize under the development
environment's ptrace restriction, so leak detection remains a separate clean-
runner check rather than a claimed local pass.

Clang-tidy remains an advisory baseline with existing diagnostics. Build and
tests are the required merge gates while that debt is reduced explicitly; the
workflow does not represent static-analysis debt as a clean result.

## Historical artifacts

`docs/replay/` contains older charts, CSV summaries, and replay outputs. They
are retained as research history, not presented as measurements of the current
runtime architecture.
