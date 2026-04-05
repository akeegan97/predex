# Replay Roadmap

Replay tooling is planned, but it is not part of the supported runtime today.

## What Exists Today

- live `trader_app` can write raw inbound websocket payloads to a binary tape
- the tape format is simple and stable enough to build tooling around:

```text
[u32 payload_len_le][payload bytes]...
```

- sample replay-oriented payload files live under [`docs/replay`](replay)

## What Does Not Exist Yet

- a first-party `replay_app`
- a maintained replay matrix runner
- a paper OMS evaluation harness
- config-profile override tooling

Older notes in this repository may refer to those workflows, but they should be treated as planned work, not current capability.

## Intended Next Steps

1. Build a Python tape decoder.
2. Add a Python replay driver that can emit JSONL or feed synthetic consumers.
3. Add market discovery and config generation on top of recorded tape.
4. Later, introduce a dedicated replay executable if the C++ side needs one.

## Recommended Direction

The current division of labor should likely be:
- C++ for live ingest and low-latency runtime components
- Python for tape decode, replay, discovery, research, and backtesting
