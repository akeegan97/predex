# Docs Guide

This directory mixes a few different kinds of material:

- canonical architecture and design docs
- operator/config references
- generated config/report artifacts
- historical replay outputs and schema captures
- planning/backlog notes

If you are new to the repo, start with the first group and ignore the rest until you need them.

## Start Here

- [../README.md](../README.md) — project overview, current status, safe defaults, and quickstart
- [architecture.md](architecture.md) — runtime topology and thread/queue model
- [oms_design.md](oms_design.md) — OMS ownership, lifecycle flow, and risk behavior
- [ownership_invariants.md](ownership_invariants.md) — concurrency and lifetime rules the runtime relies on
- [design_decisions.md](design_decisions.md) — why the architecture looks the way it does
- [data_contract.md](data_contract.md) — what moves between stages
- [predex-python.md](predex-python.md) — discovery, replay, and config-generation tooling
- [results.md](results.md) — representative soak/replay measurements and where they came from

## Canonical Runtime References

- [architecture.md](architecture.md)
- [oms_design.md](oms_design.md)
- [ownership_invariants.md](ownership_invariants.md)
- [design_decisions.md](design_decisions.md)
- [data_contract.md](data_contract.md)
- [predex-python.md](predex-python.md)
- [results.md](results.md)
- [trader_config.example.json](trader_config.example.json) — example runtime config

## Planning And Backlog

These are useful working notes, but they are not the best entry point for a new reader.

- [planning/platform_evolution.md](planning/platform_evolution.md) — phased plan for evolving from discrete-session runner to always-on service
- [planning/open_backlog.md](planning/open_backlog.md)
- [planning/replay_matrix.md](planning/replay_matrix.md)

## Generated Artifacts

These are example outputs from the discovery/config toolchain, kept under `docs/` for inspection and demos. They are not canonical documentation:

- `generated_config_subset.json` — example output from the Python discovery CLI (`predex.discovery --output`)
- `generated_discovery_report.json` — example report companion to the above

The Python CLI writes its working `generated_config.json` / `generated_config.report.json` wherever `--output` / `--report-output` point — by convention to the repo root or `docs/`, but those files are runtime-produced and not committed.

## Replay Outputs

`replay/` contains historical analysis outputs and example exports produced by replay tooling. Treat it as artifact/examples space, not as source documentation.
