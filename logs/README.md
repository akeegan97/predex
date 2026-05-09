# Logs Directory

This directory is the landing zone for generated runtime and replay analysis artifacts.

Intended layout:

- `logs/live/` for default runtime outputs such as tape, audit, and REST trace files
- `logs/replay/` for ad hoc exports such as timelines, latency plots, signal windows, and soak summaries
- `logs/runs/<run_id>/` for normalized offline analysis datasets created by `predex-replay ingest-run` (parquet by default, optional CSV sidecars)

Repository docs should stay descriptive and durable. Large generated outputs should go here instead.
