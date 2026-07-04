from __future__ import annotations

import argparse
import json

from .config_summary import format_config_summary, summarize_config
from .inspect import inspect_market_data_tape


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="predex-replay",
        description="Inspect PredEx market-data tapes.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    inspect_tape = subparsers.add_parser(
        "inspect-tape",
        help="Inspect the current PredEx market-data binary tape format.",
    )
    inspect_tape.add_argument("--config", required=True, help="PredEx app config JSON.")
    inspect_tape.add_argument("--tape", required=True, help="Market-data tape emitted by predex.")
    inspect_tape.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Number of sample records to include. Default: 20.",
    )

    config_summary = subparsers.add_parser(
        "config-summary",
        help="Summarize the market/event distribution in a PredEx app config.",
    )
    config_summary.add_argument("--config", required=True, help="PredEx app config JSON.")
    config_summary.add_argument(
        "--top-events",
        type=int,
        default=20,
        help="Number of largest events to include. Default: 20.",
    )
    config_summary.add_argument(
        "--sample-tickers",
        type=int,
        default=5,
        help="Number of representative market tickers to show per large event. Default: 5.",
    )
    config_summary.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON instead of the default text summary.",
    )

    enrich_metadata = subparsers.add_parser(
        "enrich-metadata",
        help="Fetch event metadata for existing run directories and refresh route parquet tables.",
    )
    enrich_metadata.add_argument(
        "--run-dir",
        action="append",
        default=[],
        help="Run directory to enrich. Repeat to enrich multiple runs.",
    )
    enrich_metadata.add_argument(
        "--runs-root",
        help="Directory containing run subdirectories to enrich, for example: runs.",
    )
    enrich_metadata.add_argument(
        "--api-base-url",
        default="https://api.elections.kalshi.com/trade-api/v2",
        help="Kalshi trade API base URL. Default: https://api.elections.kalshi.com/trade-api/v2.",
    )
    enrich_metadata.add_argument(
        "--event-fetch-workers",
        type=int,
        default=8,
        help="Concurrent event detail fetch workers. Default: 8.",
    )
    enrich_metadata.add_argument(
        "--no-rewrite-route-tables",
        action="store_true",
        help="Only write event_metadata.json; do not rewrite event_routes/market_routes parquet tables.",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "inspect-tape":
        payload = inspect_market_data_tape(
            config_path=args.config,
            tape_path=args.tape,
            sample_limit=args.limit,
        )
        print(json.dumps(payload, indent=2, sort_keys=False))
    elif args.command == "config-summary":
        payload = summarize_config(
            config_path=args.config,
            top_events=args.top_events,
            sample_tickers=args.sample_tickers,
        )
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=False))
        else:
            print(format_config_summary(payload))
    elif args.command == "enrich-metadata":
        from .metadata_overlay import enrich_run_metadata

        results = enrich_run_metadata(
            run_dirs=args.run_dir or None,
            runs_root=args.runs_root,
            api_base_url=args.api_base_url,
            event_fetch_workers=args.event_fetch_workers,
            rewrite_route_tables=not args.no_rewrite_route_tables,
        )
        print(
            json.dumps(
                {
                    "ok": True,
                    "runs": [
                        {
                            "run_dir": str(result.run_dir),
                            "metadata_path": str(result.metadata_path),
                            "event_count": result.event_count,
                            "fetched_event_count": result.fetched_event_count,
                            "missing_event_count": result.missing_event_count,
                            "route_rows": result.route_rows,
                        }
                        for result in results
                    ],
                },
                indent=2,
                sort_keys=False,
            )
        )
    else:
        parser.error(f"unknown command: {args.command}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
