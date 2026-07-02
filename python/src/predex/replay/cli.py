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
    else:
        parser.error(f"unknown command: {args.command}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
