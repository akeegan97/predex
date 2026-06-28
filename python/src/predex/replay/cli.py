from __future__ import annotations

import argparse
import json

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
    else:
        parser.error(f"unknown command: {args.command}")

    print(json.dumps(payload, indent=2, sort_keys=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
