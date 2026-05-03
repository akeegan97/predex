from __future__ import annotations

import argparse
import json
import sys

from predex.env import load_repo_dotenv

from .config import (
    CredentialSettings,
    DiscoverySettings,
    LocalRiskSettings,
    OmsTransportSettings,
    PipelineSettings,
    build_trader_config_result,
)
from .kalshi import KalshiPublicClient
from .models import TopologyKind


def _topology_choice(value: str) -> str:
    return TopologyKind(value).value


def _summary_line(build_report: dict[str, object]) -> str:
    topology_counts = build_report.get("topology_counts", {})
    topology_fragment = ""
    if isinstance(topology_counts, dict) and topology_counts:
        ordered_items = sorted(topology_counts.items())
        topology_fragment = " [" + ", ".join(f"{key}={value}" for key, value in ordered_items) + "]"
    return (
        "predex: included "
        f"{build_report.get('included_event_count', 0)} events / "
        f"{build_report.get('included_market_count', 0)} markets, "
        f"skipped {build_report.get('skipped_event_count', 0)} events"
        f"{topology_fragment}"
    )


def _progress_line(message: str) -> None:
    sys.stderr.write(f"predex: {message}\n")
    sys.stderr.flush()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="predex",
        description="Discover Kalshi events and emit an event-centric Predex trader config.",
    )
    parser.add_argument(
        "--event-ticker",
        action="append",
        default=[],
        help="Fetch a specific event ticker. Repeat to include multiple events.",
    )
    parser.add_argument(
        "--series-ticker",
        help="Discover events under a Kalshi series ticker when explicit event tickers are not provided.",
    )
    parser.add_argument(
        "--status",
        default="open",
        help="Event status filter used with --series-ticker discovery. Default: open.",
    )
    parser.add_argument(
        "--event-limit",
        "--limit",
        dest="event_limit",
        type=int,
        default=50,
        help="Maximum number of events to discover from the API. Default: 50.",
    )
    parser.add_argument(
        "--all-events",
        action="store_true",
        help="Fetch all matching events across paginated GET /events responses instead of stopping at --event-limit.",
    )
    parser.add_argument(
        "--market-limit",
        type=int,
        help="Maximum number of total included markets in the generated config. Events are kept whole and skipped if they would exceed the limit.",
    )
    parser.add_argument(
        "--channel",
        action="append",
        default=[],
        help="Subscription channel to include in the config. Repeat for multiple channels.",
    )
    parser.add_argument(
        "--lifecycle-channel",
        action="append",
        default=[],
        help="Global lifecycle channel to subscribe without market_ticker filters. Default: market_lifecycle_v2.",
    )
    parser.add_argument(
        "--shard-count",
        type=int,
        default=4,
        help="Shard count used by the generated runtime config. Default: 4.",
    )
    parser.add_argument(
        "--frame-pool-capacity",
        type=int,
        default=8192,
        help="Pipeline frame pool capacity written into the generated config. Default: 8192.",
    )
    parser.add_argument(
        "--io-to-router-capacity",
        type=int,
        default=8192,
        help="IO-to-router queue capacity written into the generated config. Default: 8192.",
    )
    parser.add_argument(
        "--router-to-logger-capacity",
        type=int,
        default=8192,
        help="Router-to-logger queue capacity written into the generated config. Default: 8192.",
    )
    parser.add_argument(
        "--shard-input-capacity",
        type=int,
        default=8192,
        help="Per-shard input queue capacity written into the generated config. Default: 8192.",
    )
    parser.add_argument(
        "--shard-to-logger-capacity",
        type=int,
        default=8192,
        help="Per-shard logger queue capacity written into the generated config. Default: 8192.",
    )
    parser.add_argument(
        "--include-topology",
        action="append",
        default=[],
        type=_topology_choice,
        help="Only include classified events with this topology. Repeat to include multiple topologies.",
    )
    parser.add_argument(
        "--exclude-topology",
        action="append",
        default=[],
        type=_topology_choice,
        help="Exclude classified events with this topology. Repeat to exclude multiple topologies.",
    )
    parser.add_argument(
        "--output",
        help="Write the generated config JSON to this path instead of stdout.",
    )
    parser.add_argument(
        "--report-output",
        help="Optional path for a build report describing included/skipped events and topology counts.",
    )
    parser.add_argument(
        "--api-base-url",
        default="https://api.elections.kalshi.com/trade-api/v2",
        help="Kalshi REST API base URL used for discovery.",
    )
    parser.add_argument(
        "--ws-endpoint",
        default="wss://api.elections.kalshi.com/trade-api/ws/v2",
        help="Kalshi websocket endpoint written into the generated config.",
    )
    parser.add_argument(
        "--key-id-env",
        default="KALSHI_KEY_ID",
        help="Environment variable name containing the Kalshi key id.",
    )
    parser.add_argument(
        "--private-key-env",
        default="KALSHI_PRIVATE_KEY_PEM",
        help="Environment variable name containing the Kalshi private key PEM.",
    )
    parser.add_argument(
        "--tape-output",
        default="predex_tape.bin",
        help="Tape output path written into the config.",
    )
    parser.add_argument(
        "--audit-output",
        default="predex_audit.jsonl",
        help="Audit output path written into the config.",
    )
    parser.add_argument(
        "--oms-enabled",
        action="store_true",
        help="Enable live OMS transport wiring in generated config (default: disabled).",
    )
    parser.add_argument(
        "--oms-rest-endpoint",
        default="https://api.elections.kalshi.com",
        help="OMS REST endpoint written into generated config.",
    )
    parser.add_argument(
        "--oms-private-ws-endpoint",
        default="wss://api.elections.kalshi.com/trade-api/ws/v2",
        help="OMS private websocket endpoint written into generated config.",
    )
    parser.add_argument(
        "--oms-private-ws-channel",
        action="append",
        default=[],
        help="OMS private WS channel to include. Repeat for multiple channels. Default: user_orders.",
    )
    parser.add_argument(
        "--oms-available-capital-ticks",
        type=int,
        default=10000,
        help="Portfolio capital budget in ticks for OMS pre-trade gating. Default: 10000.",
    )
    parser.add_argument(
        "--oms-max-session-loss-ticks",
        type=int,
        default=5000,
        help="Maximum tolerated session loss in ticks before halting. Default: 5000.",
    )
    parser.add_argument(
        "--oms-rest-worker-count",
        type=int,
        default=8,
        help="Number of hot REST worker sessions in the generated config. Default: 8.",
    )
    parser.add_argument(
        "--local-risk-max-net-position-lots-per-market",
        type=int,
        default=200,
        help="Maximum absolute net filled position per market. Default: 200.",
    )
    parser.add_argument(
        "--local-risk-min-seconds-to-close",
        type=int,
        default=300,
        help="Reject intents for markets closing within this many seconds. Default: 300.",
    )
    parser.add_argument(
        "--local-risk-trading-enabled",
        action="store_true",
        help="Enable local risk trading in the generated config (default: disabled).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    load_repo_dotenv()
    parser = build_parser()
    args = parser.parse_args(argv)

    channels = tuple(args.channel) if args.channel else ("trade", "orderbook_delta")
    lifecycle_channels = tuple(args.lifecycle_channel) if args.lifecycle_channel else ("market_lifecycle_v2",)
    discovery = DiscoverySettings(
        endpoint=args.ws_endpoint,
        channels=channels,
        lifecycle_channels=lifecycle_channels,
        credentials=CredentialSettings(
            key_id_env=args.key_id_env,
            private_key_pem_env=args.private_key_env,
        ),
    )
    pipeline = PipelineSettings(
        shard_count=args.shard_count,
        frame_pool_capacity=args.frame_pool_capacity,
        io_to_router_capacity=args.io_to_router_capacity,
        router_to_logger_capacity=args.router_to_logger_capacity,
        shard_input_capacity=args.shard_input_capacity,
        shard_to_logger_capacity=args.shard_to_logger_capacity,
    )
    oms_transport = OmsTransportSettings(
        enabled=args.oms_enabled,
        rest_endpoint=args.oms_rest_endpoint,
        private_ws_endpoint=args.oms_private_ws_endpoint,
        private_ws_channels=tuple(args.oms_private_ws_channel)
        if args.oms_private_ws_channel
        else ("user_orders",),
        max_session_loss_ticks=args.oms_max_session_loss_ticks,
        available_capital_ticks=args.oms_available_capital_ticks,
        rest_worker_count=args.oms_rest_worker_count,
    )
    local_risk = LocalRiskSettings(
        max_net_position_lots_per_market=args.local_risk_max_net_position_lots_per_market,
        min_seconds_to_close=args.local_risk_min_seconds_to_close,
        trading_enabled=args.local_risk_trading_enabled,
    )

    event_limit = None if args.all_events else args.event_limit

    client = KalshiPublicClient(
        base_url=args.api_base_url,
        progress_callback=_progress_line,
    )
    events = client.discover_events(
        event_tickers=args.event_ticker or None,
        series_ticker=args.series_ticker,
        status=args.status,
        limit=event_limit,
    )
    build_result = build_trader_config_result(
        events,
        discovery=discovery,
        pipeline=pipeline,
        oms_transport=oms_transport,
        local_risk=local_risk,
        tape_output_path=args.tape_output,
        audit_output_path=args.audit_output,
        include_topologies=args.include_topology or None,
        exclude_topologies=args.exclude_topology or None,
        market_limit=args.market_limit,
    )
    config = build_result.config
    report = build_result.report()

    payload = json.dumps(config, indent=2, sort_keys=False)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as output_file:
            output_file.write(payload)
            output_file.write("\n")
    else:
        sys.stdout.write(payload)
        sys.stdout.write("\n")

    if args.report_output:
        with open(args.report_output, "w", encoding="utf-8") as report_file:
            report_file.write(json.dumps(report, indent=2, sort_keys=False))
            report_file.write("\n")
    elif args.output:
        sys.stderr.write(json.dumps(report, indent=2, sort_keys=False))
        sys.stderr.write("\n")
    sys.stderr.write(_summary_line(report))
    if args.output:
        sys.stderr.write(f" | config={args.output}")
    if args.report_output:
        sys.stderr.write(f" | report={args.report_output}")
    sys.stderr.write("\n")
    return 0
