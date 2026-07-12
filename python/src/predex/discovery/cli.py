from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
import re
import sys

from predex.env import load_repo_dotenv

from .app_config import (
    KalshiMarketDataSettings,
    KalshiSettings,
    RuntimeSettings,
    build_app_config_result,
)
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

DEFAULT_TAPE_OUTPUT = "logs/live/predex_tape.bin"
DEFAULT_AUDIT_OUTPUT = "logs/live/predex_audit.jsonl"
DEFAULT_RUN_DIR_ROOT = "runs"


@dataclass(frozen=True, slots=True)
class RunArtifacts:
    run_dir: Path | None
    output: str | None
    report_output: str | None
    tape_output: str
    audit_output: str


def _topology_choice(value: str) -> str:
    return TopologyKind(value).value


def _slugify(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "run"


def _default_run_label(args: argparse.Namespace) -> str:
    parts: list[str] = []
    if args.all_events:
        parts.append("all-events")
    elif args.event_ticker:
        parts.append("selected-events")
    elif args.series_ticker:
        parts.append(args.series_ticker)
    else:
        parts.append(f"{args.status}-events")

    if args.include_topology:
        parts.extend(sorted(args.include_topology))
    elif args.exclude_topology:
        parts.append("filtered")

    return _slugify("-".join(parts))


def _resolve_run_artifacts(
    args: argparse.Namespace,
    *,
    now: datetime | None = None,
) -> RunArtifacts:
    run_dir_root = args.run_dir_root
    if not run_dir_root and args.run_label:
        run_dir_root = DEFAULT_RUN_DIR_ROOT

    if not run_dir_root:
        return RunArtifacts(
            run_dir=None,
            output=args.output,
            report_output=args.report_output,
            tape_output=args.tape_output or DEFAULT_TAPE_OUTPUT,
            audit_output=args.audit_output or DEFAULT_AUDIT_OUTPUT,
        )

    timestamp = (now or datetime.now(UTC)).strftime("%Y-%m-%d-%H%M%S")
    label = _slugify(args.run_label) if args.run_label else _default_run_label(args)
    run_dir = Path(run_dir_root) / f"predex-{timestamp}-market-data-{label}"

    if run_dir.exists() and not args.overwrite_run_dir:
        raise FileExistsError(f"run directory already exists: {run_dir}")

    return RunArtifacts(
        run_dir=run_dir,
        output=args.output or str(run_dir / "config.json"),
        report_output=args.report_output or str(run_dir / "report.json"),
        tape_output=args.tape_output or str(run_dir / "tape.bin"),
        audit_output=args.audit_output or str(run_dir / "audit.jsonl"),
    )


def _write_text_file(path: str, payload: str) -> None:
    output_path = Path(path)
    if output_path.parent != Path("."):
        output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(payload, encoding="utf-8")


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
        "--materialize",
        action="store_true",
        help="Materialize an existing run directory's config/report/tape artifacts into parquet tables.",
    )
    parser.add_argument(
        "--path",
        help="Run directory path used with --materialize.",
    )
    parser.add_argument(
        "--tables-output",
        help="Optional tables output directory used with --materialize. Default: <path>/tables.",
    )
    parser.add_argument(
        "--compress-if-verified",
        "--compress_if_verified",
        dest="compress_if_verified",
        action="store_true",
        help="With --materialize, write .gz copies of config/report/tape only after verification passes.",
    )
    parser.add_argument(
        "--remove-if-verified",
        "--remove_if_verified",
        dest="remove_if_verified",
        action="store_true",
        help=(
            "With --materialize, remove raw tape.bin only after verification passes, tape.bin.gz exists, "
            "and all expected parquet tables exist."
        ),
    )
    parser.add_argument(
        "--materialize-batch-size",
        type=int,
        default=100_000,
        help="Parquet writer batch size used with --materialize. Default: 100000.",
    )
    parser.add_argument(
        "--config-format",
        choices=("trader", "app"),
        default="trader",
        help="Config schema to emit. 'trader' is the legacy generator; 'app' targets the C++ AppConfig. Default: trader.",
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
        "--event-fetch-workers",
        type=int,
        default=8,
        help="Concurrent event detail fetch workers used during discovery. Use 1 for serial fetching. Default: 8.",
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
        "--router-queue-capacity",
        type=int,
        help="Router queue capacity written into the C++ app config. Defaults to --io-to-router-capacity.",
    )
    parser.add_argument(
        "--operator-queue-capacity",
        type=int,
        default=64,
        help="Operator command queue capacity written into the C++ app config. Default: 64.",
    )
    parser.add_argument(
        "--operator-socket-path",
        default="/tmp/predex_operator.sock",
        help="Operator Unix socket path written into the C++ app config. Default: /tmp/predex_operator.sock.",
    )
    parser.add_argument(
        "--synthetic-trading-session",
        action="store_true",
        help="Enable synthetic trading-session phase cutoffs in the generated C++ app config.",
    )
    parser.add_argument(
        "--reduce-only-after-seconds",
        type=int,
        default=0,
        help="Seconds after process start when the session enters reduce-only mode. Default: 0.",
    )
    parser.add_argument(
        "--flatten-to-zero-after-seconds",
        type=int,
        default=0,
        help="Seconds after process start when the session enters flatten-to-zero mode. Default: 0.",
    )
    parser.add_argument(
        "--stopped-after-seconds",
        type=int,
        default=0,
        help="Seconds after process start when the session enters stopped mode. Default: 0.",
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
        "--run-dir-root",
        help=(
            "Create a stable run artifact directory under this root and default config/report/tape/audit "
            "paths into it. Explicit --output/--report-output/--tape-output/--audit-output values still win."
        ),
    )
    parser.add_argument(
        "--run-label",
        help=(
            "Optional label used in the generated run directory name. "
            f"If --run-dir-root is omitted, this implies --run-dir-root {DEFAULT_RUN_DIR_ROOT}."
        ),
    )
    parser.add_argument(
        "--overwrite-run-dir",
        action="store_true",
        help="Allow writing into an existing generated run directory. Default: fail if the directory already exists.",
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
        help=f"Tape output path written into the config. Default: {DEFAULT_TAPE_OUTPUT}, or <run-dir>/tape.bin with --run-dir-root.",
    )
    parser.add_argument(
        "--audit-output",
        help=f"Audit output path written into the config. Default: {DEFAULT_AUDIT_OUTPUT}, or <run-dir>/audit.jsonl with --run-dir-root.",
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
    parser.add_argument(
        "--enable-market-data",
        action="store_true",
        help=(
            "Enable market data in the generated C++ app config. "
            "Run-directory captures enable this by default."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    load_repo_dotenv()
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.materialize:
        if not args.path:
            parser.error("--materialize requires --path")
        from predex.replay.materialize import materialize_run

        result = materialize_run(
            args.path,
            tables_dir=args.tables_output,
            batch_size=args.materialize_batch_size,
            compress_if_verified=args.compress_if_verified,
            remove_if_verified=args.remove_if_verified,
        )
        manifest = result.manifest
        sys.stdout.write(
            json.dumps(
                {
                    "ok": bool(manifest.get("verified")),
                    "run_dir": str(result.run_dir),
                    "tables_dir": str(result.tables_dir),
                    "manifest": str(result.manifest_path),
                    "checks": manifest.get("checks", {}),
                    "tables": manifest.get("tables", {}),
                    "compressed_artifacts": manifest.get("compressed_artifacts", {}),
                    "remove_raw_checks": manifest.get("remove_raw_checks", {}),
                    "missing_expected_tables": manifest.get("missing_expected_tables", []),
                    "removed_artifacts": manifest.get("removed_artifacts", {}),
                },
                indent=2,
                sort_keys=False,
            )
        )
        sys.stdout.write("\n")
        return 0 if manifest.get("verified") else 1

    try:
        run_artifacts = _resolve_run_artifacts(args)
    except FileExistsError as exc:
        parser.error(str(exc))

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
        event_fetch_workers=args.event_fetch_workers,
        progress_callback=_progress_line,
    )
    events = client.discover_events(
        event_tickers=args.event_ticker or None,
        series_ticker=args.series_ticker,
        status=args.status,
        limit=event_limit,
    )
    if args.config_format == "app":
        app_channels = tuple(dict.fromkeys(channels + lifecycle_channels))
        enable_market_data = args.enable_market_data or run_artifacts.run_dir is not None
        build_result = build_app_config_result(
            events,
            runtime=RuntimeSettings(
                shard_count=args.shard_count,
                shard_queue_capacity=args.shard_input_capacity,
                router_queue_capacity=args.router_queue_capacity or args.io_to_router_capacity,
                frame_pool_capacity=args.frame_pool_capacity,
                operator_queue_capacity=args.operator_queue_capacity,
                operator_socket_path=args.operator_socket_path,
                market_data_tape_path=run_artifacts.tape_output,
                synthetic_trading_session_enabled=args.synthetic_trading_session,
                reduce_only_after_seconds=args.reduce_only_after_seconds,
                flatten_to_zero_after_seconds=args.flatten_to_zero_after_seconds,
                stopped_after_seconds=args.stopped_after_seconds,
            ),
            kalshi=KalshiSettings(
                credentials=CredentialSettings(
                    key_id_env=args.key_id_env,
                    private_key_pem_env=args.private_key_env,
                ),
                market_data=KalshiMarketDataSettings(
                    enable_market_data=enable_market_data,
                    channels=app_channels,
                ),
            ),
            include_topologies=args.include_topology or None,
            exclude_topologies=args.exclude_topology or None,
            market_limit=args.market_limit,
        )
    else:
        build_result = build_trader_config_result(
            events,
            discovery=discovery,
            pipeline=pipeline,
            oms_transport=oms_transport,
            local_risk=local_risk,
            tape_output_path=run_artifacts.tape_output,
            audit_output_path=run_artifacts.audit_output,
            include_topologies=args.include_topology or None,
            exclude_topologies=args.exclude_topology or None,
            market_limit=args.market_limit,
        )
    config = build_result.config
    report = build_result.report()

    payload = json.dumps(config, indent=2, sort_keys=False)
    if run_artifacts.run_dir is not None:
        run_artifacts.run_dir.mkdir(parents=True, exist_ok=args.overwrite_run_dir)

    if run_artifacts.output:
        _write_text_file(run_artifacts.output, payload + "\n")
    else:
        sys.stdout.write(payload)
        sys.stdout.write("\n")

    if run_artifacts.report_output:
        _write_text_file(run_artifacts.report_output, json.dumps(report, indent=2, sort_keys=False) + "\n")
    elif run_artifacts.output:
        sys.stderr.write(json.dumps(report, indent=2, sort_keys=False))
        sys.stderr.write("\n")
    sys.stderr.write(_summary_line(report))
    if run_artifacts.run_dir is not None:
        sys.stderr.write(f" | run_dir={run_artifacts.run_dir}")
    if run_artifacts.output:
        sys.stderr.write(f" | config={run_artifacts.output}")
    if run_artifacts.report_output:
        sys.stderr.write(f" | report={run_artifacts.report_output}")
    sys.stderr.write(f" | tape={run_artifacts.tape_output}")
    sys.stderr.write("\n")
    return 0
