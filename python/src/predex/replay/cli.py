from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

from .audit import build_signal_bundles, load_audit_events
from .config import load_config_index
from .ingest import ingest_run
from .latency import DEFAULT_KINDS, export_latency_histograms
from .soak import analyze_soak
from .timeline import (
    build_event_timeline,
    write_signal_hits_parquet,
    write_signal_hits_csv,
    write_timeline_parquet,
    write_timeline_csv,
    write_timeline_html,
    write_timeline_summary_json,
)
from .verify import verify_signal_bundle
from .windows import (
    build_signal_windows,
    write_signal_windows_csv,
    write_signal_windows_summary_json,
    write_window_signals_csv,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="predex-replay",
        description="Inspect Predex tape and audit output for replay-based signal verification.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    summary = subparsers.add_parser("audit-summary", help="Summarize audit kinds and group signals.")
    summary.add_argument("--config", required=True, help="Generated Predex config JSON.")
    summary.add_argument("--audit", required=True, help="Audit JSONL emitted by trader_app.")
    summary.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of highest-edge group signals to show. Default: 10.",
    )

    inspect_signal = subparsers.add_parser(
        "inspect-signal",
        help="Replay tape for one signal and verify its reconstructed book state.",
    )
    inspect_signal.add_argument("--config", required=True, help="Generated Predex config JSON.")
    inspect_signal.add_argument("--audit", required=True, help="Audit JSONL emitted by trader_app.")
    inspect_signal.add_argument("--tape", required=True, help="Binary tape emitted by trader_app.")
    inspect_signal.add_argument("--signal-id", required=True, type=int, help="Signal id to inspect.")
    inspect_signal.add_argument("--shard-id", required=True, type=int, help="Shard id for the signal.")

    ingest = subparsers.add_parser(
        "ingest-run",
        help="Normalize tape, audit, and REST traces into CSV tables for offline analysis.",
    )
    ingest.add_argument("--config", required=True, help="Generated Predex config JSON.")
    ingest.add_argument("--audit", required=True, help="Audit JSONL emitted by trader_app.")
    ingest.add_argument("--tape", required=True, help="Binary tape emitted by trader_app.")
    ingest.add_argument(
        "--trace",
        action="append",
        default=None,
        help="Optional REST trace JSONL path. Repeat to include multiple files. Defaults to predex_rest_trace*.jsonl beside the audit file.",
    )
    ingest.add_argument(
        "--output-root",
        default="logs/runs",
        help="Root directory for ingested runs. Default: logs/runs.",
    )
    ingest.add_argument(
        "--run-id",
        default=None,
        help="Optional run id. Defaults to a UTC timestamp plus the tape stem.",
    )
    ingest.add_argument(
        "--format",
        choices=("parquet", "both"),
        default="parquet",
        help="Storage format for ingested tables. Default: parquet.",
    )

    export_timeline = subparsers.add_parser(
        "export-event-timeline",
        help="Replay one event (or a single market) into timeline CSV/HTML artifacts.",
    )
    export_timeline.add_argument("--config", required=True, help="Generated Predex config JSON.")
    export_timeline.add_argument("--audit", required=True, help="Audit JSONL emitted by trader_app.")
    export_timeline.add_argument("--tape", required=True, help="Binary tape emitted by trader_app.")
    export_timeline.add_argument(
        "--event-id",
        type=int,
        default=None,
        help="Event id to export. Optional if --market-ticker is supplied.",
    )
    export_timeline.add_argument(
        "--market-ticker",
        default=None,
        help="Optional single market focus within the selected event.",
    )
    export_timeline.add_argument(
        "--output-dir",
        default="logs/replay",
        help="Directory for generated files. Default: logs/replay.",
    )
    export_timeline.add_argument(
        "--prefix",
        default="event_timeline",
        help="Output filename prefix. Default: event_timeline.",
    )
    export_timeline.add_argument(
        "--parquet",
        action="store_true",
        help="Also write parquet outputs (*.parquet, *.signals.parquet). Requires pyarrow.",
    )

    export_windows = subparsers.add_parser(
        "export-signal-windows",
        help="Replay one event into deduplicated opportunity windows with OMS outcome annotations.",
    )
    export_windows.add_argument("--config", required=True, help="Generated Predex config JSON.")
    export_windows.add_argument("--audit", required=True, help="Audit JSONL emitted by trader_app.")
    export_windows.add_argument("--tape", required=True, help="Binary tape emitted by trader_app.")
    export_windows.add_argument(
        "--event-id",
        type=int,
        default=None,
        help="Event id to export. Optional if --market-ticker is supplied.",
    )
    export_windows.add_argument(
        "--market-ticker",
        default=None,
        help="Optional single market focus within the selected event.",
    )
    export_windows.add_argument(
        "--output-dir",
        default="logs/replay",
        help="Directory for generated files. Default: logs/replay.",
    )
    export_windows.add_argument(
        "--prefix",
        default="signal_windows",
        help="Output filename prefix. Default: signal_windows.",
    )

    latency_hist = subparsers.add_parser(
        "latency-histograms",
        help="Export interactive latency histograms from audit JSONL.",
    )
    latency_hist.add_argument("--audit", required=True, help="Audit JSONL emitted by trader_app.")
    latency_hist.add_argument(
        "--backend",
        choices=("plotly", "matplotlib", "both"),
        default="plotly",
        help="Plot backend. Default: plotly",
    )
    latency_hist.add_argument(
        "--output-html",
        default="logs/replay/latency_histograms.html",
        help="Output HTML plot path. Default: logs/replay/latency_histograms.html",
    )
    latency_hist.add_argument(
        "--output-png-prefix",
        default="logs/replay/latency_histograms",
        help="PNG output prefix for matplotlib mode. Writes <prefix>.hist.png and <prefix>.trend.png",
    )
    latency_hist.add_argument(
        "--output-csv",
        default="logs/replay/latency_histograms.csv",
        help="Output CSV path for per-sample latency rows. Default: logs/replay/latency_histograms.csv",
    )
    latency_hist.add_argument(
        "--output-json",
        default=None,
        help="Optional JSON summary path.",
    )
    latency_hist.add_argument("--event-id", type=int, default=None, help="Optional event filter.")
    latency_hist.add_argument("--market-id", type=int, default=None, help="Optional market filter.")
    latency_hist.add_argument(
        "--kinds",
        default=",".join(DEFAULT_KINDS),
        help=f"Comma-separated audit kinds. Default: {','.join(DEFAULT_KINDS)}",
    )
    latency_hist.add_argument(
        "--spans",
        default=None,
        help="Comma-separated latency span fields to include.",
    )
    latency_hist.add_argument("--bins", type=int, default=80, help="Histogram bins. Default: 80")
    latency_hist.add_argument(
        "--max-ms",
        type=float,
        default=None,
        help="Optional upper cap in milliseconds (drops larger values).",
    )
    latency_hist.add_argument(
        "--time-bucket-ms",
        type=int,
        default=1000,
        help="Bucket size for time trend p50/p95 lines. Default: 1000 ms",
    )
    latency_hist.add_argument(
        "--histogram-every-seconds",
        type=float,
        default=None,
        help="Optional: emit time-sliced histogram PNGs every N runtime seconds (matplotlib/both backend).",
    )
    latency_hist.add_argument(
        "--max-bucket-plots",
        type=int,
        default=24,
        help="Max number of time-slice histogram panels per span. Default: 24",
    )

    soak = subparsers.add_parser(
        "soak-analysis",
        help="Summarize strategy verification, per-stage latency, and edge survival for a soak run.",
    )
    soak.add_argument("--config", required=True, help="Generated Predex config JSON.")
    soak.add_argument("--audit", required=True, help="Audit JSONL emitted by trader_app.")
    soak.add_argument("--tape", required=True, help="Binary tape emitted by trader_app.")
    soak.add_argument(
        "--output-json",
        default="logs/replay/soak_analysis.json",
        help="Optional JSON summary output path. Default: logs/replay/soak_analysis.json",
    )
    soak.add_argument(
        "--mismatch-limit",
        type=int,
        default=10,
        help="Max number of signal verification mismatches to include. Default: 10.",
    )
    return parser


def _audit_summary(config_path: str, audit_path: str, limit: int) -> dict[str, object]:
    config_index = load_config_index(config_path)
    audit_events = load_audit_events(audit_path)
    bundles = build_signal_bundles(audit_events)
    kind_counts = Counter(event.kind for event in audit_events)
    ranked_signals = sorted(
        (bundle for bundle in bundles.values()),
        key=lambda bundle: bundle.group_signal.edge_ticks,
        reverse=True,
    )
    top_signals = []
    for bundle in ranked_signals[:limit]:
        route = config_index.events_by_id.get(bundle.event_id)
        top_signals.append(
            {
                "shard_id": bundle.shard_id,
                "signal_id": bundle.signal_id,
                "event_id": bundle.event_id,
                "event_market_count": len(route.markets) if route else 0,
                "topology_kind": route.topology_kind if route else "",
                "edge_ticks": bundle.group_signal.edge_ticks,
                "risk_events": len(bundle.local_risk_events),
                "submission_events": len(bundle.submission_events),
                "oms_decisions": len(bundle.oms_decisions),
                "oms_lifecycles": len(bundle.oms_lifecycles),
            }
        )
    return {
        "audit_kind_counts": dict(kind_counts),
        "group_signal_count": len(bundles),
        "top_group_signals": top_signals,
    }


def _inspect_signal(config_path: str, audit_path: str, tape_path: str, shard_id: int, signal_id: int) -> dict[str, object]:
    config_index = load_config_index(config_path)
    bundles = build_signal_bundles(load_audit_events(audit_path))
    bundle = bundles.get((shard_id, signal_id))
    if bundle is None:
        raise ValueError(f"signal ({shard_id}, {signal_id}) not found in audit")
    verification = verify_signal_bundle(bundle, config_index=config_index, tape_path=tape_path)
    return {
        "signal": {
            "shard_id": bundle.shard_id,
            "signal_id": bundle.signal_id,
            "event_id": bundle.event_id,
            "edge_ticks": bundle.group_signal.edge_ticks,
            "score": bundle.group_signal.score,
            "leg_count": bundle.group_signal.leg_count,
        },
        "legs": [
            {
                "kind": event.kind,
                "market_id": event.market_id,
                "side": event.side,
                "leg_index": event.leg_index,
                "qty_lots": event.qty_lots,
                "price_ticks": event.price_ticks,
                "decision_code": event.decision_code,
                "reject_reason": event.reject_reason,
            }
            for event in bundle.legs()
        ],
        "verification": verification.to_dict(),
        "oms": {
            "submission_count": len(bundle.submission_events),
            "decision_count": len(bundle.oms_decisions),
            "transport_count": len(bundle.oms_transports),
            "lifecycle_count": len(bundle.oms_lifecycles),
            "reconcile_count": len(bundle.shard_reconciles),
        },
    }


def _export_event_timeline(
    config_path: str,
    audit_path: str,
    tape_path: str,
    event_id: int | None,
    market_ticker: str | None,
    output_dir: str,
    prefix: str,
    parquet: bool,
) -> dict[str, object]:
    config_index = load_config_index(config_path)
    bundles = build_signal_bundles(load_audit_events(audit_path))
    timeline = build_event_timeline(
        config_index=config_index,
        bundles=bundles,
        tape_path=tape_path,
        event_id=event_id,
        market_ticker=market_ticker,
    )

    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    safe_prefix = prefix.strip() or "event_timeline"

    timeline_csv = output_root / f"{safe_prefix}.csv"
    signals_csv = output_root / f"{safe_prefix}.signals.csv"
    summary_json = output_root / f"{safe_prefix}.summary.json"
    timeline_html = output_root / f"{safe_prefix}.html"

    write_timeline_csv(timeline_csv, timeline)
    write_signal_hits_csv(signals_csv, timeline.signal_hits)
    write_timeline_summary_json(summary_json, timeline)
    write_timeline_html(timeline_html, timeline)

    outputs: dict[str, str] = {
        "timeline_csv": str(timeline_csv),
        "signals_csv": str(signals_csv),
        "summary_json": str(summary_json),
        "timeline_html": str(timeline_html),
    }
    if parquet:
        timeline_parquet = output_root / f"{safe_prefix}.parquet"
        signals_parquet = output_root / f"{safe_prefix}.signals.parquet"
        write_timeline_parquet(timeline_parquet, timeline)
        write_signal_hits_parquet(signals_parquet, timeline.signal_hits)
        outputs["timeline_parquet"] = str(timeline_parquet)
        outputs["signals_parquet"] = str(signals_parquet)

    return {
        "event_id": timeline.event_id,
        "event_ticker": timeline.event_ticker,
        "market_tickers": list(timeline.market_tickers),
        "timeline_rows": len(timeline.rows),
        "signal_candidates": timeline.signal_candidates,
        "signal_hits": len(timeline.signal_hits),
        "outputs": outputs,
    }


def _export_signal_windows(
    config_path: str,
    audit_path: str,
    tape_path: str,
    event_id: int | None,
    market_ticker: str | None,
    output_dir: str,
    prefix: str,
) -> dict[str, object]:
    config_index = load_config_index(config_path)
    bundles = build_signal_bundles(load_audit_events(audit_path))
    windows = build_signal_windows(
        config_index=config_index,
        bundles=bundles,
        tape_path=tape_path,
        event_id=event_id,
        market_ticker=market_ticker,
    )

    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    safe_prefix = prefix.strip() or "signal_windows"

    windows_csv = output_root / f"{safe_prefix}.csv"
    signals_csv = output_root / f"{safe_prefix}.signals.csv"
    summary_json = output_root / f"{safe_prefix}.summary.json"

    write_signal_windows_csv(windows_csv, windows.windows)
    write_window_signals_csv(signals_csv, windows.signals)
    write_signal_windows_summary_json(summary_json, windows)

    return {
        "event_id": windows.event_id,
        "event_ticker": windows.event_ticker,
        "market_tickers": list(windows.market_tickers),
        "window_count": len(windows.windows),
        "signal_count": len(windows.signals),
        "outputs": {
            "windows_csv": str(windows_csv),
            "signals_csv": str(signals_csv),
            "summary_json": str(summary_json),
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "audit-summary":
        payload = _audit_summary(args.config, args.audit, args.limit)
    elif args.command == "ingest-run":
        payload = ingest_run(
            config_path=args.config,
            audit_path=args.audit,
            tape_path=args.tape,
            output_root=args.output_root,
            run_id=args.run_id,
            trace_paths=args.trace,
            fmt=args.format,
        )
    elif args.command == "inspect-signal":
        payload = _inspect_signal(args.config, args.audit, args.tape, args.shard_id, args.signal_id)
    elif args.command == "latency-histograms":
        payload = export_latency_histograms(
            audit_path=args.audit,
            output_html=args.output_html,
            backend=args.backend,
            output_png_prefix=args.output_png_prefix,
            output_csv=args.output_csv,
            output_json=args.output_json,
            event_id=args.event_id,
            market_id=args.market_id,
            kinds_csv=args.kinds,
            spans_csv=args.spans,
            bins=args.bins,
            max_ms=args.max_ms,
            time_bucket_ms=args.time_bucket_ms,
            histogram_every_seconds=args.histogram_every_seconds,
            max_bucket_plots=args.max_bucket_plots,
        )
    elif args.command == "export-signal-windows":
        payload = _export_signal_windows(
            args.config,
            args.audit,
            args.tape,
            args.event_id,
            args.market_ticker,
            args.output_dir,
            args.prefix,
        )
    elif args.command == "soak-analysis":
        payload = analyze_soak(
            config_path=args.config,
            audit_path=args.audit,
            tape_path=args.tape,
            output_json=args.output_json,
            mismatch_limit=args.mismatch_limit,
        )
    else:
        payload = _export_event_timeline(
            args.config,
            args.audit,
            args.tape,
            args.event_id,
            args.market_ticker,
            args.output_dir,
            args.prefix,
            args.parquet,
        )
    print(json.dumps(payload, indent=2, sort_keys=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
