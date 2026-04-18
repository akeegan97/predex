from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

from .audit import AuditEvent, load_audit_events

LATENCY_SPAN_FIELDS: tuple[str, ...] = (
    "tick_to_signal_ns",
    "signal_to_submission_ns",
    "submission_to_decision_ns",
    "decision_to_transport_ns",
    "transport_to_first_fill_ns",
    "tick_to_first_fill_ns",
    "tick_to_terminal_ns",
)

DEFAULT_KINDS: tuple[str, ...] = (
    "pipeline_probe",
    "submission",
    "oms_decision",
    "oms_transport",
    "oms_lifecycle",
    "shard_reconcile",
)


def _require_plot_deps() -> tuple[Any, Any, Any, Any]:
    try:
        import pandas as pd
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
        import plotly.io as pio
    except ImportError as exc:
        missing = str(exc).split("No module named ")[-1].strip("'")
        raise RuntimeError(
            f"Missing dependency: {missing}. Install with: .venv/bin/pip install '.[replay-viz]'"
        ) from exc
    return pd, go, make_subplots, pio


def _require_pandas() -> Any:
    try:
        import pandas as pd
    except ImportError as exc:
        missing = str(exc).split("No module named ")[-1].strip("'")
        raise RuntimeError(
            f"Missing dependency: {missing}. Install with: .venv/bin/pip install '.[replay-viz]'"
        ) from exc
    return pd


def _require_matplotlib() -> Any:
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        missing = str(exc).split("No module named ")[-1].strip("'")
        raise RuntimeError(
            f"Missing dependency: {missing}. Install with: .venv/bin/pip install matplotlib"
        ) from exc
    return plt


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    sorted_values = sorted(values)
    rank = (len(sorted_values) - 1) * p
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return sorted_values[low]
    weight = rank - low
    return sorted_values[low] * (1.0 - weight) + sorted_values[high] * weight


def _event_matches(event: AuditEvent, event_id: int | None, market_id: int | None) -> bool:
    if event_id is not None and event.event_id != event_id:
        return False
    if market_id is not None and event.market_id != market_id:
        return False
    return True


def _selected_kinds(kinds_arg: str | None) -> set[str]:
    if not kinds_arg:
        return set(DEFAULT_KINDS)
    selected = {part.strip() for part in kinds_arg.split(",") if part.strip()}
    return selected or set(DEFAULT_KINDS)


def _selected_spans(spans_arg: str | None) -> list[str]:
    if not spans_arg:
        return list(LATENCY_SPAN_FIELDS)
    selected = [part.strip() for part in spans_arg.split(",") if part.strip()]
    if not selected:
        return list(LATENCY_SPAN_FIELDS)
    invalid = [span for span in selected if span not in LATENCY_SPAN_FIELDS]
    if invalid:
        raise ValueError(
            f"Unknown span(s): {', '.join(invalid)}. "
            f"Valid options: {', '.join(LATENCY_SPAN_FIELDS)}"
        )
    return selected


def _render_matplotlib_pngs(
    frame: Any,
    spans: list[str],
    output_png_prefix: str | Path,
    *,
    bins: int,
    time_bucket_ms: int,
    title_suffix: str,
) -> dict[str, str]:
    plt = _require_matplotlib()
    output_prefix = Path(output_png_prefix)
    output_prefix.parent.mkdir(parents=True, exist_ok=True)

    cols = min(3, max(1, len(spans)))
    rows = int(math.ceil(len(spans) / cols))

    hist_fig, hist_axes = plt.subplots(rows, cols, figsize=(6 * cols, 3.8 * rows), squeeze=False)
    for idx, span in enumerate(spans):
        ax = hist_axes[idx // cols][idx % cols]
        span_values = []
        if not frame.empty:
            span_values = frame.loc[frame["span"] == span, "value_ms"].astype(float).tolist()
        ax.hist(span_values, bins=max(10, bins), edgecolor="black", linewidth=0.6)
        ax.set_title(span)
        ax.set_xlabel("Latency (ms)")
        ax.set_ylabel("Count")
    for idx in range(len(spans), rows * cols):
        hist_axes[idx // cols][idx % cols].axis("off")
    hist_fig.suptitle(f"Predex Latency Histograms{title_suffix}")
    hist_fig.tight_layout(rect=(0, 0, 1, 0.97))
    hist_png = output_prefix.with_suffix(".hist.png")
    hist_fig.savefig(hist_png, dpi=150)
    plt.close(hist_fig)

    trend_fig, trend_axes = plt.subplots(rows, cols, figsize=(6 * cols, 3.8 * rows), squeeze=False)
    if not frame.empty:
        first_ts_ns = int(frame["ts_ns"].min())
        bucket_ns = max(1, int(time_bucket_ms) * 1_000_000)
        trend_frame = frame.copy()
        trend_frame["bucket_idx"] = ((trend_frame["ts_ns"] - first_ts_ns) // bucket_ns).astype(int)
        trend_frame["runtime_s"] = trend_frame["bucket_idx"] * (bucket_ns / 1_000_000_000.0)
    else:
        trend_frame = frame

    for idx, span in enumerate(spans):
        ax = trend_axes[idx // cols][idx % cols]
        span_df = trend_frame.loc[trend_frame["span"] == span, ["runtime_s", "value_ms"]]
        if not span_df.empty:
            grouped = span_df.groupby("runtime_s")["value_ms"]
            p50 = grouped.quantile(0.50).reset_index(name="p50_ms")
            p95 = grouped.quantile(0.95).reset_index(name="p95_ms")
            ax.plot(p50["runtime_s"], p50["p50_ms"], label="p50", linewidth=1.4)
            ax.plot(p95["runtime_s"], p95["p95_ms"], label="p95", linewidth=1.4, linestyle="--")
            ax.legend()
        ax.set_title(span)
        ax.set_xlabel("Runtime (s)")
        ax.set_ylabel("Latency (ms)")
    for idx in range(len(spans), rows * cols):
        trend_axes[idx // cols][idx % cols].axis("off")
    trend_fig.suptitle(f"Predex Latency Over Time (Bucketed p50/p95){title_suffix}")
    trend_fig.tight_layout(rect=(0, 0, 1, 0.97))
    trend_png = output_prefix.with_suffix(".trend.png")
    trend_fig.savefig(trend_png, dpi=150)
    plt.close(trend_fig)

    return {
        "hist_png": str(hist_png),
        "trend_png": str(trend_png),
    }


def _render_time_sliced_histograms(
    frame: Any,
    spans: list[str],
    output_png_prefix: str | Path,
    *,
    bins: int,
    histogram_every_seconds: float,
    max_bucket_plots: int,
    title_suffix: str,
) -> dict[str, str]:
    plt = _require_matplotlib()
    output_prefix = Path(output_png_prefix)
    output_prefix.parent.mkdir(parents=True, exist_ok=True)

    if frame.empty or histogram_every_seconds <= 0:
        return {}

    bucket_ns = max(1, int(histogram_every_seconds * 1_000_000_000.0))
    first_ts_ns = int(frame["ts_ns"].min())
    bucketed = frame.copy()
    bucketed["bucket_idx"] = ((bucketed["ts_ns"] - first_ts_ns) // bucket_ns).astype(int)

    outputs: dict[str, str] = {}
    for span in spans:
        span_df = bucketed.loc[bucketed["span"] == span, ["bucket_idx", "value_ms"]]
        if span_df.empty:
            continue

        bucket_ids = sorted(int(x) for x in span_df["bucket_idx"].unique().tolist())
        if len(bucket_ids) > max_bucket_plots:
            # Evenly sample buckets across runtime to keep figure readable.
            step = max(1, int(math.ceil(len(bucket_ids) / max_bucket_plots)))
            bucket_ids = bucket_ids[::step][:max_bucket_plots]

        cols = min(4, max(1, len(bucket_ids)))
        rows = int(math.ceil(len(bucket_ids) / cols))
        fig, axes = plt.subplots(rows, cols, figsize=(4.8 * cols, 3.6 * rows), squeeze=False)

        for idx, bucket_id in enumerate(bucket_ids):
            ax = axes[idx // cols][idx % cols]
            values = (
                span_df.loc[span_df["bucket_idx"] == bucket_id, "value_ms"]
                .astype(float)
                .tolist()
            )
            start_s = bucket_id * histogram_every_seconds
            end_s = (bucket_id + 1) * histogram_every_seconds
            ax.hist(values, bins=max(10, bins), edgecolor="black", linewidth=0.6)
            ax.set_title(f"t=[{start_s:.1f}, {end_s:.1f})s n={len(values)}")
            ax.set_xlabel("Latency (ms)")
            ax.set_ylabel("Count")

        for idx in range(len(bucket_ids), rows * cols):
            axes[idx // cols][idx % cols].axis("off")

        fig.suptitle(
            f"Predex Time-Sliced Histograms ({span}, every {histogram_every_seconds:g}s){title_suffix}"
        )
        fig.tight_layout(rect=(0, 0, 1, 0.97))
        span_png = output_prefix.with_name(f"{output_prefix.name}.{span}.by_time").with_suffix(".png")
        fig.savefig(span_png, dpi=150)
        plt.close(fig)
        outputs[f"time_sliced_{span}_png"] = str(span_png)

    return outputs


def export_latency_histograms(
    audit_path: str | Path,
    output_html: str | Path,
    *,
    backend: str = "plotly",
    output_png_prefix: str | Path = "docs/replay/latency_histograms",
    output_csv: str | Path = "docs/replay/latency_histograms.csv",
    output_json: str | Path | None = None,
    event_id: int | None = None,
    market_id: int | None = None,
    kinds_csv: str | None = None,
    spans_csv: str | None = None,
    bins: int = 80,
    max_ms: float | None = None,
    time_bucket_ms: int = 1000,
    histogram_every_seconds: float | None = None,
    max_bucket_plots: int = 24,
) -> dict[str, object]:
    if backend not in {"plotly", "matplotlib", "both"}:
        raise ValueError("backend must be one of: plotly, matplotlib, both")
    pd = _require_pandas()

    selected_kinds = _selected_kinds(kinds_csv)
    selected_spans = _selected_spans(spans_csv)
    events = load_audit_events(audit_path)

    rows: list[dict[str, object]] = []
    for event in events:
        if selected_kinds and event.kind not in selected_kinds:
            continue
        if not _event_matches(event, event_id, market_id):
            continue

        for span in selected_spans:
            value_ns = getattr(event, span, 0)
            if value_ns <= 0:
                continue
            value_ms = value_ns / 1_000_000.0
            if max_ms is not None and value_ms > max_ms:
                continue
            rows.append(
                {
                    "kind": event.kind,
                    "ts_ns": event.ts_ns,
                    "event_id": event.event_id,
                    "market_id": event.market_id,
                    "span": span,
                    "value_ns": value_ns,
                    "value_ms": value_ms,
                }
            )

    frame = pd.DataFrame(rows)
    if not frame.empty:
        first_ts_ns = int(frame["ts_ns"].min())
        bucket_ns = max(1, int(time_bucket_ms) * 1_000_000)
        frame = frame.copy()
        frame["runtime_s"] = (frame["ts_ns"] - first_ts_ns) / 1_000_000_000.0
        frame["time_bucket_s"] = ((frame["ts_ns"] - first_ts_ns) // bucket_ns) * (
            bucket_ns / 1_000_000_000.0
        )

    output_html_path = Path(output_html)
    output_html_path.parent.mkdir(parents=True, exist_ok=True)
    output_csv_path = Path(output_csv)
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_csv_path, index=False)

    title_filters = []
    if event_id is not None:
        title_filters.append(f"event_id={event_id}")
    if market_id is not None:
        title_filters.append(f"market_id={market_id}")
    title_suffix = f" ({', '.join(title_filters)})" if title_filters else ""

    summary_spans: list[dict[str, object]] = []

    for span in selected_spans:
        if frame.empty:
            values_ms: list[float] = []
        else:
            values_ms = frame.loc[frame["span"] == span, "value_ms"].astype(float).tolist()
        summary_spans.append(
            {
                "span": span,
                "count": len(values_ms),
                "p50_ms": _percentile(values_ms, 0.50),
                "p95_ms": _percentile(values_ms, 0.95),
                "p99_ms": _percentile(values_ms, 0.99),
                "max_ms": max(values_ms) if values_ms else 0.0,
            }
        )

    outputs: dict[str, str] = {}
    if backend in {"plotly", "both"}:
        _, go, make_subplots, pio = _require_plot_deps()
        cols = min(3, max(1, len(selected_spans)))
        rows_count = int(math.ceil(len(selected_spans) / cols))
        fig = make_subplots(
            rows=rows_count,
            cols=cols,
            subplot_titles=[field for field in selected_spans],
            horizontal_spacing=0.07,
            vertical_spacing=0.11,
        )
        for index, span in enumerate(selected_spans):
            row = index // cols + 1
            col = index % cols + 1
            values_ms = (
                []
                if frame.empty
                else frame.loc[frame["span"] == span, "value_ms"].astype(float).tolist()
            )
            fig.add_trace(
                go.Histogram(
                    x=values_ms,
                    nbinsx=max(10, bins),
                    name=span,
                    showlegend=False,
                    marker={"line": {"width": 1}},
                ),
                row=row,
                col=col,
            )
        fig.update_layout(
            title=f"Predex Latency Histograms{title_suffix}",
            bargap=0.06,
            template="plotly_white",
            height=max(420, 330 * rows_count),
            width=max(900, 500 * cols),
            margin={"l": 40, "r": 20, "t": 70, "b": 40},
        )
        fig.update_xaxes(title_text="Latency (ms)")
        fig.update_yaxes(title_text="Count")

        trend_fig = make_subplots(
            rows=rows_count,
            cols=cols,
            subplot_titles=[field for field in selected_spans],
            horizontal_spacing=0.07,
            vertical_spacing=0.11,
        )
        if not frame.empty:
            first_ts_ns = int(frame["ts_ns"].min())
            bucket_ns = max(1, int(time_bucket_ms) * 1_000_000)
            trend_frame = frame.copy()
            trend_frame["bucket_idx"] = ((trend_frame["ts_ns"] - first_ts_ns) // bucket_ns).astype(int)
            trend_frame["runtime_s"] = trend_frame["bucket_idx"] * (bucket_ns / 1_000_000_000.0)
            for index, span in enumerate(selected_spans):
                row = index // cols + 1
                col = index % cols + 1
                span_df = trend_frame.loc[trend_frame["span"] == span, ["runtime_s", "value_ms"]]
                if span_df.empty:
                    continue
                grouped = span_df.groupby("runtime_s")["value_ms"]
                p50 = grouped.quantile(0.50).reset_index(name="p50_ms")
                p95 = grouped.quantile(0.95).reset_index(name="p95_ms")
                trend_fig.add_trace(
                    go.Scatter(
                        x=p50["runtime_s"],
                        y=p50["p50_ms"],
                        mode="lines",
                        line={"width": 1.5},
                        showlegend=False,
                    ),
                    row=row,
                    col=col,
                )
                trend_fig.add_trace(
                    go.Scatter(
                        x=p95["runtime_s"],
                        y=p95["p95_ms"],
                        mode="lines",
                        line={"width": 1.5, "dash": "dot"},
                        showlegend=False,
                    ),
                    row=row,
                    col=col,
                )
        trend_fig.update_layout(
            title=f"Predex Latency Over Time (Bucketed p50/p95){title_suffix}",
            template="plotly_white",
            height=max(420, 330 * rows_count),
            width=max(900, 500 * cols),
            margin={"l": 40, "r": 20, "t": 70, "b": 40},
        )
        trend_fig.update_xaxes(title_text="Runtime (s)")
        trend_fig.update_yaxes(title_text="Latency (ms)")
        hist_html = pio.to_html(fig, full_html=False, include_plotlyjs="cdn")
        trend_html = pio.to_html(trend_fig, full_html=False, include_plotlyjs=False)
        output_html_path.write_text(
            (
                "<html><head><meta charset='utf-8'><title>Predex Latency Report</title></head><body>"
                "<h2>Latency Histograms</h2>"
                f"{hist_html}"
                "<h2>Latency Over Time (Bucketed p50/p95)</h2>"
                f"{trend_html}"
                "</body></html>"
            ),
            encoding="utf-8",
        )
        outputs["output_html"] = str(output_html_path)

    if backend in {"matplotlib", "both"}:
        png_outputs = _render_matplotlib_pngs(
            frame=frame,
            spans=selected_spans,
            output_png_prefix=output_png_prefix,
            bins=bins,
            time_bucket_ms=time_bucket_ms,
            title_suffix=title_suffix,
        )
        outputs.update(png_outputs)
        if histogram_every_seconds is not None:
            time_outputs = _render_time_sliced_histograms(
                frame=frame,
                spans=selected_spans,
                output_png_prefix=output_png_prefix,
                bins=bins,
                histogram_every_seconds=histogram_every_seconds,
                max_bucket_plots=max_bucket_plots,
                title_suffix=title_suffix,
            )
            outputs.update(time_outputs)

    payload: dict[str, object] = {
        "audit_path": str(audit_path),
        "backend": backend,
        "outputs": {"output_csv": str(output_csv_path), **outputs},
        "event_id": event_id,
        "market_id": market_id,
        "kinds": sorted(selected_kinds),
        "spans_selected": selected_spans,
        "bins": bins,
        "time_bucket_ms": time_bucket_ms,
        "histogram_every_seconds": histogram_every_seconds,
        "max_bucket_plots": max_bucket_plots,
        "max_ms": max_ms,
        "rows_plotted": int(len(frame.index)),
        "spans": summary_spans,
    }

    if output_json is not None:
        output_json_path = Path(output_json)
        output_json_path.parent.mkdir(parents=True, exist_ok=True)
        output_json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        payload["output_json"] = str(output_json_path)

    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="predex-replay-latency",
        description="Export latency histograms from Predex audit JSONL.",
    )
    parser.add_argument("--audit", required=True, help="Audit JSONL emitted by trader_app.")
    parser.add_argument(
        "--backend",
        choices=("plotly", "matplotlib", "both"),
        default="plotly",
        help="Plot backend. Default: plotly",
    )
    parser.add_argument(
        "--output-html",
        default="docs/replay/latency_histograms.html",
        help="Output HTML plot path. Default: docs/replay/latency_histograms.html",
    )
    parser.add_argument(
        "--output-png-prefix",
        default="docs/replay/latency_histograms",
        help="PNG output prefix for matplotlib mode. Writes <prefix>.hist.png and <prefix>.trend.png",
    )
    parser.add_argument(
        "--output-csv",
        default="docs/replay/latency_histograms.csv",
        help="Output CSV path for per-sample latency rows. Default: docs/replay/latency_histograms.csv",
    )
    parser.add_argument(
        "--output-json",
        default=None,
        help="Optional JSON summary path.",
    )
    parser.add_argument("--event-id", type=int, default=None, help="Optional event filter.")
    parser.add_argument("--market-id", type=int, default=None, help="Optional market filter.")
    parser.add_argument(
        "--kinds",
        default=",".join(DEFAULT_KINDS),
        help=(
            "Comma-separated audit kinds to include. "
            f"Default: {','.join(DEFAULT_KINDS)}"
        ),
    )
    parser.add_argument(
        "--spans",
        default=None,
        help=(
            "Comma-separated latency span fields to include. "
            f"Default: all ({','.join(LATENCY_SPAN_FIELDS)})"
        ),
    )
    parser.add_argument("--bins", type=int, default=80, help="Histogram bins. Default: 80")
    parser.add_argument(
        "--max-ms",
        type=float,
        default=None,
        help="Optional upper cap in milliseconds (drops larger values).",
    )
    parser.add_argument(
        "--time-bucket-ms",
        type=int,
        default=1000,
        help="Bucket size for time trend p50/p95 lines. Default: 1000 ms",
    )
    parser.add_argument(
        "--histogram-every-seconds",
        type=float,
        default=None,
        help="Optional: emit time-sliced histogram PNGs every N runtime seconds (matplotlib/both backend).",
    )
    parser.add_argument(
        "--max-bucket-plots",
        type=int,
        default=24,
        help="Max number of time-slice histogram panels per span. Default: 24",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
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
    print(json.dumps(payload, indent=2, sort_keys=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
