from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from predex.replay.audit import build_signal_bundles, load_audit_events
from predex.replay.config import ConfigIndex, EventRoute, load_config_index


def _require_deps() -> tuple[Any, Any, Any]:
    try:
        import pandas as pd
        import plotly.graph_objects as go
        import streamlit as st
    except ImportError as exc:
        missing = str(exc).split("No module named ")[-1].strip("'")
        raise RuntimeError(
            f"Missing dependency: {missing}. Install with: .venv/bin/pip install '.[replay-viz]'"
        ) from exc
    return pd, go, st


@dataclass(frozen=True, slots=True)
class ReplayDataset:
    prefix: str
    summary_path: Path
    summary: dict[str, object]
    timeline_parquet: Path | None
    timeline_csv: Path | None
    signals_parquet: Path | None
    signals_csv: Path | None


@dataclass(frozen=True, slots=True)
class RunData:
    config_index: ConfigIndex
    bundles: dict[tuple[int, int], Any]
    event_summary_df: Any
    market_df: Any
    signal_df: Any


def _event_ticker(route: EventRoute | None) -> str:
    if route is None or not route.markets:
        return ""
    return route.markets[0].market_ticker.rsplit("-", 1)[0]


def _scan_datasets(replay_dir: Path) -> list[ReplayDataset]:
    datasets: list[ReplayDataset] = []
    for summary_path in sorted(replay_dir.glob("*.summary.json")):
        prefix = summary_path.name[: -len(".summary.json")]
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue

        timeline_parquet = replay_dir / f"{prefix}.parquet"
        timeline_csv = replay_dir / f"{prefix}.csv"
        signals_parquet = replay_dir / f"{prefix}.signals.parquet"
        signals_csv = replay_dir / f"{prefix}.signals.csv"

        datasets.append(
            ReplayDataset(
                prefix=prefix,
                summary_path=summary_path,
                summary=summary,
                timeline_parquet=timeline_parquet if timeline_parquet.exists() else None,
                timeline_csv=timeline_csv if timeline_csv.exists() else None,
                signals_parquet=signals_parquet if signals_parquet.exists() else None,
                signals_csv=signals_csv if signals_csv.exists() else None,
            )
        )
    return datasets


def _load_timeline_frame(dataset: ReplayDataset, pd: Any):
    if dataset.timeline_parquet is not None:
        return pd.read_parquet(dataset.timeline_parquet)
    if dataset.timeline_csv is not None:
        return pd.read_csv(dataset.timeline_csv)
    raise ValueError(f"No timeline file found for dataset: {dataset.prefix}")


def _load_signals_frame(dataset: ReplayDataset, pd: Any):
    if dataset.signals_parquet is not None:
        return pd.read_parquet(dataset.signals_parquet)
    if dataset.signals_csv is not None:
        return pd.read_csv(dataset.signals_csv)
    raise ValueError(f"No signal hits file found for dataset: {dataset.prefix}")


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _run_data(config_path: Path, audit_path: Path, pd: Any) -> RunData:
    config_index = load_config_index(config_path)
    bundles = build_signal_bundles(load_audit_events(audit_path))

    market_rows: list[dict[str, object]] = []
    for event_id, route in sorted(config_index.events_by_id.items()):
        for market in route.markets:
            market_rows.append(
                {
                    "event_id": event_id,
                    "event_ticker": _event_ticker(route),
                    "topology_kind": route.topology_kind,
                    "market_id": market.market_id,
                    "market_ticker": market.market_ticker,
                    "strike_key": market.strike_key,
                }
            )

    signal_rows: list[dict[str, object]] = []
    for (shard_id, signal_id), bundle in sorted(bundles.items()):
        route = config_index.events_by_id.get(bundle.event_id)
        group_signal = bundle.group_signal
        signal_rows.append(
            {
                "event_id": bundle.event_id,
                "event_ticker": _event_ticker(route),
                "topology_kind": route.topology_kind if route else "",
                "event_market_count": len(route.markets) if route else 0,
                "shard_id": shard_id,
                "signal_id": signal_id,
                "ts_ns": group_signal.ts_ns,
                "edge_ticks": group_signal.edge_ticks,
                "score": group_signal.score,
                "risk_events": len(bundle.local_risk_events),
                "submission_events": len(bundle.submission_events),
                "decision_events": len(bundle.oms_decisions),
                "reconcile_events": len(bundle.shard_reconciles),
            }
        )

    market_df = pd.DataFrame(market_rows)
    signal_df = pd.DataFrame(signal_rows)

    event_rows: list[dict[str, object]] = []
    for event_id, route in sorted(config_index.events_by_id.items()):
        event_signal_df = signal_df[signal_df["event_id"] == event_id] if not signal_df.empty else signal_df
        signal_count = int(len(event_signal_df))
        event_rows.append(
            {
                "event_id": event_id,
                "event_ticker": _event_ticker(route),
                "topology_kind": route.topology_kind,
                "market_count": len(route.markets),
                "signal_count": signal_count,
                "first_ts_ns": int(event_signal_df["ts_ns"].min()) if signal_count else None,
                "last_ts_ns": int(event_signal_df["ts_ns"].max()) if signal_count else None,
                "max_edge_ticks": int(event_signal_df["edge_ticks"].max()) if signal_count else None,
                "mean_edge_ticks": float(event_signal_df["edge_ticks"].mean()) if signal_count else None,
            }
        )

    event_summary_df = pd.DataFrame(event_rows)
    if not event_summary_df.empty:
        event_summary_df = event_summary_df.sort_values(["signal_count", "event_id"], ascending=[False, True])

    return RunData(
        config_index=config_index,
        bundles=bundles,
        event_summary_df=event_summary_df,
        market_df=market_df,
        signal_df=signal_df,
    )


def _signal_legs_df(bundle: Any, pd: Any):
    leg_rows: list[dict[str, object]] = []
    side_names = {3: "buy", 4: "sell"}
    for leg in bundle.legs():
        leg_rows.append(
            {
                "kind": leg.kind,
                "leg_index": leg.leg_index,
                "market_id": leg.market_id,
                "side": side_names.get(leg.side, str(leg.side)),
                "qty_lots": leg.qty_lots,
                "price_ticks": leg.price_ticks,
                "decision_code": leg.decision_code,
                "reject_reason": leg.reject_reason,
            }
        )
    return pd.DataFrame(leg_rows)


def _run_overview(st: Any, run_data: RunData) -> None:
    event_df = run_data.event_summary_df
    signal_df = run_data.signal_df
    market_df = run_data.market_df

    total_events = int(len(event_df))
    events_with_signals = int((event_df["signal_count"] > 0).sum()) if not event_df.empty else 0
    total_signals = int(len(signal_df))
    total_markets = int(len(market_df))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Events In Config", total_events)
    c2.metric("Events With Signals", events_with_signals)
    c3.metric("Signals Generated", total_signals)
    c4.metric("Markets Routed", total_markets)

    st.subheader("Events")
    st.dataframe(event_df, use_container_width=True, height=280)


def _event_explorer(st: Any, run_data: RunData, datasets: list[ReplayDataset], pd: Any, go: Any) -> None:
    event_df = run_data.event_summary_df
    market_df = run_data.market_df
    signal_df = run_data.signal_df

    if event_df.empty:
        st.warning("No events in config.")
        return

    event_options = [
        f"{int(row.event_id)} | {row.event_ticker} | signals={int(row.signal_count)}"
        for row in event_df.itertuples(index=False)
    ]
    selected_label = st.selectbox("Select Event", options=event_options)
    selected_event_id = int(selected_label.split("|", 1)[0].strip())

    event_markets = market_df[market_df["event_id"] == selected_event_id].copy().sort_values("strike_key")
    event_signals = signal_df[signal_df["event_id"] == selected_event_id].copy().sort_values(
        ["ts_ns", "signal_id"],
        ascending=[True, True],
    )

    st.subheader("Submarkets")
    st.dataframe(event_markets[["market_id", "market_ticker", "strike_key"]], use_container_width=True, height=220)

    st.subheader("Signals")
    st.dataframe(
        event_signals[
            [
                "ts_ns",
                "shard_id",
                "signal_id",
                "edge_ticks",
                "score",
                "risk_events",
                "submission_events",
                "decision_events",
                "reconcile_events",
            ]
        ],
        use_container_width=True,
        height=320,
    )

    selected_signal_key: tuple[int, int] | None = None
    if not event_signals.empty:
        signal_select_options = [
            f"{int(row.shard_id)}:{int(row.signal_id)} edge={int(row.edge_ticks)} ts={int(row.ts_ns)}"
            for row in event_signals.itertuples(index=False)
        ]
        selected_signal_label = st.selectbox("Inspect Signal Legs", options=signal_select_options)
        shard_id, signal_id = selected_signal_label.split(" ", 1)[0].split(":")
        selected_signal_key = (_safe_int(shard_id), _safe_int(signal_id))
        bundle = run_data.bundles.get(selected_signal_key)
        if bundle is not None:
            st.dataframe(_signal_legs_df(bundle, pd), use_container_width=True, height=160)

    st.subheader("Replay Timeline For Event")
    event_datasets = [dataset for dataset in datasets if _safe_int(dataset.summary.get("event_id")) == selected_event_id]

    if not event_datasets:
        st.info("No replay export found for this event yet.")
        st.code(
            "./scripts/predex-replay export-event-timeline "
            "--config docs/generated_config.json --audit logs/live/predex_audit.jsonl "
            "--tape logs/live/predex_tape.bin --event-id "
            f"{selected_event_id} --parquet",
            language="bash",
        )
        return

    selected_dataset_prefix = st.selectbox(
        "Timeline Dataset",
        options=[dataset.prefix for dataset in event_datasets],
    )
    selected_dataset = next(dataset for dataset in event_datasets if dataset.prefix == selected_dataset_prefix)
    timeline_df = _load_timeline_frame(selected_dataset, pd)
    hit_df = _load_signals_frame(selected_dataset, pd)
    timeline_mode = st.radio("Timeline View", options=["Signal Pair", "Single Market"], horizontal=True)

    if timeline_mode == "Signal Pair":
        if hit_df.empty:
            st.info("No signal match rows in this replay dataset.")
            return

        hit_df = hit_df.copy()
        hit_df["shard_id"] = hit_df["shard_id"].map(_safe_int)
        hit_df["signal_id"] = hit_df["signal_id"].map(_safe_int)
        hit_df["record_index"] = hit_df["record_index"].map(_safe_int)
        hit_df = hit_df.sort_values(["record_index", "signal_id"])

        hit_options: list[str] = []
        default_index = 0
        for idx, row in enumerate(hit_df.itertuples(index=False)):
            label = (
                f"{int(row.shard_id)}:{int(row.signal_id)} rec={int(row.record_index)} "
                f"edge={int(row.recomputed_edge_ticks)} "
                f"{row.easier_market_ticker} -> {row.harder_market_ticker}"
            )
            hit_options.append(label)
            if selected_signal_key is not None and (int(row.shard_id), int(row.signal_id)) == selected_signal_key:
                default_index = idx

        selected_hit_label = st.selectbox("Signal Pair Match", options=hit_options, index=default_index)
        selected_hit_idx = hit_options.index(selected_hit_label)
        selected_hit = hit_df.iloc[selected_hit_idx]

        easier_ticker = str(selected_hit["easier_market_ticker"])
        harder_ticker = str(selected_hit["harder_market_ticker"])
        match_record = _safe_int(selected_hit["record_index"])

        easier_df = timeline_df[timeline_df["market_ticker"] == easier_ticker].copy().sort_values("record_index")
        harder_df = timeline_df[timeline_df["market_ticker"] == harder_ticker].copy().sort_values("record_index")
        for frame in (easier_df, harder_df):
            frame["record_index"] = pd.to_numeric(frame["record_index"], errors="coerce")
            frame["best_bid_ticks"] = pd.to_numeric(frame["best_bid_ticks"], errors="coerce")
            frame["best_ask_ticks"] = pd.to_numeric(frame["best_ask_ticks"], errors="coerce")
            frame.dropna(subset=["record_index"], inplace=True)
            frame["record_index"] = frame["record_index"].astype(int)

        spread_index = sorted(set(easier_df["record_index"]).union(set(harder_df["record_index"])))
        spread_df = pd.DataFrame({"record_index": spread_index})
        spread_df = spread_df.merge(
            easier_df[["record_index", "best_ask_ticks"]],
            on="record_index",
            how="left",
        ).rename(columns={"best_ask_ticks": "easier_ask_ticks"})
        spread_df = spread_df.merge(
            harder_df[["record_index", "best_bid_ticks"]],
            on="record_index",
            how="left",
        ).rename(columns={"best_bid_ticks": "harder_bid_ticks"})
        spread_df["easier_ask_ticks"] = spread_df["easier_ask_ticks"].ffill()
        spread_df["harder_bid_ticks"] = spread_df["harder_bid_ticks"].ffill()
        spread_df["spread_ticks"] = spread_df["harder_bid_ticks"] - spread_df["easier_ask_ticks"]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Easier Leg", easier_ticker)
        c2.metric("Harder Leg", harder_ticker)
        c3.metric("Matched Edge", int(selected_hit["recomputed_edge_ticks"]))
        match_spread_row = spread_df[spread_df["record_index"] <= match_record].tail(1)
        match_spread = int(match_spread_row["spread_ticks"].iloc[0]) if not match_spread_row.empty else 0
        c4.metric("Matched Spread", match_spread)
        st.caption("Spread line uses forward-filled top-of-book across asynchronous leg updates: harder_bid - easier_ask")

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=easier_df["record_index"],
                y=easier_df["best_ask_ticks"],
                mode="lines",
                name=f"{easier_ticker} ask (buy leg)",
                line={"color": "#b91c1c", "width": 2, "shape": "hv"},
            )
        )
        fig.add_trace(
            go.Scatter(
                x=harder_df["record_index"],
                y=harder_df["best_bid_ticks"],
                mode="lines",
                name=f"{harder_ticker} bid (sell leg)",
                line={"color": "#047857", "width": 2, "shape": "hv"},
            )
        )
        fig.add_trace(
            go.Scatter(
                x=easier_df["record_index"],
                y=easier_df["best_bid_ticks"],
                mode="lines",
                name=f"{easier_ticker} bid",
                line={"color": "#ef4444", "width": 1, "dash": "dot", "shape": "hv"},
            )
        )
        fig.add_trace(
            go.Scatter(
                x=harder_df["record_index"],
                y=harder_df["best_ask_ticks"],
                mode="lines",
                name=f"{harder_ticker} ask",
                line={"color": "#10b981", "width": 1, "dash": "dot", "shape": "hv"},
            )
        )
        fig.add_trace(
            go.Scatter(
                x=spread_df["record_index"],
                y=spread_df["spread_ticks"],
                mode="lines",
                name="Spread (harder_bid - easier_ask)",
                yaxis="y2",
                line={"color": "#1d4ed8", "width": 2, "shape": "hv"},
            )
        )

        fig.add_vline(
            x=match_record,
            line_width=1,
            line_dash="dash",
            line_color="#1d4ed8",
            annotation_text="match",
            annotation_position="top",
        )
        fig.add_trace(
            go.Scatter(
                x=[match_record],
                y=[_safe_int(selected_hit["easier_ask_ticks"])],
                mode="markers",
                name="Matched Easier Ask",
                marker={"color": "#dc2626", "size": 10, "symbol": "diamond"},
            )
        )
        fig.add_trace(
            go.Scatter(
                x=[match_record],
                y=[_safe_int(selected_hit["harder_bid_ticks"])],
                mode="markers",
                name="Matched Harder Bid",
                marker={"color": "#065f46", "size": 10, "symbol": "diamond"},
            )
        )

        fig.update_layout(
            height=460,
            margin={"l": 8, "r": 8, "t": 12, "b": 8},
            xaxis_title="record_index",
            yaxis_title="price_ticks",
            yaxis2={
                "title": "spread_ticks",
                "overlaying": "y",
                "side": "right",
                "showgrid": False,
                "zeroline": True,
                "zerolinecolor": "#1d4ed8",
            },
            legend={"orientation": "h", "y": 1.05, "x": 0},
        )
        st.plotly_chart(fig, use_container_width=True)

        pair_hits_df = hit_df[
            (hit_df["easier_market_ticker"] == easier_ticker) & (hit_df["harder_market_ticker"] == harder_ticker)
        ].copy()
        pair_hits_df = pair_hits_df.sort_values(["record_index", "signal_id"])
        st.dataframe(pair_hits_df, use_container_width=True, height=220)
    else:
        market_choices = sorted(str(item) for item in timeline_df["market_ticker"].dropna().unique())
        selected_market = st.selectbox("Timeline Market", options=market_choices)
        market_timeline_df = timeline_df[timeline_df["market_ticker"] == selected_market].copy().sort_values("record_index")

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=market_timeline_df["record_index"],
                y=market_timeline_df["best_bid_ticks"],
                mode="lines",
                name="Best Bid",
                line={"color": "#047857", "width": 2},
            )
        )
        fig.add_trace(
            go.Scatter(
                x=market_timeline_df["record_index"],
                y=market_timeline_df["best_ask_ticks"],
                mode="lines",
                name="Best Ask",
                line={"color": "#b91c1c", "width": 2},
            )
        )

        markers_df = market_timeline_df[market_timeline_df["signal_hit_count"].fillna(0) > 0]
        if not markers_df.empty:
            fig.add_trace(
                go.Scatter(
                    x=markers_df["record_index"],
                    y=markers_df["best_ask_ticks"],
                    mode="markers",
                    name="Signal Match",
                    marker={"color": "#1d4ed8", "size": 8, "symbol": "diamond"},
                    text=markers_df["signal_hit_ids"].astype(str),
                    hovertemplate="record=%{x}<br>ask=%{y}<br>signals=%{text}<extra></extra>",
                )
            )

        fig.update_layout(
            height=420,
            margin={"l": 8, "r": 8, "t": 12, "b": 8},
            xaxis_title="record_index",
            yaxis_title="price_ticks",
            legend={"orientation": "h", "y": 1.05, "x": 0},
        )
        st.plotly_chart(fig, use_container_width=True)

        event_hit_df = hit_df[
            (hit_df["easier_market_ticker"] == selected_market)
            | (hit_df["harder_market_ticker"] == selected_market)
        ].copy()
        event_hit_df = event_hit_df.sort_values(["record_index", "signal_id"])
        st.dataframe(event_hit_df, use_container_width=True, height=240)


def main() -> None:
    pd, go, st = _require_deps()

    st.set_page_config(page_title="Predex Replay Dashboard", layout="wide")
    st.title("Predex Replay Dashboard")
    st.caption("Run-wide event/submarket/signal explorer with timeline drill-down")

    root = Path(__file__).resolve().parents[4]
    default_replay_dir = (root / "logs" / "replay").as_posix()
    default_config = (root / "docs" / "generated_config.json").as_posix()
    default_audit = (root / "logs" / "live" / "predex_audit.jsonl").as_posix()

    replay_dir = Path(st.sidebar.text_input("Replay Dir", value=default_replay_dir)).expanduser()
    config_path = Path(st.sidebar.text_input("Config Path", value=default_config)).expanduser()
    audit_path = Path(st.sidebar.text_input("Audit Path", value=default_audit)).expanduser()

    if not config_path.exists():
        st.error(f"Config not found: {config_path}")
        st.stop()
    if not audit_path.exists():
        st.error(f"Audit not found: {audit_path}")
        st.stop()

    run_data = _run_data(config_path, audit_path, pd)
    datasets = _scan_datasets(replay_dir) if replay_dir.exists() and replay_dir.is_dir() else []

    _run_overview(st, run_data)
    st.divider()
    _event_explorer(st, run_data, datasets, pd, go)


if __name__ == "__main__":
    main()
