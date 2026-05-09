from __future__ import annotations

import csv
import json
import re
import shutil
import subprocess
from collections import Counter
from dataclasses import dataclass, fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from .audit import AuditEvent, SignalBundle, build_signal_bundles, load_audit_events
from .config import ConfigIndex, MarketRoute, load_config_index
from .tape import decode_market_event, iter_tape_payloads


_ROW_GROUP_SIZE = 4096
_EPOCH_NS_MIN = 946_684_800_000_000_000
_EPOCH_NS_MAX = 4_102_444_800_000_000_000


@dataclass(frozen=True, slots=True)
class IngestedRun:
    run_id: str
    root: Path
    manifest_path: Path
    metadata_path: Path | None
    manifest: dict[str, Any]
    metadata: dict[str, Any]

    def table_file(self, table_name: str, *, fmt: str = "parquet") -> Path | None:
        generated_tables = self.manifest.get("generated_tables")
        if not isinstance(generated_tables, dict):
            return None
        table_payload = generated_tables.get(table_name)
        if not isinstance(table_payload, dict):
            return None
        files_payload = table_payload.get("files")
        if not isinstance(files_payload, dict):
            return None
        raw_path = files_payload.get(fmt)
        if not isinstance(raw_path, str) or not raw_path:
            return None
        return Path(raw_path)


def _require_pyarrow() -> tuple[Any, Any]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError(
            "Parquet export requires pyarrow. Install with: .venv/bin/pip install pyarrow"
        ) from exc
    return pa, pq


def _parquet_schema(fields_with_types: Sequence[tuple[str, str]]) -> Any:
    pa, _ = _require_pyarrow()
    type_map = {
        "string": pa.string(),
        "int64": pa.int64(),
        "double": pa.float64(),
        "bool": pa.bool_(),
    }
    return pa.schema([pa.field(name, type_map[type_name]) for name, type_name in fields_with_types])


class _ParquetRowWriter:
    def __init__(self, path: Path, fields_with_types: Sequence[tuple[str, str]], row_group_size: int = _ROW_GROUP_SIZE) -> None:
        pa, pq = _require_pyarrow()
        self._pa = pa
        self._schema = _parquet_schema(fields_with_types)
        self._row_group_size = row_group_size
        self._buffer: list[dict[str, object]] = []
        self._writer = pq.ParquetWriter(path, self._schema, compression="zstd")
        self._row_count = 0

    def write(self, row: dict[str, object]) -> None:
        self._buffer.append(row)
        if len(self._buffer) >= self._row_group_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        table = self._pa.Table.from_pylist(self._buffer, schema=self._schema)
        self._writer.write_table(table)
        self._row_count += len(self._buffer)
        self._buffer.clear()

    def close(self) -> int:
        if self._writer is None:
            return self._row_count
        self.flush()
        self._writer.close()
        self._writer = None
        return self._row_count


class _OptionalCsvWriter:
    def __init__(self, path: Path | None, fieldnames: Sequence[str]) -> None:
        self._handle = None
        self._writer = None
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._handle = path.open("w", encoding="utf-8", newline="")
            self._writer = csv.DictWriter(self._handle, fieldnames=list(fieldnames))
            self._writer.writeheader()

    def write(self, row: dict[str, object]) -> None:
        if self._writer is not None:
            self._writer.writerow(row)

    def close(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None
            self._writer = None


def _table_paths(output_dir: Path, name: str, fmt: str) -> tuple[Path, Path | None]:
    parquet_path = output_dir / f"{name}.parquet"
    csv_path = output_dir / f"{name}.csv" if fmt == "both" else None
    return parquet_path, csv_path


def _stream_table(
    *,
    output_dir: Path,
    name: str,
    fields_with_types: Sequence[tuple[str, str]],
    rows: Iterable[dict[str, object]],
    fmt: str,
) -> tuple[int, dict[str, str]]:
    parquet_path, csv_path = _table_paths(output_dir, name, fmt)
    parquet_writer = _ParquetRowWriter(parquet_path, fields_with_types)
    csv_writer = _OptionalCsvWriter(csv_path, [field for field, _ in fields_with_types])
    try:
        for row in rows:
            parquet_writer.write(row)
            csv_writer.write(row)
    finally:
        csv_writer.close()
    row_count = parquet_writer.close()
    outputs = {"parquet": str(parquet_path)}
    if csv_path is not None:
        outputs["csv"] = str(csv_path)
    return row_count, outputs


def _compact_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=False)


def _sanitize_run_id(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    return cleaned.strip("-._") or "run"


def _default_run_id(tape_path: Path) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return _sanitize_run_id(f"{timestamp}_{tape_path.stem}")


def _source_metadata(path: Path, kind: str) -> dict[str, object]:
    stat = path.stat()
    return {
        "kind": kind,
        "path": str(path),
        "size_bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def _resolve_trace_paths(trace_paths: Sequence[str] | None, audit_path: Path) -> list[Path]:
    if trace_paths:
        return [Path(path).expanduser().resolve() for path in trace_paths]
    return sorted(audit_path.parent.glob("predex_rest_trace*.jsonl"))


def _update_time_bounds(bounds: dict[str, int | None], value: int | None) -> None:
    if value is None or value <= 0:
        return
    current_min = bounds.get("start_ts_ns")
    current_max = bounds.get("end_ts_ns")
    bounds["start_ts_ns"] = value if current_min is None or value < current_min else current_min
    bounds["end_ts_ns"] = value if current_max is None or value > current_max else current_max


def _time_bounds_payload(bounds: dict[str, int | None]) -> dict[str, object]:
    start_ts_ns = bounds.get("start_ts_ns")
    end_ts_ns = bounds.get("end_ts_ns")
    clock_domain = _clock_domain(start_ts_ns, end_ts_ns)
    payload: dict[str, object] = {
        "clock_domain": clock_domain,
        "start_ts_ns": start_ts_ns or 0,
        "end_ts_ns": end_ts_ns or 0,
        "start_utc": _ns_to_utc(start_ts_ns, clock_domain=clock_domain),
        "end_utc": _ns_to_utc(end_ts_ns, clock_domain=clock_domain),
    }
    if start_ts_ns and end_ts_ns and end_ts_ns >= start_ts_ns:
        payload["duration_s"] = (end_ts_ns - start_ts_ns) / 1_000_000_000.0
    else:
        payload["duration_s"] = 0.0
    return payload


def _merge_bounds(*bounds_list: dict[str, int | None]) -> dict[str, int | None]:
    merged = {"start_ts_ns": None, "end_ts_ns": None}
    for bounds in bounds_list:
        _update_time_bounds(merged, bounds.get("start_ts_ns"))
        _update_time_bounds(merged, bounds.get("end_ts_ns"))
    return merged


def _ns_to_utc(value_ns: int | None) -> str | None:
    if value_ns is None or value_ns <= 0:
        return None
    return datetime.fromtimestamp(value_ns / 1_000_000_000.0, tz=timezone.utc).isoformat()


def _clock_domain(*values_ns: int | None) -> str:
    positive_values = [value for value in values_ns if value is not None and value > 0]
    if positive_values and all(_EPOCH_NS_MIN <= value <= _EPOCH_NS_MAX for value in positive_values):
        return "unix_epoch_ns"
    return "monotonic_or_unknown_ns"


def _ns_to_utc(value_ns: int | None, *, clock_domain: str) -> str | None:
    if value_ns is None or value_ns <= 0 or clock_domain != "unix_epoch_ns":
        return None
    return datetime.fromtimestamp(value_ns / 1_000_000_000.0, tz=timezone.utc).isoformat()


def _git_metadata(start_dir: Path) -> dict[str, object]:
    try:
        root_result = subprocess.run(
            ["git", "-C", str(start_dir), "rev-parse", "--show-toplevel"],
            check=True,
            capture_output=True,
            text=True,
        )
        repo_root = Path(root_result.stdout.strip())
        head_result = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        branch_result = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "--abbrev-ref", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        status_result = subprocess.run(
            ["git", "-C", str(repo_root), "status", "--porcelain", "--untracked-files=no"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return {"available": False}
    return {
        "available": True,
        "repo_root": str(repo_root),
        "head": head_result.stdout.strip(),
        "branch": branch_result.stdout.strip(),
        "is_dirty": bool(status_result.stdout.strip()),
    }


def _audit_time_bounds(audit_events: Sequence[AuditEvent]) -> dict[str, int | None]:
    bounds = {"start_ts_ns": None, "end_ts_ns": None}
    for event in audit_events:
        _update_time_bounds(bounds, event.ts_ns)
        _update_time_bounds(bounds, event.tick_recv_ns)
        _update_time_bounds(bounds, event.terminal_recv_ns)
        _update_time_bounds(bounds, event.transport_response_recv_ns)
    return bounds


def _market_route_rows(config_index: ConfigIndex) -> Iterable[dict[str, object]]:
    route_fields = [field.name for field in fields(MarketRoute)]
    for route in config_index.routes:
        yield {name: getattr(route, name) for name in route_fields}


def _audit_event_rows(audit_events: Sequence[AuditEvent]) -> Iterable[dict[str, object]]:
    audit_fields = [field.name for field in fields(AuditEvent)]
    for event in audit_events:
        yield {name: getattr(event, name) for name in audit_fields}


def _signal_rows(bundles: dict[tuple[int, int], SignalBundle], config_index: ConfigIndex) -> Iterable[dict[str, object]]:
    for bundle in sorted(bundles.values(), key=lambda item: (item.shard_id, item.signal_id)):
        route = config_index.events_by_id.get(bundle.event_id)
        yield {
            "shard_id": bundle.shard_id,
            "signal_id": bundle.signal_id,
            "event_id": bundle.event_id,
            "event_ticker": route.markets[0].market_ticker.rsplit("-", 1)[0] if route and route.markets else "",
            "topology_kind": route.topology_kind if route else "",
            "signal_ts_ns": bundle.group_signal.signal_ts_ns,
            "tick_recv_ns": bundle.group_signal.tick_recv_ns,
            "edge_ticks": bundle.group_signal.edge_ticks,
            "score": bundle.group_signal.score,
            "leg_count": bundle.group_signal.leg_count,
            "event_market_count": len(route.markets) if route else 0,
            "local_risk_count": len(bundle.local_risk_events),
            "submission_count": len(bundle.submission_events),
            "oms_decision_count": len(bundle.oms_decisions),
            "oms_transport_count": len(bundle.oms_transports),
            "oms_lifecycle_count": len(bundle.oms_lifecycles),
            "shard_reconcile_count": len(bundle.shard_reconciles),
            "local_risk_codes_json": _compact_json([event.decision_code for event in bundle.local_risk_events]),
            "local_risk_reject_reasons_json": _compact_json([event.reject_reason for event in bundle.local_risk_events]),
            "transport_http_statuses_json": _compact_json(sorted({event.transport_http_status for event in bundle.oms_transports if event.transport_http_status > 0})),
            "lifecycle_kinds_json": _compact_json([event.lifecycle_kind for event in bundle.oms_lifecycles]),
            "order_statuses_json": _compact_json([event.order_status for event in bundle.oms_lifecycles]),
        }


def _signal_fields() -> list[tuple[str, str]]:
    return [
        ("shard_id", "int64"),
        ("signal_id", "int64"),
        ("event_id", "int64"),
        ("event_ticker", "string"),
        ("topology_kind", "string"),
        ("signal_ts_ns", "int64"),
        ("tick_recv_ns", "int64"),
        ("edge_ticks", "int64"),
        ("score", "int64"),
        ("leg_count", "int64"),
        ("event_market_count", "int64"),
        ("local_risk_count", "int64"),
        ("submission_count", "int64"),
        ("oms_decision_count", "int64"),
        ("oms_transport_count", "int64"),
        ("oms_lifecycle_count", "int64"),
        ("shard_reconcile_count", "int64"),
        ("local_risk_codes_json", "string"),
        ("local_risk_reject_reasons_json", "string"),
        ("transport_http_statuses_json", "string"),
        ("lifecycle_kinds_json", "string"),
        ("order_statuses_json", "string"),
    ]


def _leg_rows(bundles: dict[tuple[int, int], SignalBundle], config_index: ConfigIndex) -> Iterable[dict[str, object]]:
    for bundle in sorted(bundles.values(), key=lambda item: (item.shard_id, item.signal_id)):
        grouped: dict[tuple[int, int, int], dict[str, object]] = {}
        for event in (
            *bundle.local_risk_events,
            *bundle.submission_events,
            *bundle.oms_decisions,
            *bundle.oms_transports,
            *bundle.oms_lifecycles,
        ):
            key = (event.leg_index, event.market_id, event.side)
            route = config_index.markets_by_id.get(event.market_id)
            row = grouped.setdefault(
                key,
                {
                    "shard_id": bundle.shard_id,
                    "signal_id": bundle.signal_id,
                    "event_id": bundle.event_id,
                    "market_id": event.market_id,
                    "market_ticker": route.market_ticker if route else "",
                    "leg_index": event.leg_index,
                    "side": event.side,
                    "qty_lots": event.qty_lots,
                    "price_ticks": event.price_ticks,
                    "local_risk_decision_code": 0,
                    "local_risk_reject_reason": 0,
                    "submission_seen": 0,
                    "oms_decision_seen": 0,
                    "transport_seen": 0,
                    "lifecycle_count": 0,
                    "last_transport_http_status": 0,
                    "last_lifecycle_kind": 0,
                    "last_order_status": 0,
                },
            )
            if event.kind == "local_risk":
                row["local_risk_decision_code"] = event.decision_code
                row["local_risk_reject_reason"] = event.reject_reason
            elif event.kind == "submission":
                row["submission_seen"] = 1
            elif event.kind == "oms_decision":
                row["oms_decision_seen"] = 1
            elif event.kind == "oms_transport":
                row["transport_seen"] = 1
                row["last_transport_http_status"] = event.transport_http_status
            elif event.kind == "oms_lifecycle":
                row["lifecycle_count"] = int(row["lifecycle_count"]) + 1
                row["last_lifecycle_kind"] = event.lifecycle_kind
                row["last_order_status"] = event.order_status

        for key in sorted(grouped):
            yield grouped[key]


def _leg_fields() -> list[tuple[str, str]]:
    return [
        ("shard_id", "int64"),
        ("signal_id", "int64"),
        ("event_id", "int64"),
        ("market_id", "int64"),
        ("market_ticker", "string"),
        ("leg_index", "int64"),
        ("side", "int64"),
        ("qty_lots", "int64"),
        ("price_ticks", "int64"),
        ("local_risk_decision_code", "int64"),
        ("local_risk_reject_reason", "int64"),
        ("submission_seen", "int64"),
        ("oms_decision_seen", "int64"),
        ("transport_seen", "int64"),
        ("lifecycle_count", "int64"),
        ("last_transport_http_status", "int64"),
        ("last_lifecycle_kind", "int64"),
        ("last_order_status", "int64"),
    ]


def _latency_rows(audit_events: Sequence[AuditEvent]) -> Iterable[dict[str, object]]:
    for event in audit_events:
        yield {
            "kind": event.kind,
            "ts_ns": event.ts_ns,
            "shard_id": event.shard_id,
            "signal_id": event.signal_id,
            "event_id": event.event_id,
            "market_id": event.market_id,
            "leg_index": event.leg_index,
            "tick_to_signal_ns": event.tick_to_signal_ns,
            "signal_to_submission_ns": event.signal_to_submission_ns,
            "submission_to_decision_ns": event.submission_to_decision_ns,
            "decision_to_transport_ns": event.decision_to_transport_ns,
            "tick_to_transport_submit_ns": event.tick_to_transport_submit_ns,
            "transport_submit_to_response_ns": event.transport_submit_to_response_ns,
            "tick_to_transport_response_ns": event.tick_to_transport_response_ns,
            "transport_to_first_fill_ns": event.transport_to_first_fill_ns,
            "tick_to_first_fill_ns": event.tick_to_first_fill_ns,
            "tick_to_terminal_ns": event.tick_to_terminal_ns,
        }


def _latency_fields() -> list[tuple[str, str]]:
    return [
        ("kind", "string"),
        ("ts_ns", "int64"),
        ("shard_id", "int64"),
        ("signal_id", "int64"),
        ("event_id", "int64"),
        ("market_id", "int64"),
        ("leg_index", "int64"),
        ("tick_to_signal_ns", "int64"),
        ("signal_to_submission_ns", "int64"),
        ("submission_to_decision_ns", "int64"),
        ("decision_to_transport_ns", "int64"),
        ("tick_to_transport_submit_ns", "int64"),
        ("transport_submit_to_response_ns", "int64"),
        ("tick_to_transport_response_ns", "int64"),
        ("transport_to_first_fill_ns", "int64"),
        ("tick_to_first_fill_ns", "int64"),
        ("tick_to_terminal_ns", "int64"),
    ]


def _write_tape_tables(output_dir: Path, config_index: ConfigIndex, tape_path: Path, fmt: str) -> tuple[int, int, dict[str, dict[str, str]], dict[str, int | None]]:
    frame_fields = [
        ("record_index", "int64"),
        ("recv_ts_ns", "int64"),
        ("payload_len", "int64"),
        ("message_type", "string"),
        ("market_ticker", "string"),
        ("sequence_id", "int64"),
    ]
    market_event_fields = [
        ("record_index", "int64"),
        ("recv_ts_ns", "int64"),
        ("raw_type", "string"),
        ("market_ticker", "string"),
        ("market_id", "int64"),
        ("event_id", "int64"),
        ("topology_kind", "string"),
        ("sequence_id", "int64"),
        ("side", "string"),
        ("price_ticks", "int64"),
        ("delta_qty_lots", "int64"),
        ("trade_qty_lots", "int64"),
        ("best_bid_ticks", "int64"),
        ("best_ask_ticks", "int64"),
        ("bids_json", "string"),
        ("asks_json", "string"),
    ]

    frame_path, frame_csv_path = _table_paths(output_dir, "frames", fmt)
    market_path, market_csv_path = _table_paths(output_dir, "market_events", fmt)
    frame_writer = _ParquetRowWriter(frame_path, frame_fields)
    market_writer = _ParquetRowWriter(market_path, market_event_fields)
    frame_csv_writer = _OptionalCsvWriter(frame_csv_path, [field for field, _ in frame_fields])
    market_csv_writer = _OptionalCsvWriter(market_csv_path, [field for field, _ in market_event_fields])

    frame_count = 0
    market_event_count = 0
    tape_bounds = {"start_ts_ns": None, "end_ts_ns": None}
    try:
        for payload in iter_tape_payloads(tape_path):
            message = payload.message if isinstance(payload.message, dict) else {}
            msg = message.get("msg") if isinstance(message.get("msg"), dict) else {}
            market_ticker = str(msg.get("market_ticker", ""))
            sequence_id = message.get("seq")
            _update_time_bounds(tape_bounds, payload.recv_ts_ns)

            frame_row = {
                "record_index": payload.record_index,
                "recv_ts_ns": payload.recv_ts_ns or 0,
                "payload_len": len(payload.payload),
                "message_type": str(message.get("type", "")),
                "market_ticker": market_ticker,
                "sequence_id": int(sequence_id) if sequence_id is not None else 0,
            }
            frame_writer.write(frame_row)
            frame_csv_writer.write(frame_row)
            frame_count += 1

            market_event = decode_market_event(payload)
            if market_event is None:
                continue
            route = config_index.markets_by_ticker.get(market_event.market_ticker)
            market_row = {
                "record_index": market_event.record_index,
                "recv_ts_ns": market_event.recv_ts_ns or 0,
                "raw_type": market_event.raw_type,
                "market_ticker": market_event.market_ticker,
                "market_id": route.market_id if route else 0,
                "event_id": route.event_id if route else 0,
                "topology_kind": route.topology_kind if route else "",
                "sequence_id": market_event.sequence_id or 0,
                "side": market_event.side,
                "price_ticks": market_event.price_ticks,
                "delta_qty_lots": market_event.delta_qty_lots,
                "trade_qty_lots": market_event.trade_qty_lots,
                "best_bid_ticks": market_event.bids[0][0] if market_event.bids else 0,
                "best_ask_ticks": market_event.asks[0][0] if market_event.asks else 0,
                "bids_json": _compact_json(list(market_event.bids)) if market_event.bids else "",
                "asks_json": _compact_json(list(market_event.asks)) if market_event.asks else "",
            }
            market_writer.write(market_row)
            market_csv_writer.write(market_row)
            market_event_count += 1
    finally:
        frame_csv_writer.close()
        market_csv_writer.close()

    frame_writer.close()
    market_writer.close()
    return frame_count, market_event_count, {
        "frames": {"parquet": str(frame_path), **({"csv": str(frame_csv_path)} if frame_csv_path is not None else {})},
        "market_events": {"parquet": str(market_path), **({"csv": str(market_csv_path)} if market_csv_path is not None else {})},
    }, tape_bounds


def _trace_request_fields() -> list[tuple[str, str]]:
    return [
        ("source_file", "string"),
        ("dispatch_request_id", "int64"),
        ("group_intent_id", "int64"),
        ("group_leg_count", "int64"),
        ("item_count", "int64"),
        ("terminal_state", "string"),
        ("http_status_code", "int64"),
        ("retry_count", "int64"),
        ("reused_connection", "int64"),
        ("request_target", "string"),
        ("queued_ts_ns", "int64"),
        ("session_submit_ts_ns", "int64"),
        ("connection_start_ts_ns", "int64"),
        ("write_start_ts_ns", "int64"),
        ("request_sent_ts_ns", "int64"),
        ("response_recv_ts_ns", "int64"),
        ("completed_ts_ns", "int64"),
        ("latency_ns", "int64"),
        ("write_queue_ns", "int64"),
        ("connection_start_to_request_sent_ns", "int64"),
        ("write_start_to_request_sent_ns", "int64"),
        ("transport_submit_to_response_ns", "int64"),
        ("request_order_count", "int64"),
        ("response_order_count", "int64"),
        ("trace_error_message", "string"),
        ("completion_error_message", "string"),
    ]


def _trace_order_fields() -> list[tuple[str, str]]:
    return [
        ("source_file", "string"),
        ("dispatch_request_id", "int64"),
        ("group_intent_id", "int64"),
        ("order_index", "int64"),
        ("client_order_id", "string"),
        ("ticker", "string"),
        ("action", "string"),
        ("side", "string"),
        ("requested_count_fp", "double"),
        ("requested_yes_price", "int64"),
        ("order_id", "string"),
        ("status", "string"),
        ("fill_count_fp", "double"),
        ("remaining_count_fp", "double"),
        ("yes_price_dollars", "double"),
        ("no_price_dollars", "double"),
        ("taker_fees_dollars", "double"),
        ("taker_fill_cost_dollars", "double"),
        ("created_time", "string"),
        ("last_update_time", "string"),
    ]


def _parse_json_body(raw_value: object) -> dict[str, Any] | None:
    if not isinstance(raw_value, str) or not raw_value:
        return None
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _decimal_to_float(raw_value: object) -> float:
    if raw_value in (None, ""):
        return 0.0
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        return 0.0


def _write_trace_tables(output_dir: Path, trace_paths: Sequence[Path], fmt: str) -> tuple[int, int, dict[str, dict[str, str]], dict[str, object]]:
    request_path, request_csv_path = _table_paths(output_dir, "trace_requests", fmt)
    order_path, order_csv_path = _table_paths(output_dir, "trace_orders", fmt)
    request_writer = _ParquetRowWriter(request_path, _trace_request_fields())
    order_writer = _ParquetRowWriter(order_path, _trace_order_fields())
    request_csv_writer = _OptionalCsvWriter(request_csv_path, [field for field, _ in _trace_request_fields()])
    order_csv_writer = _OptionalCsvWriter(order_csv_path, [field for field, _ in _trace_order_fields()])

    request_count = 0
    order_count = 0
    trace_bounds = {"start_ts_ns": None, "end_ts_ns": None}
    user_ids: set[str] = set()
    subaccount_numbers: set[int] = set()
    request_targets: set[str] = set()
    try:
        for trace_path in trace_paths:
            with trace_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    record = json.loads(line)
                    request_body = _parse_json_body(record.get("request_body")) or {}
                    response_body = _parse_json_body(record.get("response_body")) or {}
                    request_orders = request_body.get("orders") if isinstance(request_body.get("orders"), list) else []
                    response_orders = response_body.get("orders") if isinstance(response_body.get("orders"), list) else []
                    request_target = str(record.get("request_target", ""))
                    if request_target:
                        request_targets.add(request_target)
                    _update_time_bounds(trace_bounds, int(record.get("queued_ts_ns", 0) or 0))
                    _update_time_bounds(trace_bounds, int(record.get("session_submit_ts_ns", 0) or 0))
                    _update_time_bounds(trace_bounds, int(record.get("request_sent_ts_ns", 0) or 0))
                    _update_time_bounds(trace_bounds, int(record.get("response_recv_ts_ns", 0) or 0))
                    _update_time_bounds(trace_bounds, int(record.get("completed_ts_ns", 0) or 0))
                    request_row = {
                        "source_file": trace_path.name,
                        "dispatch_request_id": int(record.get("dispatch_request_id", 0)),
                        "group_intent_id": int(record.get("group_intent_id", 0)),
                        "group_leg_count": int(record.get("group_leg_count", 0)),
                        "item_count": int(record.get("item_count", 0)),
                        "terminal_state": str(record.get("terminal_state", "")),
                        "http_status_code": int(record.get("http_status_code", 0)),
                        "retry_count": int(record.get("retry_count", 0)),
                        "reused_connection": int(bool(record.get("reused_connection", False))),
                        "request_target": str(record.get("request_target", "")),
                        "queued_ts_ns": int(record.get("queued_ts_ns", 0)),
                        "session_submit_ts_ns": int(record.get("session_submit_ts_ns", 0)),
                        "connection_start_ts_ns": int(record.get("connection_start_ts_ns", 0)),
                        "write_start_ts_ns": int(record.get("write_start_ts_ns", 0)),
                        "request_sent_ts_ns": int(record.get("request_sent_ts_ns", 0)),
                        "response_recv_ts_ns": int(record.get("response_recv_ts_ns", 0)),
                        "completed_ts_ns": int(record.get("completed_ts_ns", 0)),
                        "latency_ns": int(record.get("latency_ns", 0)),
                        "write_queue_ns": int(record.get("write_queue_ns", 0)),
                        "connection_start_to_request_sent_ns": int(record.get("connection_start_to_request_sent_ns", 0)),
                        "write_start_to_request_sent_ns": int(record.get("write_start_to_request_sent_ns", 0)),
                        "transport_submit_to_response_ns": int(record.get("request_sent_ts_ns", 0)) and int(record.get("response_recv_ts_ns", 0)) - int(record.get("request_sent_ts_ns", 0)),
                        "request_order_count": len(request_orders),
                        "response_order_count": len(response_orders),
                        "trace_error_message": str(record.get("trace_error_message", "")),
                        "completion_error_message": str(record.get("completion_error_message", "")),
                    }
                    request_writer.write(request_row)
                    request_csv_writer.write(request_row)
                    request_count += 1

                    for index, response_item in enumerate(response_orders):
                        response_order = response_item.get("order") if isinstance(response_item, dict) else {}
                        request_order = request_orders[index] if index < len(request_orders) and isinstance(request_orders[index], dict) else {}
                        user_id = str(response_order.get("user_id", ""))
                        if user_id:
                            user_ids.add(user_id)
                        subaccount_number = response_order.get("subaccount_number")
                        if subaccount_number not in (None, ""):
                            try:
                                subaccount_numbers.add(int(subaccount_number))
                            except (TypeError, ValueError):
                                pass
                        order_row = {
                            "source_file": trace_path.name,
                            "dispatch_request_id": int(record.get("dispatch_request_id", 0)),
                            "group_intent_id": int(record.get("group_intent_id", 0)),
                            "order_index": index,
                            "client_order_id": str(response_order.get("client_order_id", request_order.get("client_order_id", ""))),
                            "ticker": str(response_order.get("ticker", request_order.get("ticker", ""))),
                            "action": str(response_order.get("action", request_order.get("action", ""))),
                            "side": str(response_order.get("side", request_order.get("side", ""))),
                            "requested_count_fp": _decimal_to_float(request_order.get("count_fp")),
                            "requested_yes_price": int(request_order.get("yes_price", 0) or 0),
                            "order_id": str(response_order.get("order_id", "")),
                            "status": str(response_order.get("status", "")),
                            "fill_count_fp": _decimal_to_float(response_order.get("fill_count_fp")),
                            "remaining_count_fp": _decimal_to_float(response_order.get("remaining_count_fp")),
                            "yes_price_dollars": _decimal_to_float(response_order.get("yes_price_dollars")),
                            "no_price_dollars": _decimal_to_float(response_order.get("no_price_dollars")),
                            "taker_fees_dollars": _decimal_to_float(response_order.get("taker_fees_dollars")),
                            "taker_fill_cost_dollars": _decimal_to_float(response_order.get("taker_fill_cost_dollars")),
                            "created_time": str(response_order.get("created_time", "")),
                            "last_update_time": str(response_order.get("last_update_time", "")),
                        }
                        order_writer.write(order_row)
                        order_csv_writer.write(order_row)
                        order_count += 1
    finally:
        request_csv_writer.close()
        order_csv_writer.close()

    request_writer.close()
    order_writer.close()
    return request_count, order_count, {
        "trace_requests": {"parquet": str(request_path), **({"csv": str(request_csv_path)} if request_csv_path is not None else {})},
        "trace_orders": {"parquet": str(order_path), **({"csv": str(order_csv_path)} if order_csv_path is not None else {})},
    }, {
        "time_bounds": trace_bounds,
        "user_ids": sorted(user_ids),
        "subaccount_numbers": sorted(subaccount_numbers),
        "request_targets": sorted(request_targets),
    }


def _run_metadata_payload(
    *,
    run_id: str,
    created_utc: str,
    config_file: Path,
    audit_bounds: dict[str, int | None],
    tape_bounds: dict[str, int | None],
    trace_metadata: dict[str, object],
) -> dict[str, object]:
    trace_bounds = trace_metadata.get("time_bounds") if isinstance(trace_metadata.get("time_bounds"), dict) else {"start_ts_ns": None, "end_ts_ns": None}
    session_bounds = _merge_bounds(audit_bounds, tape_bounds, trace_bounds)
    return {
        "run_id": run_id,
        "created_utc": created_utc,
        "venue": "kalshi",
        "repository": _git_metadata(config_file.parent),
        "session": _time_bounds_payload(session_bounds),
        "sources": {
            "tape": _time_bounds_payload(tape_bounds),
            "audit": _time_bounds_payload(audit_bounds),
            "rest_trace": _time_bounds_payload(trace_bounds),
        },
        "account": {
            "user_ids": trace_metadata.get("user_ids", []),
            "subaccount_numbers": trace_metadata.get("subaccount_numbers", []),
        },
        "transport": {
            "request_targets": trace_metadata.get("request_targets", []),
        },
    }


def load_ingested_run(path: str | Path) -> IngestedRun:
    run_root = Path(path).expanduser().resolve()
    if run_root.is_file():
        if run_root.name != "manifest.json":
            raise ValueError(f"expected run directory or manifest.json path, got: {run_root}")
        manifest_path = run_root
        run_root = run_root.parent
    else:
        manifest_path = run_root / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest not found: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    metadata_path = run_root / "run_metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8")) if metadata_path.exists() else {}
    run_id = str(manifest.get("run_id") or run_root.name)
    return IngestedRun(
        run_id=run_id,
        root=run_root,
        manifest_path=manifest_path,
        metadata_path=metadata_path if metadata_path.exists() else None,
        manifest=manifest,
        metadata=metadata,
    )


def ingest_run(
    *,
    config_path: str | Path,
    audit_path: str | Path,
    tape_path: str | Path,
    output_root: str | Path = "logs/runs",
    run_id: str | None = None,
    trace_paths: Sequence[str] | None = None,
    fmt: str = "parquet",
) -> dict[str, object]:
    if fmt not in {"parquet", "both"}:
        raise ValueError("fmt must be one of: parquet, both")
    config_file = Path(config_path).expanduser().resolve()
    audit_file = Path(audit_path).expanduser().resolve()
    tape_file = Path(tape_path).expanduser().resolve()
    resolved_trace_paths = _resolve_trace_paths(trace_paths, audit_file)

    resolved_run_id = _sanitize_run_id(run_id or _default_run_id(tape_file))
    output_dir = Path(output_root).expanduser().resolve() / resolved_run_id
    if output_dir.exists():
        raise ValueError(f"ingest output already exists: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=False)

    config_index = load_config_index(config_file)
    audit_events = load_audit_events(audit_file)
    bundles = build_signal_bundles(audit_events)
    audit_bounds = _audit_time_bounds(audit_events)
    created_utc = datetime.now(timezone.utc).isoformat()

    shutil.copyfile(config_file, output_dir / "config.snapshot.json")

    table_counts: dict[str, int] = {}
    table_files: dict[str, dict[str, str]] = {}
    market_route_fields = [(field.name, "string" if field.name in {"market_ticker", "topology_kind"} else "int64") for field in fields(MarketRoute)]
    market_route_count, market_route_files = _stream_table(
        output_dir=output_dir,
        name="market_routes",
        fields_with_types=market_route_fields,
        rows=_market_route_rows(config_index),
        fmt=fmt,
    )
    table_counts["market_routes"] = market_route_count
    table_files["market_routes"] = market_route_files
    frame_count, market_event_count, tape_files, tape_bounds = _write_tape_tables(output_dir, config_index, tape_file, fmt)
    table_counts["frames"] = frame_count
    table_counts["market_events"] = market_event_count
    table_files.update(tape_files)
    audit_event_fields = [(field.name, "string" if field.name == "kind" else "int64") for field in fields(AuditEvent)]
    audit_count, audit_files = _stream_table(
        output_dir=output_dir,
        name="audit_events",
        fields_with_types=audit_event_fields,
        rows=_audit_event_rows(audit_events),
        fmt=fmt,
    )
    table_counts["audit_events"] = audit_count
    table_files["audit_events"] = audit_files
    signal_count, signal_files = _stream_table(
        output_dir=output_dir,
        name="signals",
        fields_with_types=_signal_fields(),
        rows=_signal_rows(bundles, config_index),
        fmt=fmt,
    )
    table_counts["signals"] = signal_count
    table_files["signals"] = signal_files
    leg_count, leg_files = _stream_table(
        output_dir=output_dir,
        name="legs",
        fields_with_types=_leg_fields(),
        rows=_leg_rows(bundles, config_index),
        fmt=fmt,
    )
    table_counts["legs"] = leg_count
    table_files["legs"] = leg_files
    latency_count, latency_files = _stream_table(
        output_dir=output_dir,
        name="latencies",
        fields_with_types=_latency_fields(),
        rows=_latency_rows(audit_events),
        fmt=fmt,
    )
    table_counts["latencies"] = latency_count
    table_files["latencies"] = latency_files

    if resolved_trace_paths:
        trace_requests_count, trace_orders_count, trace_files, trace_metadata = _write_trace_tables(output_dir, resolved_trace_paths, fmt)
    else:
        trace_requests_count = 0
        trace_orders_count = 0
        trace_files = {}
        trace_metadata = {"time_bounds": {"start_ts_ns": None, "end_ts_ns": None}, "user_ids": [], "subaccount_numbers": [], "request_targets": []}
    table_counts["trace_requests"] = trace_requests_count
    table_counts["trace_orders"] = trace_orders_count
    table_files.update(trace_files)

    audit_kind_counts = Counter(event.kind for event in audit_events)
    generated_tables = {
        table_name: {
            "row_count": table_counts[table_name],
            "files": files,
        }
        for table_name, files in table_files.items()
    }
    run_metadata = _run_metadata_payload(
        run_id=resolved_run_id,
        created_utc=created_utc,
        config_file=config_file,
        audit_bounds=audit_bounds,
        tape_bounds=tape_bounds,
        trace_metadata=trace_metadata,
    )
    metadata_path = output_dir / "run_metadata.json"
    metadata_path.write_text(json.dumps(run_metadata, indent=2, sort_keys=False), encoding="utf-8")
    manifest = {
        "run_id": resolved_run_id,
        "created_utc": created_utc,
        "storage_format": fmt,
        "run_metadata": str(metadata_path),
        "source_artifacts": {
            "config": _source_metadata(config_file, "config"),
            "audit": {**_source_metadata(audit_file, "audit"), "time_bounds": _time_bounds_payload(audit_bounds)},
            "tape": {**_source_metadata(tape_file, "tape"), "time_bounds": _time_bounds_payload(tape_bounds)},
            "rest_traces": [
                {**_source_metadata(path, "rest_trace")}
                for path in resolved_trace_paths
            ],
        },
        "generated_tables": generated_tables,
        "summary": {
            "audit_kind_counts": dict(audit_kind_counts),
            "event_count": len(config_index.events_by_id),
            "market_count": len(config_index.routes),
            "signal_count": len(bundles),
            "trace_source_count": len(resolved_trace_paths),
        },
        "sources": [
            _source_metadata(config_file, "config"),
            _source_metadata(audit_file, "audit"),
            _source_metadata(tape_file, "tape"),
            *[_source_metadata(path, "rest_trace") for path in resolved_trace_paths],
        ],
        "table_counts": table_counts,
        "table_files": table_files,
        "audit_kind_counts": dict(audit_kind_counts),
        "event_count": len(config_index.events_by_id),
        "market_count": len(config_index.routes),
        "signal_count": len(bundles),
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=False), encoding="utf-8")

    return {
        "run_id": resolved_run_id,
        "output_dir": str(output_dir),
        "manifest": str(manifest_path),
        "run_metadata": str(metadata_path),
        "trace_source_count": len(resolved_trace_paths),
        "storage_format": fmt,
        "tables": table_counts,
        "table_files": table_files,
    }