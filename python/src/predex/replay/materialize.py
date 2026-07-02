from __future__ import annotations

import gzip
import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .app_config import AppConfigIndex, load_app_config_index
from .market_data_tape import iter_market_data_records


PRICE_TICK_SCALE = 10_000
QTY_LOT_SCALE = 100
EXPECTED_TABLE_NAMES = (
    "frames",
    "deltas",
    "trades",
    "snapshots",
    "snapshot_levels",
    "lifecycles",
    "event_routes",
    "market_routes",
)


@dataclass(frozen=True, slots=True)
class MaterializeResult:
    run_dir: Path
    tables_dir: Path
    manifest_path: Path
    manifest: dict[str, Any]


def _require_pyarrow():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "materialization requires pyarrow; install the repo venv dependencies or run through ./scripts/predex"
        ) from exc
    return pa, pq


class _ParquetTableWriter:
    def __init__(self, path: Path, schema: Any, *, batch_size: int, compression: str) -> None:
        self.path = path
        self.schema = schema
        self.batch_size = batch_size
        self.compression = compression
        self.rows: list[dict[str, Any]] = []
        self.writer: Any | None = None
        self.row_count = 0
        self._pa, self._pq = _require_pyarrow()

    def append(self, row: dict[str, Any]) -> None:
        self.rows.append(row)
        if len(self.rows) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self.rows:
            return
        table = self._pa.Table.from_pylist(self.rows, schema=self.schema)
        if self.writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.writer = self._pq.ParquetWriter(self.path, self.schema, compression=self.compression)
        self.writer.write_table(table)
        self.row_count += len(self.rows)
        self.rows.clear()

    def close(self) -> None:
        self.flush()
        if self.writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._pq.write_table(self._pa.Table.from_pylist([], schema=self.schema), self.path, compression=self.compression)
        else:
            self.writer.close()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _scaled_fp_to_int(value: Any, *, scale: int, decimal_places: int, allow_negative: bool = False) -> int | None:
    if value is None:
        return None
    text = str(value)
    if not text:
        return None

    negative = False
    if text[0] in "+-":
        negative = text[0] == "-"
        if negative and not allow_negative:
            return None
        text = text[1:]
    if not text:
        return None

    whole_text, dot, frac_text = text.partition(".")
    if not whole_text or (dot and len(frac_text) > decimal_places):
        return None
    if not whole_text.isdigit() or (frac_text and not frac_text.isdigit()):
        return None

    whole = int(whole_text)
    frac = int(frac_text) if frac_text else 0
    frac *= 10 ** (decimal_places - len(frac_text))
    value_int = whole * scale + frac
    return -value_int if negative else value_int


def _price_ticks(value: Any) -> int | None:
    ticks = _scaled_fp_to_int(value, scale=PRICE_TICK_SCALE, decimal_places=4)
    if ticks is None or ticks < 0 or ticks > PRICE_TICK_SCALE:
        return None
    return ticks


def _qty_lots(value: Any, *, allow_negative: bool = False) -> int | None:
    return _scaled_fp_to_int(value, scale=QTY_LOT_SCALE, decimal_places=2, allow_negative=allow_negative)


def _first_present(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload:
            return payload[key]
    return None


def _book_side(side: str | None) -> str:
    if side in {"yes", "bid"}:
        return "bid"
    if side in {"no", "ask"}:
        return "ask"
    return "unknown"


def _reciprocal_price_ticks(price_ticks: int | None) -> int | None:
    if price_ticks is None:
        return None
    return PRICE_TICK_SCALE - price_ticks


def _market_ticker(message: dict[str, Any], route_ticker: str) -> str:
    msg = message.get("msg")
    if isinstance(msg, dict):
        return str(msg.get("market_ticker") or route_ticker)
    return route_ticker


def _parse_payload(record: Any) -> dict[str, Any] | None:
    try:
        return record.decode_json()
    except ValueError:
        return None


def _msg(message: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(message, dict):
        return {}
    payload = message.get("msg")
    return payload if isinstance(payload, dict) else {}


def _write_route_tables(index: AppConfigIndex, tables_dir: Path, *, compression: str) -> dict[str, int]:
    pa, pq = _require_pyarrow()

    event_rows = [
        {
            "event_id": event.event_id,
            "affinity_key": event.affinity_key,
            "topology": event.topology,
            "topology_code": event.topology_code,
            "shard_index": event.shard_index,
            "shard_event_index": event.shard_event_index,
            "market_count": len(event.markets),
        }
        for event in index.events
    ]
    market_rows = [
        {
            "event_id": route.event_id,
            "market_id": route.market_id,
            "market_ticker": route.market_ticker,
            "affinity_key": route.affinity_key,
            "topology": route.topology,
            "topology_code": route.topology_code,
            "shard_index": route.shard_index,
            "shard_event_index": route.shard_event_index,
            "event_market_index": route.event_market_index,
            "tradeable": route.tradeable,
            "price_level_structure": route.price_level_structure,
        }
        for route in index.routes
    ]

    event_schema = pa.schema(
        [
            ("event_id", pa.uint32()),
            ("affinity_key", pa.uint64()),
            ("topology", pa.string()),
            ("topology_code", pa.uint8()),
            ("shard_index", pa.uint32()),
            ("shard_event_index", pa.uint32()),
            ("market_count", pa.uint32()),
        ]
    )
    market_schema = pa.schema(
        [
            ("event_id", pa.uint32()),
            ("market_id", pa.uint32()),
            ("market_ticker", pa.string()),
            ("affinity_key", pa.uint64()),
            ("topology", pa.string()),
            ("topology_code", pa.uint8()),
            ("shard_index", pa.uint32()),
            ("shard_event_index", pa.uint32()),
            ("event_market_index", pa.uint32()),
            ("tradeable", pa.bool_()),
            ("price_level_structure", pa.string()),
        ]
    )

    pq.write_table(pa.Table.from_pylist(event_rows, schema=event_schema), tables_dir / "event_routes.parquet", compression=compression)
    pq.write_table(pa.Table.from_pylist(market_rows, schema=market_schema), tables_dir / "market_routes.parquet", compression=compression)
    return {"event_routes": len(event_rows), "market_routes": len(market_rows)}


def _gzip_copy(path: Path) -> Path:
    output = path.with_suffix(path.suffix + ".gz")
    with path.open("rb") as source, gzip.open(output, "wb", compresslevel=6) as target:
        shutil.copyfileobj(source, target, length=1024 * 1024)
    return output


def _expected_table_paths(tables_path: Path) -> dict[str, Path]:
    return {name: tables_path / f"{name}.parquet" for name in EXPECTED_TABLE_NAMES}


def materialize_run(
    run_dir: str | Path,
    *,
    tables_dir: str | Path | None = None,
    batch_size: int = 100_000,
    parquet_compression: str = "zstd",
    compress_if_verified: bool = False,
    remove_if_verified: bool = False,
) -> MaterializeResult:
    pa, _pq = _require_pyarrow()

    run_dir = Path(run_dir)
    config_path = run_dir / "config.json"
    report_path = run_dir / "report.json"
    tape_path = run_dir / "tape.bin"
    if not config_path.exists():
        raise FileNotFoundError(f"missing config: {config_path}")
    if not tape_path.exists():
        raise FileNotFoundError(f"missing tape: {tape_path}")

    tables_path = Path(tables_dir) if tables_dir is not None else run_dir / "tables"
    tables_path.mkdir(parents=True, exist_ok=True)

    config_index = load_app_config_index(config_path)
    route_rows = _write_route_tables(config_index, tables_path, compression=parquet_compression)

    frames = _ParquetTableWriter(
        tables_path / "frames.parquet",
        pa.schema(
            [
                ("run_id", pa.string()),
                ("record_index", pa.uint64()),
                ("universe_version", pa.uint64()),
                ("recv_ts_ns", pa.uint64()),
                ("sequence", pa.uint64()),
                ("sid", pa.uint32()),
                ("market_id", pa.uint32()),
                ("event_id", pa.uint32()),
                ("affinity_key", pa.uint64()),
                ("shard_index", pa.uint32()),
                ("shard_event_index", pa.uint32()),
                ("event_market_index", pa.uint32()),
                ("payload_len", pa.uint32()),
                ("frame_kind", pa.string()),
                ("frame_kind_code", pa.uint8()),
                ("topology", pa.string()),
                ("topology_code", pa.uint8()),
            ]
        ),
        batch_size=batch_size,
        compression=parquet_compression,
    )
    deltas = _ParquetTableWriter(
        tables_path / "deltas.parquet",
        pa.schema(
            [
                ("run_id", pa.string()),
                ("record_index", pa.uint64()),
                ("recv_ts_ns", pa.uint64()),
                ("sequence", pa.uint64()),
                ("event_id", pa.uint32()),
                ("market_id", pa.uint32()),
                ("market_ticker", pa.string()),
                ("side", pa.string()),
                ("price_ticks", pa.int64()),
                ("delta_qty_lots", pa.int64()),
            ]
        ),
        batch_size=batch_size,
        compression=parquet_compression,
    )
    trades = _ParquetTableWriter(
        tables_path / "trades.parquet",
        pa.schema(
            [
                ("run_id", pa.string()),
                ("record_index", pa.uint64()),
                ("recv_ts_ns", pa.uint64()),
                ("sequence", pa.uint64()),
                ("event_id", pa.uint32()),
                ("market_id", pa.uint32()),
                ("market_ticker", pa.string()),
                ("yes_price_ticks", pa.int64()),
                ("no_price_ticks", pa.int64()),
                ("qty_lots", pa.int64()),
                ("aggressor", pa.string()),
            ]
        ),
        batch_size=batch_size,
        compression=parquet_compression,
    )
    snapshots = _ParquetTableWriter(
        tables_path / "snapshots.parquet",
        pa.schema(
            [
                ("run_id", pa.string()),
                ("record_index", pa.uint64()),
                ("recv_ts_ns", pa.uint64()),
                ("sequence", pa.uint64()),
                ("event_id", pa.uint32()),
                ("market_id", pa.uint32()),
                ("market_ticker", pa.string()),
                ("bid_level_count", pa.uint32()),
                ("ask_level_count", pa.uint32()),
            ]
        ),
        batch_size=batch_size,
        compression=parquet_compression,
    )
    snapshot_levels = _ParquetTableWriter(
        tables_path / "snapshot_levels.parquet",
        pa.schema(
            [
                ("run_id", pa.string()),
                ("record_index", pa.uint64()),
                ("recv_ts_ns", pa.uint64()),
                ("event_id", pa.uint32()),
                ("market_id", pa.uint32()),
                ("market_ticker", pa.string()),
                ("side", pa.string()),
                ("level_index", pa.uint32()),
                ("price_ticks", pa.int64()),
                ("qty_lots", pa.int64()),
            ]
        ),
        batch_size=batch_size,
        compression=parquet_compression,
    )
    lifecycles = _ParquetTableWriter(
        tables_path / "lifecycles.parquet",
        pa.schema(
            [
                ("run_id", pa.string()),
                ("record_index", pa.uint64()),
                ("recv_ts_ns", pa.uint64()),
                ("sequence", pa.uint64()),
                ("event_id", pa.uint32()),
                ("market_id", pa.uint32()),
                ("market_ticker", pa.string()),
                ("msg_json", pa.string()),
            ]
        ),
        batch_size=batch_size,
        compression=parquet_compression,
    )

    run_id = run_dir.name
    frame_kind_counts: dict[str, int] = {}
    json_decode_failures = 0
    unknown_market_ids: set[int] = set()

    for record in iter_market_data_records(tape_path):
        header = record.header
        route = config_index.markets_by_id.get(header.market_id)
        if route is None:
            unknown_market_ids.add(header.market_id)

        frames.append(
            {
                "run_id": run_id,
                "record_index": record.record_index,
                "universe_version": header.universe_version,
                "recv_ts_ns": header.recv_ts_ns,
                "sequence": header.sequence,
                "sid": header.sid,
                "market_id": header.market_id,
                "event_id": header.event_id,
                "affinity_key": header.affinity_key,
                "shard_index": header.shard_index,
                "shard_event_index": header.shard_event_index,
                "event_market_index": header.event_market_index,
                "payload_len": header.payload_len,
                "frame_kind": header.frame_kind_name,
                "frame_kind_code": header.frame_kind,
                "topology": header.topology_name,
                "topology_code": header.topology,
            }
        )
        frame_kind_counts[header.frame_kind_name] = frame_kind_counts.get(header.frame_kind_name, 0) + 1

        message = _parse_payload(record)
        if message is None:
            json_decode_failures += 1
            continue
        msg = _msg(message)
        market_ticker = _market_ticker(message, route.market_ticker if route is not None else "")

        if header.frame_kind_name == "orderbook_delta":
            side = _book_side(str(msg.get("side", "")))
            price = _price_ticks(_first_present(msg, "price_dollars", "price"))
            if side == "ask":
                price = _reciprocal_price_ticks(price)
            deltas.append(
                {
                    "run_id": run_id,
                    "record_index": record.record_index,
                    "recv_ts_ns": header.recv_ts_ns,
                    "sequence": header.sequence,
                    "event_id": header.event_id,
                    "market_id": header.market_id,
                    "market_ticker": market_ticker,
                    "side": side,
                    "price_ticks": price,
                    "delta_qty_lots": _qty_lots(_first_present(msg, "delta_fp", "delta"), allow_negative=True),
                }
            )
        elif header.frame_kind_name == "trade":
            yes_price = _price_ticks(_first_present(msg, "price_dollars", "yes_price_dollars", "price", "yes_price"))
            trades.append(
                {
                    "run_id": run_id,
                    "record_index": record.record_index,
                    "recv_ts_ns": header.recv_ts_ns,
                    "sequence": header.sequence,
                    "event_id": header.event_id,
                    "market_id": header.market_id,
                    "market_ticker": market_ticker,
                    "yes_price_ticks": yes_price,
                    "no_price_ticks": _reciprocal_price_ticks(yes_price),
                    "qty_lots": _qty_lots(_first_present(msg, "count_fp", "qty", "count")),
                    "aggressor": str(msg.get("taker_side", "unknown")),
                }
            )
        elif header.frame_kind_name == "orderbook_snapshot":
            yes_levels = msg.get("yes_dollars_fp", msg.get("yes", []))
            no_levels = msg.get("no_dollars_fp", msg.get("no", []))
            bid_level_count = len(yes_levels) if isinstance(yes_levels, list) else 0
            ask_level_count = len(no_levels) if isinstance(no_levels, list) else 0
            snapshots.append(
                {
                    "run_id": run_id,
                    "record_index": record.record_index,
                    "recv_ts_ns": header.recv_ts_ns,
                    "sequence": header.sequence,
                    "event_id": header.event_id,
                    "market_id": header.market_id,
                    "market_ticker": market_ticker,
                    "bid_level_count": bid_level_count,
                    "ask_level_count": ask_level_count,
                }
            )
            if isinstance(yes_levels, list):
                for level_index, level in enumerate(yes_levels):
                    if not isinstance(level, list) or len(level) < 2:
                        continue
                    snapshot_levels.append(
                        {
                            "run_id": run_id,
                            "record_index": record.record_index,
                            "recv_ts_ns": header.recv_ts_ns,
                            "event_id": header.event_id,
                            "market_id": header.market_id,
                            "market_ticker": market_ticker,
                            "side": "bid",
                            "level_index": level_index,
                            "price_ticks": _price_ticks(level[0]),
                            "qty_lots": _qty_lots(level[1]),
                        }
                    )
            if isinstance(no_levels, list):
                for level_index, level in enumerate(no_levels):
                    if not isinstance(level, list) or len(level) < 2:
                        continue
                    snapshot_levels.append(
                        {
                            "run_id": run_id,
                            "record_index": record.record_index,
                            "recv_ts_ns": header.recv_ts_ns,
                            "event_id": header.event_id,
                            "market_id": header.market_id,
                            "market_ticker": market_ticker,
                            "side": "ask",
                            "level_index": level_index,
                            "price_ticks": _reciprocal_price_ticks(_price_ticks(level[0])),
                            "qty_lots": _qty_lots(level[1]),
                        }
                    )
        elif header.frame_kind_name == "lifecycle":
            lifecycles.append(
                {
                    "run_id": run_id,
                    "record_index": record.record_index,
                    "recv_ts_ns": header.recv_ts_ns,
                    "sequence": header.sequence,
                    "event_id": header.event_id,
                    "market_id": header.market_id,
                    "market_ticker": market_ticker,
                    "msg_json": json.dumps(msg, sort_keys=True, separators=(",", ":")),
                }
            )

    for writer in (frames, deltas, trades, snapshots, snapshot_levels, lifecycles):
        writer.close()

    table_rows = {
        "frames": frames.row_count,
        "deltas": deltas.row_count,
        "trades": trades.row_count,
        "snapshots": snapshots.row_count,
        "snapshot_levels": snapshot_levels.row_count,
        "lifecycles": lifecycles.row_count,
        **route_rows,
    }
    expected_kind_rows = {
        "deltas": frame_kind_counts.get("orderbook_delta", 0),
        "trades": frame_kind_counts.get("trade", 0),
        "snapshots": frame_kind_counts.get("orderbook_snapshot", 0),
        "lifecycles": frame_kind_counts.get("lifecycle", 0),
    }
    checks = {
        "frames_match_tape_records": frames.row_count == sum(frame_kind_counts.values()),
        "deltas_match_frame_kind_count": deltas.row_count == expected_kind_rows["deltas"],
        "trades_match_frame_kind_count": trades.row_count == expected_kind_rows["trades"],
        "snapshots_match_frame_kind_count": snapshots.row_count == expected_kind_rows["snapshots"],
        "lifecycles_match_frame_kind_count": lifecycles.row_count == expected_kind_rows["lifecycles"],
        "all_market_ids_join_routes": not unknown_market_ids,
        "json_decode_failures_zero": json_decode_failures == 0,
    }
    verified = all(checks.values())

    compressed_artifacts: dict[str, str] = {}
    if compress_if_verified and verified:
        for name, path in (("config", config_path), ("report", report_path), ("tape", tape_path)):
            if path.exists():
                compressed_artifacts[name] = str(_gzip_copy(path))

    tape_gzip_path = tape_path.with_suffix(tape_path.suffix + ".gz")
    expected_tables = _expected_table_paths(tables_path)
    missing_expected_tables = [str(path) for path in expected_tables.values() if not path.exists()]
    remove_raw_checks = {
        "requested": remove_if_verified,
        "verified": verified,
        "tape_bin_exists": tape_path.exists(),
        "tape_bin_gz_exists": tape_gzip_path.exists(),
        "expected_tables_exist": not missing_expected_tables,
    }
    removed_artifacts: dict[str, str] = {}
    tape_stat = tape_path.stat()
    if all(remove_raw_checks.values()):
        removed_artifacts["tape"] = str(tape_path)
        tape_path.unlink()

    manifest = {
        "run_id": run_id,
        "materializer_version": 1,
        "created_at_utc": datetime.now(UTC).isoformat(),
        "verified": verified,
        "run_dir": str(run_dir),
        "tables_dir": str(tables_path),
        "config": {
            "path": str(config_path),
            "sha256": _sha256_file(config_path),
            "size_bytes": config_path.stat().st_size,
        },
        "report": {
            "path": str(report_path),
            "size_bytes": report_path.stat().st_size if report_path.exists() else 0,
        },
        "tape": {
            "path": str(tape_path),
            "size_bytes": tape_stat.st_size,
            "mtime_ns": tape_stat.st_mtime_ns,
            "exists": tape_path.exists(),
        },
        "tables": {
            name: {
                "path": str(tables_path / f"{name}.parquet"),
                "rows": rows,
            }
            for name, rows in table_rows.items()
        },
        "frame_kind_counts": frame_kind_counts,
        "expected_kind_rows": expected_kind_rows,
        "checks": checks,
        "json_decode_failures": json_decode_failures,
        "unknown_market_ids": sorted(unknown_market_ids),
        "compressed_artifacts": compressed_artifacts,
        "remove_raw_checks": remove_raw_checks,
        "missing_expected_tables": missing_expected_tables,
        "removed_artifacts": removed_artifacts,
    }

    manifest_path = tables_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=False) + "\n", encoding="utf-8")

    return MaterializeResult(
        run_dir=run_dir,
        tables_dir=tables_path,
        manifest_path=manifest_path,
        manifest=manifest,
    )
