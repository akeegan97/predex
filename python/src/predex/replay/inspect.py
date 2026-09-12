from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from .app_config import AppConfigIndex, AppMarketRoute, load_app_config_index
from .market_data_tape import (
    RECORD_HEADER,
    MarketDataTapeRecord,
    iter_market_data_records,
    read_market_data_tape_header,
)


def _route_mismatches(record: MarketDataTapeRecord, route: AppMarketRoute | None) -> list[str]:
    if route is None:
        return ["unknown_market_id"]

    header = record.header
    mismatches: list[str] = []
    if header.event_id != route.event_id:
        mismatches.append("event_id")
    if header.affinity_key != route.affinity_key:
        mismatches.append("affinity_key")
    if header.shard_index != route.shard_index:
        mismatches.append("shard_index")
    if header.shard_event_index != route.shard_event_index:
        mismatches.append("shard_event_index")
    if header.event_market_index != route.event_market_index:
        mismatches.append("event_market_index")
    if header.topology != route.topology_code:
        mismatches.append("topology")
    return mismatches


def _message_type(message: dict[str, Any] | None) -> str:
    if message is None:
        return ""
    return str(message.get("type", ""))


def _message_market_ticker(message: dict[str, Any] | None) -> str:
    if message is None:
        return ""
    msg = message.get("msg")
    if not isinstance(msg, dict):
        return ""
    return str(msg.get("market_ticker", ""))


def _sample_payload(
    record: MarketDataTapeRecord,
    route: AppMarketRoute | None,
    message: dict[str, Any] | None,
    mismatches: list[str],
) -> dict[str, object]:
    header = record.header
    market_ticker = _message_market_ticker(message)
    if route is not None and market_ticker and market_ticker != route.market_ticker:
        mismatches = [*mismatches, "payload_market_ticker"]

    return {
        "record_index": record.record_index,
        "universe_version": header.universe_version,
        "recv_ts_ns": header.recv_ts_ns,
        "sid": header.sid,
        "sequence": header.sequence,
        "frame_kind": header.frame_kind_name,
        "payload_type": _message_type(message),
        "market_id": header.market_id,
        "event_id": header.event_id,
        "market_ticker": route.market_ticker if route is not None else "",
        "payload_market_ticker": market_ticker,
        "shard_index": header.shard_index,
        "shard_event_index": header.shard_event_index,
        "event_market_index": header.event_market_index,
        "payload_len": header.payload_len,
        "mismatches": mismatches,
    }


def _empty_payload_stats() -> dict[str, int | None]:
    return {
        "min": None,
        "max": None,
        "total": 0,
    }


def _update_payload_stats(stats: dict[str, int | None], payload_len: int) -> None:
    current_min = stats["min"]
    current_max = stats["max"]
    stats["min"] = payload_len if current_min is None else min(current_min, payload_len)
    stats["max"] = payload_len if current_max is None else max(current_max, payload_len)
    stats["total"] = int(stats["total"] or 0) + payload_len


def inspect_market_data_tape(
    *,
    config_path: str | Path,
    tape_path: str | Path,
    sample_limit: int = 20,
) -> dict[str, object]:
    config_index: AppConfigIndex = load_app_config_index(config_path)
    tape_header = read_market_data_tape_header(tape_path)

    frame_kind_counts: Counter[str] = Counter()
    topology_counts: Counter[str] = Counter()
    payload_type_counts: Counter[str] = Counter()
    universe_version_counts: Counter[int] = Counter()
    route_mismatch_counts: Counter[str] = Counter()
    unknown_market_ids: Counter[int] = Counter()
    json_decode_failures = 0
    record_count = 0
    first_recv_ts_ns: int | None = None
    last_recv_ts_ns: int | None = None
    payload_stats = _empty_payload_stats()
    samples: list[dict[str, object]] = []

    for record in iter_market_data_records(tape_path):
        record_count += 1
        header = record.header
        frame_kind_counts[header.frame_kind_name] += 1
        topology_counts[header.topology_name] += 1
        universe_version_counts[header.universe_version] += 1
        _update_payload_stats(payload_stats, header.payload_len)
        if first_recv_ts_ns is None:
            first_recv_ts_ns = header.recv_ts_ns
        last_recv_ts_ns = header.recv_ts_ns

        route = config_index.markets_by_id.get(header.market_id)
        mismatches = _route_mismatches(record, route)
        for mismatch in mismatches:
            route_mismatch_counts[mismatch] += 1
        if route is None:
            unknown_market_ids[header.market_id] += 1

        message: dict[str, Any] | None = None
        try:
            message = record.decode_json()
            payload_type_counts[_message_type(message) or "<missing>"] += 1
        except ValueError:
            json_decode_failures += 1

        if len(samples) < sample_limit:
            samples.append(_sample_payload(record, route, message, mismatches))

    return {
        "tape": {
            "path": str(tape_path),
            "version": tape_header.version,
            "flags": tape_header.flags,
            "record_header_bytes": RECORD_HEADER.size,
            "record_count": record_count,
            "first_recv_ts_ns": first_recv_ts_ns,
            "last_recv_ts_ns": last_recv_ts_ns,
            "payload_bytes": payload_stats,
            "json_decode_failures": json_decode_failures,
        },
        "config": {
            "path": str(config_path),
            "shard_count": config_index.shard_count,
            "event_count": len(config_index.events),
            "market_count": len(config_index.routes),
        },
        "counts": {
            "frame_kinds": dict(frame_kind_counts),
            "topologies": dict(topology_counts),
            "payload_types": dict(payload_type_counts),
            "universe_versions": {str(key): value for key, value in universe_version_counts.items()},
            "route_mismatches": dict(route_mismatch_counts),
            "unknown_market_ids": {str(key): value for key, value in unknown_market_ids.most_common(20)},
        },
        "samples": samples,
    }
