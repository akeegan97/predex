from __future__ import annotations

import json
import math
from collections import Counter
from pathlib import Path

from .audit import build_signal_bundles, load_audit_events
from .config import load_config_index
from .timeline import build_event_timeline
from .windows import build_all_signal_edge_lifetimes


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


def _summarize_ms(values_ns: list[int]) -> dict[str, float | int]:
    values_ms = [value / 1_000_000.0 for value in values_ns if value > 0]
    if not values_ms:
        return {
            "count": 0,
            "mean_ms": 0.0,
            "p50_ms": 0.0,
            "p95_ms": 0.0,
            "p99_ms": 0.0,
            "max_ms": 0.0,
        }
    return {
        "count": len(values_ms),
        "mean_ms": sum(values_ms) / len(values_ms),
        "p50_ms": _percentile(values_ms, 0.50),
        "p95_ms": _percentile(values_ms, 0.95),
        "p99_ms": _percentile(values_ms, 0.99),
        "max_ms": max(values_ms),
    }


def _survival_curve_ms(values_ms: list[float], thresholds_ms: list[float]) -> dict[str, float]:
    if not values_ms:
        return {str(threshold): 0.0 for threshold in thresholds_ms}
    denom = float(len(values_ms))
    return {
        str(threshold): sum(1 for value in values_ms if value >= threshold) / denom
        for threshold in thresholds_ms
    }


def analyze_soak(
    *,
    config_path: str | Path,
    audit_path: str | Path,
    tape_path: str | Path,
    output_json: str | Path | None = None,
    mismatch_limit: int = 10,
) -> dict[str, object]:
    config_index = load_config_index(config_path)
    audit_events = load_audit_events(audit_path)
    bundles = build_signal_bundles(audit_events)

    bundles_by_event_id: dict[int, list[tuple[tuple[int, int], object]]] = {}
    for key, bundle in bundles.items():
        bundles_by_event_id.setdefault(bundle.event_id, []).append((key, bundle))

    matched_signal_keys: set[tuple[int, int]] = set()
    mismatches: list[dict[str, object]] = []
    for event_id, event_bundles in sorted(bundles_by_event_id.items()):
        timeline = build_event_timeline(
            config_index=config_index,
            bundles={key: bundle for key, bundle in event_bundles},
            tape_path=tape_path,
            event_id=event_id,
        )
        matched_keys_for_event = {(hit.shard_id, hit.signal_id) for hit in timeline.signal_hits}
        matched_signal_keys |= matched_keys_for_event

        for key, bundle in event_bundles:
            if key in matched_keys_for_event:
                continue
            pair_markets = [leg.market_id for leg in bundle.legs()]
            mismatches.append(
                {
                    "event_id": bundle.event_id,
                    "event_ticker": timeline.event_ticker,
                    "shard_id": bundle.shard_id,
                    "signal_id": bundle.signal_id,
                    "reason": "no replayed top-of-book match in event timeline",
                    "edge_ticks": bundle.group_signal.edge_ticks,
                    "market_ids": pair_markets,
                }
            )

    stage_sources = {
        "tick_to_signal_ms": [
            event.tick_to_signal_ns for event in audit_events if event.kind == "pipeline_probe"
        ],
        "signal_to_submission_ms": [
            event.signal_to_submission_ns for event in audit_events if event.kind == "submission"
        ],
        "submission_to_decision_ms": [
            event.submission_to_decision_ns for event in audit_events if event.kind == "oms_decision"
        ],
        "decision_to_transport_ms": [
            event.decision_to_transport_ns for event in audit_events if event.kind == "oms_transport"
        ],
        "tick_to_transport_submit_ms": [
            event.tick_to_transport_submit_ns for event in audit_events if event.kind == "oms_transport"
        ],
        "transport_submit_to_response_ms": [
            event.transport_submit_to_response_ns
            for event in audit_events
            if event.kind == "oms_transport"
        ],
        "tick_to_transport_response_ms": [
            event.tick_to_transport_response_ns for event in audit_events if event.kind == "oms_transport"
        ],
    }
    latency_summary = {
        stage_name: _summarize_ms(stage_values)
        for stage_name, stage_values in stage_sources.items()
    }

    transport_status_counts = Counter(
        event.transport_http_status
        for event in audit_events
        if event.kind == "oms_transport" and event.transport_http_status > 0
    )

    lifetimes = build_all_signal_edge_lifetimes(
        config_index=config_index,
        bundles=bundles,
        tape_path=tape_path,
    )
    all_lifetime_ms = [
        lifetime.duration_ms for lifetime in lifetimes.lifetimes if lifetime.duration_ms is not None
    ]
    signaled_lifetime_ms = [
        lifetime.duration_ms
        for lifetime in lifetimes.lifetimes
        if lifetime.duration_ms is not None and lifetime.signal_count > 0
    ]
    transported_lifetime_ms = [
        lifetime.duration_ms
        for lifetime in lifetimes.lifetimes
        if lifetime.duration_ms is not None and lifetime.transported_signal_count > 0
    ]
    thresholds_ms = [0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0]

    tick_to_send_p50 = float(latency_summary["tick_to_transport_submit_ms"]["p50_ms"])
    tick_to_send_p95 = float(latency_summary["tick_to_transport_submit_ms"]["p95_ms"])
    send_to_response_p50 = float(latency_summary["transport_submit_to_response_ms"]["p50_ms"])
    send_to_response_p95 = float(latency_summary["transport_submit_to_response_ms"]["p95_ms"])

    payload: dict[str, object] = {
        "audit_counts": dict(Counter(event.kind for event in audit_events)),
        "strategy_verification": {
            "signal_count": len(bundles),
            "matched_count": len(matched_signal_keys),
            "mismatched_count": len(bundles) - len(matched_signal_keys),
            "match_rate": (len(matched_signal_keys) / len(bundles))
            if bundles
            else 0.0,
            "mismatches": mismatches[:mismatch_limit],
        },
        "latency_by_stage_ms": latency_summary,
        "transport_http_status_counts": {
            str(status): count for status, count in sorted(transport_status_counts.items())
        },
        "signal_decay": {
            "all_edge_lifetimes": {
                "count": len(all_lifetime_ms),
                "p50_ms": _percentile(all_lifetime_ms, 0.50) if all_lifetime_ms else 0.0,
                "p95_ms": _percentile(all_lifetime_ms, 0.95) if all_lifetime_ms else 0.0,
                "survival_curve": _survival_curve_ms(all_lifetime_ms, thresholds_ms),
            },
            "signaled_edge_lifetimes": {
                "count": len(signaled_lifetime_ms),
                "p50_ms": _percentile(signaled_lifetime_ms, 0.50) if signaled_lifetime_ms else 0.0,
                "p95_ms": _percentile(signaled_lifetime_ms, 0.95) if signaled_lifetime_ms else 0.0,
                "survival_curve": _survival_curve_ms(signaled_lifetime_ms, thresholds_ms),
            },
            "transported_edge_lifetimes": {
                "count": len(transported_lifetime_ms),
                "p50_ms": _percentile(transported_lifetime_ms, 0.50)
                if transported_lifetime_ms
                else 0.0,
                "p95_ms": _percentile(transported_lifetime_ms, 0.95)
                if transported_lifetime_ms
                else 0.0,
                "survival_curve": _survival_curve_ms(transported_lifetime_ms, thresholds_ms),
            },
            "latency_reference_ms": {
                "tick_to_transport_submit_p50_ms": tick_to_send_p50,
                "tick_to_transport_submit_p95_ms": tick_to_send_p95,
                "transport_submit_to_response_p50_ms": send_to_response_p50,
                "transport_submit_to_response_p95_ms": send_to_response_p95,
            },
            "survival_vs_latency": {
                "signaled_ge_tick_to_send_p50": (
                    sum(1 for value in signaled_lifetime_ms if value >= tick_to_send_p50)
                    / len(signaled_lifetime_ms)
                    if signaled_lifetime_ms
                    else 0.0
                ),
                "signaled_ge_tick_to_send_p95": (
                    sum(1 for value in signaled_lifetime_ms if value >= tick_to_send_p95)
                    / len(signaled_lifetime_ms)
                    if signaled_lifetime_ms
                    else 0.0
                ),
                "signaled_ge_full_loop_p50": (
                    sum(1 for value in signaled_lifetime_ms if value >= send_to_response_p50)
                    / len(signaled_lifetime_ms)
                    if signaled_lifetime_ms
                    else 0.0
                ),
                "signaled_ge_full_loop_p95": (
                    sum(1 for value in signaled_lifetime_ms if value >= send_to_response_p95)
                    / len(signaled_lifetime_ms)
                    if signaled_lifetime_ms
                    else 0.0
                ),
            },
        },
    }

    if output_json is not None:
        output_path = Path(output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")
        payload["output_json"] = str(output_path)

    return payload
