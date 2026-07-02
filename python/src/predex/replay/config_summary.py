from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean, median
from typing import Any

from .app_config import AppConfigIndex, AppEventRoute, load_app_config_index


def _distribution(values: list[int]) -> dict[str, int | float]:
    if not values:
        return {
            "count": 0,
            "total": 0,
            "average": 0.0,
            "median": 0.0,
            "min": 0,
            "max": 0,
        }

    return {
        "count": len(values),
        "total": sum(values),
        "average": mean(values),
        "median": median(values),
        "min": min(values),
        "max": max(values),
    }


def _event_summary(event: AppEventRoute, *, sample_tickers: int) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "topology": event.topology,
        "shard_index": event.shard_index,
        "shard_event_index": event.shard_event_index,
        "market_count": len(event.markets),
        "sample_tickers": [market.market_ticker for market in event.markets[:sample_tickers]],
    }


def summarize_config(
    config_path: str | Path,
    *,
    top_events: int = 20,
    sample_tickers: int = 5,
) -> dict[str, Any]:
    index: AppConfigIndex = load_app_config_index(config_path)
    event_sizes = [len(event.markets) for event in index.events]

    topology_sizes: dict[str, list[int]] = defaultdict(list)
    shard_events: dict[int, list[AppEventRoute]] = defaultdict(list)
    for event in index.events:
        topology_sizes[event.topology].append(len(event.markets))
        shard_events[event.shard_index].append(event)

    topology_summaries = {
        topology: _distribution(sizes)
        for topology, sizes in sorted(topology_sizes.items())
    }

    shard_summaries: dict[str, dict[str, int | float]] = {}
    for shard_index in range(index.shard_count):
        events = shard_events.get(shard_index, [])
        sizes = [len(event.markets) for event in events]
        summary = _distribution(sizes)
        summary["shard_index"] = shard_index
        summary["market_count"] = sum(sizes)
        summary["largest_event_markets"] = max(sizes, default=0)
        shard_summaries[str(shard_index)] = summary

    price_level_counts = Counter(route.price_level_structure for route in index.routes)
    tradeable_counts = Counter("true" if route.tradeable else "false" for route in index.routes)
    market_count_histogram = Counter(event_sizes)
    largest_events = sorted(index.events, key=lambda event: len(event.markets), reverse=True)[:top_events]

    return {
        "config_path": str(config_path),
        "event_count": len(index.events),
        "market_count": len(index.routes),
        "shard_count": index.shard_count,
        "markets_per_event": _distribution(event_sizes),
        "market_count_histogram": {
            str(market_count): count
            for market_count, count in sorted(market_count_histogram.items())
        },
        "topology_summaries": topology_summaries,
        "shard_summaries": shard_summaries,
        "price_level_structure_counts": dict(sorted(price_level_counts.items())),
        "tradeable_counts": dict(sorted(tradeable_counts.items())),
        "largest_events": [
            _event_summary(event, sample_tickers=sample_tickers)
            for event in largest_events
        ],
    }


def _format_number(value: int | float) -> str:
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def _format_distribution(summary: dict[str, int | float]) -> str:
    return (
        f"count={summary['count']} total={summary['total']} "
        f"avg={_format_number(summary['average'])} "
        f"median={_format_number(summary['median'])} "
        f"min={summary['min']} max={summary['max']}"
    )


def format_config_summary(summary: dict[str, Any]) -> str:
    lines: list[str] = [
        "Config Summary",
        f"  config: {summary['config_path']}",
        f"  events: {summary['event_count']}",
        f"  markets: {summary['market_count']}",
        f"  shards: {summary['shard_count']}",
        "",
        "Markets/Event",
        f"  {_format_distribution(summary['markets_per_event'])}",
        "",
        "Topology",
    ]

    for topology, topology_summary in summary["topology_summaries"].items():
        lines.append(f"  {topology}: {_format_distribution(topology_summary)}")

    lines.extend(["", "Shard Balance"])
    for shard_index, shard_summary in summary["shard_summaries"].items():
        lines.append(
            "  "
            f"shard {shard_index}: events={shard_summary['count']} "
            f"markets={shard_summary['market_count']} "
            f"largest_event={shard_summary['largest_event_markets']} "
            f"avg_event_size={_format_number(shard_summary['average'])}"
        )

    lines.extend(["", "Market Count Histogram"])
    for market_count, count in summary["market_count_histogram"].items():
        lines.append(f"  {market_count}: {count}")

    lines.extend(["", "Price Level Structure"])
    for price_level, count in summary["price_level_structure_counts"].items():
        lines.append(f"  {price_level}: {count}")

    lines.extend(["", "Tradeable"])
    for tradeable, count in summary["tradeable_counts"].items():
        lines.append(f"  {tradeable}: {count}")

    lines.extend(["", "Largest Events"])
    for event in summary["largest_events"]:
        sample = ", ".join(event["sample_tickers"])
        lines.append(
            "  "
            f"event_id={event['event_id']} "
            f"topology={event['topology']} "
            f"shard={event['shard_index']} "
            f"markets={event['market_count']} "
            f"sample=[{sample}]"
        )

    return "\n".join(lines)
