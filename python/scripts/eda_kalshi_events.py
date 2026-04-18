from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import asdict, dataclass
from typing import Any

from predex.discovery.kalshi import KalshiPublicClient

_NUMERIC_STRIKE_TYPES = {"greater", "greater_or_equal", "less", "less_or_equal", "between"}


@dataclass(slots=True)
class EventPatternExample:
    event_ticker: str
    series_ticker: str
    market_count: int
    title: str
    mutually_exclusive: bool
    strike_types: list[str]
    distinct_close_times: bool
    same_close_time: bool
    sample_market_tickers: list[str]
    sample_yes_sub_titles: list[str]


@dataclass(slots=True)
class NonMutexBucketExample:
    event_ticker: str
    series_ticker: str
    market_count: int
    title: str
    strike_types: list[str]
    distinct_close_times: bool
    same_close_time: bool
    bucket: str
    sufficiency: str
    sample_markets: list[dict[str, Any]]


def _presence_counts(items: list[dict[str, Any]], fields: list[str]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for item in items:
        for field in fields:
            if field in item and item.get(field) not in (None, "", [], {}):
                counts[field] += 1
    return dict(counts)


def _strike_types(markets: list[dict[str, Any]]) -> list[str]:
    return sorted(
        {
            str(market.get("strike_type"))
            for market in markets
            if market.get("strike_type") not in (None, "")
        }
    )


def _close_time_flags(markets: list[dict[str, Any]]) -> tuple[bool, bool]:
    close_times = [market.get("close_time") for market in markets]
    if not close_times:
        return False, False
    return len(set(close_times)) == len(close_times), len(set(close_times)) == 1


def _non_mutex_bucket(event: dict[str, Any]) -> tuple[str, str]:
    markets = event.get("markets") or []
    strike_types = _strike_types(markets)
    distinct_close_times, same_close_time = _close_time_flags(markets)

    all_have_numeric_bound = all(
        (market.get("floor_strike") not in (None, ""))
        or (market.get("cap_strike") not in (None, ""))
        for market in markets
    )
    all_have_custom = all(
        market.get("custom_strike") not in (None, "", {}) for market in markets
    )
    custom_values = {
        json.dumps(market.get("custom_strike"), sort_keys=True)
        for market in markets
        if market.get("custom_strike") not in (None, "", {})
    }

    if strike_types and set(strike_types).issubset(_NUMERIC_STRIKE_TYPES) and all_have_numeric_bound:
        return "numeric_strike", "sufficient"
    if not strike_types and distinct_close_times:
        return "date_ladder_no_strike", "sufficient"
    if strike_types == ["custom"] and distinct_close_times and all_have_custom and len(custom_values) == 1:
        return "date_ladder_custom_same_entity", "sufficient"
    if strike_types == ["structured"] and distinct_close_times and all_have_custom and len(custom_values) == 1:
        return "date_ladder_structured_same_entity", "sufficient"
    if strike_types == ["custom"] and same_close_time and all_have_custom and len(custom_values) == len(markets):
        return "same_close_custom_distinct_entities", "sufficient"
    if strike_types == ["structured"] and same_close_time and all_have_custom and len(custom_values) == len(markets):
        return "same_close_structured_distinct_entities", "sufficient"
    if not strike_types and same_close_time:
        return "same_close_no_strike_entities", "partial"
    if strike_types == ["custom"] and same_close_time and all_have_custom and len(custom_values) == 1:
        return "same_close_custom_same_entity_outlier", "insufficient"
    if strike_types == ["structured"] and same_close_time and all_have_custom and len(custom_values) == 1:
        return "same_close_structured_same_entity_outlier", "insufficient"
    return "unresolved_other", "insufficient"


def _event_examples(events: list[dict[str, Any]], *, limit: int) -> list[EventPatternExample]:
    examples: list[EventPatternExample] = []
    for event in events:
        markets = event.get("markets") or []
        strike_types = _strike_types(markets)
        distinct_close_times, same_close_time = _close_time_flags(markets)
        examples.append(
            EventPatternExample(
                event_ticker=str(event.get("event_ticker", "")),
                series_ticker=str(event.get("series_ticker", "")),
                market_count=len(markets),
                title=str(event.get("title", "")),
                mutually_exclusive=bool(event.get("mutually_exclusive", False)),
                strike_types=strike_types,
                distinct_close_times=distinct_close_times,
                same_close_time=same_close_time,
                sample_market_tickers=[str(market.get("ticker", "")) for market in markets[:6]],
                sample_yes_sub_titles=[
                    str(market.get("yes_sub_title", "")) for market in markets[:6]
                ],
            )
        )
        if len(examples) >= limit:
            break
    return examples


def _non_mutex_bucket_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    bucket_counts: Counter[str] = Counter()
    sufficiency_counts: Counter[str] = Counter()
    examples_by_bucket: dict[str, list[dict[str, Any]]] = {}

    for event in events:
        markets = event.get("markets") or []
        if bool(event.get("mutually_exclusive", False)) or len(markets) <= 1:
            continue

        bucket, sufficiency = _non_mutex_bucket(event)
        bucket_counts[bucket] += 1
        sufficiency_counts[sufficiency] += 1

        examples = examples_by_bucket.setdefault(bucket, [])
        if len(examples) >= 6:
            continue

        distinct_close_times, same_close_time = _close_time_flags(markets)
        examples.append(
            asdict(
                NonMutexBucketExample(
                    event_ticker=str(event.get("event_ticker", "")),
                    series_ticker=str(event.get("series_ticker", "")),
                    market_count=len(markets),
                    title=str(event.get("title", "")),
                    strike_types=_strike_types(markets),
                    distinct_close_times=distinct_close_times,
                    same_close_time=same_close_time,
                    bucket=bucket,
                    sufficiency=sufficiency,
                    sample_markets=[
                        {
                            "ticker": str(market.get("ticker", "")),
                            "yes_sub_title": str(market.get("yes_sub_title", "")),
                            "strike_type": market.get("strike_type"),
                            "floor_strike": market.get("floor_strike"),
                            "cap_strike": market.get("cap_strike"),
                            "close_time": market.get("close_time"),
                            "custom_strike": market.get("custom_strike"),
                        }
                        for market in markets[:5]
                    ],
                )
            )
        )

    return {
        "non_mutex_multi_market_total": sum(bucket_counts.values()),
        "bucket_counts": dict(bucket_counts.most_common()),
        "sufficiency_counts": dict(sufficiency_counts.most_common()),
        "examples_by_bucket": examples_by_bucket,
    }


def collect_summary(*, limit: int, status: str) -> dict[str, Any]:
    client = KalshiPublicClient()
    payload = client._get_json(  # noqa: SLF001 - this script wants the raw list response.
        "/events",
        {
            "limit": limit,
            "status": status,
            "with_nested_markets": "true",
        },
    )
    events = payload.get("events") or []

    market_count_distribution: Counter[int] = Counter()
    category_counts: Counter[str] = Counter()
    strike_type_counts: Counter[str] = Counter()
    pattern_counts: Counter[str] = Counter()
    market_rows: list[dict[str, Any]] = []

    for event in events:
        markets = event.get("markets") or []
        market_count_distribution[len(markets)] += 1
        category_counts[str(event.get("category", ""))] += 1

        strike_types = _strike_types(markets)
        distinct_close_times, same_close_time = _close_time_flags(markets)
        pattern_key = (
            f"mutex={bool(event.get('mutually_exclusive', False))}"
            f"|strike_types={','.join(strike_types) or '<none>'}"
            f"|distinct_close={distinct_close_times}"
            f"|same_close={same_close_time}"
        )
        pattern_counts[pattern_key] += 1

        for market in markets:
            market_rows.append(market)
            strike_type = market.get("strike_type")
            if strike_type not in (None, ""):
                strike_type_counts[str(strike_type)] += 1

    return {
        "sample_limit": limit,
        "status": status,
        "events_sampled": len(events),
        "cursor": payload.get("cursor"),
        "market_count_distribution": dict(market_count_distribution.most_common()),
        "top_categories": dict(category_counts.most_common(12)),
        "event_field_presence": _presence_counts(
            events,
            [
                "event_ticker",
                "series_ticker",
                "sub_title",
                "title",
                "collateral_return_type",
                "mutually_exclusive",
                "available_on_brokers",
                "product_metadata",
                "category",
                "strike_date",
                "strike_period",
                "markets",
            ],
        ),
        "market_field_presence": _presence_counts(
            market_rows,
            [
                "ticker",
                "event_ticker",
                "market_type",
                "yes_sub_title",
                "no_sub_title",
                "title",
                "subtitle",
                "status",
                "strike_type",
                "floor_strike",
                "cap_strike",
                "functional_strike",
                "custom_strike",
                "price_ranges",
                "mve_collection_ticker",
                "primary_participant_key",
                "close_time",
                "expected_expiration_time",
                "expiration_time",
            ],
        ),
        "strike_type_counts": dict(strike_type_counts.most_common()),
        "top_patterns": dict(pattern_counts.most_common(20)),
        "non_mutex_bucket_summary": _non_mutex_bucket_summary(events),
        "sample_events": [asdict(example) for example in _event_examples(events, limit=20)],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sample Kalshi GET /events and summarize structural fields for discovery EDA."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Number of events to request from GET /events. Max 200.",
    )
    parser.add_argument(
        "--status",
        default="open",
        help="Event status filter passed to GET /events. Default: open.",
    )
    parser.add_argument(
        "--output",
        help="Optional path for the JSON summary. Defaults to stdout.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    summary = collect_summary(limit=args.limit, status=args.status)
    payload = json.dumps(summary, indent=2, sort_keys=False)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as output_file:
            output_file.write(payload)
            output_file.write("\n")
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
