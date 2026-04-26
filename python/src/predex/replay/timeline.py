from __future__ import annotations

import csv
import html
import json
from collections import defaultdict
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .audit import SignalBundle
from .books import ReplayBookStore
from .config import ConfigIndex, EventRoute, MarketRoute
from .monotonic import SIDE_BUY, SIDE_SELL, evaluate_pair_state
from .tape import iter_market_events


@dataclass(frozen=True, slots=True)
class SignalCandidate:
    shard_id: int
    signal_id: int
    event_id: int
    easier_market: MarketRoute
    harder_market: MarketRoute
    buy_price_ticks: int | None
    sell_price_ticks: int | None
    target_edge_ticks: int
    target_qty_lots: int


@dataclass(frozen=True, slots=True)
class SignalHit:
    shard_id: int
    signal_id: int
    event_id: int
    record_index: int
    easier_market_ticker: str
    harder_market_ticker: str
    easier_ask_ticks: int
    harder_bid_ticks: int
    recomputed_edge_ticks: int
    audited_edge_ticks: int


@dataclass(frozen=True, slots=True)
class TimelineRow:
    record_index: int
    market_ticker: str
    sequence_id: int | None
    best_bid_ticks: int | None
    best_bid_qty_lots: int | None
    best_ask_ticks: int | None
    best_ask_qty_lots: int | None
    last_trade_ticks: int | None
    last_trade_qty_lots: int | None


@dataclass(frozen=True, slots=True)
class EventTimeline:
    event_id: int
    event_ticker: str
    market_tickers: tuple[str, ...]
    rows: tuple[TimelineRow, ...]
    signal_hits: tuple[SignalHit, ...]
    signal_candidates: int


def _candidate_from_bundle(bundle: SignalBundle, *, config_index: ConfigIndex) -> SignalCandidate | None:
    event_route = config_index.events_by_id.get(bundle.event_id)
    if event_route is None or event_route.topology_kind != "monotonic_chain":
        return None

    legs = bundle.legs()
    buy_leg = next((event for event in legs if event.side == SIDE_BUY), None)
    sell_leg = next((event for event in legs if event.side == SIDE_SELL), None)
    easier_market = config_index.markets_by_id.get(buy_leg.market_id) if buy_leg is not None else None
    harder_market = config_index.markets_by_id.get(sell_leg.market_id) if sell_leg is not None else None

    ordered_markets = list(event_route.markets)
    market_index_by_id = {market.market_id: index for index, market in enumerate(ordered_markets)}

    if easier_market is None and harder_market is None:
        return None

    if easier_market is None and harder_market is not None:
        harder_index = market_index_by_id.get(harder_market.market_id)
        if harder_index is None or harder_index == 0:
            return None
        easier_market = ordered_markets[harder_index - 1]

    if harder_market is None and easier_market is not None:
        easier_index = market_index_by_id.get(easier_market.market_id)
        if easier_index is None or easier_index + 1 >= len(ordered_markets):
            return None
        harder_market = ordered_markets[easier_index + 1]

    if easier_market is None or harder_market is None:
        return None
    if easier_market.strike_key >= harder_market.strike_key:
        return None

    target_qty = max(
        buy_leg.qty_lots if buy_leg is not None else 0,
        sell_leg.qty_lots if sell_leg is not None else 0,
        1,
    )
    return SignalCandidate(
        shard_id=bundle.shard_id,
        signal_id=bundle.signal_id,
        event_id=bundle.event_id,
        easier_market=easier_market,
        harder_market=harder_market,
        buy_price_ticks=buy_leg.price_ticks if buy_leg is not None else None,
        sell_price_ticks=sell_leg.price_ticks if sell_leg is not None else None,
        target_edge_ticks=bundle.group_signal.edge_ticks,
        target_qty_lots=target_qty,
    )


def _resolve_event_route(
    *,
    config_index: ConfigIndex,
    event_id: int | None,
    market_ticker: str | None,
) -> EventRoute:
    if event_id is None and market_ticker is None:
        raise ValueError("event_id or market_ticker is required")

    if market_ticker is not None:
        market = config_index.markets_by_ticker.get(market_ticker)
        if market is None:
            raise ValueError(f"market_ticker not found in config: {market_ticker}")
        if event_id is None:
            event_id = market.event_id
        elif event_id != market.event_id:
            raise ValueError("market_ticker does not belong to the requested event_id")

    assert event_id is not None
    event_route = config_index.events_by_id.get(event_id)
    if event_route is None:
        raise ValueError(f"event_id not found in config: {event_id}")
    return event_route


def build_event_timeline(
    *,
    config_index: ConfigIndex,
    bundles: dict[tuple[int, int], SignalBundle],
    tape_path: str | Path,
    event_id: int | None = None,
    market_ticker: str | None = None,
) -> EventTimeline:
    event_route = _resolve_event_route(config_index=config_index, event_id=event_id, market_ticker=market_ticker)
    selected_markets = {
        market_ticker
    } if market_ticker is not None else {market.market_ticker for market in event_route.markets}

    signal_candidates: dict[tuple[int, int], SignalCandidate] = {}
    signals_by_market: dict[str, set[tuple[int, int]]] = defaultdict(set)

    for key, bundle in bundles.items():
        if bundle.event_id != event_route.event_id:
            continue
        candidate = _candidate_from_bundle(bundle, config_index=config_index)
        if candidate is None:
            continue
        if candidate.easier_market.market_ticker not in selected_markets and candidate.harder_market.market_ticker not in selected_markets:
            continue

        signal_candidates[key] = candidate
        signals_by_market[candidate.easier_market.market_ticker].add(key)
        signals_by_market[candidate.harder_market.market_ticker].add(key)

    books = ReplayBookStore()
    rows: list[TimelineRow] = []
    signal_hits: list[SignalHit] = []
    unmatched_keys = set(signal_candidates.keys())

    for market_event in iter_market_events(tape_path):
        if market_event.market_ticker not in selected_markets and market_event.market_ticker not in signals_by_market:
            continue

        state = books.apply(market_event)

        if market_event.market_ticker in selected_markets:
            best_bid = state.best_bid()
            best_ask = state.best_ask()
            rows.append(
                TimelineRow(
                    record_index=market_event.record_index,
                    market_ticker=market_event.market_ticker,
                    sequence_id=market_event.sequence_id,
                    best_bid_ticks=best_bid.price_ticks if best_bid is not None else None,
                    best_bid_qty_lots=best_bid.qty_lots if best_bid is not None else None,
                    best_ask_ticks=best_ask.price_ticks if best_ask is not None else None,
                    best_ask_qty_lots=best_ask.qty_lots if best_ask is not None else None,
                    last_trade_ticks=state.last_trade_price_ticks,
                    last_trade_qty_lots=state.last_trade_qty_lots,
                )
            )

        for key in list(signals_by_market.get(market_event.market_ticker, ())):
            if key not in unmatched_keys:
                continue
            candidate = signal_candidates[key]

            easier_state = books.books.get(candidate.easier_market.market_ticker)
            harder_state = books.books.get(candidate.harder_market.market_ticker)
            if easier_state is None or harder_state is None:
                continue

            easier_ask = easier_state.best_ask()
            harder_bid = harder_state.best_bid()
            if easier_ask is None or harder_bid is None:
                continue

            pair_state = evaluate_pair_state(
                easier_ask.price_ticks,
                easier_ask.qty_lots,
                harder_bid.price_ticks,
                harder_bid.qty_lots,
                default_order_qty_lots=candidate.target_qty_lots,
            )
            if pair_state is None:
                continue
            net_edge_ticks = pair_state.net_edge_ticks

            if (
                (candidate.buy_price_ticks is None or easier_ask.price_ticks == candidate.buy_price_ticks)
                and (candidate.sell_price_ticks is None or harder_bid.price_ticks == candidate.sell_price_ticks)
                and net_edge_ticks == candidate.target_edge_ticks
            ):
                signal_hits.append(
                    SignalHit(
                        shard_id=candidate.shard_id,
                        signal_id=candidate.signal_id,
                        event_id=candidate.event_id,
                        record_index=market_event.record_index,
                        easier_market_ticker=candidate.easier_market.market_ticker,
                        harder_market_ticker=candidate.harder_market.market_ticker,
                        easier_ask_ticks=easier_ask.price_ticks,
                        harder_bid_ticks=harder_bid.price_ticks,
                        recomputed_edge_ticks=net_edge_ticks,
                        audited_edge_ticks=candidate.target_edge_ticks,
                    )
                )
                unmatched_keys.remove(key)

    event_ticker = event_route.markets[0].market_ticker.rsplit("-", 1)[0] if event_route.markets else ""
    return EventTimeline(
        event_id=event_route.event_id,
        event_ticker=event_ticker,
        market_tickers=tuple(sorted(selected_markets)),
        rows=tuple(rows),
        signal_hits=tuple(signal_hits),
        signal_candidates=len(signal_candidates),
    )


def write_timeline_csv(path: str | Path, timeline: EventTimeline) -> None:
    signal_hits_by_record: dict[int, list[SignalHit]] = defaultdict(list)
    for hit in timeline.signal_hits:
        signal_hits_by_record[hit.record_index].append(hit)

    with Path(path).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "record_index",
                "market_ticker",
                "sequence_id",
                "best_bid_ticks",
                "best_bid_qty_lots",
                "best_ask_ticks",
                "best_ask_qty_lots",
                "last_trade_ticks",
                "last_trade_qty_lots",
                "signal_hit_count",
                "signal_hit_ids",
                "signal_hit_edges",
            ]
        )
        for row in timeline.rows:
            hits = signal_hits_by_record.get(row.record_index, [])
            writer.writerow(
                [
                    row.record_index,
                    row.market_ticker,
                    row.sequence_id,
                    row.best_bid_ticks,
                    row.best_bid_qty_lots,
                    row.best_ask_ticks,
                    row.best_ask_qty_lots,
                    row.last_trade_ticks,
                    row.last_trade_qty_lots,
                    len(hits),
                    ";".join(f"{hit.shard_id}:{hit.signal_id}" for hit in hits),
                    ";".join(str(hit.recomputed_edge_ticks) for hit in hits),
                ]
            )


def write_signal_hits_csv(path: str | Path, signal_hits: Iterable[SignalHit]) -> None:
    with Path(path).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "shard_id",
                "signal_id",
                "event_id",
                "record_index",
                "easier_market_ticker",
                "harder_market_ticker",
                "easier_ask_ticks",
                "harder_bid_ticks",
                "recomputed_edge_ticks",
                "audited_edge_ticks",
            ]
        )
        for hit in sorted(signal_hits, key=lambda item: (item.record_index, item.shard_id, item.signal_id)):
            writer.writerow(
                [
                    hit.shard_id,
                    hit.signal_id,
                    hit.event_id,
                    hit.record_index,
                    hit.easier_market_ticker,
                    hit.harder_market_ticker,
                    hit.easier_ask_ticks,
                    hit.harder_bid_ticks,
                    hit.recomputed_edge_ticks,
                    hit.audited_edge_ticks,
                ]
            )


def _write_parquet_records(path: str | Path, records: list[dict[str, object]]) -> None:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError(
            "Parquet export requires pyarrow. Install with: .venv/bin/pip install pyarrow"
        ) from exc

    table = pa.Table.from_pylist(records)
    pq.write_table(table, path)


def write_timeline_parquet(path: str | Path, timeline: EventTimeline) -> None:
    signal_hits_by_record: dict[int, list[SignalHit]] = defaultdict(list)
    for hit in timeline.signal_hits:
        signal_hits_by_record[hit.record_index].append(hit)

    records: list[dict[str, object]] = []
    for row in timeline.rows:
        hits = signal_hits_by_record.get(row.record_index, [])
        records.append(
            {
                "record_index": row.record_index,
                "market_ticker": row.market_ticker,
                "sequence_id": row.sequence_id,
                "best_bid_ticks": row.best_bid_ticks,
                "best_bid_qty_lots": row.best_bid_qty_lots,
                "best_ask_ticks": row.best_ask_ticks,
                "best_ask_qty_lots": row.best_ask_qty_lots,
                "last_trade_ticks": row.last_trade_ticks,
                "last_trade_qty_lots": row.last_trade_qty_lots,
                "signal_hit_count": len(hits),
                "signal_hit_ids": [f"{hit.shard_id}:{hit.signal_id}" for hit in hits],
                "signal_hit_edges": [hit.recomputed_edge_ticks for hit in hits],
            }
        )
    _write_parquet_records(path, records)


def write_signal_hits_parquet(path: str | Path, signal_hits: Iterable[SignalHit]) -> None:
    records = [asdict(hit) for hit in sorted(signal_hits, key=lambda item: (item.record_index, item.shard_id, item.signal_id))]
    _write_parquet_records(path, records)


def write_timeline_summary_json(path: str | Path, timeline: EventTimeline) -> None:
    payload = {
        "event_id": timeline.event_id,
        "event_ticker": timeline.event_ticker,
        "market_tickers": list(timeline.market_tickers),
        "timeline_rows": len(timeline.rows),
        "signal_candidates": timeline.signal_candidates,
        "signal_hits": len(timeline.signal_hits),
        "first_record_index": min((row.record_index for row in timeline.rows), default=None),
        "last_record_index": max((row.record_index for row in timeline.rows), default=None),
    }
    Path(path).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _polyline(points: list[tuple[float, float]], *, stroke: str, stroke_width: int = 2) -> str:
    if not points:
        return ""
    point_text = " ".join(f"{x:.2f},{y:.2f}" for x, y in points)
    return (
        f'<polyline fill="none" stroke="{stroke}" stroke-width="{stroke_width}" '
        f'stroke-linejoin="round" stroke-linecap="round" points="{point_text}" />'
    )


def _market_svg(rows: list[TimelineRow], signal_hit_indexes: list[int], width: int = 1080, height: int = 260) -> str:
    if not rows:
        return "<svg width=\"1080\" height=\"120\"></svg>"
    ticks = [
        tick
        for row in rows
        for tick in (row.best_bid_ticks, row.best_ask_ticks)
        if tick is not None
    ]
    if not ticks:
        return (
            f'<svg width="{width}" height="120" viewBox="0 0 {width} 120" role="img">'
            f'<rect x="0" y="0" width="{width}" height="120" fill="#ffffff" />'
            '<text x="12" y="68" font-size="13" fill="#6b7280">No top-of-book levels available yet.</text>'
            '</svg>'
        )

    left = 48
    right = 16
    top = 16
    bottom = 28
    inner_w = max(1, width - left - right)
    inner_h = max(1, height - top - bottom)

    min_tick = min(ticks)
    max_tick = max(ticks)
    span = max(1, max_tick - min_tick)

    def x_for_idx(idx: int) -> float:
        if len(rows) <= 1:
            return float(left)
        return left + (idx / (len(rows) - 1)) * inner_w

    def y_for_tick(tick: int) -> float:
        return top + (max_tick - tick) * inner_h / span

    bid_points = [(x_for_idx(i), y_for_tick(row.best_bid_ticks)) for i, row in enumerate(rows) if row.best_bid_ticks is not None]
    ask_points = [(x_for_idx(i), y_for_tick(row.best_ask_ticks)) for i, row in enumerate(rows) if row.best_ask_ticks is not None]

    signal_lines = "".join(
        f'<line x1="{x_for_idx(i):.2f}" y1="{top}" x2="{x_for_idx(i):.2f}" y2="{top + inner_h}" '
        'stroke="#1d4ed8" stroke-width="1" stroke-dasharray="3 3" opacity="0.7" />'
        for i in signal_hit_indexes
    )

    y_ticks = [min_tick, min_tick + span // 2, max_tick]
    y_guides = "".join(
        f'<line x1="{left}" y1="{y_for_tick(tick):.2f}" x2="{left + inner_w}" y2="{y_for_tick(tick):.2f}" '
        'stroke="#d1d5db" stroke-width="1" opacity="0.75" />'
        for tick in y_ticks
    )
    y_labels = "".join(
        f'<text x="8" y="{y_for_tick(tick) + 4:.2f}" font-size="11" fill="#374151">{tick}</text>'
        for tick in y_ticks
    )

    return (
        f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">'
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="#ffffff" />'
        f'{y_guides}{signal_lines}'
        f'{_polyline(bid_points, stroke="#047857", stroke_width=2)}'
        f'{_polyline(ask_points, stroke="#b91c1c", stroke_width=2)}'
        f'{y_labels}'
        f'<text x="{left}" y="{height - 8}" font-size="11" fill="#6b7280">record progression</text>'
        f'</svg>'
    )


def write_timeline_html(path: str | Path, timeline: EventTimeline) -> None:
    rows_by_market: dict[str, list[TimelineRow]] = defaultdict(list)
    for row in timeline.rows:
        rows_by_market[row.market_ticker].append(row)

    hit_records_by_market: dict[str, set[int]] = defaultdict(set)
    for hit in timeline.signal_hits:
        hit_records_by_market[hit.easier_market_ticker].add(hit.record_index)
        hit_records_by_market[hit.harder_market_ticker].add(hit.record_index)

    market_sections: list[str] = []
    for market in sorted(rows_by_market):
        market_rows = rows_by_market[market]
        record_index_lookup = {row.record_index: idx for idx, row in enumerate(market_rows)}
        signal_hit_indexes = sorted(
            record_index_lookup[record_index]
            for record_index in hit_records_by_market.get(market, set())
            if record_index in record_index_lookup
        )
        signal_count = sum(1 for hit in timeline.signal_hits if hit.easier_market_ticker == market or hit.harder_market_ticker == market)

        market_sections.append(
            "\n".join(
                [
                    '<section class="card">',
                    f"<h3>{html.escape(market)}</h3>",
                    f'<p class="meta">updates: {len(market_rows)} | signal touches: {signal_count}</p>',
                    _market_svg(market_rows, signal_hit_indexes),
                    '<p class="legend"><span class="bid">Bid</span> <span class="ask">Ask</span> <span class="sig">Signal Match</span></p>',
                    '</section>',
                ]
            )
        )

    signal_rows = "\n".join(
        f"<tr><td>{hit.record_index}</td><td>{hit.shard_id}:{hit.signal_id}</td><td>{html.escape(hit.easier_market_ticker)}</td>"
        f"<td>{html.escape(hit.harder_market_ticker)}</td><td>{hit.easier_ask_ticks}</td><td>{hit.harder_bid_ticks}</td>"
        f"<td>{hit.recomputed_edge_ticks}</td></tr>"
        for hit in sorted(timeline.signal_hits, key=lambda item: (item.record_index, item.shard_id, item.signal_id))
    )

    html_text = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Predex Event Timeline</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 24px; color: #111827; background: #f9fafb; }}
    h1 {{ margin-bottom: 8px; }}
    .summary {{ margin-bottom: 18px; color: #374151; }}
    .card {{ background: white; border: 1px solid #e5e7eb; border-radius: 10px; padding: 14px; margin: 14px 0; overflow-x: auto; }}
    .meta {{ margin: 6px 0 10px; color: #6b7280; font-size: 14px; }}
    .legend {{ font-size: 12px; color: #6b7280; }}
    .legend span {{ margin-right: 12px; }}
    .legend .bid {{ color: #047857; }}
    .legend .ask {{ color: #b91c1c; }}
    .legend .sig {{ color: #1d4ed8; }}
    table {{ width: 100%; border-collapse: collapse; background: white; border: 1px solid #e5e7eb; border-radius: 10px; overflow: hidden; }}
    th, td {{ text-align: left; padding: 8px 10px; border-bottom: 1px solid #f3f4f6; font-size: 13px; }}
    th {{ background: #f3f4f6; font-weight: 600; }}
  </style>
</head>
<body>
  <h1>Predex Event Timeline</h1>
  <div class=\"summary\">event_id: {timeline.event_id} ({html.escape(timeline.event_ticker)}) | markets: {len(timeline.market_tickers)} | updates: {len(timeline.rows)} | signal candidates: {timeline.signal_candidates} | signal hits: {len(timeline.signal_hits)}</div>
  {''.join(market_sections)}
  <h2>Signal Matches</h2>
  <table>
    <thead>
      <tr><th>record_index</th><th>signal</th><th>easier_market</th><th>harder_market</th><th>ask</th><th>bid</th><th>edge</th></tr>
    </thead>
    <tbody>
      {signal_rows}
    </tbody>
  </table>
</body>
</html>
"""
    Path(path).write_text(html_text, encoding="utf-8")
