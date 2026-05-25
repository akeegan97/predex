#!/usr/bin/env python3
"""
Analyze predex_rest_trace*.jsonl files and report:
  - Batch status breakdown (filled/filled, filled/canceled, canceled/canceled)
  - Locked-in PnL for fully-filled 2-leg spread batches
"""

import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

TRACES_DIR = Path(__file__).parents[2] / "logs" / "live"


@dataclass
class OrderResult:
    ticker: str
    action: str           # buy | sell
    side: str             # yes | no
    outcome_side: str
    status: str           # executed | canceled
    yes_price_dollars: float
    no_price_dollars: float
    fill_count: float
    initial_count: float
    taker_fill_cost: float
    taker_fees: float
    maker_fill_cost: float
    maker_fees: float
    client_order_id: str
    order_id: str

    @property
    def total_cost(self) -> float:
        """Cash outlay for this leg (fill cost + fees)."""
        return self.taker_fill_cost + self.taker_fees + self.maker_fill_cost + self.maker_fees

    @property
    def fill_price(self) -> float:
        """Effective fill price per contract (yes-side)."""
        return self.yes_price_dollars if self.action == "buy" else self.no_price_dollars


@dataclass
class Batch:
    dispatch_request_id: int
    group_intent_id: int
    worker: int
    http_status: int
    latency_ms: float
    orders: list[OrderResult] = field(default_factory=list)

    @property
    def statuses(self) -> tuple[str, ...]:
        return tuple(o.status for o in self.orders)

    @property
    def status_label(self) -> str:
        label_map = {"executed": "filled", "canceled": "canceled"}
        return "/".join(label_map.get(s, s) for s in self.statuses)

    @property
    def is_fully_filled(self) -> bool:
        return all(o.status == "executed" for o in self.orders)

    @property
    def is_partial_fill(self) -> bool:
        statuses = set(self.statuses)
        return "executed" in statuses and "canceled" in statuses

    @property
    def is_fully_canceled(self) -> bool:
        return all(o.status == "canceled" for o in self.orders)

    def locked_in_pnl(self) -> Optional[float]:
        """
        For a fully-filled 2-leg mutually-exclusive spread:
          each filled contract pays out exactly $1.00 on one side.
          locked-in PnL = $1.00 per contract - total cash outlay.
        Returns None if not fully filled or if leg counts differ.
        """
        if not self.is_fully_filled:
            return None
        counts = [o.fill_count for o in self.orders]
        if len(set(counts)) != 1:
            return None
        count = counts[0]
        total_cost = sum(o.total_cost for o in self.orders)
        guaranteed_payoff = 1.00 * count  # one leg will resolve YES
        return guaranteed_payoff - total_cost

    def entry_cost(self) -> float:
        return sum(o.total_cost for o in self.orders if o.status == "executed")

    def unhedged_exposure(self) -> list[str]:
        """For partial fills: describe the unhedged positions."""
        return [
            f"{o.action.upper()} {o.ticker} {'YES' if o.action == 'buy' else 'NO'} "
            f"@ ${o.fill_price:.2f} x{o.fill_count:.0f}"
            for o in self.orders if o.status == "executed"
        ]


def parse_order(raw: dict) -> OrderResult:
    return OrderResult(
        ticker=raw["ticker"],
        action=raw["action"],
        side=raw["side"],
        outcome_side=raw["outcome_side"],
        status=raw["status"],
        yes_price_dollars=float(raw["yes_price_dollars"]),
        no_price_dollars=float(raw["no_price_dollars"]),
        fill_count=float(raw["fill_count_fp"]),
        initial_count=float(raw["initial_count_fp"]),
        taker_fill_cost=float(raw["taker_fill_cost_dollars"]),
        taker_fees=float(raw["taker_fees_dollars"]),
        maker_fill_cost=float(raw["maker_fill_cost_dollars"]),
        maker_fees=float(raw["maker_fees_dollars"]),
        client_order_id=raw["client_order_id"],
        order_id=raw["order_id"],
    )


def load_batches(traces_dir: Path) -> list[Batch]:
    batches: list[Batch] = []
    for trace_file in sorted(traces_dir.glob("predex_rest_trace*.jsonl")):
        worker = int(trace_file.stem.split("worker")[-1]) if "worker" in trace_file.stem else 0
        with trace_file.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                resp_raw = record.get("response_body", "{}")
                resp = json.loads(resp_raw) if isinstance(resp_raw, str) else resp_raw
                raw_orders = resp.get("orders", [])
                if not raw_orders:
                    continue
                batch = Batch(
                    dispatch_request_id=record.get("dispatch_request_id", 0),
                    group_intent_id=record.get("group_intent_id", 0),
                    worker=worker,
                    http_status=record.get("http_status_code", 0),
                    latency_ms=record.get("latency_ns", 0) / 1e6,
                    orders=[parse_order(o["order"]) for o in raw_orders if "order" in o],
                )
                batches.append(batch)
    batches.sort(key=lambda b: b.dispatch_request_id)
    return batches


def print_report(batches: list[Batch]) -> None:
    from collections import Counter

    status_counts: Counter = Counter()
    for b in batches:
        status_counts[b.status_label] += 1

    print("=" * 60)
    print("REST TRACE PnL REPORT")
    print("=" * 60)
    print(f"Total batches analyzed: {len(batches)}")
    print()

    print("Status Breakdown")
    print("-" * 40)
    for label, count in sorted(status_counts.items()):
        print(f"  {label:<30} {count:>4}")
    print()

    # --- Filled / Filled ---
    filled_filled = [b for b in batches if b.is_fully_filled]
    if filled_filled:
        print("=" * 60)
        print(f"FILLED/FILLED  ({len(filled_filled)} batches)")
        print("=" * 60)
        total_pnl = 0.0
        for b in filled_filled:
            pnl = b.locked_in_pnl()
            total_pnl += pnl if pnl is not None else 0.0
            print(f"  Batch {b.dispatch_request_id}  (group {b.group_intent_id}, worker {b.worker}, {b.latency_ms:.1f}ms)")
            for o in b.orders:
                side_desc = f"BUY  YES" if o.action == "buy" else "SELL YES"
                print(f"    {side_desc}  {o.ticker:<28} @ ${o.fill_price:.4f}  "
                      f"fill={o.fill_count:.2f}  cost=${o.total_cost:.4f}  "
                      f"fees=${o.taker_fees + o.maker_fees:.4f}")
            if pnl is not None:
                entry = b.entry_cost()
                payoff = b.orders[0].fill_count * 1.0
                print(f"    Entry cost: ${entry:.4f}  |  Guaranteed payoff: ${payoff:.4f}  |  "
                      f"Locked-in PnL: ${pnl:+.4f}")
            print()
        print(f"  Total locked-in PnL: ${total_pnl:+.4f}")
        print()

    # --- Filled / Canceled ---
    partial = [b for b in batches if b.is_partial_fill]
    if partial:
        print("=" * 60)
        print(f"FILLED/CANCELED  ({len(partial)} batches)  — unhedged positions!")
        print("=" * 60)
        for b in partial:
            print(f"  Batch {b.dispatch_request_id}  (group {b.group_intent_id}, worker {b.worker}, {b.latency_ms:.1f}ms)")
            for o in b.orders:
                side_desc = f"BUY  YES" if o.action == "buy" else "SELL YES"
                status_tag = "[FILLED]  " if o.status == "executed" else "[CANCELED]"
                print(f"    {status_tag} {side_desc}  {o.ticker:<28} @ ${o.fill_price:.4f}  "
                      f"fill={o.fill_count:.2f}/{o.initial_count:.2f}")
            print(f"    Unhedged: {', '.join(b.unhedged_exposure())}")
            print(f"    Entry cost so far: ${b.entry_cost():.4f}")
            print()

    # --- Canceled / Canceled ---
    canceled = [b for b in batches if b.is_fully_canceled]
    if canceled:
        print("=" * 60)
        print(f"CANCELED/CANCELED  ({len(canceled)} batches)")
        print("=" * 60)
        for b in canceled:
            print(f"  Batch {b.dispatch_request_id}  (group {b.group_intent_id}, worker {b.worker}, {b.latency_ms:.1f}ms)")
            for o in b.orders:
                side_desc = f"BUY  YES" if o.action == "buy" else "SELL YES"
                print(f"    {side_desc}  {o.ticker:<28} @ ${o.yes_price_dollars:.4f} (yes) / ${o.no_price_dollars:.4f} (no)")
            print()

    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    filled = [b for b in batches if b.is_fully_filled]
    total_locked_pnl = sum(p for b in filled if (p := b.locked_in_pnl()) is not None)
    total_entry_cost = sum(b.entry_cost() for b in batches)
    print(f"  Fully filled batches:    {len(filled_filled)}")
    print(f"  Partial fill batches:    {len(partial)}")
    print(f"  Full cancel batches:     {len(canceled)}")
    print(f"  Total entry cost:        ${total_entry_cost:.4f}")
    print(f"  Total locked-in PnL:     ${total_locked_pnl:+.4f}")
    if filled:
        fill_rate = len(filled) / len(batches) * 100
        print(f"  Fill rate (both legs):   {fill_rate:.1f}%")


def main() -> None:
    traces_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else TRACES_DIR
    if not traces_dir.is_dir():
        print(f"ERROR: traces dir not found: {traces_dir}", file=sys.stderr)
        sys.exit(1)
    batches = load_batches(traces_dir)
    if not batches:
        print("No batches found.")
        return
    print_report(batches)


if __name__ == "__main__":
    main()
