from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from struct import pack

from predex.replay import (
    ReplayBookStore,
    build_event_timeline,
    build_signal_edge_lifetimes,
    build_signal_windows,
    build_signal_bundles,
    iter_market_events,
    load_audit_events,
    load_config_index,
    verify_signal_bundle,
)


class ReplayHarnessTests(unittest.TestCase):
    def test_market_event_decoder_handles_snapshot_and_delta(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tape_path = Path(tmp_dir) / "sample.bin"
            records = [
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "EV-LOW",
                        "yes_dollars_fp": [["0.20", "5.00"]],
                        "no_dollars_fp": [["0.75", "2.00"]],
                    },
                },
                {
                    "type": "orderbook_delta",
                    "sid": 1,
                    "seq": 2,
                    "msg": {
                        "market_ticker": "EV-LOW",
                        "price_dollars": "0.21",
                        "delta_fp": "3.00",
                        "side": "yes",
                    },
                },
            ]
            with tape_path.open("wb") as handle:
                for record in records:
                    payload = json.dumps(record).encode("utf-8")
                    handle.write(pack("<I", len(payload)))
                    handle.write(payload)

            events = list(iter_market_events(tape_path))
            self.assertEqual(len(events), 2)
            self.assertEqual(events[0].bids, ((200, 500),))
            self.assertEqual(events[0].asks, ((250, 200),))
            self.assertEqual(events[1].side, "bid")
            self.assertEqual(events[1].price_ticks, 210)
            self.assertEqual(events[1].delta_qty_lots, 300)
            self.assertIsNone(events[0].recv_ts_ns)

    def test_market_event_decoder_handles_timestamped_tape_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tape_path = Path(tmp_dir) / "sample_v2.bin"
            records = [
                (
                    111_000_000,
                    {
                        "type": "orderbook_snapshot",
                        "sid": 1,
                        "seq": 1,
                        "msg": {
                            "market_ticker": "EV-LOW",
                            "yes_dollars_fp": [["0.20", "5.00"]],
                            "no_dollars_fp": [["0.75", "2.00"]],
                        },
                    },
                ),
                (
                    222_000_000,
                    {
                        "type": "orderbook_delta",
                        "sid": 1,
                        "seq": 2,
                        "msg": {
                            "market_ticker": "EV-LOW",
                            "price_dollars": "0.21",
                            "delta_fp": "3.00",
                            "side": "yes",
                        },
                    },
                ),
            ]
            with tape_path.open("wb") as handle:
                handle.write(b"PDT2")
                handle.write(pack("<HH", 2, 0))
                for recv_ts_ns, record in records:
                    payload = json.dumps(record).encode("utf-8")
                    handle.write(pack("<QI", recv_ts_ns, len(payload)))
                    handle.write(payload)

            events = list(iter_market_events(tape_path))
            self.assertEqual(len(events), 2)
            self.assertEqual(events[0].recv_ts_ns, 111_000_000)
            self.assertEqual(events[1].recv_ts_ns, 222_000_000)
            self.assertEqual(events[1].price_ticks, 210)
            self.assertEqual(events[1].delta_qty_lots, 300)

    def test_replay_book_store_tracks_top_of_book(self) -> None:
        store = ReplayBookStore()
        snapshot = next(
            iter(
                [
                    event
                    for event in [
                        *iter_market_events(self._write_tape(
                            [
                                {
                                    "type": "orderbook_snapshot",
                                    "sid": 1,
                                    "seq": 1,
                                    "msg": {
                                        "market_ticker": "EV-LOW",
                                        "yes_dollars_fp": [["0.20", "5.00"]],
                                        "no_dollars_fp": [["0.75", "2.00"]],
                                    },
                                }
                            ]
                        ))
                    ]
                ]
            )
        )
        store.apply(snapshot)
        state = store.books["EV-LOW"]
        self.assertEqual(state.best_bid().price_ticks, 200)  # type: ignore[union-attr]
        self.assertEqual(state.best_ask().price_ticks, 250)  # type: ignore[union-attr]
        self.assertEqual(state.best_bid().qty_lots, 500)  # type: ignore[union-attr]
        self.assertEqual(state.best_ask().qty_lots, 200)  # type: ignore[union-attr]

    def test_signal_bundle_verification_matches_replayed_books(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            config_path = tmp / "config.json"
            audit_path = tmp / "audit.jsonl"
            tape_path = tmp / "tape.bin"

            config_path.write_text(
                json.dumps(
                    {
                        "kalshi": {"market_tickers": ["EV-EASY", "EV-HARD"]},
                        "market_routes": [
                            {
                                "market_ticker": "EV-EASY",
                                "market_id": 101,
                                "event_id": 501,
                                "affinity_key": 77,
                                "topology_kind": "monotonic_chain",
                                "strike_key": 1,
                            },
                            {
                                "market_ticker": "EV-HARD",
                                "market_id": 202,
                                "event_id": 501,
                                "affinity_key": 77,
                                "topology_kind": "monotonic_chain",
                                "strike_key": 2,
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            records = [
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "EV-EASY",
                        "yes_dollars_fp": [["0.10", "2.00"]],
                        "no_dollars_fp": [["0.87", "3.00"]],
                    },
                },
                {
                    "type": "orderbook_snapshot",
                    "sid": 2,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "EV-HARD",
                        "yes_dollars_fp": [["0.72", "4.00"]],
                        "no_dollars_fp": [["0.40", "1.00"]],
                    },
                },
            ]
            with tape_path.open("wb") as handle:
                for record in records:
                    payload = json.dumps(record).encode("utf-8")
                    handle.write(pack("<I", len(payload)))
                    handle.write(payload)

            audit_rows = [
                {
                    "kind": "group_signal",
                    "ts_ns": 123,
                    "shard_id": 1,
                    "signal_id": 7,
                    "group_id": 0,
                    "local_intent_id": 0,
                    "oms_request_id": 0,
                    "exchange": 1,
                    "event_id": 501,
                    "market_id": 0,
                    "side": 0,
                    "leg_index": 0,
                    "leg_count": 2,
                    "qty_lots": 0,
                    "aux_qty_lots": 0,
                    "price_ticks": 0,
                    "aux_price_ticks": 0,
                    "edge_ticks": 560,
                    "score": 560,
                    "decision_code": 0,
                    "reject_reason": 0,
                    "lifecycle_kind": 0,
                    "order_status": 0,
                    "event_exposure_lots": 0,
                    "market_exposure_lots": 0,
                },
                {
                    "kind": "local_risk",
                    "ts_ns": 123,
                    "shard_id": 1,
                    "signal_id": 7,
                    "group_id": 6,
                    "local_intent_id": 11,
                    "oms_request_id": 0,
                    "exchange": 1,
                    "event_id": 501,
                    "market_id": 101,
                    "side": 3,
                    "leg_index": 0,
                    "leg_count": 2,
                    "qty_lots": 100,
                    "aux_qty_lots": 0,
                    "price_ticks": 130,
                    "aux_price_ticks": 0,
                    "edge_ticks": 0,
                    "score": 0,
                    "decision_code": 2,
                    "reject_reason": 3,
                    "lifecycle_kind": 0,
                    "order_status": 0,
                    "event_exposure_lots": 4,
                    "market_exposure_lots": 4,
                },
                {
                    "kind": "local_risk",
                    "ts_ns": 123,
                    "shard_id": 1,
                    "signal_id": 7,
                    "group_id": 6,
                    "local_intent_id": 12,
                    "oms_request_id": 0,
                    "exchange": 1,
                    "event_id": 501,
                    "market_id": 202,
                    "side": 4,
                    "leg_index": 1,
                    "leg_count": 2,
                    "qty_lots": 100,
                    "aux_qty_lots": 0,
                    "price_ticks": 720,
                    "aux_price_ticks": 0,
                    "edge_ticks": 0,
                    "score": 0,
                    "decision_code": 2,
                    "reject_reason": 3,
                    "lifecycle_kind": 0,
                    "order_status": 0,
                    "event_exposure_lots": 4,
                    "market_exposure_lots": 4,
                },
            ]
            audit_path.write_text(
                "\n".join(json.dumps(row) for row in audit_rows) + "\n",
                encoding="utf-8",
            )

            config_index = load_config_index(config_path)
            bundle = build_signal_bundles(load_audit_events(audit_path))[(1, 7)]
            verification = verify_signal_bundle(bundle, config_index=config_index, tape_path=tape_path)

            self.assertTrue(verification.matched)
            self.assertEqual(verification.recomputed_edge_ticks, 560)
            self.assertEqual(verification.easier_ask_ticks, 130)
            self.assertEqual(verification.harder_bid_ticks, 720)

            timeline = build_event_timeline(
                config_index=config_index,
                bundles=build_signal_bundles(load_audit_events(audit_path)),
                tape_path=tape_path,
                event_id=501,
            )
            self.assertEqual(timeline.event_id, 501)
            self.assertEqual(len(timeline.signal_hits), 1)
            self.assertGreaterEqual(len(timeline.rows), 2)

    def test_signal_windows_collapse_duplicate_fires_into_one_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            config_path = tmp / "config.json"
            audit_path = tmp / "audit.jsonl"
            tape_path = tmp / "tape_v2.bin"

            config_path.write_text(
                json.dumps(
                    {
                        "market_routes": [
                            {
                                "market_ticker": "EV-EASY",
                                "market_id": 101,
                                "event_id": 501,
                                "affinity_key": 77,
                                "topology_kind": "monotonic_chain",
                                "strike_key": 1,
                            },
                            {
                                "market_ticker": "EV-HARD",
                                "market_id": 202,
                                "event_id": 501,
                                "affinity_key": 77,
                                "topology_kind": "monotonic_chain",
                                "strike_key": 2,
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            records = [
                (
                    100_000_000,
                    {
                        "type": "orderbook_snapshot",
                        "sid": 1,
                        "seq": 1,
                        "msg": {
                            "market_ticker": "EV-EASY",
                            "yes_dollars_fp": [["0.10", "2.00"]],
                            "no_dollars_fp": [["0.87", "3.00"]],
                        },
                    },
                ),
                (
                    200_000_000,
                    {
                        "type": "orderbook_snapshot",
                        "sid": 2,
                        "seq": 1,
                        "msg": {
                            "market_ticker": "EV-HARD",
                            "yes_dollars_fp": [["0.72", "4.00"]],
                            "no_dollars_fp": [["0.40", "1.00"]],
                        },
                    },
                ),
                (
                    300_000_000,
                    {
                        "type": "orderbook_delta",
                        "sid": 2,
                        "seq": 2,
                        "msg": {
                            "market_ticker": "EV-HARD",
                            "price_dollars": "0.72",
                            "delta_fp": "-4.00",
                            "side": "yes",
                        },
                    },
                ),
            ]
            with tape_path.open("wb") as handle:
                handle.write(b"PDT2")
                handle.write(pack("<HH", 2, 0))
                for recv_ts_ns, record in records:
                    payload = json.dumps(record).encode("utf-8")
                    handle.write(pack("<QI", recv_ts_ns, len(payload)))
                    handle.write(payload)

            audit_rows = []
            for signal_id, signal_ts in ((7, 210_000_000), (8, 250_000_000)):
                audit_rows.extend(
                    [
                        {
                            "kind": "group_signal",
                            "ts_ns": signal_ts,
                            "shard_id": 1,
                            "signal_id": signal_id,
                            "group_id": 0,
                            "local_intent_id": 0,
                            "oms_request_id": 0,
                            "exchange": 1,
                            "event_id": 501,
                            "market_id": 0,
                            "side": 0,
                            "leg_index": 0,
                            "leg_count": 2,
                            "qty_lots": 0,
                            "aux_qty_lots": 0,
                            "price_ticks": 0,
                            "aux_price_ticks": 0,
                            "edge_ticks": 560,
                            "score": 560,
                            "decision_code": 0,
                            "reject_reason": 0,
                            "lifecycle_kind": 0,
                            "order_status": 0,
                            "event_exposure_lots": 0,
                            "market_exposure_lots": 0,
                        },
                        {
                            "kind": "submission",
                            "ts_ns": signal_ts,
                            "shard_id": 1,
                            "signal_id": signal_id,
                            "group_id": signal_id,
                            "local_intent_id": signal_id * 10 + 1,
                            "oms_request_id": 0,
                            "exchange": 1,
                            "event_id": 501,
                            "market_id": 101,
                            "side": 3,
                            "leg_index": 0,
                            "leg_count": 2,
                            "qty_lots": 100,
                            "aux_qty_lots": 0,
                            "price_ticks": 130,
                            "aux_price_ticks": 0,
                            "edge_ticks": 0,
                            "score": 0,
                            "decision_code": 0,
                            "reject_reason": 0,
                            "lifecycle_kind": 0,
                            "order_status": 0,
                            "event_exposure_lots": 0,
                            "market_exposure_lots": 0,
                            "tick_recv_ns": signal_ts,
                            "signal_ts_ns": signal_ts,
                            "submission_enqueued_ns": signal_ts,
                            "signal_to_submission_ns": 0,
                        },
                        {
                            "kind": "submission",
                            "ts_ns": signal_ts,
                            "shard_id": 1,
                            "signal_id": signal_id,
                            "group_id": signal_id,
                            "local_intent_id": signal_id * 10 + 2,
                            "oms_request_id": 0,
                            "exchange": 1,
                            "event_id": 501,
                            "market_id": 202,
                            "side": 4,
                            "leg_index": 1,
                            "leg_count": 2,
                            "qty_lots": 100,
                            "aux_qty_lots": 0,
                            "price_ticks": 720,
                            "aux_price_ticks": 0,
                            "edge_ticks": 0,
                            "score": 0,
                            "decision_code": 0,
                            "reject_reason": 0,
                            "lifecycle_kind": 0,
                            "order_status": 0,
                            "event_exposure_lots": 0,
                            "market_exposure_lots": 0,
                            "tick_recv_ns": signal_ts,
                            "signal_ts_ns": signal_ts,
                            "submission_enqueued_ns": signal_ts,
                            "signal_to_submission_ns": 0,
                        },
                        {
                            "kind": "oms_decision",
                            "ts_ns": signal_ts + 1,
                            "shard_id": 1,
                            "signal_id": signal_id,
                            "group_id": signal_id,
                            "local_intent_id": signal_id * 10 + 1,
                            "oms_request_id": signal_id * 100 + 1,
                            "event_id": 501,
                            "market_id": 101,
                            "leg_index": 0,
                            "leg_count": 2,
                            "qty_lots": 100,
                            "decision_code": 1,
                            "reject_reason": 8,
                            "tick_recv_ns": signal_ts,
                            "signal_ts_ns": signal_ts,
                            "submission_enqueued_ns": signal_ts,
                            "oms_decision_ts_ns": signal_ts + 1,
                            "signal_to_submission_ns": 0,
                            "submission_to_decision_ns": 1,
                        },
                        {
                            "kind": "oms_decision",
                            "ts_ns": signal_ts + 1,
                            "shard_id": 1,
                            "signal_id": signal_id,
                            "group_id": signal_id,
                            "local_intent_id": signal_id * 10 + 2,
                            "oms_request_id": signal_id * 100 + 2,
                            "event_id": 501,
                            "market_id": 202,
                            "leg_index": 1,
                            "leg_count": 2,
                            "qty_lots": 100,
                            "decision_code": 1,
                            "reject_reason": 8,
                            "tick_recv_ns": signal_ts,
                            "signal_ts_ns": signal_ts,
                            "submission_enqueued_ns": signal_ts,
                            "oms_decision_ts_ns": signal_ts + 1,
                            "signal_to_submission_ns": 0,
                            "submission_to_decision_ns": 1,
                        },
                    ]
                )

            audit_rows.extend(
                [
                    {
                        "kind": "shard_reconcile",
                        "ts_ns": 350_000_000,
                        "shard_id": 1,
                        "signal_id": 7,
                        "group_id": 7,
                        "local_intent_id": 71,
                        "oms_request_id": 701,
                        "event_id": 501,
                        "market_id": 101,
                        "leg_index": 0,
                        "leg_count": 2,
                        "qty_lots": 0,
                        "tick_recv_ns": 210_000_000,
                        "signal_ts_ns": 210_000_000,
                        "submission_enqueued_ns": 210_000_000,
                        "oms_decision_ts_ns": 210_000_001,
                        "terminal_recv_ns": 350_000_000,
                        "signal_to_submission_ns": 0,
                        "submission_to_decision_ns": 1,
                        "tick_to_terminal_ns": 140_000_000,
                    },
                    {
                        "kind": "shard_reconcile",
                        "ts_ns": 350_000_000,
                        "shard_id": 1,
                        "signal_id": 7,
                        "group_id": 7,
                        "local_intent_id": 72,
                        "oms_request_id": 702,
                        "event_id": 501,
                        "market_id": 202,
                        "leg_index": 1,
                        "leg_count": 2,
                        "qty_lots": 0,
                        "tick_recv_ns": 210_000_000,
                        "signal_ts_ns": 210_000_000,
                        "submission_enqueued_ns": 210_000_000,
                        "oms_decision_ts_ns": 210_000_001,
                        "terminal_recv_ns": 350_000_000,
                        "signal_to_submission_ns": 0,
                        "submission_to_decision_ns": 1,
                        "tick_to_terminal_ns": 140_000_000,
                    },
                ]
            )
            audit_path.write_text(
                "\n".join(json.dumps(row) for row in audit_rows) + "\n",
                encoding="utf-8",
            )

            config_index = load_config_index(config_path)
            bundles = build_signal_bundles(load_audit_events(audit_path))
            windows = build_signal_windows(
                config_index=config_index,
                bundles=bundles,
                tape_path=tape_path,
                event_id=501,
            )

            self.assertEqual(windows.event_id, 501)
            self.assertEqual(len(windows.windows), 1)
            self.assertEqual(len(windows.signals), 2)
            window = windows.windows[0]
            self.assertEqual(window.signal_count, 2)
            self.assertEqual(window.accepted_signal_count, 2)
            self.assertEqual(window.terminal_after_window_signal_count, 1)
            self.assertAlmostEqual(window.duration_ms, 100.0)

    def test_signal_edge_lifetimes_split_when_edge_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            config_path = tmp / "config.json"
            audit_path = tmp / "audit.jsonl"
            tape_path = tmp / "tape_v2.bin"

            config_path.write_text(
                json.dumps(
                    {
                        "market_routes": [
                            {
                                "market_ticker": "EV-EASY",
                                "market_id": 101,
                                "event_id": 501,
                                "affinity_key": 77,
                                "topology_kind": "monotonic_chain",
                                "strike_key": 1,
                            },
                            {
                                "market_ticker": "EV-HARD",
                                "market_id": 202,
                                "event_id": 501,
                                "affinity_key": 77,
                                "topology_kind": "monotonic_chain",
                                "strike_key": 2,
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            records = [
                (
                    100_000_000,
                    {
                        "type": "orderbook_snapshot",
                        "sid": 1,
                        "seq": 1,
                        "msg": {
                            "market_ticker": "EV-EASY",
                            "yes_dollars_fp": [["0.10", "2.00"]],
                            "no_dollars_fp": [["0.87", "3.00"]],
                        },
                    },
                ),
                (
                    200_000_000,
                    {
                        "type": "orderbook_snapshot",
                        "sid": 2,
                        "seq": 1,
                        "msg": {
                            "market_ticker": "EV-HARD",
                            "yes_dollars_fp": [["0.72", "4.00"]],
                            "no_dollars_fp": [["0.40", "1.00"]],
                        },
                    },
                ),
                (
                    260_000_000,
                    {
                        "type": "orderbook_delta",
                        "sid": 2,
                        "seq": 2,
                        "msg": {
                            "market_ticker": "EV-HARD",
                            "price_dollars": "0.72",
                            "delta_fp": "-4.00",
                            "side": "yes",
                        },
                    },
                ),
                (
                    260_000_000,
                    {
                        "type": "orderbook_delta",
                        "sid": 2,
                        "seq": 3,
                        "msg": {
                            "market_ticker": "EV-HARD",
                            "price_dollars": "0.68",
                            "delta_fp": "4.00",
                            "side": "yes",
                        },
                    },
                ),
                (
                    300_000_000,
                    {
                        "type": "orderbook_delta",
                        "sid": 2,
                        "seq": 4,
                        "msg": {
                            "market_ticker": "EV-HARD",
                            "price_dollars": "0.68",
                            "delta_fp": "-4.00",
                            "side": "yes",
                        },
                    },
                ),
            ]
            with tape_path.open("wb") as handle:
                handle.write(b"PDT2")
                handle.write(pack("<HH", 2, 0))
                for recv_ts_ns, record in records:
                    payload = json.dumps(record).encode("utf-8")
                    handle.write(pack("<QI", recv_ts_ns, len(payload)))
                    handle.write(payload)

            audit_rows = []
            for signal_id, signal_ts, edge_ticks in (
                (7, 210_000_000, 560),
                (8, 250_000_000, 560),
                (9, 280_000_000, 520),
            ):
                audit_rows.extend(
                    [
                        {
                            "kind": "group_signal",
                            "ts_ns": signal_ts,
                            "shard_id": 1,
                            "signal_id": signal_id,
                            "group_id": 0,
                            "local_intent_id": 0,
                            "oms_request_id": 0,
                            "exchange": 1,
                            "event_id": 501,
                            "market_id": 0,
                            "side": 0,
                            "leg_index": 0,
                            "leg_count": 2,
                            "qty_lots": 0,
                            "aux_qty_lots": 0,
                            "price_ticks": 0,
                            "aux_price_ticks": 0,
                            "edge_ticks": edge_ticks,
                            "score": edge_ticks,
                            "decision_code": 0,
                            "reject_reason": 0,
                            "lifecycle_kind": 0,
                            "order_status": 0,
                            "event_exposure_lots": 0,
                            "market_exposure_lots": 0,
                        },
                        {
                            "kind": "submission",
                            "ts_ns": signal_ts,
                            "shard_id": 1,
                            "signal_id": signal_id,
                            "group_id": signal_id,
                            "local_intent_id": signal_id * 10 + 1,
                            "oms_request_id": 0,
                            "exchange": 1,
                            "event_id": 501,
                            "market_id": 101,
                            "side": 3,
                            "leg_index": 0,
                            "leg_count": 2,
                            "qty_lots": 100,
                            "aux_qty_lots": 0,
                            "price_ticks": 130,
                            "aux_price_ticks": 0,
                            "edge_ticks": 0,
                            "score": 0,
                            "decision_code": 0,
                            "reject_reason": 0,
                            "lifecycle_kind": 0,
                            "order_status": 0,
                            "event_exposure_lots": 0,
                            "market_exposure_lots": 0,
                            "tick_recv_ns": signal_ts,
                            "signal_ts_ns": signal_ts,
                            "submission_enqueued_ns": signal_ts,
                            "signal_to_submission_ns": 0,
                        },
                        {
                            "kind": "submission",
                            "ts_ns": signal_ts,
                            "shard_id": 1,
                            "signal_id": signal_id,
                            "group_id": signal_id,
                            "local_intent_id": signal_id * 10 + 2,
                            "oms_request_id": 0,
                            "exchange": 1,
                            "event_id": 501,
                            "market_id": 202,
                            "side": 4,
                            "leg_index": 1,
                            "leg_count": 2,
                            "qty_lots": 100,
                            "aux_qty_lots": 0,
                            "price_ticks": 720 if edge_ticks == 560 else 680,
                            "aux_price_ticks": 0,
                            "edge_ticks": 0,
                            "score": 0,
                            "decision_code": 0,
                            "reject_reason": 0,
                            "lifecycle_kind": 0,
                            "order_status": 0,
                            "event_exposure_lots": 0,
                            "market_exposure_lots": 0,
                            "tick_recv_ns": signal_ts,
                            "signal_ts_ns": signal_ts,
                            "submission_enqueued_ns": signal_ts,
                            "signal_to_submission_ns": 0,
                        },
                    ]
                )

            audit_path.write_text(
                "\n".join(json.dumps(row) for row in audit_rows) + "\n",
                encoding="utf-8",
            )

            config_index = load_config_index(config_path)
            bundles = build_signal_bundles(load_audit_events(audit_path))
            lifetimes = build_signal_edge_lifetimes(
                config_index=config_index,
                bundles=bundles,
                tape_path=tape_path,
                event_id=501,
            )

            self.assertEqual(lifetimes.event_id, 501)
            self.assertEqual(len(lifetimes.lifetimes), 2)
            first, second = lifetimes.lifetimes
            self.assertEqual(first.edge_ticks, 560)
            self.assertEqual(first.signal_count, 2)
            self.assertAlmostEqual(first.duration_ms, 60.0)
            self.assertFalse(first.censored)
            self.assertEqual(second.edge_ticks, 520)
            self.assertEqual(second.signal_count, 1)
            self.assertAlmostEqual(second.duration_ms, 40.0)
            self.assertFalse(second.censored)

    @staticmethod
    def _write_tape(records: list[dict[str, object]]) -> Path:
        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        path = Path(tmp_file.name)
        tmp_file.close()
        with path.open("wb") as handle:
            for record in records:
                payload = json.dumps(record).encode("utf-8")
                handle.write(pack("<I", len(payload)))
                handle.write(payload)
        return path


if __name__ == "__main__":
    unittest.main()
