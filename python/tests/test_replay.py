from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from struct import pack

from predex.replay import (
    ReplayBookStore,
    build_event_timeline,
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
            self.assertEqual(events[0].bids, ((2000, 5),))
            self.assertEqual(events[0].asks, ((2500, 2),))
            self.assertEqual(events[1].side, "bid")
            self.assertEqual(events[1].price_ticks, 2100)
            self.assertEqual(events[1].delta_qty_lots, 3)

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
        self.assertEqual(state.best_bid().price_ticks, 2000)  # type: ignore[union-attr]
        self.assertEqual(state.best_ask().price_ticks, 2500)  # type: ignore[union-attr]

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
                    "edge_ticks": 5600,
                    "score": 5600,
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
                    "qty_lots": 1,
                    "aux_qty_lots": 0,
                    "price_ticks": 1300,
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
                    "qty_lots": 1,
                    "aux_qty_lots": 0,
                    "price_ticks": 7200,
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
            self.assertEqual(verification.recomputed_edge_ticks, 5600)
            self.assertEqual(verification.easier_ask_ticks, 1300)
            self.assertEqual(verification.harder_bid_ticks, 7200)

            timeline = build_event_timeline(
                config_index=config_index,
                bundles=build_signal_bundles(load_audit_events(audit_path)),
                tape_path=tape_path,
                event_id=501,
            )
            self.assertEqual(timeline.event_id, 501)
            self.assertEqual(len(timeline.signal_hits), 1)
            self.assertGreaterEqual(len(timeline.rows), 2)

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
