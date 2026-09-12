from __future__ import annotations

import json
import struct
import tempfile
import unittest
from importlib.util import find_spec
from pathlib import Path

from predex.replay.config_summary import format_config_summary, summarize_config
from predex.replay.market_data_tape import FILE_HEADER, RECORD_HEADER, TAPE_MAGIC, TAPE_VERSION
from predex.replay.materialize import materialize_run
from predex.replay.metadata_overlay import enrich_run_metadata


class ConfigSummaryTests(unittest.TestCase):
    def test_summarize_config_reports_distribution_and_largest_events(self) -> None:
        config = {
            "runtime": {"shard_count": 2},
            "universe": {
                "events": [
                    {
                        "event_id": "1",
                        "affinity_key": "0",
                        "topology": "monotonic_chain",
                        "markets": [
                            {
                                "market_id": "10",
                                "kalshi_ticker": "EV1-M1",
                                "tradeable": True,
                                "price_level_structure": "linear_cent",
                            },
                            {
                                "market_id": "11",
                                "kalshi_ticker": "EV1-M2",
                                "tradeable": False,
                                "price_level_structure": "tapered_deci_cent",
                            },
                        ],
                    },
                    {
                        "event_id": "2",
                        "affinity_key": "1",
                        "topology": "monotonic_chain",
                        "markets": [
                            {
                                "market_id": "20",
                                "kalshi_ticker": "EV2-M1",
                                "tradeable": True,
                                "price_level_structure": "deci_cent",
                            }
                        ],
                    },
                ]
            },
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "config.json"
            config_path.write_text(json.dumps(config), encoding="utf-8")

            summary = summarize_config(config_path, top_events=1, sample_tickers=1)

        self.assertEqual(summary["event_count"], 2)
        self.assertEqual(summary["market_count"], 3)
        self.assertEqual(summary["markets_per_event"]["average"], 1.5)
        self.assertEqual(summary["market_count_histogram"], {"1": 1, "2": 1})
        self.assertEqual(summary["shard_summaries"]["0"]["market_count"], 2)
        self.assertEqual(summary["shard_summaries"]["1"]["market_count"], 1)
        self.assertEqual(summary["price_level_structure_counts"]["linear_cent"], 1)
        self.assertEqual(summary["tradeable_counts"], {"false": 1, "true": 2})
        self.assertEqual(summary["largest_events"][0]["event_id"], 1)
        self.assertEqual(summary["largest_events"][0]["sample_tickers"], ["EV1-M1"])

        formatted = format_config_summary(summary)
        self.assertIn("Config Summary", formatted)
        self.assertIn("Markets/Event", formatted)
        self.assertIn("Largest Events", formatted)


class MetadataOverlayTests(unittest.TestCase):
    def test_enrichment_requires_an_explicit_run_selection(self) -> None:
        with self.assertRaisesRegex(ValueError, "provide at least one"):
            enrich_run_metadata(progress_callback=None)


@unittest.skipIf(find_spec("pyarrow") is None, "pyarrow not installed")
class MaterializeRunTests(unittest.TestCase):
    def test_materialize_run_writes_tables_manifest_and_compressed_artifacts(self) -> None:
        config = {
            "runtime": {"shard_count": 1},
            "universe": {
                "events": [
                    {
                        "event_id": "1",
                        "affinity_key": "0",
                        "topology": "monotonic_chain",
                        "markets": [
                            {
                                "market_id": "10",
                                "kalshi_ticker": "EV1-M1",
                                "tradeable": True,
                                "price_level_structure": "linear_cent",
                            }
                        ],
                    }
                ]
            },
        }

        def payload(message_type: str, sequence: int, msg: dict[str, object]) -> bytes:
            return json.dumps(
                {
                    "type": message_type,
                    "sid": 1,
                    "seq": sequence,
                    "msg": {"market_ticker": "EV1-M1", **msg},
                },
                separators=(",", ":"),
            ).encode("utf-8")

        records = [
            (1, 1_000, payload("orderbook_snapshot", 1, {"yes_dollars_fp": [["0.5000", "10.00"]], "no_dollars_fp": [["0.4900", "2.00"]]}), 1),
            (2, 2_000, payload("orderbook_delta", 2, {"side": "yes", "price_dollars": "0.5000", "delta_fp": "-1.00"}), 2),
            (3, 3_000, payload("trade", 3, {"price_dollars": "0.5000", "count_fp": "1.00", "taker_side": "yes"}), 3),
            (4, 4_000, payload("market_lifecycle_v2", 4, {"status": "active"}), 6),
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            run_dir = Path(tmp_dir) / "run"
            run_dir.mkdir()
            (run_dir / "config.json").write_text(json.dumps(config), encoding="utf-8")
            (run_dir / "report.json").write_text("{}", encoding="utf-8")
            with (run_dir / "tape.bin").open("wb") as tape:
                tape.write(FILE_HEADER.pack(TAPE_MAGIC, TAPE_VERSION, 0))
                for record_index, recv_ts_ns, body, frame_kind in records:
                    tape.write(
                        RECORD_HEADER.pack(
                            1,
                            recv_ts_ns,
                            record_index,
                            0,
                            1,
                            10,
                            1,
                            0,
                            0,
                            0,
                            len(body),
                            frame_kind,
                            1,
                            0,
                        )
                    )
                    tape.write(body)

            result = materialize_run(run_dir, batch_size=2, compress_if_verified=True)

            self.assertTrue(result.manifest["verified"])
            self.assertEqual(result.manifest["tables"]["frames"]["rows"], 4)
            self.assertEqual(result.manifest["tables"]["deltas"]["rows"], 1)
            self.assertEqual(result.manifest["tables"]["trades"]["rows"], 1)
            self.assertEqual(result.manifest["tables"]["snapshots"]["rows"], 1)
            self.assertEqual(result.manifest["tables"]["lifecycles"]["rows"], 1)
            self.assertEqual(result.manifest["tables"]["snapshot_levels"]["rows"], 2)
            self.assertTrue((run_dir / "tables" / "frames.parquet").exists())
            self.assertTrue((run_dir / "config.json.gz").exists())
            self.assertTrue((run_dir / "report.json.gz").exists())
            self.assertTrue((run_dir / "tape.bin.gz").exists())
            self.assertTrue((run_dir / "tape.bin").exists())

            remove_result = materialize_run(run_dir, batch_size=2, remove_if_verified=True)

            self.assertTrue(remove_result.manifest["verified"])
            self.assertTrue(remove_result.manifest["remove_raw_checks"]["verified"])
            self.assertTrue(remove_result.manifest["remove_raw_checks"]["tape_bin_gz_exists"])
            self.assertTrue(remove_result.manifest["remove_raw_checks"]["expected_tables_exist"])
            self.assertEqual(remove_result.manifest["removed_artifacts"]["tape"], str(run_dir / "tape.bin"))
            self.assertFalse((run_dir / "tape.bin").exists())
            self.assertTrue((run_dir / "tape.bin.gz").exists())
            self.assertTrue((run_dir / "config.json").exists())
            self.assertTrue((run_dir / "report.json").exists())


if __name__ == "__main__":
    unittest.main()
