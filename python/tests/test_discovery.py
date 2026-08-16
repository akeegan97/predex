from __future__ import annotations

import unittest
from argparse import Namespace
from datetime import UTC, datetime
from email.message import Message
import time
from urllib.error import HTTPError

from predex.discovery.cli import _resolve_run_artifacts, build_parser
from predex.discovery import (
    EventRecord,
    KalshiMarketDataSettings,
    KalshiSettings,
    MarketRecord,
    RuntimeSettings,
    ThreadPollingSettings,
    TopologyKind,
    build_app_config_result,
    build_trader_config,
    build_trader_config_result,
    classify_event,
)
from predex.discovery.config import PipelineSettings
from predex.discovery.kalshi import KalshiPublicClient


class ClassifierTests(unittest.TestCase):
    def test_app_generator_defaults_to_harvest_thread_polling(self) -> None:
        args = build_parser().parse_args(["--config-format", "app"])

        self.assertEqual(args.thread_polling_profile, "harvest")
        self.assertEqual(args.thread_spin_iterations, 64)
        self.assertEqual(args.thread_yield_iterations, 64)
        self.assertEqual(args.thread_min_sleep_us, 50)
        self.assertEqual(args.thread_max_sleep_us, 1000)

    def test_run_artifacts_bundle_defaults_into_run_directory(self) -> None:
        args = Namespace(
            run_dir_root="runs",
            run_label=None,
            all_events=True,
            event_ticker=[],
            series_ticker=None,
            status="open",
            include_topology=["monotonic_chain"],
            exclude_topology=[],
            overwrite_run_dir=True,
            output=None,
            report_output=None,
            tape_output=None,
            audit_output=None,
        )

        artifacts = _resolve_run_artifacts(
            args,
            now=datetime(2026, 6, 28, 14, 30, 12, tzinfo=UTC),
        )

        self.assertEqual(
            str(artifacts.run_dir),
            "runs/predex-2026-06-28-143012-market-data-all-events-monotonic-chain",
        )
        self.assertEqual(
            artifacts.output,
            "runs/predex-2026-06-28-143012-market-data-all-events-monotonic-chain/config.json",
        )
        self.assertEqual(
            artifacts.report_output,
            "runs/predex-2026-06-28-143012-market-data-all-events-monotonic-chain/report.json",
        )
        self.assertEqual(
            artifacts.tape_output,
            "runs/predex-2026-06-28-143012-market-data-all-events-monotonic-chain/tape.bin",
        )
        self.assertEqual(
            artifacts.audit_output,
            "runs/predex-2026-06-28-143012-market-data-all-events-monotonic-chain/audit.jsonl",
        )

    def test_run_artifacts_explicit_paths_win_over_bundle_defaults(self) -> None:
        args = Namespace(
            run_dir_root="runs",
            run_label="overnight soak",
            all_events=False,
            event_ticker=[],
            series_ticker=None,
            status="open",
            include_topology=[],
            exclude_topology=[],
            overwrite_run_dir=True,
            output="custom/config.json",
            report_output="custom/report.json",
            tape_output="custom/tape.bin",
            audit_output="custom/audit.jsonl",
        )

        artifacts = _resolve_run_artifacts(
            args,
            now=datetime(2026, 6, 28, 14, 30, 12, tzinfo=UTC),
        )

        self.assertEqual(str(artifacts.run_dir), "runs/predex-2026-06-28-143012-market-data-overnight-soak")
        self.assertEqual(artifacts.output, "custom/config.json")
        self.assertEqual(artifacts.report_output, "custom/report.json")
        self.assertEqual(artifacts.tape_output, "custom/tape.bin")
        self.assertEqual(artifacts.audit_output, "custom/audit.jsonl")

    def test_single_market_event_classifies_as_single_market(self) -> None:
        event = EventRecord(
            event_ticker="EV-SINGLE",
            markets=[MarketRecord(ticker="MKT-1", event_ticker="EV-SINGLE")],
        )

        classified = classify_event(event)

        self.assertEqual(classified.topology_kind, TopologyKind.SINGLE_MARKET)
        self.assertEqual(classified.markets[0].strike_key, 0)

    def test_mutually_exclusive_flag_wins(self) -> None:
        event = EventRecord(
            event_ticker="EV-MUTEX",
            mutually_exclusive=True,
            markets=[
                MarketRecord(ticker="MKT-A", event_ticker="EV-MUTEX"),
                MarketRecord(ticker="MKT-B", event_ticker="EV-MUTEX"),
            ],
        )

        classified = classify_event(event)

        self.assertEqual(classified.topology_kind, TopologyKind.MUTUALLY_EXCLUSIVE)

    def test_greater_thresholds_form_monotonic_chain(self) -> None:
        event = EventRecord(
            event_ticker="EV-GREATER",
            markets=[
                MarketRecord(
                    ticker="MKT-60",
                    event_ticker="EV-GREATER",
                    strike_type="greater",
                    floor_strike=60,
                ),
                MarketRecord(
                    ticker="MKT-70",
                    event_ticker="EV-GREATER",
                    strike_type="greater",
                    floor_strike=70,
                ),
                MarketRecord(
                    ticker="MKT-80",
                    event_ticker="EV-GREATER",
                    strike_type="greater",
                    floor_strike=80,
                ),
            ],
        )

        classified = classify_event(event)

        self.assertEqual(classified.topology_kind, TopologyKind.MONOTONIC_CHAIN)
        self.assertEqual(
            [market.market.ticker for market in classified.markets],
            ["MKT-60", "MKT-70", "MKT-80"],
        )

    def test_less_thresholds_reverse_into_easiest_to_hardest_order(self) -> None:
        event = EventRecord(
            event_ticker="EV-LESS",
            markets=[
                MarketRecord(
                    ticker="MKT-LT-10",
                    event_ticker="EV-LESS",
                    strike_type="less",
                    cap_strike=10,
                ),
                MarketRecord(
                    ticker="MKT-LT-20",
                    event_ticker="EV-LESS",
                    strike_type="less",
                    cap_strike=20,
                ),
            ],
        )

        classified = classify_event(event)

        self.assertEqual(classified.topology_kind, TopologyKind.MONOTONIC_CHAIN)
        self.assertEqual(
            [market.market.ticker for market in classified.markets],
            ["MKT-LT-20", "MKT-LT-10"],
        )

    def test_numeric_thresholds_with_distinct_close_times_stay_unordered(self) -> None:
        event = EventRecord(
            event_ticker="EV-NUMERIC-HORIZON-MISMATCH",
            markets=[
                MarketRecord(
                    ticker="MKT-2025",
                    event_ticker="EV-NUMERIC-HORIZON-MISMATCH",
                    strike_type="less",
                    cap_strike=3317.5,
                    close_time="2025-12-31T23:00:00Z",
                ),
                MarketRecord(
                    ticker="MKT-2030",
                    event_ticker="EV-NUMERIC-HORIZON-MISMATCH",
                    strike_type="less",
                    cap_strike=4909.9,
                    close_time="2030-12-31T23:00:00Z",
                ),
            ],
        )

        classified = classify_event(event)

        self.assertEqual(classified.topology_kind, TopologyKind.UNORDERED_GROUP)

    def test_numeric_thresholds_with_same_close_time_form_monotonic_chain(self) -> None:
        event = EventRecord(
            event_ticker="EV-NUMERIC-SAME-HORIZON",
            markets=[
                MarketRecord(
                    ticker="MKT-5",
                    event_ticker="EV-NUMERIC-SAME-HORIZON",
                    strike_type="greater",
                    floor_strike=5,
                    close_time="2030-01-01T00:00:00Z",
                ),
                MarketRecord(
                    ticker="MKT-6",
                    event_ticker="EV-NUMERIC-SAME-HORIZON",
                    strike_type="greater",
                    floor_strike=6,
                    close_time="2030-01-01T00:00:00Z",
                ),
            ],
        )

        classified = classify_event(event)

        self.assertEqual(classified.topology_kind, TopologyKind.MONOTONIC_CHAIN)
        self.assertEqual(
            [market.market.ticker for market in classified.markets],
            ["MKT-5", "MKT-6"],
        )

    def test_before_close_time_ladder_reverses_into_easiest_to_hardest_order(self) -> None:
        event = EventRecord(
            event_ticker="EV-DATE",
            markets=[
                MarketRecord(
                    ticker="MKT-2027",
                    event_ticker="EV-DATE",
                    yes_sub_title="Before 2027",
                    close_time="2027-01-01T00:00:00Z",
                ),
                MarketRecord(
                    ticker="MKT-2028",
                    event_ticker="EV-DATE",
                    yes_sub_title="Before 2028",
                    close_time="2028-01-01T00:00:00Z",
                ),
                MarketRecord(
                    ticker="MKT-2029",
                    event_ticker="EV-DATE",
                    yes_sub_title="Before 2029",
                    close_time="2029-01-01T00:00:00Z",
                ),
            ],
        )

        classified = classify_event(event)

        self.assertEqual(classified.topology_kind, TopologyKind.MONOTONIC_CHAIN)
        self.assertEqual(
            [market.market.ticker for market in classified.markets],
            ["MKT-2029", "MKT-2028", "MKT-2027"],
        )
        self.assertTrue(classified.markets[0].strike_key < classified.markets[1].strike_key)

    def test_after_close_time_ladder_keeps_earliest_to_latest_order(self) -> None:
        event = EventRecord(
            event_ticker="EV-AFTER-DATE",
            title="Will the album release after these dates?",
            markets=[
                MarketRecord(
                    ticker="MKT-2026",
                    event_ticker="EV-AFTER-DATE",
                    yes_sub_title="After 2026",
                    close_time="2026-01-01T00:00:00Z",
                ),
                MarketRecord(
                    ticker="MKT-2027",
                    event_ticker="EV-AFTER-DATE",
                    yes_sub_title="After 2027",
                    close_time="2027-01-01T00:00:00Z",
                ),
                MarketRecord(
                    ticker="MKT-2028",
                    event_ticker="EV-AFTER-DATE",
                    yes_sub_title="After 2028",
                    close_time="2028-01-01T00:00:00Z",
                ),
            ],
        )

        classified = classify_event(event)

        self.assertEqual(classified.topology_kind, TopologyKind.MONOTONIC_CHAIN)
        self.assertEqual(
            [market.market.ticker for market in classified.markets],
            ["MKT-2026", "MKT-2027", "MKT-2028"],
        )

    def test_close_time_ladder_with_same_custom_entity_forms_monotonic_chain(self) -> None:
        event = EventRecord(
            event_ticker="EV-CUSTOM-DATE",
            title="Will Airtable IPO before these dates?",
            markets=[
                MarketRecord(
                    ticker="MKT-Q2",
                    event_ticker="EV-CUSTOM-DATE",
                    strike_type="custom",
                    custom_strike={"Company": "Airtable"},
                    yes_sub_title="Before Q2 closes",
                    close_time="2026-07-01T03:59:00Z",
                ),
                MarketRecord(
                    ticker="MKT-Q3",
                    event_ticker="EV-CUSTOM-DATE",
                    strike_type="custom",
                    custom_strike={"Company": "Airtable"},
                    yes_sub_title="Before Q3 closes",
                    close_time="2026-10-01T03:59:00Z",
                ),
            ],
        )

        classified = classify_event(event)

        self.assertEqual(classified.topology_kind, TopologyKind.MONOTONIC_CHAIN)
        self.assertEqual(
            [market.market.ticker for market in classified.markets],
            ["MKT-Q3", "MKT-Q2"],
        )

    def test_close_time_ladder_with_same_numeric_threshold_forms_monotonic_chain(self) -> None:
        event = EventRecord(
            event_ticker="EV-NUMERIC-DATE",
            title="When will this hit 100?",
            markets=[
                MarketRecord(
                    ticker="MKT-2027",
                    event_ticker="EV-NUMERIC-DATE",
                    strike_type="greater_or_equal",
                    floor_strike=100,
                    yes_sub_title="Before 2027",
                    close_time="2027-01-01T00:00:00Z",
                ),
                MarketRecord(
                    ticker="MKT-2028",
                    event_ticker="EV-NUMERIC-DATE",
                    strike_type="greater_or_equal",
                    floor_strike=100,
                    yes_sub_title="Before 2028",
                    close_time="2028-01-01T00:00:00Z",
                ),
            ],
        )

        classified = classify_event(event)

        self.assertEqual(classified.topology_kind, TopologyKind.MONOTONIC_CHAIN)
        self.assertEqual(
            [market.market.ticker for market in classified.markets],
            ["MKT-2028", "MKT-2027"],
        )

    def test_close_time_ladder_with_different_numeric_thresholds_stays_unordered(self) -> None:
        event = EventRecord(
            event_ticker="EV-NUMERIC-DATE-MIXED",
            title="When will this hit these levels?",
            markets=[
                MarketRecord(
                    ticker="MKT-100-2027",
                    event_ticker="EV-NUMERIC-DATE-MIXED",
                    strike_type="greater_or_equal",
                    floor_strike=100,
                    yes_sub_title="Before 2027",
                    close_time="2027-01-01T00:00:00Z",
                ),
                MarketRecord(
                    ticker="MKT-200-2028",
                    event_ticker="EV-NUMERIC-DATE-MIXED",
                    strike_type="greater_or_equal",
                    floor_strike=200,
                    yes_sub_title="Before 2028",
                    close_time="2028-01-01T00:00:00Z",
                ),
            ],
        )

        classified = classify_event(event)

        self.assertEqual(classified.topology_kind, TopologyKind.UNORDERED_GROUP)

    def test_close_time_ladder_without_direction_hint_stays_unordered(self) -> None:
        event = EventRecord(
            event_ticker="EV-AMBIGUOUS-DATE",
            markets=[
                MarketRecord(
                    ticker="MKT-Q2",
                    event_ticker="EV-AMBIGUOUS-DATE",
                    close_time="2026-07-01T03:59:00Z",
                ),
                MarketRecord(
                    ticker="MKT-Q3",
                    event_ticker="EV-AMBIGUOUS-DATE",
                    close_time="2026-10-01T03:59:00Z",
                ),
            ],
        )

        classified = classify_event(event)

        self.assertEqual(classified.topology_kind, TopologyKind.UNORDERED_GROUP)

    def test_same_close_time_distinct_custom_entities_becomes_mutex(self) -> None:
        event = EventRecord(
            event_ticker="EV-CUSTOM-MUTEX",
            markets=[
                MarketRecord(
                    ticker="MKT-RAMP",
                    event_ticker="EV-CUSTOM-MUTEX",
                    strike_type="custom",
                    custom_strike={"Company": "Ramp"},
                    close_time="2040-01-01T04:59:00Z",
                ),
                MarketRecord(
                    ticker="MKT-BREX",
                    event_ticker="EV-CUSTOM-MUTEX",
                    strike_type="custom",
                    custom_strike={"Company": "Brex"},
                    close_time="2040-01-01T04:59:00Z",
                ),
            ],
        )

        classified = classify_event(event)

        self.assertEqual(classified.topology_kind, TopologyKind.MUTUALLY_EXCLUSIVE)
        self.assertEqual([market.strike_key for market in classified.markets], [0, 0])

    def test_same_close_time_without_structured_entity_stays_unordered(self) -> None:
        event = EventRecord(
            event_ticker="EV-PARTIAL",
            markets=[
                MarketRecord(
                    ticker="MKT-USA",
                    event_ticker="EV-PARTIAL",
                    yes_sub_title="United States",
                    close_time="2031-01-01T15:00:00Z",
                ),
                MarketRecord(
                    ticker="MKT-PRC",
                    event_ticker="EV-PARTIAL",
                    yes_sub_title="China",
                    close_time="2031-01-01T15:00:00Z",
                ),
            ],
        )

        classified = classify_event(event)

        self.assertEqual(classified.topology_kind, TopologyKind.UNORDERED_GROUP)

    def test_same_close_time_same_custom_entity_stays_unordered(self) -> None:
        event = EventRecord(
            event_ticker="EV-OUTLIER",
            markets=[
                MarketRecord(
                    ticker="MKT-2027",
                    event_ticker="EV-OUTLIER",
                    strike_type="custom",
                    custom_strike={"Person": "Max Verstappen"},
                    close_time="2030-04-01T03:59:00Z",
                ),
                MarketRecord(
                    ticker="MKT-2028",
                    event_ticker="EV-OUTLIER",
                    strike_type="custom",
                    custom_strike={"Person": "Max Verstappen"},
                    close_time="2030-04-01T03:59:00Z",
                ),
            ],
        )

        classified = classify_event(event)

        self.assertEqual(classified.topology_kind, TopologyKind.UNORDERED_GROUP)

    def test_mixed_numeric_entities_fail_closed_as_unordered(self) -> None:
        event = EventRecord(
            event_ticker="KXNBA1HSPREAD-26APR26BOSPHI",
            markets=[
                MarketRecord(
                    ticker="KXNBA1HSPREAD-26APR26BOSPHI-PHI1",
                    event_ticker="KXNBA1HSPREAD-26APR26BOSPHI",
                    strike_type="greater",
                    floor_strike=1.5,
                ),
                MarketRecord(
                    ticker="KXNBA1HSPREAD-26APR26BOSPHI-BOS2",
                    event_ticker="KXNBA1HSPREAD-26APR26BOSPHI",
                    strike_type="greater",
                    floor_strike=2.5,
                ),
                MarketRecord(
                    ticker="KXNBA1HSPREAD-26APR26BOSPHI-PHI4",
                    event_ticker="KXNBA1HSPREAD-26APR26BOSPHI",
                    strike_type="greater",
                    floor_strike=4.5,
                ),
                MarketRecord(
                    ticker="KXNBA1HSPREAD-26APR26BOSPHI-BOS5",
                    event_ticker="KXNBA1HSPREAD-26APR26BOSPHI",
                    strike_type="greater",
                    floor_strike=5.5,
                ),
            ],
        )

        classified = classify_event(event)

        self.assertEqual(classified.topology_kind, TopologyKind.UNORDERED_GROUP)

    def test_non_monotonic_event_falls_back_to_unordered_group(self) -> None:
        event = EventRecord(
            event_ticker="EV-UNORDERED",
            markets=[
                MarketRecord(
                    ticker="MKT-BETWEEN",
                    event_ticker="EV-UNORDERED",
                    strike_type="between",
                    floor_strike=1,
                    cap_strike=2,
                    close_time="2028-01-01T00:00:00Z",
                ),
                MarketRecord(
                    ticker="MKT-STRUCTURED",
                    event_ticker="EV-UNORDERED",
                    strike_type="structured",
                    custom_strike={"team": "abc"},
                    close_time="2028-01-01T00:00:00Z",
                ),
            ],
        )

        classified = classify_event(event)

        self.assertEqual(classified.topology_kind, TopologyKind.UNORDERED_GROUP)


class ConfigTests(unittest.TestCase):
    def test_market_record_from_api_preserves_price_level_structure(self) -> None:
        market = MarketRecord.from_api(
            {
                "ticker": "MKT-PRICE",
                "event_ticker": "EV-PRICE",
                "price_level_structure": "tapered_deci_cent",
            }
        )

        self.assertEqual(market.price_level_structure, "tapered_deci_cent")

    def test_config_builder_assigns_stable_per_event_affinity(self) -> None:
        events = [
            EventRecord(
                event_ticker="EV-CFG-1",
                markets=[
                    MarketRecord(
                        ticker="MKT-1A",
                        event_ticker="EV-CFG-1",
                        strike_type="custom",
                        custom_strike={"Company": "Ramp"},
                        close_time="2040-01-01T04:59:00Z",
                    ),
                    MarketRecord(
                        ticker="MKT-1B",
                        event_ticker="EV-CFG-1",
                        strike_type="custom",
                        custom_strike={"Company": "Brex"},
                        close_time="2040-01-01T04:59:00Z",
                    ),
                ],
            ),
            EventRecord(
                event_ticker="EV-CFG-2",
                title="Will this happen after these dates?",
                markets=[
                    MarketRecord(
                        ticker="MKT-2A",
                        event_ticker="EV-CFG-2",
                        yes_sub_title="After 2027",
                        close_time="2027-01-01T00:00:00Z",
                    ),
                    MarketRecord(
                        ticker="MKT-2B",
                        event_ticker="EV-CFG-2",
                        yes_sub_title="After 2028",
                        close_time="2028-01-01T00:00:00Z",
                    ),
                ],
            ),
        ]

        config = build_trader_config(events)

        self.assertEqual(config["kalshi"]["market_tickers"], ["MKT-1A", "MKT-1B", "MKT-2A", "MKT-2B"])
        self.assertEqual(
            config["oms_transport"],
            {
                "enabled": False,
                "rest_endpoint": "https://api.elections.kalshi.com",
                "private_ws_endpoint": "wss://api.elections.kalshi.com/trade-api/ws/v2",
                "private_ws_channels": ["user_orders"],
                "max_session_loss_ticks": 5000,
                "available_capital_ticks": 10000,
                "rest_worker_count": 8,
            },
        )
        self.assertEqual(
            config["pipeline"],
            {
                "frame_pool_capacity": 8192,
                "shard_count": 4,
                "io_to_router_capacity": 8192,
                "router_to_logger_capacity": 8192,
                "shard_input_capacity": 8192,
                "shard_to_logger_capacity": 8192,
            },
        )
        self.assertEqual(
            config["local_risk"],
            {
                "max_net_position_lots_per_market": 200,
                "min_seconds_to_close": 300,
                "trading_enabled": False,
            },
        )
        self.assertEqual(len(config["market_routes"]), 4)
        first_event_routes = [
            route
            for route in config["market_routes"]
            if route["event_id"] == config["market_routes"][0]["event_id"]
        ]
        second_event_routes = [
            route
            for route in config["market_routes"]
            if route["event_id"] == config["market_routes"][2]["event_id"]
        ]
        self.assertEqual(len({route["affinity_key"] for route in first_event_routes}), 1)
        self.assertEqual(config["market_routes"][0]["topology_kind"], "mutually_exclusive")
        self.assertEqual(config["market_routes"][2]["topology_kind"], "monotonic_chain")
        self.assertTrue(second_event_routes[0]["strike_key"] < second_event_routes[1]["strike_key"])

    def test_config_builder_supports_topology_filters_and_report(self) -> None:
        events = [
            EventRecord(
                event_ticker="EV-FILTER-MUTEX",
                markets=[
                    MarketRecord(
                        ticker="MKT-FILTER-A",
                        event_ticker="EV-FILTER-MUTEX",
                        strike_type="custom",
                        custom_strike={"Company": "Ramp"},
                        close_time="2040-01-01T04:59:00Z",
                    ),
                    MarketRecord(
                        ticker="MKT-FILTER-B",
                        event_ticker="EV-FILTER-MUTEX",
                        strike_type="custom",
                        custom_strike={"Company": "Brex"},
                        close_time="2040-01-01T04:59:00Z",
                    ),
                ],
            ),
            EventRecord(
                event_ticker="EV-FILTER-CHAIN",
                title="Will this happen after these dates?",
                markets=[
                    MarketRecord(
                        ticker="MKT-FILTER-C1",
                        event_ticker="EV-FILTER-CHAIN",
                        yes_sub_title="After 2027",
                        close_time="2027-01-01T00:00:00Z",
                    ),
                    MarketRecord(
                        ticker="MKT-FILTER-C2",
                        event_ticker="EV-FILTER-CHAIN",
                        yes_sub_title="After 2028",
                        close_time="2028-01-01T00:00:00Z",
                    ),
                ],
            ),
        ]

        result = build_trader_config_result(
            events,
            include_topologies=[TopologyKind.MONOTONIC_CHAIN],
        )

        self.assertEqual(result.topology_counts, {"monotonic_chain": 1})
        self.assertEqual(len(result.included_events), 1)
        self.assertEqual(result.included_events[0].event_ticker, "EV-FILTER-CHAIN")
        self.assertEqual(len(result.skipped_events), 1)
        self.assertEqual(result.skipped_events[0].event_ticker, "EV-FILTER-MUTEX")
        self.assertEqual(
            result.report()["included_events"][0]["topology_kind"],
            "monotonic_chain",
        )
        self.assertEqual(
            result.config["kalshi"]["market_tickers"],
            ["MKT-FILTER-C1", "MKT-FILTER-C2"],
        )

    def test_config_builder_splits_bosphi_1h_spread_into_two_synthetic_events(self) -> None:
        event = EventRecord(
            event_ticker="KXNBA1HSPREAD-26APR26BOSPHI",
            series_ticker="KXNBA1HSPREAD",
            markets=[
                MarketRecord(
                    ticker="KXNBA1HSPREAD-26APR26BOSPHI-PHI1",
                    event_ticker="KXNBA1HSPREAD-26APR26BOSPHI",
                    strike_type="greater",
                    floor_strike=1.5,
                ),
                MarketRecord(
                    ticker="KXNBA1HSPREAD-26APR26BOSPHI-BOS2",
                    event_ticker="KXNBA1HSPREAD-26APR26BOSPHI",
                    strike_type="greater",
                    floor_strike=2.5,
                ),
                MarketRecord(
                    ticker="KXNBA1HSPREAD-26APR26BOSPHI-PHI4",
                    event_ticker="KXNBA1HSPREAD-26APR26BOSPHI",
                    strike_type="greater",
                    floor_strike=4.5,
                ),
                MarketRecord(
                    ticker="KXNBA1HSPREAD-26APR26BOSPHI-BOS5",
                    event_ticker="KXNBA1HSPREAD-26APR26BOSPHI",
                    strike_type="greater",
                    floor_strike=5.5,
                ),
                MarketRecord(
                    ticker="KXNBA1HSPREAD-26APR26BOSPHI-PHI7",
                    event_ticker="KXNBA1HSPREAD-26APR26BOSPHI",
                    strike_type="greater",
                    floor_strike=7.5,
                ),
                MarketRecord(
                    ticker="KXNBA1HSPREAD-26APR26BOSPHI-BOS8",
                    event_ticker="KXNBA1HSPREAD-26APR26BOSPHI",
                    strike_type="greater",
                    floor_strike=8.5,
                ),
                MarketRecord(
                    ticker="KXNBA1HSPREAD-26APR26BOSPHI-PHI10",
                    event_ticker="KXNBA1HSPREAD-26APR26BOSPHI",
                    strike_type="greater",
                    floor_strike=10.5,
                ),
                MarketRecord(
                    ticker="KXNBA1HSPREAD-26APR26BOSPHI-BOS11",
                    event_ticker="KXNBA1HSPREAD-26APR26BOSPHI",
                    strike_type="greater",
                    floor_strike=11.5,
                ),
                MarketRecord(
                    ticker="KXNBA1HSPREAD-26APR26BOSPHI-PHI13",
                    event_ticker="KXNBA1HSPREAD-26APR26BOSPHI",
                    strike_type="greater",
                    floor_strike=13.5,
                ),
                MarketRecord(
                    ticker="KXNBA1HSPREAD-26APR26BOSPHI-PHI16",
                    event_ticker="KXNBA1HSPREAD-26APR26BOSPHI",
                    strike_type="greater",
                    floor_strike=16.5,
                ),
                MarketRecord(
                    ticker="KXNBA1HSPREAD-26APR26BOSPHI-PHI19",
                    event_ticker="KXNBA1HSPREAD-26APR26BOSPHI",
                    strike_type="greater",
                    floor_strike=19.5,
                ),
            ],
        )

        result = build_trader_config_result([event])

        self.assertEqual(len(result.included_events), 2)
        self.assertEqual(len({included.event_id for included in result.included_events}), 2)
        self.assertEqual(len({included.affinity_key for included in result.included_events}), 1)

        routes_by_event_id: dict[int, list[dict[str, object]]] = {}
        for route in result.config["market_routes"]:
            routes_by_event_id.setdefault(route["event_id"], []).append(route)

        self.assertEqual(len(routes_by_event_id), 2)
        grouped_tickers = {
            tuple(route["market_ticker"] for route in sorted(routes, key=lambda item: item["strike_key"]))
            for routes in routes_by_event_id.values()
        }
        self.assertEqual(
            grouped_tickers,
            {
                (
                    "KXNBA1HSPREAD-26APR26BOSPHI-BOS2",
                    "KXNBA1HSPREAD-26APR26BOSPHI-BOS5",
                    "KXNBA1HSPREAD-26APR26BOSPHI-BOS8",
                    "KXNBA1HSPREAD-26APR26BOSPHI-BOS11",
                ),
                (
                    "KXNBA1HSPREAD-26APR26BOSPHI-PHI1",
                    "KXNBA1HSPREAD-26APR26BOSPHI-PHI4",
                    "KXNBA1HSPREAD-26APR26BOSPHI-PHI7",
                    "KXNBA1HSPREAD-26APR26BOSPHI-PHI10",
                    "KXNBA1HSPREAD-26APR26BOSPHI-PHI13",
                    "KXNBA1HSPREAD-26APR26BOSPHI-PHI16",
                    "KXNBA1HSPREAD-26APR26BOSPHI-PHI19",
                ),
            },
        )

    def test_config_builder_keeps_valid_numeric_subchain_and_drops_singleton_leftover(self) -> None:
        event = EventRecord(
            event_ticker="KXWNBASPREAD-26MAY09DALIND",
            series_ticker="KXWNBASPREAD",
            markets=[
                MarketRecord(
                    ticker="KXWNBASPREAD-26MAY09DALIND-DAL4",
                    event_ticker="KXWNBASPREAD-26MAY09DALIND",
                    strike_type="greater",
                    floor_strike=3.5,
                ),
                MarketRecord(
                    ticker="KXWNBASPREAD-26MAY09DALIND-IND6",
                    event_ticker="KXWNBASPREAD-26MAY09DALIND",
                    strike_type="greater",
                    floor_strike=5.5,
                ),
                MarketRecord(
                    ticker="KXWNBASPREAD-26MAY09DALIND-IND11",
                    event_ticker="KXWNBASPREAD-26MAY09DALIND",
                    strike_type="greater",
                    floor_strike=10.5,
                ),
            ],
        )

        result = build_trader_config_result([event])

        self.assertEqual(len(result.included_events), 1)
        self.assertEqual(
            result.report()["included_events"][0]["topology_kind"],
            "monotonic_chain",
        )
        self.assertEqual(
            result.config["kalshi"]["market_tickers"],
            [
                "KXWNBASPREAD-26MAY09DALIND-IND6",
                "KXWNBASPREAD-26MAY09DALIND-IND11",
            ],
        )

    def test_config_builder_raises_when_filters_remove_everything(self) -> None:
        events = [
            EventRecord(
                event_ticker="EV-FILTER-EMPTY",
                markets=[MarketRecord(ticker="MKT-ONLY", event_ticker="EV-FILTER-EMPTY")],
            )
        ]

        with self.assertRaisesRegex(ValueError, "no events remained"):
            build_trader_config(
                events,
                include_topologies=[TopologyKind.MONOTONIC_CHAIN],
            )

    def test_config_builder_market_limit_skips_whole_events(self) -> None:
        events = [
            EventRecord(
                event_ticker="EV-LIMIT-1",
                title="Will this happen after these dates?",
                markets=[
                    MarketRecord(
                        ticker="MKT-LIMIT-1A",
                        event_ticker="EV-LIMIT-1",
                        yes_sub_title="After 2027",
                        close_time="2027-01-01T00:00:00Z",
                    ),
                    MarketRecord(
                        ticker="MKT-LIMIT-1B",
                        event_ticker="EV-LIMIT-1",
                        yes_sub_title="After 2028",
                        close_time="2028-01-01T00:00:00Z",
                    ),
                ],
            ),
            EventRecord(
                event_ticker="EV-LIMIT-2",
                markets=[
                    MarketRecord(
                        ticker="MKT-LIMIT-2A",
                        event_ticker="EV-LIMIT-2",
                        strike_type="custom",
                        custom_strike={"Company": "Ramp"},
                        close_time="2040-01-01T04:59:00Z",
                    ),
                    MarketRecord(
                        ticker="MKT-LIMIT-2B",
                        event_ticker="EV-LIMIT-2",
                        strike_type="custom",
                        custom_strike={"Company": "Brex"},
                        close_time="2040-01-01T04:59:00Z",
                    ),
                ],
            ),
        ]

        result = build_trader_config_result(events, market_limit=2)

        self.assertEqual(len(result.included_events), 1)
        self.assertEqual(result.included_events[0].event_ticker, "EV-LIMIT-1")
        self.assertEqual(result.report()["included_market_count"], 2)
        self.assertEqual(len(result.skipped_events), 1)
        self.assertEqual(result.skipped_events[0].event_ticker, "EV-LIMIT-2")
        self.assertIn("market_limit", result.skipped_events[0].reason)

    def test_config_builder_uses_custom_pipeline_settings(self) -> None:
        events = [
            EventRecord(
                event_ticker="EV-PIPELINE",
                title="Will this happen after these dates?",
                markets=[
                    MarketRecord(
                        ticker="MKT-PIPELINE-A",
                        event_ticker="EV-PIPELINE",
                        yes_sub_title="After 2027",
                        close_time="2027-01-01T00:00:00Z",
                    ),
                    MarketRecord(
                        ticker="MKT-PIPELINE-B",
                        event_ticker="EV-PIPELINE",
                        yes_sub_title="After 2028",
                        close_time="2028-01-01T00:00:00Z",
                    ),
                ],
            )
        ]

        config = build_trader_config(
            events,
            pipeline=PipelineSettings(
                shard_count=8,
                frame_pool_capacity=16000,
                io_to_router_capacity=8000,
                router_to_logger_capacity=7000,
                shard_input_capacity=2000,
                shard_to_logger_capacity=1500,
            ),
        )

        self.assertEqual(
            config["pipeline"],
            {
                "frame_pool_capacity": 16000,
                "shard_count": 8,
                "io_to_router_capacity": 8000,
                "router_to_logger_capacity": 7000,
                "shard_input_capacity": 2000,
                "shard_to_logger_capacity": 1500,
            },
        )

    def test_app_config_builder_emits_cpp_runtime_shape(self) -> None:
        events = [
            EventRecord(
                event_ticker="EV-APP-1",
                markets=[
                    MarketRecord(
                        ticker="MKT-APP-1A",
                        event_ticker="EV-APP-1",
                        strike_type="custom",
                        custom_strike={"Company": "Ramp"},
                        close_time="2040-01-01T04:59:00Z",
                        status="active",
                        price_level_structure="linear_cent",
                    ),
                    MarketRecord(
                        ticker="MKT-APP-1B",
                        event_ticker="EV-APP-1",
                        strike_type="custom",
                        custom_strike={"Company": "Brex"},
                        close_time="2040-01-01T04:59:00Z",
                        price_level_structure="tapered_deci_cent",
                    ),
                ],
            )
        ]

        result = build_app_config_result(
            events,
            runtime=RuntimeSettings(
                shard_count=2,
                shard_queue_capacity=1024,
                router_queue_capacity=2048,
                frame_pool_capacity=4096,
                operator_queue_capacity=32,
                operator_socket_path="/tmp/predex-test.sock",
            ),
            kalshi=KalshiSettings(
                market_data=KalshiMarketDataSettings(
                    enable_market_data=True,
                    channels=("orderbook_delta", "trade", "market_lifecycle_v2"),
                )
            ),
        )

        config = result.config

        self.assertEqual(
            config["runtime"],
            {
                "shard_count": 2,
                "shard_queue_capacity": 1024,
                "router_queue_capacity": 2048,
                "frame_pool_capacity": 4096,
                "operator_queue_capacity": 32,
                "operator_socket_path": "/tmp/predex-test.sock",
                "market_data_tape_path": "logs/live/predex_tape.bin",
                "thread_polling": {
                    "profile": "harvest",
                    "spin_iterations": 64,
                    "yield_iterations": 64,
                    "min_sleep_us": 50,
                    "max_sleep_us": 1000,
                },
                "synthetic_trading_session_enabled": False,
                "reduce_only_after_seconds": 0,
                "flatten_to_zero_after_seconds": 0,
                "stopped_after_seconds": 0,
            },
        )
        self.assertEqual(
            config["kalshi"]["market_data"],
            {
                "enable_market_data": True,
                "channels": ["orderbook_delta", "trade", "market_lifecycle_v2"],
            },
        )
        self.assertNotIn("market_routes", config)
        self.assertEqual(len(config["universe"]["events"]), 1)

        event_config = config["universe"]["events"][0]
        self.assertEqual(event_config["topology"], "mutually_exclusive")
        self.assertIsInstance(event_config["event_id"], str)
        self.assertIsInstance(event_config["affinity_key"], str)
        self.assertTrue(event_config["markets"][0]["market_id"].isdigit())
        self.assertTrue(event_config["markets"][1]["market_id"].isdigit())
        self.assertEqual(
            [
                {
                    key: market_config[key]
                    for key in ("kalshi_ticker", "tradeable", "price_level_structure")
                }
                for market_config in event_config["markets"]
            ],
            [
                {
                    "kalshi_ticker": "MKT-APP-1A",
                    "tradeable": True,
                    "price_level_structure": "linear_cent",
                },
                {
                    "kalshi_ticker": "MKT-APP-1B",
                    "tradeable": False,
                    "price_level_structure": "tapered_deci_cent",
                },
            ],
        )
        self.assertEqual(result.topology_counts, {"mutually_exclusive": 1})
        self.assertEqual(result.report()["included_market_count"], 2)

    def test_app_config_builder_emits_custom_thread_polling(self) -> None:
        runtime = RuntimeSettings(
            thread_polling=ThreadPollingSettings(
                profile="low_latency",
                spin_iterations=0,
                yield_iterations=0,
                min_sleep_us=1,
                max_sleep_us=1,
            )
        )

        self.assertEqual(
            runtime.to_dict()["thread_polling"],
            {
                "profile": "low_latency",
                "spin_iterations": 0,
                "yield_iterations": 0,
                "min_sleep_us": 1,
                "max_sleep_us": 1,
            },
        )

    def test_app_config_builder_resolves_32_bit_market_id_collisions(self) -> None:
        events = [
            EventRecord(
                event_ticker="EV-COLLISION-1",
                markets=[MarketRecord(ticker="KXFED-26OCT-T4.00", event_ticker="EV-COLLISION-1")],
            ),
            EventRecord(
                event_ticker="EV-COLLISION-2",
                markets=[MarketRecord(ticker="KXRIVN-26AUGDELIV-9000", event_ticker="EV-COLLISION-2")],
            ),
        ]

        result = build_app_config_result(events)
        market_ids = [
            market["market_id"]
            for event in result.config["universe"]["events"]
            for market in event["markets"]
        ]

        self.assertEqual(len(market_ids), 2)
        self.assertEqual(len(set(market_ids)), 2)


class KalshiClientTests(unittest.TestCase):
    @staticmethod
    def _http_error(code: int, headers: dict[str, str] | None = None) -> HTTPError:
        message = Message()
        for key, value in (headers or {}).items():
            message[key] = value
        return HTTPError(
            "https://example.com",
            code,
            "HTTP error",
            message,
            None,
        )

    def test_list_event_tickers_supports_all_events_pagination(self) -> None:
        class FakeClient(KalshiPublicClient):
            def __init__(self) -> None:
                super().__init__()
                self.calls: list[dict[str, object]] = []

            def _get_json(self, path: str, params: dict[str, object] | None = None) -> dict[str, object]:
                self.calls.append({"path": path, "params": dict(params or {})})
                cursor = (params or {}).get("cursor")
                if cursor is None:
                    return {
                        "events": [
                            {"event_ticker": "EV-1"},
                            {"event_ticker": "EV-2"},
                        ],
                        "cursor": "page-2",
                    }
                return {
                    "events": [
                        {"event_ticker": "EV-3"},
                    ],
                    "cursor": "",
                }

        client = FakeClient()
        tickers = client.list_event_tickers(limit=None)

        self.assertEqual(tickers, ["EV-1", "EV-2", "EV-3"])
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(client.calls[0]["params"]["limit"], 200)
        self.assertEqual(client.calls[1]["params"]["cursor"], "page-2")

    def test_get_json_retries_429_with_retry_after(self) -> None:
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

            def read(self) -> bytes:
                return b'{"ok": true}'

        class FakeClient(KalshiPublicClient):
            def __init__(self) -> None:
                super().__init__(max_retries=3)
                self.sleeps: list[float] = []
                self.attempts = 0

            def _sleep(self, seconds: float) -> None:
                self.sleeps.append(seconds)

            def _urlopen(self, request):  # type: ignore[override]
                self.attempts += 1
                if self.attempts < 3:
                    raise KalshiClientTests._http_error(429, {"Retry-After": "2"})
                return FakeResponse()

        client = FakeClient()
        payload = client._get_json("/events", {"limit": 1})

        self.assertEqual(payload, {"ok": True})
        self.assertEqual(client.sleeps, [2.0, 2.0])

    def test_retry_delay_falls_back_to_exponential_backoff(self) -> None:
        client = KalshiPublicClient(
            initial_backoff_seconds=0.5,
            max_backoff_seconds=3.0,
        )
        error = self._http_error(429)

        self.assertEqual(client._retry_delay(0, error), 0.5)
        self.assertEqual(client._retry_delay(1, error), 1.0)
        self.assertEqual(client._retry_delay(2, error), 2.0)
        self.assertEqual(client._retry_delay(3, error), 3.0)

    def test_parallel_discover_events_preserves_input_order(self) -> None:
        class FakeClient(KalshiPublicClient):
            def __init__(self) -> None:
                super().__init__(event_fetch_workers=3)

            def get_event(self, event_ticker: str) -> EventRecord:
                delays = {"EV-1": 0.03, "EV-2": 0.02, "EV-3": 0.01}
                time.sleep(delays[event_ticker])
                return EventRecord(
                    event_ticker=event_ticker,
                    markets=[MarketRecord(ticker=f"{event_ticker}-MKT", event_ticker=event_ticker)],
                )

        client = FakeClient()
        events = client.discover_events(event_tickers=["EV-1", "EV-2", "EV-3"])

        self.assertEqual([event.event_ticker for event in events], ["EV-1", "EV-2", "EV-3"])

    def test_parallel_discover_events_skips_failed_events(self) -> None:
        class FakeClient(KalshiPublicClient):
            def __init__(self) -> None:
                super().__init__(event_fetch_workers=2)

            def get_event(self, event_ticker: str) -> EventRecord:
                if event_ticker == "EV-BAD":
                    raise ValueError("bad event")
                return EventRecord(
                    event_ticker=event_ticker,
                    markets=[MarketRecord(ticker=f"{event_ticker}-MKT", event_ticker=event_ticker)],
                )

        client = FakeClient()
        events = client.discover_events(event_tickers=["EV-1", "EV-BAD", "EV-2"])

        self.assertEqual([event.event_ticker for event in events], ["EV-1", "EV-2"])


if __name__ == "__main__":
    unittest.main()
