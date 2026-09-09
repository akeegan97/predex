#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>

#include "predex/config/app_config.hpp"

namespace predex::config{
namespace{

    class TemporaryConfigFile{
        public:
            TemporaryConfigFile()
                : path_(std::filesystem::temp_directory_path() /
                        "predex-thread-polling-config-test.json"){}

            ~TemporaryConfigFile(){
                std::error_code error;
                std::filesystem::remove(path_, error);
            }

            [[nodiscard]] const std::filesystem::path& path() const noexcept{
                return path_;
            }

        private:
            std::filesystem::path path_;
    };

    TEST(AppConfigTests, LegacyConfigDefaultsToLowLatencyPolling){
        const auto config = default_app_config();

        EXPECT_EQ(
            config.runtime.thread_polling.profile,
            utils::ThreadPollingProfile::kLOW_LATENCY);
    }

    TEST(AppConfigTests, DefaultsToValidatedBroadUniverseCapacity){
        const auto config = default_app_config();

        EXPECT_EQ(config.runtime.frame_pool_capacity, 65'536U);
        EXPECT_EQ(config.runtime.router_queue_capacity, 32'768U);
        EXPECT_EQ(config.runtime.shard_queue_capacity, 32'768U);
    }

    TEST(AppConfigTests, LoadsHarvestThreadPollingConfiguration){
        TemporaryConfigFile file;
        {
            std::ofstream output{file.path()};
            ASSERT_TRUE(output.is_open());
            output << R"json({
                "runtime": {
                    "thread_polling": {
                        "profile": "harvest",
                        "spin_iterations": 11,
                        "yield_iterations": 7,
                        "min_sleep_us": 25,
                        "max_sleep_us": 400
                    }
                },
                "universe": {
                    "events": [{
                        "event_id": "1",
                        "affinity_key": "2",
                        "topology": "single_market",
                        "markets": [{
                            "market_id": "3",
                            "kalshi_ticker": "TEST-MARKET",
                            "price_level_structure": "linear_cent"
                        }]
                    }]
                }
            })json";
        }

        const auto config = load_app_config(file.path().string());

        EXPECT_EQ(
            config.runtime.thread_polling.profile,
            utils::ThreadPollingProfile::kHARVEST);
        EXPECT_EQ(config.runtime.thread_polling.spin_iterations, 11U);
        EXPECT_EQ(config.runtime.thread_polling.yield_iterations, 7U);
        EXPECT_EQ(
            config.runtime.thread_polling.min_sleep,
            std::chrono::microseconds{25});
        EXPECT_EQ(
            config.runtime.thread_polling.max_sleep,
            std::chrono::microseconds{400});
    }

    TEST(AppConfigTests, ThrowsWhenMonotonicChainMarketMissingStrikeKey){
        TemporaryConfigFile file;
        {
            std::ofstream output{file.path()};
            ASSERT_TRUE(output.is_open());
            output << R"json({
                "runtime": {
                    "thread_polling": {
                        "profile": "low_latency"
                    }
                },
                "universe": {
                    "events": [{
                        "event_id": "1",
                        "affinity_key": "2",
                        "topology": "monotonic_chain",
                        "markets": [{
                            "market_id": "3",
                            "kalshi_ticker": "TEST-MARKET",
                            "price_level_structure": "linear_cent"
                        }]
                    }]
                }
            })json";
        }

        EXPECT_THROW(
            auto ___ =load_app_config(file.path().string()),
            std::runtime_error);
    }

    TEST(AppConfigTests, ParsesNegativeStrikeKeyForMonotonicChainMarket){
        TemporaryConfigFile file;
        {
            std::ofstream output{file.path()};
            ASSERT_TRUE(output.is_open());
            output << R"json({
                "runtime": {
                    "thread_polling": {
                        "profile": "low_latency"
                    }
                },
                "universe": {
                    "events": [{
                        "event_id": "1",
                        "affinity_key": "2",
                        "topology": "monotonic_chain",
                        "markets": [{
                            "market_id": "3",
                            "kalshi_ticker": "TEST-MARKET",
                            "price_level_structure": "linear_cent",
                            "strike_key": -123456789
                        }]
                    }]
                }
            })json";
        }

        const auto config = load_app_config(file.path().string());

        ASSERT_EQ(config.universe.events.size(), 1U);
        const auto& event = config.universe.events[0];
        ASSERT_EQ(event.markets.size(), 1U);
        const auto& market = event.markets[0];
        EXPECT_TRUE(market.strike_key.has_value());
        EXPECT_EQ(market.strike_key.value(), -123456789);
    }

    TEST(AppConfigTests, LoadsOmsAllocationAndReconciliationPolicy){
        TemporaryConfigFile file;
        {
            std::ofstream output{file.path()};
            ASSERT_TRUE(output.is_open());
            output << R"json({
                "oms": {
                    "strategy_allocation_limit_ticks": 1000000,
                    "venue_safety_reserve_ticks": 25000,
                    "maximum_group_reservation_ticks": 200000,
                    "maximum_group_legs": 6,
                    "maximum_group_repair_attempts": 3,
                    "maximum_group_intent_age_ns": 100000000,
                    "portfolio_reconciliation_interval_ns": 5000000000
                },
                "universe": {
                    "events": [{
                        "event_id": "1",
                        "affinity_key": "2",
                        "topology": "single_market",
                        "markets": [{
                            "market_id": "3",
                            "kalshi_ticker": "TEST-MARKET",
                            "price_level_structure": "linear_cent"
                        }]
                    }]
                }
            })json";
        }

        const auto config = load_app_config(file.path().string());
        EXPECT_EQ(config.oms.strategy_allocation_limit_ticks, 1'000'000);
        EXPECT_EQ(config.oms.venue_safety_reserve_ticks, 25'000);
        EXPECT_EQ(config.oms.maximum_group_reservation_ticks, 200'000U);
        EXPECT_EQ(config.oms.maximum_group_legs, 6U);
        EXPECT_EQ(config.oms.maximum_group_repair_attempts, 3U);
        EXPECT_EQ(config.oms.maximum_group_intent_age_ns, 100'000'000U);
        EXPECT_EQ(
            config.oms.portfolio_reconciliation_interval_ns,
            5'000'000'000U);
    }

    TEST(AppConfigTests, LoadsExplicitMonotonicArbitrageStrategyPolicy){
        TemporaryConfigFile file;
        {
            std::ofstream output{file.path()};
            ASSERT_TRUE(output.is_open());
            output << R"json({
                "kalshi": {
                    "auth": {
                        "key_id_env": "KALSHI_KEY_ID",
                        "private_key_pem_env": "KALSHI_PRIVATE_KEY"
                    },
                    "market_data": {
                        "enable_market_data": true,
                        "channels": ["orderbook_delta"]
                    },
                    "order_rest": {
                        "enable_order_rest": true,
                        "endpoint": "https://example.test",
                        "max_concurrent_streams": 2
                    },
                    "private_order_feed": {
                        "enable_private_order_feed": true,
                        "channels": ["user_orders", "fill", "market_positions"]
                    }
                },
                "oms": {
                    "strategy_allocation_limit_ticks": 100000,
                    "maximum_group_reservation_ticks": 50000,
                    "maximum_group_legs": 2,
                    "maximum_group_intent_age_ns": 100000000
                },
                "strategy": {
                    "enable_monotonic_arb": true,
                    "strategy_id": 17,
                    "maximum_observation_age_ns": 25000000,
                    "monotonic_arb": {
                        "order_quantity_lots": 100,
                        "minimum_net_edge_ticks": 350,
                        "edge_cushion_ticks": 50,
                        "require_top_gap_continuity": false,
                        "require_near_top_multilevel_support": false,
                        "maximum_easier_book_levels": 1,
                        "maximum_harder_book_levels": 1,
                        "taker_fee_rate_numerator": 7,
                        "taker_fee_rate_denominator": 100
                    }
                },
                "universe": {
                    "events": [{
                        "event_id": "1",
                        "affinity_key": "2",
                        "topology": "monotonic_chain",
                        "markets": [{
                            "market_id": "3",
                            "kalshi_ticker": "TEST-EASIER",
                            "tradeable": true,
                            "price_level_structure": "linear_cent",
                            "strike_key": 10
                        }, {
                            "market_id": "4",
                            "kalshi_ticker": "TEST-HARDER",
                            "tradeable": true,
                            "price_level_structure": "linear_cent",
                            "strike_key": 20
                        }]
                    }]
                }
            })json";
        }

        const auto config = load_app_config(file.path().string());
        EXPECT_TRUE(config.strategy.enable_monotonic_arb);
        EXPECT_EQ(config.strategy.strategy_id, 17U);
        EXPECT_EQ(config.strategy.maximum_observation_age_ns, 25'000'000U);
        EXPECT_EQ(config.strategy.monotonic_arb.order_quantity_lots, 100U);
        EXPECT_EQ(config.strategy.monotonic_arb.minimum_net_edge_ticks, 350U);
        EXPECT_EQ(config.strategy.monotonic_arb.edge_cushion_ticks, 50U);
        EXPECT_FALSE(
            config.strategy.monotonic_arb.require_top_gap_continuity);
        EXPECT_EQ(
            config.strategy.monotonic_arb.maximum_easier_book_levels,
            1U);
    }

}
}
