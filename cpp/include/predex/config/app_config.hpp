#pragma once 

#include "predex/exchange/kalshi/kalshi_ws_protocol.hpp"
#include "predex/strategy/monotonic_arb.hpp"
#include "predex/utils/idle_backoff.hpp"
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <optional>

namespace predex::config{

    constexpr std::size_t kDefaultShardCount = 4;
    constexpr std::size_t kDefaultShardQueueCapacity = 32768;
    constexpr std::size_t kDefaultRouterQueueCapacity = 32768;
    constexpr std::size_t kDefaultFramePoolCapacity = 65536;
    constexpr std::size_t kDefaultOperatorQueueCapacity = 64;
    constexpr std::string_view kDefaultOperatorSocketPath = "/tmp/predex_operator.sock";
    constexpr std::string_view kDefaultMarketDataTapePath = "logs/live/predex_tape.bin";
    constexpr std::size_t kMaxConcurrentStreams = 10;

    struct RuntimeConfig{
        std::size_t shard_count{kDefaultShardCount};
        std::size_t shard_queue_capacity{kDefaultShardQueueCapacity};
        std::size_t router_queue_capacity{kDefaultRouterQueueCapacity};
        std::size_t frame_pool_capacity{kDefaultFramePoolCapacity};
        std::size_t operator_queue_capacity{kDefaultOperatorQueueCapacity};
        std::string operator_socket_path{kDefaultOperatorSocketPath};
        std::string market_data_tape_path{kDefaultMarketDataTapePath};
        utils::IdleBackoffConfig thread_polling{};
        bool synthetic_trading_session_enabled{false};
        std::uint64_t reduce_only_after_seconds{0};
        std::uint64_t flatten_to_zero_after_seconds{0};
        std::uint64_t stopped_after_seconds{0};
    };

    struct KalshiAuthConfig{
        std::string key_id_env;
        std::string private_key_pem_env;
    };

    struct KalshiMarketDataConfig{
        bool enable_market_data{false};
        std::vector<exchange::kalshi::KalshiMarketDataChannel> channels;
    };

    struct KalshiOrderRestConfig{
        bool enable_order_rest{false};
        std::string endpoint;
        std::size_t max_concurrent_streams{kMaxConcurrentStreams};
    };

    struct KalshiPrivateOrderFeedConfig{
        bool enable_private_order_feed{false};
        std::vector<exchange::kalshi::KalshiOrderDataChannel> channels;
    };

    struct KalshiConfig{
        KalshiAuthConfig auth;
        KalshiMarketDataConfig market_data;
        KalshiOrderRestConfig order_rest;
        KalshiPrivateOrderFeedConfig private_order_feed;
    };

    struct MarketConfig{
        std::string market_id;
        std::string kalshi_ticker;
        bool tradeable{false};
        std::string price_level_structure;
        std::optional<std::int64_t> strike_key;
        std::uint64_t market_time_s{};
        std::uint64_t market_close_time_s{};
        std::uint64_t market_expected_expiration_time_s{};
        std::uint64_t market_expiration_time_s{};
    };

    struct EventConfig{
        std::string event_id;
        std::string affinity_key;
        std::string topology;
        std::vector<MarketConfig> markets;
    };

    struct UniverseConfig{
        std::vector<EventConfig> events;
    };

    struct OmsConfig{
        std::int64_t strategy_allocation_limit_ticks{};
        std::int64_t venue_safety_reserve_ticks{};
        std::uint64_t maximum_group_reservation_ticks{};
        std::uint8_t maximum_group_legs{10};
        std::uint8_t maximum_group_repair_attempts{2};
        std::uint64_t maximum_group_intent_age_ns{};
        std::uint64_t portfolio_reconciliation_interval_ns{
            5'000'000'000
        };
    };

    struct StrategyConfig{
        bool enable_monotonic_arb{false};
        std::uint32_t strategy_id{1};
        std::uint64_t maximum_observation_age_ns{50'000'000};
        strategy::MonotonicArbConfig monotonic_arb{};
    };

    struct AppConfig{
        RuntimeConfig runtime;
        KalshiConfig kalshi;
        OmsConfig oms;
        StrategyConfig strategy;
        UniverseConfig universe;
    };

    [[nodiscard]] AppConfig default_app_config();
    [[nodiscard]] AppConfig load_app_config(std::string_view config_path);

    void validate_app_config(const AppConfig& config);

}
