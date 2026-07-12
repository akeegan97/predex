#pragma once 

#include "predex/exchange/kalshi/kalshi_ws_protocol.hpp"
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace predex::config{

    constexpr std::size_t kDefaultShardCount = 4;
    constexpr std::size_t kDefaultShardQueueCapacity = 8192;
    constexpr std::size_t kDefaultRouterQueueCapacity = 8192;
    constexpr std::size_t kDefaultFramePoolCapacity = 8192;
    constexpr std::size_t kDefaultOperatorQueueCapacity = 64;
    constexpr std::string_view kDefaultOperatorSocketPath = "/tmp/predex_operator.sock";
    constexpr std::string_view kDefaultMarketDataTapePath = "logs/live/predex_tape.bin";

    struct RuntimeConfig{
        std::size_t shard_count{kDefaultShardCount};
        std::size_t shard_queue_capacity{kDefaultShardQueueCapacity};
        std::size_t router_queue_capacity{kDefaultRouterQueueCapacity};
        std::size_t frame_pool_capacity{kDefaultFramePoolCapacity};
        std::size_t operator_queue_capacity{kDefaultOperatorQueueCapacity};
        std::string operator_socket_path{kDefaultOperatorSocketPath};
        std::string market_data_tape_path{kDefaultMarketDataTapePath};
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
        std::size_t max_concurrent_streams{10};
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

    struct AppConfig{
        RuntimeConfig runtime;
        KalshiConfig kalshi;
        UniverseConfig universe;
    };

    [[nodiscard]] AppConfig default_app_config();
    [[nodiscard]] AppConfig load_app_config(std::string_view config_path);

    void validate_app_config(const AppConfig& config);

}
