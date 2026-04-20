#pragma once
#include <string>
#include <cstdint>
#include <cstddef>
#include <vector>
#include <memory>

#include "predex/internal/event_topology.hpp"


//might make each a separate constant but for now just using a single global default capacity
namespace predex{
    inline constexpr std::size_t kDefaultCapacity = 4096;
    inline constexpr std::uint32_t kSpinItersRouter = 20000;
    inline constexpr std::uint32_t kSpinItersShard = 20000;
    inline constexpr std::uint32_t kSpinItersOms = 10000;
    inline constexpr std::uint32_t kSpinItersLogger = 200;
    inline constexpr std::uint32_t kSpinItersAudit = 200;
    inline constexpr std::uint32_t kYieldEvery = 64;
    inline constexpr std::uint32_t kSleepAfterIdleIters = 2000;
    inline constexpr std::uint32_t kSleepMicros = 25;

    struct MarketRouteConfig{
        std::string market_ticker;
        std::uint64_t market_id{0};
        std::uint64_t event_id{0};
        std::uint64_t affinity_key{0};
        internal::EventTopologyKind topology_kind{internal::EventTopologyKind::kUnknown};
        std::int64_t strike_key{0};
        std::uint64_t close_time_s{0};
        bool tradeable{false};
    };

    struct KalshiWsConfig{
        std::string endpoint{"wss://api.elections.kalshi.com/trade-api/ws/v2"};
        std::string key_id;
        std::string private_key_pem;
        std::vector<std::string> channels;
        std::vector<std::string> lifecycle_channels;
        std::vector<std::string> market_tickers;
    };

    struct IdlePolicyConfig{
        std::uint32_t spin_iters_router{kSpinItersRouter};
        std::uint32_t spin_iters_shard{kSpinItersShard};
        std::uint32_t spin_iters_oms{kSpinItersOms};
        std::uint32_t spin_iters_logger{kSpinItersLogger};
        std::uint32_t spin_iters_audit{kSpinItersAudit};

        std::uint32_t yield_every{kYieldEvery};
        std::uint32_t sleep_after_idle_iters{kSleepAfterIdleIters};
        std::uint32_t sleep_micros{kSleepMicros};

    };

    struct PipelineConfig{
        std::size_t frame_pool_capacity{kDefaultCapacity};
        std::size_t shard_count{1};
        std::size_t io_to_router_capacity{kDefaultCapacity};
        std::size_t router_to_logger_capacity{kDefaultCapacity};
        std::size_t shard_input_capacity{kDefaultCapacity};
        std::size_t shard_to_logger_capacity{kDefaultCapacity};
        IdlePolicyConfig idle_policy{};
    };



    struct TapeConfig{
        std::string output_path{"predex_tape.bin"};
    };

    struct AuditConfig{
        std::string output_path{"predex_audit.jsonl"};
    };

    struct OmsTransportConfig {
        bool enabled{false};
        std::string rest_endpoint{"https://api.elections.kalshi.com"};
        std::string private_ws_endpoint{"wss://api.elections.kalshi.com/trade-api/ws/v2"};
        std::vector<std::string> private_ws_channels;
        std::int64_t max_session_loss_ticks{0};
    };

    struct LocalRiskConfig {
        // Maximum absolute net filled position (long or short) per market. 0 = disabled.
        std::int64_t max_net_position_lots_per_market{0};
        // Reject intents for markets closing within this many seconds. 0 = disabled.
        std::uint64_t min_seconds_to_close{0};
        bool trading_enabled{true};
    };

    struct AppConfig{
        KalshiWsConfig kalshi_ws;
        PipelineConfig pipeline;
        TapeConfig tape;
        AuditConfig audit;
        OmsTransportConfig oms_transport;
        LocalRiskConfig local_risk;
        std::vector<MarketRouteConfig> market_routes;
    };

    class App{
        public:
            explicit App(AppConfig config);
            ~App();

            App(const App&) = delete;
            App& operator=(const App&) = delete;

            App(App&&) = delete;
            App& operator=(App&&) = delete;

            [[nodiscard]] bool start();
            void run();
            void stop();
            [[nodiscard]] std::string last_error() const noexcept;
        private:
            struct Runtime;
            std::unique_ptr<Runtime> runtime_;
    };
}
