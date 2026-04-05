#pragma once
#include <string>
#include <cstdint>
#include <cstddef>
#include <string_view>
#include <vector>
#include <memory>



//might make each a separate constant but for now just using a single global default capacity
namespace predex{
    inline constexpr std::size_t kDefaultCapacity = 4096;
    struct MarketRouteConfig{
        std::string market_ticker;
        std::uint64_t market_id{0};
        std::uint64_t affinity_key{0};
    };

    struct KalshiWsConfig{
        std::string endpoint{"wss://api.elections.kalshi.com/trade-api/ws/v2"};
        std::string key_id;
        std::string private_key_pem;
        std::vector<std::string> channels;
        std::vector<std::string> market_tickers;  
    };

    struct PipelineConfig{
        std::size_t frame_pool_capacity{kDefaultCapacity};
        std::size_t shard_count{1};
        std::size_t io_to_router_capacity{kDefaultCapacity};
        std::size_t router_to_logger_capacity{kDefaultCapacity};
        std::size_t shard_input_capacity{kDefaultCapacity};
        std::size_t shard_to_logger_capacity{kDefaultCapacity};
    };

    struct TapeConfig{
        std::string output_path{"predex_tape.bin"};
    };

    struct AppConfig{
        KalshiWsConfig kalshi_ws;
        PipelineConfig pipeline;
        TapeConfig tape;
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