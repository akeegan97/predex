#pragma once
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "predex/internal/event_topology.hpp"
#include "predex/operator/operator_commands.hpp"
#include "predex/operator/unix_socket_admin.hpp"

// might make each a separate constant but for now just using a single global default capacity
namespace predex {
inline constexpr std::size_t kDefaultCapacity = 8192;
inline constexpr std::uint32_t kSpinItersRouter = 20000;
inline constexpr std::uint32_t kSpinItersShard = 20000;
inline constexpr std::uint32_t kSpinItersOms = 10000;
inline constexpr std::uint32_t kSpinItersLogger = 200;
inline constexpr std::uint32_t kSpinItersAudit = 200;
inline constexpr std::uint32_t kYieldEvery = 64;
inline constexpr std::uint32_t kSleepAfterIdleIters = 2000;
inline constexpr std::uint32_t kSleepMicros = 25;

struct MarketRouteConfig {
    std::string market_ticker;
    std::uint64_t market_id{0};
    std::uint64_t event_id{0};
    std::uint64_t affinity_key{0};
    internal::EventTopologyKind topology_kind{internal::EventTopologyKind::kUnknown};
    std::int64_t strike_key{0};
    std::uint64_t close_time_s{0};
    bool tradeable{false};
};

struct KalshiWsConfig {
    std::string endpoint{"wss://api.elections.kalshi.com/trade-api/ws/v2"};
    std::string key_id;
    std::string private_key_pem;
    std::vector<std::string> channels;
    std::vector<std::string> lifecycle_channels;
    std::vector<std::string> market_tickers;
};

struct IdlePolicyConfig {
    std::uint32_t spin_iters_router{kSpinItersRouter};
    std::uint32_t spin_iters_shard{kSpinItersShard};
    std::uint32_t spin_iters_oms{kSpinItersOms};
    std::uint32_t spin_iters_logger{kSpinItersLogger};
    std::uint32_t spin_iters_audit{kSpinItersAudit};

    std::uint32_t yield_every{kYieldEvery};
    std::uint32_t sleep_after_idle_iters{kSleepAfterIdleIters};
    std::uint32_t sleep_micros{kSleepMicros};
};

struct PipelineConfig {
    std::size_t frame_pool_capacity{kDefaultCapacity};
    std::size_t shard_count{1};
    std::size_t io_to_router_capacity{kDefaultCapacity};
    std::size_t router_to_logger_capacity{kDefaultCapacity};
    std::size_t shard_input_capacity{kDefaultCapacity};
    std::size_t shard_to_logger_capacity{kDefaultCapacity};
    IdlePolicyConfig idle_policy{};
};

struct TapeConfig {
    std::string output_path{"logs/live/predex_tape.bin"};
};

struct AuditConfig {
    std::string output_path{"logs/live/predex_audit.jsonl"};
};

using OperatorAdminConfig = operator_admin::UnixSocketAdminConfig;

// Policy applied at startup when the OMS finds (or refuses to look for) live
// orders resting at the venue from a prior session. The strict default
// (kRefuseIfPresent) matches the current discrete-session runtime: today no
// configured strategy posts resting orders, so this fires zero times. The
// non-default modes are scaffolded for the long-range strategy landing
// (MM / soft-monotonic), where session-to-session order continuity becomes
// load-bearing. See docs/oms_design.md and [[project_operational_modes]].
enum class StartupOpenOrdersPolicy : std::uint8_t {
    kIgnore = 0,          // skip REST reconciliation entirely (dev/loose mode)
    kRefuseIfPresent = 1, // fetch; abort startup if any open order is found
    kCancelAll = 2,       // fetch + cancel everything before going live
    kAdopt = 3,           // fetch + seed configured-market orders into OMS
};

struct OmsTransportConfig {
    bool enabled{false};
    std::string rest_endpoint{"https://api.elections.kalshi.com"};
    std::string private_ws_endpoint{"wss://api.elections.kalshi.com/trade-api/ws/v2"};
    std::vector<std::string> private_ws_channels;
    std::size_t rest_worker_count{4};
    // 0 = no limit
    std::int64_t max_session_loss_ticks{0};
    std::int64_t available_capital_ticks{0};
    StartupOpenOrdersPolicy startup_open_orders_policy{StartupOpenOrdersPolicy::kRefuseIfPresent};
};

struct LocalRiskConfig {
    // Maximum absolute net filled position (long or short) per market. 0 = disabled.
    std::int64_t max_net_position_lots_per_market{0};
    // Reject intents for markets closing within this many seconds. 0 = disabled.
    std::uint64_t min_seconds_to_close{0};
    bool trading_enabled{true};
};

struct AppConfig {
    KalshiWsConfig kalshi_ws;
    PipelineConfig pipeline;
    TapeConfig tape;
    AuditConfig audit;
    OperatorAdminConfig operator_admin;
    OmsTransportConfig oms_transport;
    LocalRiskConfig local_risk;
    std::vector<MarketRouteConfig> market_routes;
};

class App {
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
    [[nodiscard]] bool submit_operator_command(
        core::operator_commands::OperatorCommand command) noexcept;
    [[nodiscard]] bool try_pop_operator_response(
        core::operator_commands::OperatorResponse& response_out) noexcept;
    [[nodiscard]] core::operator_commands::OperatorStatusSnapshot operator_status() const;
    [[nodiscard]] std::string last_error() const noexcept;

  private:
    struct Runtime;
    std::unique_ptr<Runtime> runtime_;
};
} // namespace predex
