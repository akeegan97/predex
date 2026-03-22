#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "trading/internal/market_types.hpp"
#include "trading/oms/global_risk_gate.hpp"
#include "trading/oms/portfolio_risk_gate.hpp"
#include "trading/pipeline/live_pipeline.hpp"
#include "trading/strategy/shard_risk_gate.hpp"

namespace trading::config {

enum class TraderExecutionMode : std::uint8_t {
    kLive = 0,
    kPaper = 1,
    kMarketDataOnly = 2,
};

[[nodiscard]] std::string_view execution_mode_name(TraderExecutionMode mode);

enum class TraderStrategyMode : std::uint8_t {
    kNoop = 0,
    kPaperTradeProbe = 1,
};

[[nodiscard]] std::string_view strategy_mode_name(TraderStrategyMode mode);

struct CredentialConfig {
    std::string key_id;
    std::string private_key_pem;
    std::string key_id_env{"KALSHI_KEY_ID"};
    std::string private_key_pem_env{"KALSHI_PRIVATE_KEY_PEM"};
};

struct KalshiWsConfig {
    std::string endpoint{"wss://api.elections.kalshi.com/trade-api/ws/v2"};
    std::vector<std::string> channels{"trades"};
    CredentialConfig credentials{};
};

struct MarketUniverseConfig {
    std::vector<std::string> tickers;
};

struct RiskConfig {
    strategy::ShardRiskConfig shard{};
    oms::GlobalRiskConfig oms_global{};
    oms::PortfolioRiskConfig oms_portfolio{};
};

struct StrategyConfig {
    TraderStrategyMode mode{TraderStrategyMode::kNoop};
    std::string client_order_id_prefix{"paper-probe"};
    internal::Side side{internal::Side::kBuy};
    internal::QtyLots qty_lots{1};
    std::size_t max_orders_per_shard{1};
    std::optional<internal::PriceTicks> limit_price_ticks_override;
};

struct PaperOmsConfig {
    bool auto_fill_on_place{true};
    std::size_t fill_parts{1};
    std::size_t place_reject_bps{0};
    std::chrono::milliseconds ack_delay{std::chrono::milliseconds{0}};
    std::chrono::milliseconds fill_delay{std::chrono::milliseconds{0}};
    std::chrono::milliseconds fill_interval{std::chrono::milliseconds{0}};
    std::chrono::milliseconds reject_delay{std::chrono::milliseconds{0}};
};

struct TraderRuntimeConfig {
    std::string mode{"dev"};
    TraderExecutionMode execution_mode{TraderExecutionMode::kLive};
    KalshiWsConfig kalshi{};
    MarketUniverseConfig market_universe{};
    RiskConfig risk{};
    StrategyConfig strategy{};
    PaperOmsConfig paper_oms{};
    pipeline::LivePipelineConfig pipeline{};
    std::size_t pump_batch_size{pipeline::LivePipeline::kDefaultPumpBatchSize};
    std::chrono::milliseconds pump_idle_sleep{std::chrono::milliseconds{1}};
    std::chrono::milliseconds stats_log_interval{std::chrono::milliseconds{30'000}};
};

struct ConfigLoadResult {
    bool ok{false};
    TraderRuntimeConfig config{};
    std::string error;
};

[[nodiscard]] ConfigLoadResult load_trader_config_from_json(std::string_view json_text);
[[nodiscard]] ConfigLoadResult load_trader_config_from_file(const std::string& path);

} // namespace trading::config
