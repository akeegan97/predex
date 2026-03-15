#include "trading/config/trader_config.hpp"

#include <cstdint>
#include <fstream>
#include <sstream>
#include <string>
#include <utility>

#include <nlohmann/json.hpp>

namespace trading::config {
namespace {

using nlohmann::json;

bool parse_exchange_id(std::string_view name, decode::ExchangeId& out_decode,
                       internal::ExchangeId& out_router) {
    if (name == "kalshi") {
        out_decode = decode::ExchangeId::kKalshi;
        out_router = internal::ExchangeId::kKalshi;
        return true;
    }
    if (name == "polymarket") {
        out_decode = decode::ExchangeId::kPolymarket;
        out_router = internal::ExchangeId::kPolymarket;
        return true;
    }
    return false;
}

bool parse_execution_mode(std::string_view mode_name, TraderExecutionMode& out_mode) {
    if (mode_name == "live") {
        out_mode = TraderExecutionMode::kLive;
        return true;
    }
    if (mode_name == "paper") {
        out_mode = TraderExecutionMode::kPaper;
        return true;
    }
    if (mode_name == "md-only" || mode_name == "md_only" || mode_name == "market-data-only" ||
        mode_name == "market_data_only") {
        out_mode = TraderExecutionMode::kMarketDataOnly;
        return true;
    }
    return false;
}

bool parse_strategy_mode(std::string_view mode_name, TraderStrategyMode& out_mode) {
    if (mode_name == "noop") {
        out_mode = TraderStrategyMode::kNoop;
        return true;
    }
    if (mode_name == "paper-trade-probe" || mode_name == "paper_trade_probe") {
        out_mode = TraderStrategyMode::kPaperTradeProbe;
        return true;
    }
    return false;
}

bool parse_side(std::string_view side_name, internal::Side& out_side) {
    if (side_name == "buy") {
        out_side = internal::Side::kBuy;
        return true;
    }
    if (side_name == "sell") {
        out_side = internal::Side::kSell;
        return true;
    }
    if (side_name == "bid") {
        out_side = internal::Side::kBid;
        return true;
    }
    if (side_name == "ask") {
        out_side = internal::Side::kAsk;
        return true;
    }
    return false;
}

bool parse_positive_size_t(const json& value, std::size_t& out, std::string& error,
                           std::string_view field_name) {
    if (!value.is_number_unsigned()) {
        error = std::string(field_name) + " must be an unsigned integer";
        return false;
    }
    const auto parsed = value.get<std::size_t>();
    if (parsed == 0) {
        error = std::string(field_name) + " must be > 0";
        return false;
    }
    out = parsed;
    return true;
}

bool parse_non_negative_size_t(const json& value, std::size_t& out, std::string& error,
                               std::string_view field_name) {
    if (!value.is_number_unsigned()) {
        error = std::string(field_name) + " must be an unsigned integer";
        return false;
    }
    out = value.get<std::size_t>();
    return true;
}

bool parse_non_negative_ms(const json& value, std::chrono::milliseconds& out, std::string& error,
                           std::string_view field_name) {
    if (!value.is_number_integer()) {
        error = std::string(field_name) + " must be an integer";
        return false;
    }
    const auto parsed = value.get<std::int64_t>();
    if (parsed < 0) {
        error = std::string(field_name) + " must be >= 0";
        return false;
    }
    out = std::chrono::milliseconds{parsed};
    return true;
}

bool parse_int64(const json& value, std::int64_t& out, std::string& error,
                 std::string_view field_name) {
    if (!value.is_number_integer()) {
        error = std::string(field_name) + " must be an integer";
        return false;
    }
    out = value.get<std::int64_t>();
    return true;
}

bool parse_non_negative_qty(const json& value, internal::QtyLots& out, std::string& error,
                            std::string_view field_name) {
    std::int64_t parsed = 0;
    if (!parse_int64(value, parsed, error, field_name)) {
        return false;
    }
    if (parsed < 0) {
        error = std::string(field_name) + " must be >= 0";
        return false;
    }
    out = static_cast<internal::QtyLots>(parsed);
    return true;
}

bool parse_positive_qty(const json& value, internal::QtyLots& out, std::string& error,
                        std::string_view field_name) {
    if (!parse_non_negative_qty(value, out, error, field_name)) {
        return false;
    }
    if (out <= 0) {
        error = std::string(field_name) + " must be > 0";
        return false;
    }
    return true;
}

bool parse_bool(const json& value, bool& out, std::string& error, std::string_view field_name) {
    if (!value.is_boolean()) {
        error = std::string(field_name) + " must be a boolean";
        return false;
    }
    out = value.get<bool>();
    return true;
}

bool parse_string_array(const json& value, std::vector<std::string>& out, std::string& error,
                        std::string_view field_name) {
    if (!value.is_array()) {
        error = std::string(field_name) + " must be an array";
        return false;
    }

    std::vector<std::string> parsed;
    parsed.reserve(value.size());
    for (const auto& item : value) {
        if (!item.is_string()) {
            error = std::string(field_name) + " entries must be strings";
            return false;
        }
        parsed.push_back(item.get<std::string>());
    }
    out = std::move(parsed);
    return true;
}

// NOLINTBEGIN(readability-function-cognitive-complexity)
ConfigLoadResult parse_from_json(const json& root) {
    ConfigLoadResult result{
        .ok = true,
        .config = TraderRuntimeConfig{},
        .error = {},
    };

    if (!root.is_object()) {
        result.ok = false;
        result.error = "config root must be a JSON object";
        return result;
    }

    if (const auto mode_it = root.find("mode"); mode_it != root.end()) {
        if (!mode_it->is_string()) {
            result.ok = false;
            result.error = "mode must be a string";
            return result;
        }
        result.config.mode = mode_it->get<std::string>();
    }

    if (const auto kalshi_it = root.find("kalshi"); kalshi_it != root.end()) {
        if (!kalshi_it->is_object()) {
            result.ok = false;
            result.error = "kalshi must be an object";
            return result;
        }
        const auto& kalshi = *kalshi_it;

        if (const auto endpoint_it = kalshi.find("endpoint"); endpoint_it != kalshi.end()) {
            if (!endpoint_it->is_string()) {
                result.ok = false;
                result.error = "kalshi.endpoint must be a string";
                return result;
            }
            result.config.kalshi.endpoint = endpoint_it->get<std::string>();
        }
        if (const auto channels_it = kalshi.find("channels"); channels_it != kalshi.end()) {
            if (!parse_string_array(channels_it.value(), result.config.kalshi.channels,
                                    result.error, "kalshi.channels")) {
                result.ok = false;
                return result;
            }
        }
        if (const auto creds_it = kalshi.find("credentials"); creds_it != kalshi.end()) {
            if (!creds_it->is_object()) {
                result.ok = false;
                result.error = "kalshi.credentials must be an object";
                return result;
            }
            const auto& creds = *creds_it;
            if (const auto value_it = creds.find("key_id"); value_it != creds.end()) {
                if (!value_it->is_string()) {
                    result.ok = false;
                    result.error = "kalshi.credentials.key_id must be a string";
                    return result;
                }
                result.config.kalshi.credentials.key_id = value_it->get<std::string>();
            }
            if (const auto value_it = creds.find("private_key_pem"); value_it != creds.end()) {
                if (!value_it->is_string()) {
                    result.ok = false;
                    result.error = "kalshi.credentials.private_key_pem must be a string";
                    return result;
                }
                result.config.kalshi.credentials.private_key_pem = value_it->get<std::string>();
            }
            if (const auto value_it = creds.find("key_id_env"); value_it != creds.end()) {
                if (!value_it->is_string()) {
                    result.ok = false;
                    result.error = "kalshi.credentials.key_id_env must be a string";
                    return result;
                }
                result.config.kalshi.credentials.key_id_env = value_it->get<std::string>();
            }
            if (const auto value_it = creds.find("private_key_pem_env"); value_it != creds.end()) {
                if (!value_it->is_string()) {
                    result.ok = false;
                    result.error = "kalshi.credentials.private_key_pem_env must be a string";
                    return result;
                }
                result.config.kalshi.credentials.private_key_pem_env = value_it->get<std::string>();
            }
        }
    }

    if (const auto market_universe_it = root.find("market_universe");
        market_universe_it != root.end()) {
        if (!market_universe_it->is_object()) {
            result.ok = false;
            result.error = "market_universe must be an object";
            return result;
        }
        const auto& market_universe = *market_universe_it;
        if (const auto tickers_it = market_universe.find("tickers");
            tickers_it != market_universe.end()) {
            if (!parse_string_array(tickers_it.value(), result.config.market_universe.tickers,
                                    result.error, "market_universe.tickers")) {
                result.ok = false;
                return result;
            }
        }
    }

    if (const auto risk_it = root.find("risk"); risk_it != root.end()) {
        if (!risk_it->is_object()) {
            result.ok = false;
            result.error = "risk must be an object";
            return result;
        }
        const auto& risk = *risk_it;

        if (const auto shard_it = risk.find("shard"); shard_it != risk.end()) {
            if (!shard_it->is_object()) {
                result.ok = false;
                result.error = "risk.shard must be an object";
                return result;
            }
            const auto& shard = *shard_it;
            if (const auto value_it = shard.find("max_open_orders_per_market");
                value_it != shard.end()) {
                if (!parse_non_negative_size_t(value_it.value(),
                                               result.config.risk.shard.max_open_orders_per_market,
                                               result.error,
                                               "risk.shard.max_open_orders_per_market")) {
                    result.ok = false;
                    return result;
                }
            }
            if (const auto value_it = shard.find("max_working_qty_per_market");
                value_it != shard.end()) {
                if (!parse_non_negative_qty(value_it.value(),
                                            result.config.risk.shard.max_working_qty_per_market,
                                            result.error,
                                            "risk.shard.max_working_qty_per_market")) {
                    result.ok = false;
                    return result;
                }
            }
            if (const auto value_it = shard.find("max_abs_net_position_per_market");
                value_it != shard.end()) {
                if (!parse_non_negative_qty(value_it.value(),
                                            result.config.risk.shard.max_abs_net_position_per_market,
                                            result.error,
                                            "risk.shard.max_abs_net_position_per_market")) {
                    result.ok = false;
                    return result;
                }
            }
        }

        if (const auto oms_global_it = risk.find("oms_global"); oms_global_it != risk.end()) {
            if (!oms_global_it->is_object()) {
                result.ok = false;
                result.error = "risk.oms_global must be an object";
                return result;
            }
            const auto& oms_global = *oms_global_it;
            if (const auto value_it = oms_global.find("max_active_orders_global");
                value_it != oms_global.end()) {
                if (!parse_non_negative_size_t(value_it.value(),
                                               result.config.risk.oms_global.max_active_orders_global,
                                               result.error,
                                               "risk.oms_global.max_active_orders_global")) {
                    result.ok = false;
                    return result;
                }
            }
            if (const auto value_it = oms_global.find("max_active_orders_per_market");
                value_it != oms_global.end()) {
                if (!parse_non_negative_size_t(
                        value_it.value(),
                        result.config.risk.oms_global.max_active_orders_per_market, result.error,
                        "risk.oms_global.max_active_orders_per_market")) {
                    result.ok = false;
                    return result;
                }
            }
            if (const auto value_it = oms_global.find("max_outstanding_qty_global");
                value_it != oms_global.end()) {
                if (!parse_non_negative_qty(
                        value_it.value(), result.config.risk.oms_global.max_outstanding_qty_global,
                        result.error, "risk.oms_global.max_outstanding_qty_global")) {
                    result.ok = false;
                    return result;
                }
            }
            if (const auto value_it = oms_global.find("max_outstanding_qty_per_market");
                value_it != oms_global.end()) {
                if (!parse_non_negative_qty(
                        value_it.value(),
                        result.config.risk.oms_global.max_outstanding_qty_per_market, result.error,
                        "risk.oms_global.max_outstanding_qty_per_market")) {
                    result.ok = false;
                    return result;
                }
            }
        }

        if (const auto oms_portfolio_it = risk.find("oms_portfolio");
            oms_portfolio_it != risk.end()) {
            if (!oms_portfolio_it->is_object()) {
                result.ok = false;
                result.error = "risk.oms_portfolio must be an object";
                return result;
            }
            const auto& oms_portfolio = *oms_portfolio_it;
            if (const auto value_it = oms_portfolio.find("max_abs_net_position_per_market");
                value_it != oms_portfolio.end()) {
                if (!parse_non_negative_qty(
                        value_it.value(),
                        result.config.risk.oms_portfolio.max_abs_net_position_per_market,
                        result.error, "risk.oms_portfolio.max_abs_net_position_per_market")) {
                    result.ok = false;
                    return result;
                }
            }
            if (const auto value_it = oms_portfolio.find("max_abs_position_gross_global");
                value_it != oms_portfolio.end()) {
                if (!parse_non_negative_qty(
                        value_it.value(),
                        result.config.risk.oms_portfolio.max_abs_position_gross_global, result.error,
                        "risk.oms_portfolio.max_abs_position_gross_global")) {
                    result.ok = false;
                    return result;
                }
            }
            if (const auto value_it = oms_portfolio.find("min_realized_pnl_ticks");
                value_it != oms_portfolio.end()) {
                if (!parse_int64(value_it.value(),
                                 result.config.risk.oms_portfolio.min_realized_pnl_ticks,
                                 result.error, "risk.oms_portfolio.min_realized_pnl_ticks")) {
                    result.ok = false;
                    return result;
                }
            }
            if (const auto value_it = oms_portfolio.find("enforce_realized_pnl_floor");
                value_it != oms_portfolio.end()) {
                if (!parse_bool(value_it.value(),
                                result.config.risk.oms_portfolio.enforce_realized_pnl_floor,
                                result.error, "risk.oms_portfolio.enforce_realized_pnl_floor")) {
                    result.ok = false;
                    return result;
                }
            }
        }
    }

    if (const auto strategy_it = root.find("strategy"); strategy_it != root.end()) {
        if (!strategy_it->is_object()) {
            result.ok = false;
            result.error = "strategy must be an object";
            return result;
        }
        const auto& strategy = *strategy_it;

        if (const auto mode_it = strategy.find("mode"); mode_it != strategy.end()) {
            if (!mode_it->is_string()) {
                result.ok = false;
                result.error = "strategy.mode must be a string";
                return result;
            }
            if (!parse_strategy_mode(mode_it->get<std::string>(), result.config.strategy.mode)) {
                result.ok = false;
                result.error = "strategy.mode must be one of: noop, paper-trade-probe";
                return result;
            }
        }
        if (const auto prefix_it = strategy.find("client_order_id_prefix");
            prefix_it != strategy.end()) {
            if (!prefix_it->is_string()) {
                result.ok = false;
                result.error = "strategy.client_order_id_prefix must be a string";
                return result;
            }
            result.config.strategy.client_order_id_prefix = prefix_it->get<std::string>();
        }
        if (const auto side_it = strategy.find("side"); side_it != strategy.end()) {
            if (!side_it->is_string()) {
                result.ok = false;
                result.error = "strategy.side must be a string";
                return result;
            }
            if (!parse_side(side_it->get<std::string>(), result.config.strategy.side)) {
                result.ok = false;
                result.error = "strategy.side must be one of: buy, sell, bid, ask";
                return result;
            }
        }
        if (const auto qty_it = strategy.find("qty_lots"); qty_it != strategy.end()) {
            if (!parse_positive_qty(qty_it.value(), result.config.strategy.qty_lots, result.error,
                                    "strategy.qty_lots")) {
                result.ok = false;
                return result;
            }
        }
        if (const auto max_orders_it = strategy.find("max_orders_per_shard");
            max_orders_it != strategy.end()) {
            if (!parse_positive_size_t(max_orders_it.value(),
                                       result.config.strategy.max_orders_per_shard, result.error,
                                       "strategy.max_orders_per_shard")) {
                result.ok = false;
                return result;
            }
        }
        if (const auto limit_price_it = strategy.find("limit_price_ticks_override");
            limit_price_it != strategy.end()) {
            if (limit_price_it->is_null()) {
                result.config.strategy.limit_price_ticks_override.reset();
            } else {
                internal::PriceTicks limit_price_ticks = 0;
                if (!parse_non_negative_qty(limit_price_it.value(), limit_price_ticks, result.error,
                                            "strategy.limit_price_ticks_override")) {
                    result.ok = false;
                    return result;
                }
                result.config.strategy.limit_price_ticks_override = limit_price_ticks;
            }
        }
    }

    if (const auto paper_oms_it = root.find("paper_oms"); paper_oms_it != root.end()) {
        if (!paper_oms_it->is_object()) {
            result.ok = false;
            result.error = "paper_oms must be an object";
            return result;
        }
        const auto& paper_oms = *paper_oms_it;
        if (const auto value_it = paper_oms.find("auto_fill_on_place");
            value_it != paper_oms.end()) {
            if (!parse_bool(value_it.value(), result.config.paper_oms.auto_fill_on_place,
                            result.error, "paper_oms.auto_fill_on_place")) {
                result.ok = false;
                return result;
            }
        }
        if (const auto value_it = paper_oms.find("fill_parts"); value_it != paper_oms.end()) {
            if (!parse_positive_size_t(value_it.value(), result.config.paper_oms.fill_parts,
                                       result.error, "paper_oms.fill_parts")) {
                result.ok = false;
                return result;
            }
        }
        if (const auto value_it = paper_oms.find("place_reject_bps");
            value_it != paper_oms.end()) {
            if (!parse_non_negative_size_t(value_it.value(),
                                           result.config.paper_oms.place_reject_bps, result.error,
                                           "paper_oms.place_reject_bps")) {
                result.ok = false;
                return result;
            }
            if (result.config.paper_oms.place_reject_bps > 10'000U) {
                result.ok = false;
                result.error = "paper_oms.place_reject_bps must be in [0, 10000]";
                return result;
            }
        }
        if (const auto value_it = paper_oms.find("ack_delay_ms"); value_it != paper_oms.end()) {
            if (!parse_non_negative_ms(value_it.value(), result.config.paper_oms.ack_delay,
                                       result.error, "paper_oms.ack_delay_ms")) {
                result.ok = false;
                return result;
            }
        }
        if (const auto value_it = paper_oms.find("fill_delay_ms"); value_it != paper_oms.end()) {
            if (!parse_non_negative_ms(value_it.value(), result.config.paper_oms.fill_delay,
                                       result.error, "paper_oms.fill_delay_ms")) {
                result.ok = false;
                return result;
            }
        }
        if (const auto value_it = paper_oms.find("fill_interval_ms");
            value_it != paper_oms.end()) {
            if (!parse_non_negative_ms(value_it.value(), result.config.paper_oms.fill_interval,
                                       result.error, "paper_oms.fill_interval_ms")) {
                result.ok = false;
                return result;
            }
        }
        if (const auto value_it = paper_oms.find("reject_delay_ms");
            value_it != paper_oms.end()) {
            if (!parse_non_negative_ms(value_it.value(), result.config.paper_oms.reject_delay,
                                       result.error, "paper_oms.reject_delay_ms")) {
                result.ok = false;
                return result;
            }
        }
    }

    if (const auto pipeline_it = root.find("pipeline"); pipeline_it != root.end()) {
        if (!pipeline_it->is_object()) {
            result.ok = false;
            result.error = "pipeline must be an object";
            return result;
        }
        const auto& pipeline = *pipeline_it;

        if (const auto source_it = pipeline.find("source"); source_it != pipeline.end()) {
            if (!source_it->is_string()) {
                result.ok = false;
                result.error = "pipeline.source must be a string";
                return result;
            }
            result.config.pipeline.source = source_it->get<std::string>();
        }

        if (const auto exchange_it = pipeline.find("exchange"); exchange_it != pipeline.end()) {
            if (!exchange_it->is_string()) {
                result.ok = false;
                result.error = "pipeline.exchange must be a string";
                return result;
            }
            const auto exchange_name = exchange_it->get<std::string>();
            if (!parse_exchange_id(exchange_name, result.config.pipeline.decode_exchange,
                                   result.config.pipeline.router_exchange)) {
                result.ok = false;
                result.error = "pipeline.exchange must be one of: kalshi, polymarket";
                return result;
            }
        }

        if (const auto frame_pool_it = pipeline.find("frame_pool_capacity");
            frame_pool_it != pipeline.end()) {
            if (!parse_positive_size_t(frame_pool_it.value(),
                                       result.config.pipeline.frame_pool_capacity, result.error,
                                       "pipeline.frame_pool_capacity")) {
                result.ok = false;
                return result;
            }
        }
        if (const auto frame_queue_it = pipeline.find("frame_queue_capacity");
            frame_queue_it != pipeline.end()) {
            if (!parse_positive_size_t(frame_queue_it.value(),
                                       result.config.pipeline.frame_queue_capacity, result.error,
                                       "pipeline.frame_queue_capacity")) {
                result.ok = false;
                return result;
            }
        }
        if (const auto shard_count_it = pipeline.find("shard_count");
            shard_count_it != pipeline.end()) {
            if (!parse_positive_size_t(shard_count_it.value(), result.config.pipeline.shard_count,
                                       result.error, "pipeline.shard_count")) {
                result.ok = false;
                return result;
            }
        }
        if (const auto shard_queue_it = pipeline.find("per_shard_queue_capacity");
            shard_queue_it != pipeline.end()) {
            if (!parse_positive_size_t(shard_queue_it.value(),
                                       result.config.pipeline.per_shard_queue_capacity,
                                       result.error, "pipeline.per_shard_queue_capacity")) {
                result.ok = false;
                return result;
            }
        }
        if (const auto shard_idle_it = pipeline.find("shard_idle_sleep_ms");
            shard_idle_it != pipeline.end()) {
            if (!parse_non_negative_ms(shard_idle_it.value(),
                                       result.config.pipeline.shard_idle_sleep, result.error,
                                       "pipeline.shard_idle_sleep_ms")) {
                result.ok = false;
                return result;
            }
        }
    }

    if (const auto runtime_it = root.find("runtime"); runtime_it != root.end()) {
        if (!runtime_it->is_object()) {
            result.ok = false;
            result.error = "runtime must be an object";
            return result;
        }
        const auto& runtime = *runtime_it;

        if (const auto pump_batch_it = runtime.find("pump_batch_size");
            pump_batch_it != runtime.end()) {
            if (!parse_positive_size_t(pump_batch_it.value(), result.config.pump_batch_size,
                                       result.error, "runtime.pump_batch_size")) {
                result.ok = false;
                return result;
            }
        }
        if (const auto idle_it = runtime.find("pump_idle_sleep_ms"); idle_it != runtime.end()) {
            if (!parse_non_negative_ms(idle_it.value(), result.config.pump_idle_sleep, result.error,
                                       "runtime.pump_idle_sleep_ms")) {
                result.ok = false;
                return result;
            }
        }
        if (const auto stats_interval_it = runtime.find("stats_log_interval_ms");
            stats_interval_it != runtime.end()) {
            if (!parse_non_negative_ms(stats_interval_it.value(), result.config.stats_log_interval,
                                       result.error, "runtime.stats_log_interval_ms")) {
                result.ok = false;
                return result;
            }
        }
        if (const auto mode_it = runtime.find("execution_mode"); mode_it != runtime.end()) {
            if (!mode_it->is_string()) {
                result.ok = false;
                result.error = "runtime.execution_mode must be a string";
                return result;
            }
            if (!parse_execution_mode(mode_it->get<std::string>(), result.config.execution_mode)) {
                result.ok = false;
                result.error =
                    "runtime.execution_mode must be one of: live, paper, md-only";
                return result;
            }
        }
    }

    return result;
}
// NOLINTEND(readability-function-cognitive-complexity)

} // namespace

ConfigLoadResult load_trader_config_from_json(std::string_view json_text) {
    try {
        const json root = json::parse(json_text);
        return parse_from_json(root);
    } catch (const std::exception& exception) {
        return ConfigLoadResult{
            .ok = false,
            .config = TraderRuntimeConfig{},
            .error = std::string{"failed to parse config JSON: "} + exception.what(),
        };
    }
}

ConfigLoadResult load_trader_config_from_file(const std::string& path) {
    std::ifstream config_stream(path);
    if (!config_stream) {
        return ConfigLoadResult{
            .ok = false,
            .config = TraderRuntimeConfig{},
            .error = "failed to open config file: " + path,
        };
    }

    std::stringstream buffer;
    buffer << config_stream.rdbuf();
    auto result = load_trader_config_from_json(buffer.str());
    if (!result.ok && result.error.find(path) == std::string::npos) {
        result.error = path + ": " + result.error;
    }
    return result;
}

std::string_view execution_mode_name(TraderExecutionMode mode) {
    switch (mode) {
    case TraderExecutionMode::kLive:
        return "live";
    case TraderExecutionMode::kPaper:
        return "paper";
    case TraderExecutionMode::kMarketDataOnly:
        return "md-only";
    }
    return "unknown";
}

std::string_view strategy_mode_name(TraderStrategyMode mode) {
    switch (mode) {
    case TraderStrategyMode::kNoop:
        return "noop";
    case TraderStrategyMode::kPaperTradeProbe:
        return "paper-trade-probe";
    }
    return "unknown";
}

} // namespace trading::config
