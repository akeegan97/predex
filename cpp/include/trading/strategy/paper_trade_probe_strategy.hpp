#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <utility>

#include "trading/strategy/strategy.hpp"

namespace trading::strategy {

struct PaperTradeProbeStrategyConfig {
    std::string client_order_id_prefix{"paper-probe"};
    internal::Side side{internal::Side::kBuy};
    internal::QtyLots qty_lots{1};
    internal::OmsTimeInForce time_in_force{internal::OmsTimeInForce::kGtc};
    std::size_t max_orders{1};
    std::optional<internal::PriceTicks> limit_price_ticks_override;
};

class PaperTradeProbeStrategy final : public IStrategy {
  public:
    explicit PaperTradeProbeStrategy(PaperTradeProbeStrategyConfig config = {}, std::size_t shard_id = 0)
        : config_(std::move(config)), shard_id_(shard_id) {}

    [[nodiscard]] StrategyDecision on_event(const internal::NormalizedEvent& event) override {
        StrategyDecision decision{};
        if (event.type != internal::EventType::kTrade || emitted_order_count_ >= config_.max_orders ||
            config_.side == internal::Side::kUnknown || config_.qty_lots <= 0) {
            return decision;
        }

        const auto* trade = std::get_if<internal::TradeData>(&event.data);
        if (trade == nullptr) {
            return decision;
        }

        const auto limit_price_ticks =
            config_.limit_price_ticks_override.value_or(trade->price_ticks);
        if (limit_price_ticks < 0) {
            return decision;
        }

        internal::OrderIntent intent{};
        intent.action = internal::OmsAction::kPlace;
        intent.client_order_id =
            config_.client_order_id_prefix + "-s" + std::to_string(shard_id_) + "-n" +
            std::to_string(next_local_order_id_++);
        intent.side = config_.side;
        intent.qty_lots = config_.qty_lots;
        intent.limit_price_ticks = limit_price_ticks;
        intent.time_in_force = config_.time_in_force;
        decision.intents.push_back(std::move(intent));
        ++emitted_order_count_;
        return decision;
    }

    [[nodiscard]] std::size_t emitted_order_count() const { return emitted_order_count_; }

  private:
    PaperTradeProbeStrategyConfig config_;
    std::size_t shard_id_{0};
    std::size_t emitted_order_count_{0};
    std::size_t next_local_order_id_{0};
};

} // namespace trading::strategy
