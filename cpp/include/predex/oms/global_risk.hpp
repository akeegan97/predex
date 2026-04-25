#pragma once

#include <cstddef>
#include <cstdint>
#include <cassert>
#include <limits>
#include <optional>
#include <unordered_map>

#include "predex/internal/market_types.hpp"
#include "predex/oms/oms_types.hpp"

namespace predex::core::oms::kalshi {

constexpr std::size_t kDefaultMaxOpenOrdersGlobal = 1'000'000;
constexpr std::size_t kDefaultMaxOpenOrdersPerEvent = 1'000'000;
constexpr internal::QtyLots kDefaultMaxGlobalExposureLots =
    std::numeric_limits<internal::QtyLots>::max() / 4;
constexpr internal::QtyLots kDefaultMaxEventExposureLots =
    std::numeric_limits<internal::QtyLots>::max() / 4;
constexpr std::int64_t kDefaultAvailableCapitalTicks = 0;

struct GlobalRiskLimits {
    std::size_t max_open_orders_global{kDefaultMaxOpenOrdersGlobal};
    std::size_t max_open_orders_per_event{kDefaultMaxOpenOrdersPerEvent};
    internal::QtyLots max_global_exposure_lots{kDefaultMaxGlobalExposureLots};
    internal::QtyLots max_event_exposure_lots{kDefaultMaxEventExposureLots};
    std::int64_t available_capital_ticks{kDefaultAvailableCapitalTicks};
    bool trading_enabled{true};
};

struct GlobalRiskState {
    std::size_t open_orders_global{0};
    std::size_t open_orders_for_target_event{0};
    internal::QtyLots global_exposure_lots{0};
    internal::QtyLots target_event_exposure_lots{0};
    std::int64_t locked_capital_ticks{0};
};

class GlobalRisk {
  public:
    explicit GlobalRisk(GlobalRiskLimits limits = {}) : limits_(limits) {}

    [[nodiscard]] const GlobalRiskLimits& limits() const noexcept { return limits_; }

    [[nodiscard]] GlobalRiskState state_for_event(internal::EventId event_id) const noexcept {
        GlobalRiskState state = global_state_;
        const auto event_it = event_state_.find(event_id);
        if (event_it == event_state_.end()) {
            state.open_orders_for_target_event = 0;
            state.target_event_exposure_lots = 0;
            return state;
        }
        state.open_orders_for_target_event = event_it->second.open_orders;
        state.target_event_exposure_lots = event_it->second.exposure_lots;
        return state;
    }

    [[nodiscard]] const GlobalRiskState& global_state() const noexcept { return global_state_; }

    [[nodiscard]] RiskToOmsDecision evaluate_new_order(
        const NewOrderIntent& intent) const noexcept {
        if (!limits_.trading_enabled) {
            return RiskRejected{.reason = IntentRejectReason::kSoftHalt};
        }
        if (intent.context.event_id == 0 || intent.context.market_id == 0 || intent.qty_lots <= 0 ||
            intent.side == internal::Side::kUnknown) {
            return RiskRejected{.reason = IntentRejectReason::kInvalidParams};
        }

        const GlobalRiskState state = state_for_event(intent.context.event_id);
        if (state.open_orders_global >= limits_.max_open_orders_global ||
            state.open_orders_for_target_event >= limits_.max_open_orders_per_event ||
            state.global_exposure_lots + intent.qty_lots > limits_.max_global_exposure_lots ||
            state.target_event_exposure_lots + intent.qty_lots >
                limits_.max_event_exposure_lots) {
            return RiskRejected{.reason = IntentRejectReason::kGlobalRiskExceeded};
        }

        const auto reserve_result = required_capital_ticks(intent);
        if (!reserve_result.has_value()) {
            return RiskRejected{.reason = IntentRejectReason::kInvalidParams};
        }
        if (limits_.available_capital_ticks > 0 &&
            state.locked_capital_ticks + *reserve_result > limits_.available_capital_ticks) {
            return RiskRejected{.reason = IntentRejectReason::kGlobalRiskExceeded};
        }

        return RiskApproved{.capital_reserved_ticks = reserve_result.value_or(0)};
    }

    [[nodiscard]] RiskToOmsDecision evaluate_modify(
        const NewOrderIntent& replacement,
        internal::QtyLots current_working_qty_lots,
        std::optional<internal::PriceTicks> current_working_limit_price_ticks,
        internal::EventId event_id) const noexcept {
        if (!limits_.trading_enabled) {
            return RiskRejected{.reason = IntentRejectReason::kSoftHalt};
        }
        if (replacement.qty_lots <= 0 || replacement.side == internal::Side::kUnknown ||
            replacement.context.market_id == 0 || replacement.context.event_id == 0) {
            return RiskRejected{.reason = IntentRejectReason::kInvalidParams};
        }

        const GlobalRiskState state = state_for_event(event_id);
        const internal::QtyLots current_exposure = current_working_qty_lots;
        const internal::QtyLots replacement_exposure = replacement.qty_lots;
        const internal::QtyLots event_exposure_after_modify =
            state.target_event_exposure_lots >= current_exposure
                ? state.target_event_exposure_lots - current_exposure + replacement_exposure
                : replacement_exposure;
        const internal::QtyLots global_exposure_after_modify =
            state.global_exposure_lots >= current_exposure
                ? state.global_exposure_lots - current_exposure + replacement_exposure
                : replacement_exposure;
        if (global_exposure_after_modify > limits_.max_global_exposure_lots ||
            event_exposure_after_modify > limits_.max_event_exposure_lots) {
            return RiskRejected{.reason = IntentRejectReason::kGlobalRiskExceeded};
        }

        const auto current_locked =
            working_capital_ticks(current_working_qty_lots, current_working_limit_price_ticks);
        if (!current_locked.has_value()) {
            return RiskRejected{.reason = IntentRejectReason::kInvalidParams};
        }
        const auto replacement_locked = required_capital_ticks(replacement);
        if (!replacement_locked.has_value()) {
            return RiskRejected{.reason = IntentRejectReason::kInvalidParams};
        }
        const std::int64_t delta_locked = *replacement_locked - *current_locked;
        if (limits_.available_capital_ticks > 0 && delta_locked > 0 &&
            state.locked_capital_ticks + delta_locked > limits_.available_capital_ticks) {
            return RiskRejected{.reason = IntentRejectReason::kGlobalRiskExceeded};
        }
        return RiskApproved{.capital_reserved_ticks = delta_locked};
    }

    void on_new_order_accepted(const NewOrderIntent& intent,
                               std::int64_t capital_reserved_ticks) noexcept {
        auto& event_state = event_state_[intent.context.event_id];
        ++global_state_.open_orders_global;
        ++event_state.open_orders;

        global_state_.global_exposure_lots += intent.qty_lots;
        event_state.exposure_lots += intent.qty_lots;
        if (capital_reserved_ticks > 0) {
            global_state_.locked_capital_ticks += capital_reserved_ticks;
        }
    }

    void on_modify_accepted(internal::EventId event_id,
                            internal::QtyLots previous_working_qty_lots,
                            internal::QtyLots replacement_working_qty_lots,
                            std::int64_t capital_delta_ticks) noexcept {
        auto event_it = event_state_.find(event_id);
        assert(event_it != event_state_.end());
        if (event_it == event_state_.end()) {
            return;
        }
        auto& event_state = event_it->second;
        global_state_.global_exposure_lots =
            replacement_working_qty_lots >= previous_working_qty_lots
                ? global_state_.global_exposure_lots +
                      (replacement_working_qty_lots - previous_working_qty_lots)
                : saturating_subtract(global_state_.global_exposure_lots,
                                      previous_working_qty_lots - replacement_working_qty_lots);
        event_state.exposure_lots =
            replacement_working_qty_lots >= previous_working_qty_lots
                ? event_state.exposure_lots +
                      (replacement_working_qty_lots - previous_working_qty_lots)
                : saturating_subtract(event_state.exposure_lots,
                                      previous_working_qty_lots - replacement_working_qty_lots);
        if (capital_delta_ticks > 0) {
            global_state_.locked_capital_ticks += capital_delta_ticks;
        } else if (capital_delta_ticks < 0) {
            global_state_.locked_capital_ticks =
                saturating_subtract_i64(global_state_.locked_capital_ticks, -capital_delta_ticks);
        }
    }
//NOLINTNEXTLINE
    void on_fill(internal::EventId event_id,
                 internal::QtyLots fill_qty_lots,
                 std::int64_t released_capital_ticks) noexcept {
        if (fill_qty_lots > 0) {
            global_state_.global_exposure_lots =
                saturating_subtract(global_state_.global_exposure_lots, fill_qty_lots);
            auto event_it = event_state_.find(event_id);
            if (event_it != event_state_.end()) {
                event_it->second.exposure_lots =
                    saturating_subtract(event_it->second.exposure_lots, fill_qty_lots);
            }
        }
        if (released_capital_ticks > 0) {
            global_state_.locked_capital_ticks =
                saturating_subtract_i64(global_state_.locked_capital_ticks, released_capital_ticks);
        }
    }
//NOLINTNEXTLINE
    void on_order_terminal(internal::EventId event_id,
                           internal::QtyLots remaining_open_qty_lots,
                           std::int64_t released_capital_ticks) noexcept {
        if (global_state_.open_orders_global > 0) {
            --global_state_.open_orders_global;
        }
        auto event_it = event_state_.find(event_id);
        if (event_it != event_state_.end()) {
            if (event_it->second.open_orders > 0) {
                --event_it->second.open_orders;
            }
            event_it->second.exposure_lots =
                saturating_subtract(event_it->second.exposure_lots, remaining_open_qty_lots);
            if (event_it->second.open_orders == 0 && event_it->second.exposure_lots == 0) {
                event_state_.erase(event_it);
            }
        }
        global_state_.global_exposure_lots =
            saturating_subtract(global_state_.global_exposure_lots, remaining_open_qty_lots);
        if (released_capital_ticks > 0) {
            global_state_.locked_capital_ticks =
                saturating_subtract_i64(global_state_.locked_capital_ticks, released_capital_ticks);
        }
    }

  private:
    struct EventRiskState {
        std::size_t open_orders{0};
        internal::QtyLots exposure_lots{0};
    };

    [[nodiscard]] static std::optional<std::int64_t> required_capital_ticks(
        const NewOrderIntent& intent) noexcept {
        return working_capital_ticks(intent.qty_lots, intent.limit_price_ticks);
    }

    [[nodiscard]] static std::optional<std::int64_t> working_capital_ticks(
        internal::QtyLots qty_lots,
        std::optional<internal::PriceTicks> limit_price_ticks) noexcept {
        if (qty_lots <= 0) {
            return std::nullopt;
        }
        if (!limit_price_ticks.has_value()) {
            return 0;
        }
        if (*limit_price_ticks <= 0) {
            return std::nullopt;
        }
        return static_cast<std::int64_t>(qty_lots) *
               static_cast<std::int64_t>(*limit_price_ticks);
    }

    [[nodiscard]] static internal::QtyLots saturating_subtract(internal::QtyLots value,
                                                               internal::QtyLots delta) noexcept {
        if (delta <= 0) {
            return value;
        }
        if (delta >= value) {
            return 0;
        }
        return value - delta;
    }

    [[nodiscard]] static std::int64_t saturating_subtract_i64(std::int64_t value,
                                                              std::int64_t delta) noexcept {
        if (delta <= 0) {
            return value;
        }
        if (delta >= value) {
            return 0;
        }
        return value - delta;
    }

    GlobalRiskLimits limits_{};
    GlobalRiskState global_state_{};
    std::unordered_map<internal::EventId, EventRiskState> event_state_;
};

} // namespace predex::core::oms::kalshi
