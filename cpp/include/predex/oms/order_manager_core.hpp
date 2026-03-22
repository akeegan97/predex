#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "trading/internal/oms_types.hpp"
#include "trading/oms/global_risk_gate.hpp"
#include "trading/oms/portfolio_risk_gate.hpp"

namespace trading::oms {

enum class InFlightStatus : std::uint8_t {
    kPending = 0,
    kAccepted = 1,
    kPartiallyFilled = 2,
    kFilled = 3,
    kCanceled = 4,
    kRejected = 5,
    kReplaced = 6,
};

struct InFlightOrderSnapshot {
    internal::OrderRequestId request_id{0};
    internal::OmsAction last_action{internal::OmsAction::kUnknown};
    InFlightStatus status{InFlightStatus::kPending};
    internal::ClientOrderId client_order_id;
    std::optional<internal::ExchangeOrderId> exchange_order_id;
    std::optional<internal::ClientOrderId> replace_target_client_order_id;
    std::string market_ticker;
    internal::Side side{internal::Side::kUnknown};
    internal::QtyLots requested_qty_lots{0};
    internal::QtyLots filled_qty_lots{0};
    internal::TimestampNs created_ts_ns{0};
    internal::TimestampNs last_update_ts_ns{0};
};

struct OrderManagerCoreStats {
    std::uint64_t risk_reject_count{0};
    std::uint64_t portfolio_risk_reject_count{0};
    std::uint64_t transition_applied_count{0};
    std::uint64_t transition_reject_count{0};
    std::uint64_t unknown_order_update_count{0};
    std::uint64_t policy_reject_count{0};
    std::size_t tracked_order_count{0};
    std::size_t active_order_count{0};
};

class OrderManagerCore final {
  public:
    explicit OrderManagerCore(GlobalRiskConfig global_risk_config = {},
                              PortfolioRiskConfig portfolio_risk_config = {});

    void reset();
    [[nodiscard]] OrderManagerCoreStats stats() const;

    [[nodiscard]] bool validate_and_track_submission(internal::OrderRequestId request_id,
                                                     const internal::OrderIntent& intent,
                                                     std::string& error_message);
    [[nodiscard]] std::optional<InFlightOrderSnapshot>
    in_flight_order(std::string_view client_order_id) const;

    [[nodiscard]] std::optional<GlobalRiskDecision>
    evaluate_global_risk(const internal::OrderIntent& intent, std::string& error_message);
    [[nodiscard]] static internal::OrderStateUpdate
    make_global_risk_reject_update(const internal::OrderIntent& intent,
                                   const GlobalRiskDecision& decision);
    [[nodiscard]] std::optional<PortfolioRiskDecision>
    evaluate_portfolio_risk(const internal::OrderIntent& intent,
                            const PortfolioRiskSnapshot& snapshot, std::string& error_message);
    [[nodiscard]] static internal::OrderStateUpdate
    make_portfolio_risk_reject_update(const internal::OrderIntent& intent,
                                      const PortfolioRiskDecision& decision);

    [[nodiscard]] bool apply_update_transition(internal::OrderStateUpdate& update,
                                               std::string& error_message);

  private:
    struct InFlightOrder {
        internal::OrderRequestId request_id{0};
        internal::OmsAction last_action{internal::OmsAction::kUnknown};
        InFlightStatus status{InFlightStatus::kPending};
        internal::ClientOrderId client_order_id;
        std::optional<internal::ExchangeOrderId> exchange_order_id;
        std::optional<internal::ClientOrderId> replace_target_client_order_id;
        std::string market_ticker;
        internal::Side side{internal::Side::kUnknown};
        internal::QtyLots requested_qty_lots{0};
        internal::QtyLots filled_qty_lots{0};
        internal::TimestampNs created_ts_ns{0};
        internal::TimestampNs last_update_ts_ns{0};
    };

    [[nodiscard]] static bool is_terminal_state(InFlightStatus status);
    [[nodiscard]] static std::optional<InFlightStatus>
    to_in_flight_status(internal::OmsOrderStatus status);
    [[nodiscard]] static bool can_transition(InFlightStatus current, InFlightStatus next);
    [[nodiscard]] static internal::QtyLots outstanding_qty(const InFlightOrder& order);
    [[nodiscard]] static internal::TimestampNs monotonic_now_ns();
    [[nodiscard]] GlobalRiskSnapshot
    build_risk_snapshot(std::string_view market_ticker, std::string_view skip_client_order_id) const;

    GlobalRiskGate global_risk_gate_;
    PortfolioRiskGate portfolio_risk_gate_;
    std::unordered_map<internal::ClientOrderId, InFlightOrder> in_flight_orders_;
    std::unordered_map<internal::ExchangeOrderId, internal::ClientOrderId> exchange_to_client_;

    std::uint64_t risk_reject_count_{0};
    std::uint64_t portfolio_risk_reject_count_{0};
    std::uint64_t transition_applied_count_{0};
    std::uint64_t transition_reject_count_{0};
    std::uint64_t unknown_order_update_count_{0};
    std::uint64_t policy_reject_count_{0};
};

} // namespace trading::oms
