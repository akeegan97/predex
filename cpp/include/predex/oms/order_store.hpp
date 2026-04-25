#pragma once

#include <cstddef>
#include <optional>
#include <unordered_map>
#include <vector>

#include "predex/internal/market_types.hpp"
#include "predex/oms/oms_types.hpp"

namespace predex::core::oms::kalshi {

struct PendingReplacement{
    internal::QtyLots requested_working_qty_lots{0};
    std::optional<internal::PriceTicks> requested_working_limit_price_ticks;
    internal::TimestampNs request_ts_ns{0};
};

struct OrderState {
    IntentContext context{};
    OmsOrderRef order{};

    internal::ExchangeId exchange{internal::ExchangeId::kUnknown};
    internal::Side side{internal::Side::kUnknown};
    Outcome outcome{Outcome::kYes};

    internal::QtyLots initial_qty_lots{0};
    internal::QtyLots working_qty_lots{0};
    internal::QtyLots cumulative_filled_qty_lots{0};

    std::optional<internal::PriceTicks> initial_limit_price_ticks;
    std::optional<internal::PriceTicks> working_limit_price_ticks;

    TimeInForce time_in_force{TimeInForce::kGtc};
    LiquidityIntent liquidity_intent{LiquidityIntent::kDefault};
    OrderTypeIntent order_type_intent{OrderTypeIntent::kLimit};

    OrderStatus status{OrderStatus::kPendingSubmit};
    VenueRejectReason last_venue_reject_reason{VenueRejectReason::kNone};

    std::optional<PendingReplacement> pending_replacement;
    std::optional<OrderStatus> status_before_pending_transition;

    internal::TimestampNs intent_ts_ns{0};
    internal::TimestampNs oms_decision_ts_ns{0};
    internal::TimestampNs venue_submit_ts_ns{0};
    internal::TimestampNs venue_ack_ts_ns{0};
    internal::TimestampNs first_fill_ts_ns{0};
    internal::TimestampNs last_fill_ts_ns{0};
    internal::TimestampNs terminal_ts_ns{0};
    internal::TimestampNs last_update_ts_ns{0};
};

struct BeginSubmitTransition {
    ShardOrderCorrelation corr{};
    NewOrderIntent intent{};
    internal::TimestampNs oms_decision_ts_ns{0};
};

struct MarkSubmittedTransition {
    OmsOrderRef order{};
    internal::TimestampNs venue_submit_ts_ns{0};
};

struct BeginCancelTransition {
    ShardOrderCorrelation corr{};
    internal::TimestampNs ts_ns{0};
};

struct BeginModifyTransition {
    ShardOrderCorrelation corr{};
    NewOrderIntent replacement{};
    internal::TimestampNs ts_ns{0};
};

class OrderStore {
  public:
    struct LookupKey {
        std::optional<OmsRequestId> oms_request_id;
        ClientOrderId client_order_id{};
        std::optional<ExchangeOrderId> exchange_order_id;
    };

    struct VenueApplyResult {
        bool ok{false};
        bool order_found{false};
        bool first_fill_observed{false};
        bool became_terminal{false};
        internal::QtyLots fill_qty_lots{0};
        internal::QtyLots remaining_open_qty_lots{0};
    };

    OrderStore() = default;

    [[nodiscard]] OrderState* find(const LookupKey& key) noexcept;
    [[nodiscard]] const OrderState* find(const LookupKey& key) const noexcept;
    [[nodiscard]] OrderState* find(const OmsOrderRef& order) noexcept;
    [[nodiscard]] const OrderState* find(const OmsOrderRef& order) const noexcept;

    void insert_pending_submit(const BeginSubmitTransition& transition);
    void mark_submitted(const MarkSubmittedTransition& transition) noexcept;

    [[nodiscard]] bool mark_pending_cancel(const BeginCancelTransition& transition) noexcept;
    [[nodiscard]] bool mark_pending_modify(const BeginModifyTransition& transition) noexcept;

    [[nodiscard]] VenueApplyResult apply_venue_event(const SourcedKalshiEvent& event);

    // Seeds or refreshes tracked state from an already-known open order
    // snapshot, typically during startup or reconnect reconciliation.
    void adopt_reconciled_order(OrderState state);

    [[nodiscard]] std::vector<CancelOrderCmd> build_cancel_all_cmds(
        internal::TimestampNs ts_ns) const;

    [[nodiscard]] std::size_t live_order_count() const noexcept;

  private:
    [[nodiscard]] OrderState* find_by_request_id(OmsRequestId oms_request_id) noexcept;
    [[nodiscard]] const OrderState* find_by_request_id(OmsRequestId oms_request_id) const noexcept;
    [[nodiscard]] OrderState* find_by_client_order_id(const ClientOrderId& client_order_id) noexcept;
    [[nodiscard]] const OrderState* find_by_client_order_id(
        const ClientOrderId& client_order_id) const noexcept;
    [[nodiscard]] OrderState* find_by_exchange_order_id(
        const ExchangeOrderId& exchange_order_id) noexcept;
    [[nodiscard]] const OrderState* find_by_exchange_order_id(
        const ExchangeOrderId& exchange_order_id) const noexcept;

    void bind_client_order_id(const ClientOrderId& client_order_id,
                              OmsRequestId oms_request_id);
    void bind_exchange_order_id(const ExchangeOrderId& exchange_order_id,
                                OmsRequestId oms_request_id);
    void erase_indices_for(const OrderState& state);

    std::unordered_map<OmsRequestId, OrderState> orders_by_request_id_;
    std::unordered_map<ClientOrderId, OmsRequestId> request_by_client_order_id_;
    std::unordered_map<ExchangeOrderId, OmsRequestId> request_by_exchange_order_id_;
};

} // namespace predex::core::oms::kalshi
