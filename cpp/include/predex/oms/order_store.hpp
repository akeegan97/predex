#pragma once
#include <cstddef>
#include <optional>
#include <unordered_map>
#include <vector>

#include "predex/internal/market_types.hpp"
#include "predex/oms/oms_types.hpp"

namespace predex::core::oms::kalshi {

// Owns all live order state and lookup indices. Single writer: OMS coordinator thread.
class OrderStore {
  public:
    struct LifecycleApplyResult {
        bool ok{false};
        internal::EventId event_id{0};
        internal::QtyLots fill_qty_lots{0};
        internal::TimestampNs first_fill_recv_ns{0};
        bool is_terminal{false};
        internal::QtyLots remaining_open_qty_lots{0};
    };

    OrderStore() = default;

    void insert_live_order(const AcceptedIntent& accepted_intent,
                           internal::TimestampNs decision_ts_ns);

    void set_transport_submit_ts(OmsRequestId oms_request_id,
                                  internal::TimestampNs ts_ns) noexcept;

    // Returns nullptr if the event cannot be matched to a tracked order.
    [[nodiscard]] OrderState* find_order_state(const OrderLifecycleEvent& event);

    // Looks up an order by any available identifier, preferring oms_request_id.
    [[nodiscard]] OrderState* find_order_for_action(
        std::optional<OmsRequestId> oms_request_id,
        const ClientOrderId& client_order_id,
        const std::optional<ExchangeOrderId>& exchange_order_id);

    // Applies state transition and erases the order on terminal events.
    // Does NOT call risk callbacks — the coordinator handles those via the result.
    [[nodiscard]] LifecycleApplyResult apply_lifecycle_event(const OrderLifecycleEvent& event);

    // Seeds a pre-built OrderState from an external source (e.g., REST reconciliation
    // at startup). Must be called before the OMS thread starts.
    void adopt_orphaned(OrderState state);

    [[nodiscard]] std::vector<CancelOrderCmd> build_cancel_all_cmds(
        internal::TimestampNs ts_ns) const;

    [[nodiscard]] std::size_t live_order_count() const noexcept;

  private:
    std::unordered_map<OmsRequestId, OrderState> orders_by_request_id_;
    std::unordered_map<ClientOrderId, OmsRequestId> request_by_client_order_id_;
    std::unordered_map<ExchangeOrderId, OmsRequestId> request_by_exchange_order_id_;
};

} // namespace predex::core::oms::kalshi
