#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <unordered_map>
#include <vector>

#include "predex/audit/audit_types.hpp"
#include "predex/oms/global_risk.hpp"
#include "predex/oms/oms_types.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::oms::kalshi {

struct OmsTransportQueues {
    utils::SPSCQueue<SubmitOrderCmd>* submit_queue{nullptr};
    utils::SPSCQueue<CancelOrderCmd>* cancel_queue{nullptr};
    utils::SPSCQueue<ModifyOrderCmd>* modify_queue{nullptr};

    // Optional seam for a dedicated private/user-order WS ingress thread.
    utils::SPSCQueue<OrderLifecycleEvent>* inbound_update_queue{nullptr};
};

enum class OmsProcessCode : std::uint8_t {
    kIdle = 0,
    kProcessedIntent = 1,
    kProcessedTransportUpdate = 2,
    kShardBackpressure = 3,
    kTransportBackpressure = 4,
    kError = 5,
};

struct OmsPumpResult {
    OmsProcessCode code{OmsProcessCode::kIdle};
    std::uint32_t processed_intents{0};
    std::uint32_t processed_transport_updates{0};
};

class Oms {
  public:
    using SubmissionQueue = utils::SPSCQueue<OmsSubmission>;
    using DecisionQueue = utils::SPSCQueue<IntentDecision>;
    using LifecycleQueue = utils::SPSCQueue<OrderLifecycleEvent>;
    using AuditQueue = utils::SPSCQueue<predex::core::audit::AuditEvent>;

    explicit Oms(std::vector<SubmissionQueue*> shard_intent_queues,
                 std::vector<DecisionQueue*> shard_decision_queues,
                 std::vector<LifecycleQueue*> shard_lifecycle_queues,
                 OmsTransportQueues transport_queues = {},
                 GlobalRiskManager global_risk = GlobalRiskManager{},
                 AuditQueue* audit_queue = nullptr);

    [[nodiscard]] OmsPumpResult pump(std::size_t max_transport_updates,
                                     std::size_t max_shard_intents) noexcept;

    [[nodiscard]] const GlobalRiskState& global_risk_state() const noexcept;

    [[nodiscard]] std::size_t live_order_count() const noexcept;

    [[nodiscard]] std::uint64_t processed_intent_count() const noexcept;

    [[nodiscard]] std::uint64_t processed_transport_update_count() const noexcept;

    [[nodiscard]] std::uint64_t rejected_intent_count() const noexcept;
  private:
    struct EventRiskState {
        std::size_t open_orders{0};
        internal::QtyLots exposure_lots{0};
    };

    GlobalRiskManager global_risk_;
    GlobalRiskState global_risk_state_{};
    std::unordered_map<internal::EventId, EventRiskState> event_risk_state_by_event_id_;

    std::vector<SubmissionQueue*> shard_intent_queues_;
    std::vector<DecisionQueue*> shard_decision_queues_;
    std::vector<LifecycleQueue*> shard_lifecycle_queues_;
    OmsTransportQueues transport_queues_{};
    AuditQueue* audit_queue_{nullptr};

    std::unordered_map<OmsRequestId, OrderState> orders_by_request_id_;
    std::unordered_map<ClientOrderId, OmsRequestId> request_by_client_order_id_;
    std::unordered_map<ExchangeOrderId, OmsRequestId> request_by_exchange_order_id_;
    std::unordered_map<OmsRequestId, internal::TimestampNs> decision_ts_by_request_id_;
    std::unordered_map<OmsRequestId, internal::TimestampNs> transport_submit_ts_by_request_id_;
    std::unordered_map<OmsRequestId, internal::TimestampNs> first_fill_ts_by_request_id_;

    std::size_t next_shard_index_{0};
    OmsRequestId next_oms_request_id_{1};
    std::uint64_t next_client_order_seq_{1};

    std::uint64_t processed_intent_count_{0};
    std::uint64_t processed_transport_update_count_{0};
    std::uint64_t rejected_intent_count_{0};

    [[nodiscard]] OmsProcessCode process_one_transport_update() noexcept;

    [[nodiscard]] OmsProcessCode process_one_shard_intent() noexcept;

    [[nodiscard]] OmsProcessCode process_submission(OmsSubmission submission) noexcept;

    [[nodiscard]] OmsProcessCode process_intent(OrderIntent intent) noexcept;

    [[nodiscard]] OmsProcessCode process_cancel_intent(CancelIntent intent) noexcept;

    [[nodiscard]] OmsProcessCode process_modify_intent(ModifyIntent intent) noexcept;

    [[nodiscard]] IntentDecision make_transport_reject(
        const OrderIntent& intent,
        internal::TimestampNs decision_ts_ns) const;

    [[nodiscard]] bool emit_intent_decision(const IntentDecision& decision) noexcept;

    [[nodiscard]] bool emit_lifecycle_event(const OrderLifecycleEvent& event) noexcept;

    void emit_audit(const predex::core::audit::AuditEvent& event) noexcept;

    [[nodiscard]] static IntentOrigin extract_origin(const IntentDecision& decision);

    [[nodiscard]] std::optional<std::size_t> shard_index_for_origin(
        const IntentOrigin& origin) const noexcept;

    [[nodiscard]] ClientOrderId make_client_order_id(OmsRequestId oms_request_id);

    void insert_live_order(const AcceptedIntent& accepted_intent,
                           internal::TimestampNs decision_ts_ns);

    [[nodiscard]] bool apply_transport_update(const OrderLifecycleEvent& event);

    [[nodiscard]] OrderState* find_order_state(const OrderLifecycleEvent& event);

    [[nodiscard]] OrderState* find_order_for_action(std::optional<OmsRequestId> oms_request_id,
                                                    const ClientOrderId& client_order_id,
                                                    const std::optional<ExchangeOrderId>&
                                                        exchange_order_id);

    [[nodiscard]] GlobalRiskState make_risk_state_for_event(
        internal::EventId event_id) const noexcept;

    void on_intent_accepted_risk(const AcceptedIntent& accepted_intent) noexcept;

    void on_fill_risk(internal::EventId event_id, internal::QtyLots fill_qty_lots) noexcept;

    void on_order_terminal_risk(internal::EventId event_id,
                                internal::QtyLots remaining_open_qty_lots) noexcept;
};

} // namespace predex::core::oms::kalshi
