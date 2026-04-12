#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

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

    explicit Oms(std::vector<SubmissionQueue*> shard_intent_queues,
                 std::vector<DecisionQueue*> shard_decision_queues,
                 std::vector<LifecycleQueue*> shard_lifecycle_queues,
                 OmsTransportQueues transport_queues = {},
                 GlobalRiskManager global_risk = GlobalRiskManager{});

    [[nodiscard]] OmsPumpResult pump(std::size_t max_transport_updates,
                                     std::size_t max_shard_intents) noexcept;

    [[nodiscard]] const GlobalRiskState& global_risk_state() const noexcept;

    [[nodiscard]] std::size_t live_order_count() const noexcept;

    [[nodiscard]] std::uint64_t processed_intent_count() const noexcept;

    [[nodiscard]] std::uint64_t processed_transport_update_count() const noexcept;

    [[nodiscard]] std::uint64_t rejected_intent_count() const noexcept;
  private:
    GlobalRiskManager global_risk_;
    GlobalRiskState global_risk_state_{};

    std::vector<SubmissionQueue*> shard_intent_queues_;
    std::vector<DecisionQueue*> shard_decision_queues_;
    std::vector<LifecycleQueue*> shard_lifecycle_queues_;
    OmsTransportQueues transport_queues_{};

    std::unordered_map<OmsRequestId, OrderState> orders_by_request_id_;
    std::unordered_map<ClientOrderId, OmsRequestId> request_by_client_order_id_;
    std::unordered_map<ExchangeOrderId, OmsRequestId> request_by_exchange_order_id_;

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

    [[nodiscard]] IntentDecision make_transport_reject(
        const OrderIntent& intent,
        internal::TimestampNs decision_ts_ns) const;

    [[nodiscard]] bool emit_intent_decision(const IntentDecision& decision) noexcept;

    [[nodiscard]] bool emit_lifecycle_event(const OrderLifecycleEvent& event) noexcept;

    [[nodiscard]] static IntentOrigin extract_origin(const IntentDecision& decision);

    [[nodiscard]] std::optional<std::size_t> shard_index_for_origin(
        const IntentOrigin& origin) const noexcept;

    [[nodiscard]] ClientOrderId make_client_order_id(OmsRequestId oms_request_id);

    void insert_live_order(const AcceptedIntent& accepted_intent,
                           internal::TimestampNs decision_ts_ns);

    [[nodiscard]] bool apply_transport_update(const OrderLifecycleEvent& event);

    [[nodiscard]] OrderState* find_order_state(const OrderLifecycleEvent& event);
};

} // namespace predex::core::oms::kalshi
