#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include "predex/audit/audit_types.hpp"
#include "predex/oms/execution_transport.hpp"
#include "predex/oms/order_store.hpp"
#include "predex/oms/oms_types.hpp"
#include "predex/oms/risk_engine.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::oms::kalshi {

enum class HaltMode : std::uint8_t {
    kNone = 0,
    kSoft = 1,  // block new submissions; existing orders survive to fill/settle
    kHard = 2,  // block new submissions + cancel-all (processed on OMS thread)
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

    // OmsTransportQueues is now defined in execution_transport.hpp.
    // app.cpp callers should update their include accordingly.
    explicit Oms(std::vector<SubmissionQueue*> shard_intent_queues,
                 std::vector<DecisionQueue*> shard_decision_queues,
                 std::vector<LifecycleQueue*> shard_lifecycle_queues,
                 OmsTransportQueues transport_queues = {},
                 GlobalRiskManager global_risk = GlobalRiskManager{},
                 AuditQueue* audit_queue = nullptr,
                 std::int64_t max_session_loss_ticks = 0);

    [[nodiscard]] OmsPumpResult pump(std::size_t max_transport_updates,
                                     std::size_t max_shard_intents) noexcept;

    // Seeds an order from a prior session's REST snapshot into the OMS tracking state.
    // Must be called before the OMS thread is started.
    // Returns the assigned synthetic oms_request_id.
    OmsRequestId seed_orphaned_order(OrderState state,
                                     internal::Side side,
                                     Outcome outcome) noexcept;

    // Thread-safe: may be called from any thread.
    // Soft halt blocks new order submission; existing orders survive to fill/settle.
    void request_soft_halt() noexcept;
    // Hard halt blocks new order submission and schedules cancel-all on the OMS thread.
    void request_hard_halt() noexcept;

    // OMS thread only: iterates OrderStore and enqueues CancelOrderCmd for every live order.
    // Returns false if enqueue backpressure prevented one or more cancels.
    [[nodiscard]] bool cancel_all_live_orders() noexcept;

    [[nodiscard]] bool is_halted() const noexcept;
    [[nodiscard]] std::int64_t session_net_ticks() const noexcept;

    [[nodiscard]] const GlobalRiskState& global_risk_state() const noexcept;

    [[nodiscard]] std::size_t live_order_count() const noexcept;

    [[nodiscard]] std::uint64_t processed_intent_count() const noexcept;

    [[nodiscard]] std::uint64_t processed_transport_update_count() const noexcept;

    [[nodiscard]] std::uint64_t rejected_intent_count() const noexcept;

    [[nodiscard]] std::uint64_t unknown_fill_side_count() const noexcept;

  private:
    RiskEngine risk_engine_;
    OrderStore order_store_;
    ExecutionTransport transport_;

    std::vector<SubmissionQueue*> shard_intent_queues_;
    std::vector<DecisionQueue*> shard_decision_queues_;
    std::vector<LifecycleQueue*> shard_lifecycle_queues_;
    AuditQueue* audit_queue_{nullptr};

    std::size_t next_shard_index_{0};
    OmsRequestId next_oms_request_id_{1};
    std::uint64_t next_client_order_seq_{1};

    std::uint64_t processed_intent_count_{0};
    std::uint64_t processed_transport_update_count_{0};
    std::uint64_t rejected_intent_count_{0};
    std::uint64_t unknown_fill_side_count_{0};

    std::atomic<std::uint8_t> halt_mode_{0};
    bool hard_halt_cancel_triggered_{false};
    std::int64_t session_net_ticks_{0};
    std::int64_t max_session_loss_ticks_{0};

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
};

} // namespace predex::core::oms::kalshi
