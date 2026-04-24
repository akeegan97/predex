#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include "predex/oms2/execution_transport.hpp"
#include "predex/oms2/global_risk.hpp"
#include "predex/oms2/order_store.hpp"
#include "predex/oms2/oms_types.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::oms2::kalshi {

enum class HaltMode : std::uint8_t {
    kNone = 0,
    kSoft = 1,
    kHard = 2,
};

enum class OmsProcessCode : std::uint8_t {
    kIdle = 0,
    kProcessedShardRequest = 1,
    kProcessedKalshiEvent = 2,
    kShardBackpressure = 3,
    kVenueBackpressure = 4,
    kError = 5,
};

struct OmsPumpResult {
    OmsProcessCode code{OmsProcessCode::kIdle};
    std::uint32_t processed_shard_requests{0};
    std::uint32_t processed_kalshi_events{0};
};

// OMS2 remains a single-writer coordinator. It owns canonical order state and
// sequences transitions across the surrounding seams:
// shard -> OMS -> risk -> OMS -> Kalshi -> OMS -> shard.
class Oms {
  public:
    using ShardRequestQueue = utils::SPSCQueue<ShardOmsRequest>;
    using ShardDecisionQueue = utils::SPSCQueue<OmsToShardDecision>;
    using ShardLifecycleQueue = utils::SPSCQueue<OmsToShardLifecycleEvent>;

    explicit Oms(std::vector<ShardRequestQueue*> shard_request_queues,
                 std::vector<ShardDecisionQueue*> shard_decision_queues,
                 std::vector<ShardLifecycleQueue*> shard_lifecycle_queues,
                 ExecutionTransportQueues transport_queues = {},
                 GlobalRiskLimits global_risk_limits = {});

    [[nodiscard]] OmsPumpResult pump(std::size_t max_kalshi_events,
                                     std::size_t max_shard_requests) noexcept;

    // Seeds an already-live order into canonical OMS state before the main OMS
    // loop begins, typically from startup reconciliation.
    OmsRequestId seed_reconciled_order(OrderState state) noexcept;

    void request_soft_halt() noexcept;
    void request_hard_halt() noexcept;

    [[nodiscard]] bool is_halted() const noexcept;
    [[nodiscard]] std::size_t live_order_count() const noexcept;

  private:
    GlobalRisk global_risk_;
    OrderStore order_store_{};
    ExecutionTransport transport_;

    std::vector<ShardRequestQueue*> shard_request_queues_;
    std::vector<ShardDecisionQueue*> shard_decision_queues_;
    std::vector<ShardLifecycleQueue*> shard_lifecycle_queues_;

    std::size_t next_shard_index_{0};
    OmsRequestId next_oms_request_id_{1};
    std::uint64_t next_client_order_seq_{1};

    std::uint64_t processed_shard_request_count_{0};
    std::uint64_t processed_kalshi_event_count_{0};

    std::atomic<std::uint8_t> halt_mode_{0};
    bool hard_halt_cancel_triggered_{false};

    [[nodiscard]] OmsProcessCode process_one_shard_request() noexcept;
    [[nodiscard]] OmsProcessCode process_one_kalshi_event() noexcept;

    [[nodiscard]] OmsProcessCode handle_shard_request(const ShardOmsRequest& request) noexcept;
    [[nodiscard]] OmsProcessCode handle_new_order_intent(
        const NewOrderIntent& intent) noexcept;
    [[nodiscard]] OmsProcessCode handle_group_order_intent(
        const GroupOrderIntent& intent) noexcept;
    [[nodiscard]] OmsProcessCode handle_cancel_order_intent(
        const CancelOrderIntent& intent) noexcept;
    [[nodiscard]] OmsProcessCode handle_modify_order_intent(
        const ModifyOrderIntent& intent) noexcept;
    [[nodiscard]] bool modify_preserves_immutable_fields(
        const OrderState& order_state,
        const NewOrderIntent& replacement) const noexcept;

    [[nodiscard]] OmsProcessCode handle_kalshi_event(const SourcedKalshiEvent& event) noexcept;

    [[nodiscard]] std::optional<ShardOrderCorrelation> resolve_correlation(
        const OmsOrderRef& order) const noexcept;
    [[nodiscard]] std::optional<OrderStore::LookupKey> make_lookup_key(
        const CancelOrderIntent& intent) const noexcept;
    [[nodiscard]] std::optional<OrderStore::LookupKey> make_lookup_key(
        const ModifyOrderIntent& intent) const noexcept;

    [[nodiscard]] OmsOrderRef make_order_ref();
    [[nodiscard]] ClientOrderId make_client_order_id(OmsRequestId oms_request_id);

    [[nodiscard]] bool emit_kalshi_command(const OmsToKalshiCommand& command) noexcept;
    [[nodiscard]] bool emit_shard_decision(const OmsToShardDecision& decision,
                                           std::uint16_t shard_id) noexcept;
    [[nodiscard]] bool emit_shard_lifecycle(const OmsToShardLifecycleEvent& event,
                                            std::uint16_t shard_id) noexcept;

    [[nodiscard]] std::optional<std::size_t> shard_index_for(
        std::uint16_t shard_id) const noexcept;
};

} // namespace predex::core::oms2::kalshi
