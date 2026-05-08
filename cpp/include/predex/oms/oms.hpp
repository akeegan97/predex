#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "predex/audit/audit_types.hpp"
#include "predex/oms/execution_transport.hpp"
#include "predex/oms/global_risk.hpp"
#include "predex/oms/oms_types.hpp"
#include "predex/oms/order_store.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::oms::kalshi {

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

    explicit Oms(
        std::vector<ShardRequestQueue*> shard_request_queues,
        std::vector<ShardDecisionQueue*> shard_decision_queues,
        std::vector<ShardLifecycleQueue*> shard_lifecycle_queues,
        ExecutionTransportQueues transport_queues = {}, GlobalRiskLimits global_risk_limits = {},
        utils::SPSCQueue<predex::core::audit::AuditEvent>* audit_queue = nullptr,
        std::function<std::optional<std::string>(internal::MarketId)> market_ticker_resolver = {});

    [[nodiscard]] OmsPumpResult pump(std::size_t max_kalshi_events,
                                     std::size_t max_shard_requests) noexcept;

    // Seeds an already-live order into canonical OMS state before the main OMS
    // loop begins, typically from startup reconciliation.
    OmsRequestId seed_reconciled_order(OrderState state) noexcept;

    void request_soft_halt() noexcept;
    void request_hard_halt() noexcept;

    [[nodiscard]] bool is_halted() const noexcept;
    [[nodiscard]] std::size_t live_order_count() const noexcept;
    [[nodiscard]] std::uint64_t processed_shard_request_count() const noexcept;
    [[nodiscard]] std::uint64_t processed_kalshi_event_count() const noexcept;
    [[nodiscard]] std::uint64_t emitted_decision_count() const noexcept;
    [[nodiscard]] std::uint64_t emitted_transport_count() const noexcept;
    [[nodiscard]] std::uint64_t emitted_lifecycle_count() const noexcept;
    [[nodiscard]] std::uint64_t rejected_decision_count() const noexcept;

  private:
    GlobalRisk global_risk_;
    OrderStore order_store_;
    ExecutionTransport transport_;
    utils::SPSCQueue<predex::core::audit::AuditEvent>* audit_queue_{nullptr};
    std::function<std::optional<std::string>(internal::MarketId)> market_ticker_resolver_;

    std::vector<ShardRequestQueue*> shard_request_queues_;
    std::vector<ShardDecisionQueue*> shard_decision_queues_;
    std::vector<ShardLifecycleQueue*> shard_lifecycle_queues_;

    std::size_t next_shard_index_{0};
    OmsRequestId next_oms_request_id_{1};
    std::uint64_t next_client_order_seq_{1};
    internal::TimestampNs client_order_session_nonce_{0};

    std::uint64_t processed_shard_request_count_{0};
    std::uint64_t processed_kalshi_event_count_{0};
    std::uint64_t emitted_decision_count_{0};
    std::uint64_t emitted_transport_count_{0};
    std::uint64_t emitted_lifecycle_count_{0};
    std::uint64_t rejected_decision_count_{0};

    std::atomic<std::uint8_t> halt_mode_{0};
    bool hard_halt_cancel_triggered_{false};

    [[nodiscard]] OmsProcessCode process_one_shard_request() noexcept;
    [[nodiscard]] OmsProcessCode process_one_kalshi_event() noexcept;

    [[nodiscard]] OmsProcessCode handle_shard_request(const ShardOmsRequest& request) noexcept;
    [[nodiscard]] OmsProcessCode handle_new_order_intent(
        const NewOrderIntent& intent,
        std::optional<GroupExecutionPolicy> group_execution_policy = std::nullopt) noexcept;
    [[nodiscard]] OmsProcessCode handle_group_order_intent(const GroupOrderIntent& intent) noexcept;
    [[nodiscard]] OmsProcessCode
    handle_cancel_order_intent(const CancelOrderIntent& intent) noexcept;
    [[nodiscard]] OmsProcessCode
    handle_modify_order_intent(const ModifyOrderIntent& intent) noexcept;
    [[nodiscard]] bool
    modify_preserves_immutable_fields(const OrderState& order_state,
                                      const NewOrderIntent& replacement) const noexcept;

    [[nodiscard]] OmsProcessCode handle_kalshi_event(const SourcedKalshiEvent& event) noexcept;

    [[nodiscard]] std::optional<ShardOrderCorrelation>
    resolve_correlation(const OmsOrderRef& order) const noexcept;
    [[nodiscard]] std::optional<OrderStore::LookupKey>
    make_lookup_key(const CancelOrderIntent& intent) const noexcept;
    [[nodiscard]] std::optional<OrderStore::LookupKey>
    make_lookup_key(const ModifyOrderIntent& intent) const noexcept;

    [[nodiscard]] OmsOrderRef make_order_ref();
    [[nodiscard]] ClientOrderId make_client_order_id(OmsRequestId oms_request_id) noexcept;

    [[nodiscard]] bool emit_kalshi_command(const OmsToKalshiCommand& command) noexcept;
    [[nodiscard]] bool emit_shard_decision(const OmsToShardDecision& decision,
                                           std::uint16_t shard_id) noexcept;
    [[nodiscard]] bool emit_shard_lifecycle(const OmsToShardLifecycleEvent& event,
                                            std::uint16_t shard_id) noexcept;
    void emit_audit(const predex::core::audit::AuditEvent& event) noexcept;
    void emit_decision_audit(const IntentContext& context, OmsRequestId oms_request_id,
                             internal::TimestampNs decision_ts_ns, std::uint8_t decision_code,
                             std::uint8_t reject_reason, internal::QtyLots qty_lots) noexcept;
    void emit_transport_audit(const ShardOrderCorrelation& corr,
                              internal::TimestampNs oms_decision_ts_ns,
                              internal::TimestampNs transport_submit_ts_ns,
                              internal::TimestampNs transport_response_ts_ns,
                              std::uint8_t decision_code, std::uint16_t transport_http_status,
                              std::uint16_t transport_retry_count, internal::QtyLots qty_lots,
                              internal::PriceTicks price_ticks) noexcept;
    void emit_lifecycle_audit(const ShardOrderCorrelation& corr,
                              internal::TimestampNs lifecycle_ts_ns, std::uint8_t lifecycle_kind,
                              std::uint8_t order_status, std::uint8_t reject_reason,
                              internal::QtyLots qty_lots, internal::PriceTicks price_ticks,
                              internal::TimestampNs first_fill_ts_ns,
                              internal::TimestampNs terminal_ts_ns) noexcept;

    [[nodiscard]] std::optional<std::size_t> shard_index_for(std::uint16_t shard_id) const noexcept;
};

} // namespace predex::core::oms::kalshi
