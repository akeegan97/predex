#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <unordered_map>
#include <vector>

#include "predex/oms/oms_types.hpp"
#include "predex/oms/order_intents.hpp"
#include "predex/strategy/monotonic_arb.hpp"
#include "predex/strategy/strategy_types.hpp"
#include "predex/utils/latency_histogram.hpp"
#include "predex/utils/spsc.hpp"

namespace predex::strategy {

struct StrategyShardInput {
    std::uint32_t shard_index{};
    utils::SPSCQueue<ShardToStrategyMessage>* queue{};
};

struct StrategyQueues {
    std::vector<StrategyShardInput> shard_inputs;
    utils::SPSCQueue<oms::intent::StrategyIntent>* strategy_to_oms_queue{};
    utils::SPSCQueue<oms::OmsToStrategyMessage>* oms_to_strategy_queue{};
};

struct MonotonicArbStrategyConfig {
    std::uint32_t strategy_id{1};
    MonotonicArbConfig arb_config{};
    std::uint64_t maximum_observation_age_ns{};
};

enum class StrategyPumpCode : std::uint8_t {
    kNO_WORK = 0,
    kOBSERVATION_REJECTED,
    kCANDIDATE_FOUND,
    kEVENT_UNAVAILABLE,
    kSHARD_UNAVAILABLE,
    kMESSAGE_REJECTED,
    kOMS_MESSAGE_HANDLED,
    kCANDIDATE_SUPPRESSED,
    kINTENT_PUBLISHED,
    kINTENT_BACKPRESSURE,
};

enum class StrategyMessageRejectReason : std::uint8_t {
    kNONE = 0,
    kSHARD_INDEX_MISMATCH,
    kUNIVERSE_VERSION_MISMATCH,
    kOBSERVATION_TOO_OLD,
    kINVALID_TIMESTAMP_ORDER,
    kNON_MONOTONIC_EVENT_REVISION,
    kOMS_STRATEGY_INDEX_MISMATCH,
    kSTALE_OMS_SEQUENCE,
};

enum class StrategyIntentSuppressReason : std::uint8_t {
    kNONE = 0,
    kPORTFOLIO_UNKNOWN,
    kPORTFOLIO_NOT_RECONCILED,
    kINSUFFICIENT_CAPITAL,
    kEVENT_GROUP_ACTIVE,
    kEXECUTION_BLOCKED,
    kIDENTITY_EXHAUSTED,
};

struct StrategyPumpResult {
    StrategyPumpCode code{StrategyPumpCode::kNO_WORK};

    std::uint32_t source_shard_index{};
    StrategyMessageRejectReason message_reject_reason{
        StrategyMessageRejectReason::kNONE
    };

    MonotonicArbRejectReason evaluation_reason{
        MonotonicArbRejectReason::kNONE
    };
    StrategyIntentSuppressReason intent_suppress_reason{
        StrategyIntentSuppressReason::kNONE
    };

    std::optional<MonotonicArbCandidate> candidate;
    std::optional<oms::intent::GroupOrderIntent> published_intent;
};

struct StrategyShardInputStats {
    std::uint32_t shard_index{};

    std::uint64_t messages_seen{};
    std::uint64_t observations_seen{};
    std::uint64_t event_unavailable_seen{};
    std::uint64_t shard_unavailable_seen{};
    std::uint64_t message_rejects{};

    utils::LatencyHistogram shard_to_strategy_latency{};
    utils::LatencyHistogram ingress_to_strategy_latency{};
};

struct StrategyStats {
    std::uint64_t messages_seen{};
    std::uint64_t observations_evaluated{};
    std::uint64_t candidates_found{};
    std::uint64_t event_unavailable_seen{};
    std::uint64_t shard_unavailable_seen{};
    std::uint64_t message_rejects{};
    std::uint64_t stale_observations{};
    std::uint64_t oms_messages_seen{};
    std::uint64_t intents_published{};
    std::uint64_t intent_backpressure{};
    std::uint64_t candidates_suppressed{};
    std::uint64_t group_admissions_accepted{};
    std::uint64_t group_admissions_rejected{};
    std::uint64_t terminal_groups_seen{};
    std::uint64_t unsafe_groups_seen{};

    utils::LatencyHistogram shard_to_strategy_latency{};
    utils::LatencyHistogram evaluation_latency{};
    utils::LatencyHistogram ingress_to_evaluation_latency{};
};

class MonotonicArbStrategy {
public:
    MonotonicArbStrategy(
        std::uint16_t strategy_index,
        std::uint64_t active_universe_version,
        MonotonicArbStrategyConfig config,
        StrategyQueues queues);

    [[nodiscard]] StrategyPumpResult pump_once() noexcept;

    [[nodiscard]] std::uint16_t strategy_index() const noexcept;
    [[nodiscard]] std::uint64_t active_universe_version() const noexcept;
    [[nodiscard]] const StrategyStats& stats() const noexcept;

    [[nodiscard]] const std::vector<StrategyShardInputStats>&
    shard_input_stats() const noexcept;

private:
    [[nodiscard]] StrategyPumpResult dispatch_message(
        std::size_t input_index,
        const ShardToStrategyMessage& message,
        std::uint64_t dequeue_timestamp_ns) noexcept;

    [[nodiscard]] StrategyPumpResult handle_message(
        std::size_t input_index,
        const MonotonicPairObservation& observation,
        std::uint64_t dequeue_timestamp_ns) noexcept;

    [[nodiscard]] StrategyPumpResult handle_message(
        std::size_t input_index,
        const StrategyEventUnavailable& message,
        std::uint64_t dequeue_timestamp_ns) noexcept;

    [[nodiscard]] StrategyPumpResult handle_message(
        std::size_t input_index,
        const StrategyShardUnavailable& message,
        std::uint64_t dequeue_timestamp_ns) noexcept;

    [[nodiscard]] StrategyPumpResult handle_oms_message(
        const oms::OmsToStrategyMessage& message) noexcept;

    [[nodiscard]] std::optional<oms::intent::GroupOrderIntent>
    make_group_intent(
        const MonotonicArbCandidate& candidate,
        std::uint64_t strategy_dequeue_timestamp_ns,
        std::uint64_t evaluation_complete_timestamp_ns,
        std::uint64_t intent_publish_timestamp_ns) noexcept;

    [[nodiscard]] std::optional<std::uint64_t>
    required_capital_ticks(
        const MonotonicArbCandidate& candidate) const noexcept;

    [[nodiscard]] bool message_matches_input(
        std::size_t input_index,
        std::uint32_t message_shard_index,
        std::uint64_t message_universe_version,
        StrategyPumpResult& result) noexcept;

    std::uint16_t strategy_index_{};
    std::uint64_t active_universe_version_{};

    MonotonicArbStrategyConfig config_;
    StrategyQueues queues_;

    std::size_t next_input_index_{};

    StrategyStats stats_;
    std::vector<StrategyShardInputStats> shard_input_stats_;
    std::array<
    std::uint64_t,
    static_cast<std::size_t>(
        MonotonicArbRejectReason::kCOUNT)>
    evaluation_rejections{};

    struct ActiveGroup {
        EventId event_id{};
        std::uint64_t pending_capital_ticks{};
        bool admission_pending{true};
    };

    bool portfolio_seen_{false};
    bool portfolio_reconciled_{false};
    bool execution_blocked_{false};
    std::int64_t available_capital_ticks_{};
    oms::PortfolioSequence last_portfolio_sequence_{};
    std::uint64_t pending_capital_ticks_{};

    std::uint64_t next_group_intent_id_{1};
    std::uint64_t next_strategy_intent_id_{1};
    std::uint64_t next_signal_id_{1};

    std::unordered_map<MarketId, std::uint64_t> last_pair_revision_;
    std::unordered_map<EventId, std::uint64_t> unavailable_event_revision_;
    std::unordered_map<EventId, oms::intent::GroupIntentId>
        active_group_by_event_;
    std::unordered_map<oms::intent::GroupIntentId, ActiveGroup>
        active_group_by_id_;
};

} // namespace predex::strategy
