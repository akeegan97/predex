#include "predex/strategy/strategy.hpp"

#include <algorithm>
#include <chrono>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <unordered_set>

#include "predex/utils/monotonic_clock.hpp"

namespace {

[[nodiscard]] std::uint64_t wall_clock_now_s() noexcept {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
                                          std::chrono::system_clock::now().time_since_epoch())
                                          .count());
}

[[nodiscard]] bool terminal_group_state(predex::oms::GroupExecutionState state) noexcept {
    using State = predex::oms::GroupExecutionState;
    return state == State::kCOMPLETED || state == State::kFLATTENED || state == State::kABORTED ||
           state == State::kREJECTED;
}

[[nodiscard]] bool unsafe_group_state(predex::oms::GroupExecutionState state) noexcept {
    using State = predex::oms::GroupExecutionState;
    return state == State::kREPAIR_REQUIRED || state == State::kUNCERTAIN ||
           state == State::kREPAIR_FAILED;
}

} // namespace

namespace predex::strategy {

MonotonicArbStrategy::MonotonicArbStrategy(std::uint16_t strategy_index, //NOLINT -- suppress warning for easily swapped params
                                           std::uint64_t active_universe_version,
                                           MonotonicArbStrategyConfig config, StrategyQueues queues)
    : strategy_index_(strategy_index), active_universe_version_(active_universe_version),
      config_(config), queues_(std::move(queues)) {
    if (active_universe_version_ == 0 || config_.strategy_id == 0 ||
        config_.maximum_observation_age_ns == 0 ||
        !valid_monotonic_arb_config(config_.arb_config) ||
        queues_.strategy_to_oms_queue == nullptr || queues_.oms_to_strategy_queue == nullptr ||
        queues_.shard_inputs.empty()) {
        throw std::invalid_argument("invalid monotonic arbitrage strategy configuration");
    }

    std::unordered_set<std::uint32_t> shard_indices;
    shard_input_stats_.reserve(queues_.shard_inputs.size());
    for (const auto& input : queues_.shard_inputs) {
        if (input.queue == nullptr || !shard_indices.insert(input.shard_index).second) {
            throw std::invalid_argument("invalid or duplicate strategy shard input");
        }
        shard_input_stats_.push_back(StrategyShardInputStats{.shard_index = input.shard_index});
    }
}

StrategyPumpResult MonotonicArbStrategy::pump_once() noexcept {
    oms::OmsToStrategyMessage oms_message{};
    if (queues_.oms_to_strategy_queue->try_pop(oms_message)) {
        return handle_oms_message(oms_message);
    }

    for (std::size_t offset = 0; offset < queues_.shard_inputs.size(); ++offset) {
        const auto input_index = (next_input_index_ + offset) % queues_.shard_inputs.size();
        ShardToStrategyMessage message{};
        if (!queues_.shard_inputs[input_index].queue->try_pop(message)) {
            continue;
        }
        next_input_index_ = (input_index + 1) % queues_.shard_inputs.size();
        return dispatch_message(input_index, message, utils::monotonic_now_ns());
    }
    return StrategyPumpResult{};
}

std::uint16_t MonotonicArbStrategy::strategy_index() const noexcept { return strategy_index_; }

std::uint64_t MonotonicArbStrategy::active_universe_version() const noexcept {
    return active_universe_version_;
}

const StrategyStats& MonotonicArbStrategy::stats() const noexcept { return stats_; }

const std::vector<StrategyShardInputStats>&
MonotonicArbStrategy::shard_input_stats() const noexcept {
    return shard_input_stats_;
}

StrategyPumpResult //NOLINTNEXTLINE - bugprone-exception-escape guaranteed to not throw regardless of std::visits throw-ness
MonotonicArbStrategy::dispatch_message(std::size_t input_index,
                                       const ShardToStrategyMessage& message,
                                       std::uint64_t dequeue_timestamp_ns) noexcept {
    ++stats_.messages_seen;
    ++shard_input_stats_[input_index].messages_seen;
    
    assert(!message.valueless_by_exception());

    return std::visit(
        [this, input_index, dequeue_timestamp_ns](const auto& item) noexcept {
            return handle_message(input_index, item, dequeue_timestamp_ns);
        },
        message);
}

bool MonotonicArbStrategy::message_matches_input(std::size_t input_index,
                                                 std::uint32_t message_shard_index, //NOLINT -- suppress warning for easily swapped params
                                                 std::uint64_t message_universe_version,
                                                 StrategyPumpResult& result) noexcept {
    result.source_shard_index = message_shard_index;
    if (input_index >= queues_.shard_inputs.size() ||
        queues_.shard_inputs[input_index].shard_index != message_shard_index) {
        result.code = StrategyPumpCode::kMESSAGE_REJECTED;
        result.message_reject_reason = StrategyMessageRejectReason::kSHARD_INDEX_MISMATCH;
    } else if (message_universe_version != active_universe_version_) {
        result.code = StrategyPumpCode::kMESSAGE_REJECTED;
        result.message_reject_reason = StrategyMessageRejectReason::kUNIVERSE_VERSION_MISMATCH;
    } else {
        return true;
    }
    ++stats_.message_rejects;
    ++shard_input_stats_[input_index].message_rejects;
    return false;
}

StrategyPumpResult
MonotonicArbStrategy::handle_message(std::size_t input_index,
                                     const MonotonicPairObservation& observation,
                                     std::uint64_t dequeue_timestamp_ns) noexcept {
    StrategyPumpResult result{};
    if (!message_matches_input(input_index, observation.shard_index, observation.universe_version,
                               result)) {
        return result;
    }

    ++shard_input_stats_[input_index].observations_seen;
    utils::record_elapsed_ns(shard_input_stats_[input_index].shard_to_strategy_latency,
                             observation.publish_timestamp_ns, dequeue_timestamp_ns);
    utils::record_elapsed_ns(shard_input_stats_[input_index].ingress_to_strategy_latency,
                             observation.ingress_timestamp_ns, dequeue_timestamp_ns);
    utils::record_elapsed_ns(stats_.shard_to_strategy_latency, observation.publish_timestamp_ns,
                             dequeue_timestamp_ns);

    if (observation.ingress_timestamp_ns == 0 ||
        observation.ingress_timestamp_ns > observation.book_apply_timestamp_ns ||
        observation.book_apply_timestamp_ns > observation.publish_timestamp_ns ||
        observation.publish_timestamp_ns > dequeue_timestamp_ns) {
        result.code = StrategyPumpCode::kMESSAGE_REJECTED;
        result.message_reject_reason = StrategyMessageRejectReason::kINVALID_TIMESTAMP_ORDER;
        ++stats_.message_rejects;
        ++shard_input_stats_[input_index].message_rejects;
        return result;
    }
    if (dequeue_timestamp_ns - observation.publish_timestamp_ns >
        config_.maximum_observation_age_ns) {
        result.code = StrategyPumpCode::kMESSAGE_REJECTED;
        result.message_reject_reason = StrategyMessageRejectReason::kOBSERVATION_TOO_OLD;
        ++stats_.message_rejects;
        ++stats_.stale_observations;
        ++shard_input_stats_[input_index].message_rejects;
        return result;
    }

    const auto unavailable_it = unavailable_event_revision_.find(observation.event_id);
    if (unavailable_it != unavailable_event_revision_.end() &&
        observation.event_revision <= unavailable_it->second) {
        result.code = StrategyPumpCode::kMESSAGE_REJECTED;
        result.message_reject_reason = StrategyMessageRejectReason::kNON_MONOTONIC_EVENT_REVISION;
        ++stats_.message_rejects;
        ++shard_input_stats_[input_index].message_rejects;
        return result;
    }

    auto [revision_it, inserted] =
        last_pair_revision_.try_emplace(observation.easier.market_id, observation.event_revision);
    if (!inserted) {
        if (observation.event_revision <= revision_it->second) {
            result.code = StrategyPumpCode::kMESSAGE_REJECTED;
            result.message_reject_reason =
                StrategyMessageRejectReason::kNON_MONOTONIC_EVENT_REVISION;
            ++stats_.message_rejects;
            ++shard_input_stats_[input_index].message_rejects;
            return result;
        }
        revision_it->second = observation.event_revision;
    }

    if (execution_blocked_) {
        result.code = StrategyPumpCode::kCANDIDATE_SUPPRESSED;
        result.intent_suppress_reason = StrategyIntentSuppressReason::kEXECUTION_BLOCKED;
        ++stats_.candidates_suppressed;
        return result;
    }
    if (active_group_by_event_.contains(observation.event_id)) {
        result.code = StrategyPumpCode::kCANDIDATE_SUPPRESSED;
        result.intent_suppress_reason = StrategyIntentSuppressReason::kEVENT_GROUP_ACTIVE;
        ++stats_.candidates_suppressed;
        return result;
    }

    const auto evaluation_start = utils::monotonic_now_ns();
    const auto evaluation =
        evaluate_monotonic_arb(config_.arb_config, observation, wall_clock_now_s());
    const auto evaluation_complete = utils::monotonic_now_ns();
    ++stats_.observations_evaluated;
    utils::record_elapsed_ns(stats_.evaluation_latency, evaluation_start, evaluation_complete);
    utils::record_elapsed_ns(stats_.ingress_to_evaluation_latency, observation.ingress_timestamp_ns,
                             evaluation_complete);

    result.evaluation_reason = evaluation.reason;
    if (!evaluation.accepted()) {
        result.code = StrategyPumpCode::kOBSERVATION_REJECTED;
        const auto index = static_cast<std::size_t>(evaluation.reason);
        if (index < evaluation_rejections.size()) {
            ++evaluation_rejections[index];
        }
        return result;
    }

    result.candidate = evaluation.candidate;
    
    ++stats_.candidates_found;
    
    const auto& candidate = *evaluation.candidate; //NOLINT - bugprone-unchecked-optional-access checked above with .accepted() 211

    const auto required_capital = required_capital_ticks(candidate);
    if (!required_capital.has_value()) {
        result.code = StrategyPumpCode::kCANDIDATE_SUPPRESSED;
        result.intent_suppress_reason = StrategyIntentSuppressReason::kINSUFFICIENT_CAPITAL;
        ++stats_.candidates_suppressed;
        return result;
    }
    if (!portfolio_seen_) {
        result.code = StrategyPumpCode::kCANDIDATE_SUPPRESSED;
        result.intent_suppress_reason = StrategyIntentSuppressReason::kPORTFOLIO_UNKNOWN;
        ++stats_.candidates_suppressed;
        return result;
    }
    if (!portfolio_reconciled_) {
        result.code = StrategyPumpCode::kCANDIDATE_SUPPRESSED;
        result.intent_suppress_reason = StrategyIntentSuppressReason::kPORTFOLIO_NOT_RECONCILED;
        ++stats_.candidates_suppressed;
        return result;
    }
    const auto available = std::max<std::int64_t>(0, available_capital_ticks_);
    if (pending_capital_ticks_ > static_cast<std::uint64_t>(available) ||
        *required_capital > static_cast<std::uint64_t>(available) - pending_capital_ticks_) {
        result.code = StrategyPumpCode::kCANDIDATE_SUPPRESSED;
        result.intent_suppress_reason = StrategyIntentSuppressReason::kINSUFFICIENT_CAPITAL;
        ++stats_.candidates_suppressed;
        return result;
    }

    const auto publish_timestamp = utils::monotonic_now_ns();
    auto intent =
        make_group_intent(candidate, dequeue_timestamp_ns, evaluation_complete, publish_timestamp);
    if (!intent.has_value()) {
        result.code = StrategyPumpCode::kCANDIDATE_SUPPRESSED;
        result.intent_suppress_reason = StrategyIntentSuppressReason::kIDENTITY_EXHAUSTED;
        ++stats_.candidates_suppressed;
        execution_blocked_ = true;
        return result;
    }

    const auto group_intent_id = intent->context.group_intent_id;
    if (!queues_.strategy_to_oms_queue->try_push(oms::intent::StrategyIntent{*intent})) {
        result.code = StrategyPumpCode::kINTENT_BACKPRESSURE;
        ++stats_.intent_backpressure;
        return result;
    }

    active_group_by_event_[candidate.event_id] = group_intent_id;
    active_group_by_id_[group_intent_id] = ActiveGroup{
        .event_id = candidate.event_id,
        .pending_capital_ticks = *required_capital,
        .admission_pending = true,
    };
    pending_capital_ticks_ += *required_capital;
    ++stats_.intents_published;
    result.code = StrategyPumpCode::kINTENT_PUBLISHED;
    result.published_intent = intent;
    return result;
}

StrategyPumpResult
MonotonicArbStrategy::handle_message(std::size_t input_index,
                                     const StrategyEventUnavailable& message,
                                     std::uint64_t /*dequeue_timestamp_ns*/) noexcept {
    StrategyPumpResult result{};
    if (!message_matches_input(input_index, message.shard_index, message.universe_version,
                               result)) {
        return result;
    }
    ++stats_.event_unavailable_seen;
    ++shard_input_stats_[input_index].event_unavailable_seen;
    auto& revision = unavailable_event_revision_[message.event_id];
    revision = std::max(revision, message.event_revision);
    result.code = StrategyPumpCode::kEVENT_UNAVAILABLE;
    return result;
}

StrategyPumpResult
MonotonicArbStrategy::handle_message(std::size_t input_index,
                                     const StrategyShardUnavailable& message,
                                     std::uint64_t /*dequeue_timestamp_ns*/) noexcept {
    StrategyPumpResult result{};
    if (!message_matches_input(input_index, message.shard_index, message.universe_version,
                               result)) {
        return result;
    }
    ++stats_.shard_unavailable_seen;
    ++shard_input_stats_[input_index].shard_unavailable_seen;
    result.code = StrategyPumpCode::kSHARD_UNAVAILABLE;
    return result;
}

StrategyPumpResult
MonotonicArbStrategy::handle_oms_message(const oms::OmsToStrategyMessage& message) noexcept { //NOLINT -- suppress warning for cognitively complex function due to the lambda call inside
    StrategyPumpResult result{.code = StrategyPumpCode::kOMS_MESSAGE_HANDLED};
    ++stats_.oms_messages_seen;

    std::visit(
        [this, &result](const auto& item) { //NOLINT -- suppress warning for cognitively complex lambda
            using T = std::decay_t<decltype(item)>;
            if constexpr (std::is_same_v<T, oms::StrategyPortfolioUpdate>) {
                if (item.strategy_index != strategy_index_) {
                    result.code = StrategyPumpCode::kMESSAGE_REJECTED;
                    result.message_reject_reason =
                        StrategyMessageRejectReason::kOMS_STRATEGY_INDEX_MISMATCH;
                    ++stats_.message_rejects;
                    return;
                }
                if (item.sequence <= last_portfolio_sequence_) {
                    result.code = StrategyPumpCode::kMESSAGE_REJECTED;
                    result.message_reject_reason = StrategyMessageRejectReason::kSTALE_OMS_SEQUENCE;
                    ++stats_.message_rejects;
                    return;
                }
                last_portfolio_sequence_ = item.sequence;
                portfolio_seen_ = true;
                portfolio_reconciled_ = item.venue_portfolio_reconciled;
                available_capital_ticks_ = item.available_capital_ticks;
                for (auto& [_, group] : active_group_by_id_) {
                    if (!group.admission_pending && group.pending_capital_ticks > 0) {
                        pending_capital_ticks_ -=
                            std::min(pending_capital_ticks_, group.pending_capital_ticks);
                        group.pending_capital_ticks = 0;
                    }
                }
            } else if constexpr (std::is_same_v<T, oms::StrategyMarketPositionUpdate>) {
                if (item.strategy_index != strategy_index_) {
                    result.code = StrategyPumpCode::kMESSAGE_REJECTED;
                    result.message_reject_reason =
                        StrategyMessageRejectReason::kOMS_STRATEGY_INDEX_MISMATCH;
                    ++stats_.message_rejects;
                } else if (item.sequence <= last_portfolio_sequence_) {
                    result.code = StrategyPumpCode::kMESSAGE_REJECTED;
                    result.message_reject_reason = StrategyMessageRejectReason::kSTALE_OMS_SEQUENCE;
                    ++stats_.message_rejects;
                } else {
                    last_portfolio_sequence_ = item.sequence;
                }
            } else if constexpr (std::is_same_v<T, oms::GroupAdmissionResponse>) {
                if (item.context.strategy_index != strategy_index_) {
                    result.code = StrategyPumpCode::kMESSAGE_REJECTED;
                    result.message_reject_reason =
                        StrategyMessageRejectReason::kOMS_STRATEGY_INDEX_MISMATCH;
                    ++stats_.message_rejects;
                    return;
                }
                auto group_it = active_group_by_id_.find(item.context.group_intent_id);
                if (group_it == active_group_by_id_.end()) {
                    return;
                }
                if (item.admission_state == oms::GroupAdmissionState::kACCEPTED) {
                    group_it->second.admission_pending = false;
                    ++stats_.group_admissions_accepted;
                    return;
                }
                pending_capital_ticks_ -=
                    std::min(pending_capital_ticks_, group_it->second.pending_capital_ticks);
                active_group_by_event_.erase(group_it->second.event_id);
                active_group_by_id_.erase(group_it);
                ++stats_.group_admissions_rejected;
            } else if constexpr (std::is_same_v<T, oms::GroupStateUpdate>) {
                if (item.context.strategy_index != strategy_index_) {
                    result.code = StrategyPumpCode::kMESSAGE_REJECTED;
                    result.message_reject_reason =
                        StrategyMessageRejectReason::kOMS_STRATEGY_INDEX_MISMATCH;
                    ++stats_.message_rejects;
                    return;
                }
                auto group_it = active_group_by_id_.find(item.context.group_intent_id);
                if (group_it == active_group_by_id_.end()) {
                    return;
                }
                if (unsafe_group_state(item.execution_state)) {
                    execution_blocked_ = true;
                    ++stats_.unsafe_groups_seen;
                    return;
                }
                if (terminal_group_state(item.execution_state)) {
                    pending_capital_ticks_ -=
                        std::min(pending_capital_ticks_, group_it->second.pending_capital_ticks);
                    active_group_by_event_.erase(group_it->second.event_id);
                    active_group_by_id_.erase(group_it);
                    ++stats_.terminal_groups_seen;
                }
            } else if constexpr (std::is_same_v<T, oms::OmsResponse> ||
                                 std::is_same_v<T, oms::OrderStateUpdate>) {
                if (item.context.context.strategy_index != strategy_index_) {
                    result.code = StrategyPumpCode::kMESSAGE_REJECTED;
                    result.message_reject_reason =
                        StrategyMessageRejectReason::kOMS_STRATEGY_INDEX_MISMATCH;
                    ++stats_.message_rejects;
                }
            }
        },
        message);
    return result;
}

std::optional<std::uint64_t> MonotonicArbStrategy::required_capital_ticks(
    const MonotonicArbCandidate& candidate) const noexcept {
    using Wide = unsigned __int128;
    const auto leg_exposure = [](std::uint64_t quantity_lots) {
        const Wide product = static_cast<Wide>(quantity_lots) * kPriceTicksPerDollar;
        return product / kQuantityLotsPerContract +
               static_cast<Wide>(product % kQuantityLotsPerContract != 0);
    };
    const Wide exposure = leg_exposure(candidate.easier_leg.quantity_lots) +
                          leg_exposure(candidate.harder_leg.quantity_lots);
    if (exposure > std::numeric_limits<std::uint64_t>::max()) {
        return std::nullopt;
    }
    return static_cast<std::uint64_t>(exposure);
}

std::optional<oms::intent::GroupOrderIntent>
MonotonicArbStrategy::make_group_intent(const MonotonicArbCandidate& candidate,
                                        std::uint64_t strategy_dequeue_timestamp_ns,
                                        std::uint64_t evaluation_complete_timestamp_ns,
                                        std::uint64_t intent_publish_timestamp_ns) noexcept {
    if (next_group_intent_id_ == 0 || next_strategy_intent_id_ == 0 ||
        next_strategy_intent_id_ > std::numeric_limits<std::uint32_t>::max() - 1ULL ||
        next_signal_id_ == 0 || next_signal_id_ > std::numeric_limits<std::uint32_t>::max() ||
        candidate.easier_leg.limit_price_ticks >
            static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
        candidate.harder_leg.limit_price_ticks >
            static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
        candidate.easier_leg.quantity_lots >
            static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
        candidate.harder_leg.quantity_lots >
            static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
        return std::nullopt;
    }

    const auto group_id = next_group_intent_id_++;
    const auto signal_id = static_cast<std::uint32_t>(next_signal_id_++);
    const auto first_intent_id = static_cast<std::uint32_t>(next_strategy_intent_id_++);
    const auto second_intent_id = static_cast<std::uint32_t>(next_strategy_intent_id_++);

    oms::intent::IntentContext group_context{
        .strategy_index = strategy_index_,
        .market_id = candidate.easier_leg.market_id,
        .event_id = candidate.event_id,
        .strategy_id = config_.strategy_id,
        .signal_id = signal_id,
        .group_intent_id = group_id,
        .leg_count = 2,
        .universe_version = candidate.universe_version,
        .event_revision = candidate.event_revision,
        .source_shard_index = candidate.shard_index,
        .ingress_timestamp_ns = candidate.ingress_timestamp_ns,
        .book_apply_timestamp_ns = candidate.book_apply_timestamp_ns,
        .observation_publish_timestamp_ns = candidate.observation_publish_timestamp_ns,
        .strategy_dequeue_timestamp_ns = strategy_dequeue_timestamp_ns,
        .evaluation_complete_timestamp_ns = evaluation_complete_timestamp_ns,
        .intent_publish_timestamp_ns = intent_publish_timestamp_ns,
    };

    auto make_leg = [&](const MonotonicArbLeg& candidate_leg, 
        std::uint8_t leg_index,//NOLINT -- suppress warning for easily swapped params 
        std::uint32_t strategy_intent_id
    ) {
        auto context = group_context;
        context.market_id = candidate_leg.market_id;
        context.strategy_intent_id = strategy_intent_id;
        context.leg_index = leg_index;
        return oms::intent::NewOrderIntent{
            .context = context,
            .outcome = oms::intent::Outcome::kYES,
            .action = candidate_leg.action == ArbAction::kBUY_YES ? oms::intent::OrderAction::kBUY
                                                                  : oms::intent::OrderAction::kSELL,
            .liquidity_intent = oms::intent::LiquidityIntent::kTAKER,
            .order_type = oms::intent::OrderType::kMARKETABLE_LIMIT,
            .time_in_force = oms::intent::TimeInForce::kFOK,
            .price_ticks = static_cast<std::int64_t>(candidate_leg.limit_price_ticks),
            .quantity_lots = static_cast<std::int64_t>(candidate_leg.quantity_lots),
        };
    };

    oms::intent::GroupOrderIntent intent{
        .context = group_context,
        .leg_count = 2,
        .admission_policy = oms::intent::GroupAdmissionPolicy::kALL_OR_NONE,
        .expected_gross_edge_ticks = candidate.gross_edge_ticks,
        .estimated_fee_ticks = candidate.estimated_fee_ticks,
        .expected_net_edge_ticks = candidate.net_edge_ticks,
    };
    intent.new_orders[0] = make_leg(candidate.easier_leg, 0, first_intent_id);
    intent.new_orders[1] = make_leg(candidate.harder_leg, 1, second_intent_id);
    return intent;
}

} // namespace predex::strategy
