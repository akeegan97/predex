#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>

#include "predex/shards/applied_event_update.hpp"
#include "predex/shards/local_risk.hpp"
#include "predex/shards/signal_types.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::shards::kalshi {
constexpr std::size_t kMaxSignalsPerEvent = 16;

enum class PipelineDecisionCode : std::uint8_t {
    kAccepted = 1,
    kDeclined = 2,
    kBackpressure = 3,
    kError = 4,
};

struct PipelineResult {
    PipelineDecisionCode code{PipelineDecisionCode::kDeclined};
    std::uint32_t signal_count{0};
    std::uint32_t accepted_intent_count{0};
};

struct NoopShardPipeline {
    [[nodiscard]] PipelineResult on_event(const AppliedEventUpdate& /*update*/) noexcept {
        return {.code = PipelineDecisionCode::kDeclined};
    }

    [[nodiscard]] std::size_t drain_oms_updates(std::size_t /*max_batch_size*/) noexcept {
        return 0;
    }
};

template <typename LocalRisk, typename... Strategies>
class DefaultShardPipeline {
  public:
    explicit DefaultShardPipeline(
        std::uint16_t shard_id,
        LocalRisk risk,
        utils::SPSCQueue<OmsSubmission>* intent_queue,
        utils::SPSCQueue<predex::core::oms::kalshi::IntentDecision>* decision_queue,
        utils::SPSCQueue<predex::core::oms::kalshi::OrderLifecycleEvent>* lifecycle_queue,
        Strategies... strategies)
        : shard_id_(shard_id),
          risk_(std::move(risk)),
          intent_queue_(intent_queue),
          decision_queue_(decision_queue),
          lifecycle_queue_(lifecycle_queue),
          strategies_(std::move(strategies)...) {}

    [[nodiscard]] PipelineResult on_event(const AppliedEventUpdate& update) noexcept {
        reset_buffers();
        StrategySignalSink signal_sink{*this};
        fan_out_to_strategies(update, signal_sink);

        for (std::size_t signal_index = 0; signal_index < signal_count_; ++signal_index) {
            const Signal& signal = signals_buffer_[signal_index];
            auto candidate_intent = build_candidate_intent(update, signal);
            if (!candidate_intent.has_value()) {
                return {
                    .code = PipelineDecisionCode::kError,
                    .signal_count = static_cast<std::uint32_t>(signal_count_ + group_signal_count_),
                    .accepted_intent_count = static_cast<std::uint32_t>(accepted_submission_leg_count()),
                };
            }

            const RiskDecision risk_decision =
                risk_.evaluate(update, *candidate_intent, risk_state_);
            if (risk_decision.code == RiskDecisionCode::kError) {
                return {
                    .code = PipelineDecisionCode::kError,
                    .signal_count = static_cast<std::uint32_t>(signal_count_ + group_signal_count_),
                    .accepted_intent_count = static_cast<std::uint32_t>(accepted_submission_leg_count()),
                };
            }
            if (!risk_decision.accepted_intent.has_value()) {
                continue;
            }
            if (!try_push_submission(OmsSubmission{*risk_decision.accepted_intent})) {
                return {
                    .code = PipelineDecisionCode::kError,
                    .signal_count = static_cast<std::uint32_t>(signal_count_ + group_signal_count_),
                    .accepted_intent_count = static_cast<std::uint32_t>(accepted_submission_leg_count()),
                };
            }
        }

        for (std::size_t group_signal_index = 0; group_signal_index < group_signal_count_;
             ++group_signal_index) {
            const GroupSignal& group_signal = group_signals_buffer_[group_signal_index];
            auto candidate_group = build_candidate_group_intent(update, group_signal);
            if (!candidate_group.has_value()) {
                return {
                    .code = PipelineDecisionCode::kError,
                    .signal_count = static_cast<std::uint32_t>(signal_count_ + group_signal_count_),
                    .accepted_intent_count = static_cast<std::uint32_t>(accepted_submission_leg_count()),
                };
            }

            auto preview_risk_state = risk_state_;
            bool group_ok = true;
            for (std::size_t leg_index = 0; leg_index < candidate_group->leg_count; ++leg_index) {
                const OmsOrderIntent& leg = candidate_group->legs[leg_index];
                const RiskDecision leg_decision =
                    risk_.evaluate(update, leg, preview_risk_state);
                if (leg_decision.code == RiskDecisionCode::kError) {
                    return {
                        .code = PipelineDecisionCode::kError,
                        .signal_count = static_cast<std::uint32_t>(signal_count_ + group_signal_count_),
                        .accepted_intent_count = static_cast<std::uint32_t>(accepted_submission_leg_count()),
                    };
                }
                if (!leg_decision.accepted_intent.has_value()) {
                    group_ok = false;
                    break;
                }
                preview_risk_state = preview_state_after_intent(preview_risk_state, leg);
            }

            if (!group_ok) {
                continue;
            }
            if (!try_push_submission(OmsSubmission{*candidate_group})) {
                return {
                    .code = PipelineDecisionCode::kError,
                    .signal_count = static_cast<std::uint32_t>(signal_count_ + group_signal_count_),
                    .accepted_intent_count = static_cast<std::uint32_t>(accepted_submission_leg_count()),
                };
            }
        }

        for (std::size_t submission_index = 0; submission_index < submission_count_; ++submission_index) {
            const OmsSubmission& submission = submissions_buffer_[submission_index];
            if (intent_queue_ == nullptr || !intent_queue_->try_push(submission)) {
                return {
                    .code = PipelineDecisionCode::kBackpressure,
                    .signal_count = static_cast<std::uint32_t>(signal_count_ + group_signal_count_),
                    .accepted_intent_count = static_cast<std::uint32_t>(accepted_submission_leg_count()),
                };
            }
            on_submission_enqueued(submission);
        }

        return {
            .code = submission_count_ > 0 ? PipelineDecisionCode::kAccepted
                                          : PipelineDecisionCode::kDeclined,
            .signal_count = static_cast<std::uint32_t>(signal_count_ + group_signal_count_),
            .accepted_intent_count = static_cast<std::uint32_t>(accepted_submission_leg_count()),
        };
    }

    [[nodiscard]] std::size_t drain_oms_updates(std::size_t max_batch_size) noexcept {
        std::size_t processed = 0;
        while (processed < max_batch_size) {
            if (process_one_intent_decision()) {
                ++processed;
                continue;
            }
            if (process_one_lifecycle_event()) {
                ++processed;
                continue;
            }
            break;
        }
        return processed;
    }

  private:
    struct TrackedIntentState {
        predex::core::oms::kalshi::IntentOrigin origin{};
        internal::QtyLots open_qty_lots{0};
        std::optional<predex::core::oms::kalshi::OmsRequestId> oms_request_id;
    };

    struct StrategySignalSink {
        explicit StrategySignalSink(DefaultShardPipeline& pipeline) : pipeline_(pipeline) {}

        [[nodiscard]] bool try_push_signal(const Signal& signal) noexcept {
            return pipeline_.try_push_signal(signal);
        }

        [[nodiscard]] bool try_push_group_signal(const GroupSignal& group_signal) noexcept {
            return pipeline_.try_push_group_signal(group_signal);
        }

      private:
        DefaultShardPipeline& pipeline_;
    };

    std::uint16_t shard_id_{0};
    LocalRisk risk_;
    LocalRiskState risk_state_{};
    utils::SPSCQueue<OmsSubmission>* intent_queue_{nullptr};
    utils::SPSCQueue<predex::core::oms::kalshi::IntentDecision>* decision_queue_{nullptr};
    utils::SPSCQueue<predex::core::oms::kalshi::OrderLifecycleEvent>* lifecycle_queue_{nullptr};
    std::tuple<Strategies...> strategies_;

    std::array<Signal, kMaxSignalsPerEvent> signals_buffer_{};
    std::size_t signal_count_{0};
    std::array<GroupSignal, kMaxSignalsPerEvent> group_signals_buffer_{};
    std::size_t group_signal_count_{0};
    std::array<OmsSubmission, kMaxSignalsPerEvent> submissions_buffer_{};
    std::size_t submission_count_{0};

    predex::core::oms::kalshi::LocalIntentId next_local_intent_id_{1};
    predex::core::oms::kalshi::GroupIntentId next_group_intent_id_{1};

    std::unordered_map<predex::core::oms::kalshi::LocalIntentId, TrackedIntentState>
        tracked_intents_;
    std::unordered_map<predex::core::oms::kalshi::OmsRequestId,
                       predex::core::oms::kalshi::LocalIntentId>
        local_intent_id_by_request_id_;

    void reset_buffers() noexcept {
        signal_count_ = 0;
        group_signal_count_ = 0;
        submission_count_ = 0;
    }

    [[nodiscard]] bool try_push_signal(const Signal& signal) noexcept {
        if (signal_count_ >= kMaxSignalsPerEvent) {
            return false;
        }
        signals_buffer_[signal_count_++] = signal;
        return true;
    }

    [[nodiscard]] bool try_push_group_signal(const GroupSignal& group_signal) noexcept {
        if (group_signal_count_ >= kMaxSignalsPerEvent) {
            return false;
        }
        group_signals_buffer_[group_signal_count_++] = group_signal;
        return true;
    }

    [[nodiscard]] bool try_push_submission(const OmsSubmission& submission) noexcept {
        if (submission_count_ >= kMaxSignalsPerEvent) {
            return false;
        }
        submissions_buffer_[submission_count_++] = submission;
        return true;
    }

    template <typename SignalSink>
    void fan_out_to_strategies(const AppliedEventUpdate& update, SignalSink& signal_sink) noexcept {
        std::apply(
            [&](auto&... strategies) {
                (strategies.on_event(update, signal_sink), ...);
            },
            strategies_);
    }

    [[nodiscard]] std::optional<OmsOrderIntent> build_candidate_intent(
        const AppliedEventUpdate& update,
        const Signal& signal) noexcept {
        if (signal.kind == SignalKind::kUnknown || signal.market_id == 0 ||
            signal.target_qty_lots <= 0) {
            return std::nullopt;
        }

        return OmsOrderIntent{
            .origin = predex::core::oms::kalshi::IntentOrigin{
                .shard_id = shard_id_,
                .affinity_key = update.update.meta.affinity_key,
                .group_id = 0,
                .local_intent_id = next_local_intent_id_++,
                .leg_index = 0,
                .leg_count = 1,
                .signal_id = signal.signal_id,
                .event_id = signal.event_id == 0 ? update.event.event_id : signal.event_id,
                .market_id = signal.market_id,
            },
            .exchange = signal.exchange == internal::ExchangeId::kUnknown
                ? update.update.meta.exchange
                : signal.exchange,
            .side = signal.side,
            .qty_lots = signal.target_qty_lots,
            .limit_price_ticks = signal.reference_price_ticks,
            .time_in_force = predex::core::oms::kalshi::OmsTimeInForce::kGtc,
            .intent_ts_ns = signal.signal_ts_ns == 0 ? update.update.meta.recv_ns
                                                     : signal.signal_ts_ns,
        };
    }

    [[nodiscard]] std::optional<predex::core::oms::kalshi::GroupOrderIntent>
    build_candidate_group_intent(const AppliedEventUpdate& update,
                                 const GroupSignal& group_signal) noexcept {
        if (group_signal.kind == SignalKind::kUnknown || group_signal.leg_count == 0 ||
            group_signal.leg_count > predex::core::oms::kalshi::kMaxGroupOrderLegs) {
            return std::nullopt;
        }

        predex::core::oms::kalshi::GroupOrderIntent group_intent{};
        group_intent.group_id = next_group_intent_id_++;
        group_intent.execution_policy = group_signal.execution_policy;
        group_intent.leg_count = group_signal.leg_count;
        group_intent.intent_ts_ns = group_signal.signal_ts_ns == 0
            ? update.update.meta.recv_ns
            : group_signal.signal_ts_ns;

        const internal::EventId event_id =
            group_signal.event_id == 0 ? update.event.event_id : group_signal.event_id;
        const internal::ExchangeId exchange =
            group_signal.exchange == internal::ExchangeId::kUnknown
                ? update.update.meta.exchange
                : group_signal.exchange;

        for (std::size_t leg_index = 0; leg_index < group_signal.leg_count; ++leg_index) {
            const SubmissionLeg& leg = group_signal.legs[leg_index];
            if (leg.market_id == 0 || leg.qty_lots <= 0 || leg.side == internal::Side::kUnknown) {
                return std::nullopt;
            }

            group_intent.legs[leg_index] = OmsOrderIntent{
                .origin = predex::core::oms::kalshi::IntentOrigin{
                    .shard_id = shard_id_,
                    .affinity_key = update.update.meta.affinity_key,
                    .group_id = group_intent.group_id,
                    .local_intent_id = next_local_intent_id_++,
                    .leg_index = static_cast<std::uint16_t>(leg_index),
                    .leg_count = static_cast<std::uint16_t>(group_signal.leg_count),
                    .signal_id = group_signal.signal_id,
                    .event_id = event_id,
                    .market_id = leg.market_id,
                },
                .exchange = exchange,
                .side = leg.side,
                .qty_lots = leg.qty_lots,
                .limit_price_ticks = leg.limit_price_ticks,
                .time_in_force = leg.time_in_force,
                .intent_ts_ns = group_intent.intent_ts_ns,
            };
        }

        return group_intent;
    }

    void on_submission_enqueued(const OmsSubmission& submission) noexcept {
        if (const auto* intent = std::get_if<OmsOrderIntent>(&submission)) {
            on_intent_enqueued(*intent);
            return;
        }
        if (const auto* group = std::get_if<predex::core::oms::kalshi::GroupOrderIntent>(&submission)) {
            for (std::size_t leg_index = 0; leg_index < group->leg_count; ++leg_index) {
                on_intent_enqueued(group->legs[leg_index]);
            }
        }
    }

    void on_intent_enqueued(const OmsOrderIntent& intent) noexcept {
        ++risk_state_.open_intents_for_event;
        ++risk_state_.open_intents_for_market;
        risk_state_.event_exposure_lots += intent.qty_lots;
        risk_state_.market_exposure_lots += intent.qty_lots;
        tracked_intents_[intent.origin.local_intent_id] = TrackedIntentState{
            .origin = intent.origin,
            .open_qty_lots = intent.qty_lots,
            .oms_request_id = std::nullopt,
        };
    }

    [[nodiscard]] static LocalRiskState preview_state_after_intent(
        LocalRiskState state,
        const OmsOrderIntent& intent) noexcept {
        ++state.open_intents_for_event;
        ++state.open_intents_for_market;
        state.event_exposure_lots += intent.qty_lots;
        state.market_exposure_lots += intent.qty_lots;
        return state;
    }

    [[nodiscard]] std::size_t accepted_submission_leg_count() const noexcept {
        std::size_t total = 0;
        for (std::size_t submission_index = 0; submission_index < submission_count_;
             ++submission_index) {
            const OmsSubmission& submission = submissions_buffer_[submission_index];
            if (std::holds_alternative<OmsOrderIntent>(submission)) {
                ++total;
                continue;
            }
            const auto& group =
                std::get<predex::core::oms::kalshi::GroupOrderIntent>(submission);
            total += group.leg_count;
        }
        return total;
    }

    [[nodiscard]] bool process_one_intent_decision() noexcept {
        if (decision_queue_ == nullptr) {
            return false;
        }

        predex::core::oms::kalshi::IntentDecision decision{};
        if (!decision_queue_->try_pop(decision)) {
            return false;
        }

        if (const auto* accepted =
                std::get_if<predex::core::oms::kalshi::AcceptedIntent>(&decision.data)) {
            auto* tracked = find_tracked_intent(accepted->intent.origin, accepted->oms_request_id);
            if (tracked != nullptr) {
                tracked->oms_request_id = accepted->oms_request_id;
                local_intent_id_by_request_id_[accepted->oms_request_id] =
                    accepted->intent.origin.local_intent_id;
            }
            return true;
        }

        if (const auto* rejected =
                std::get_if<predex::core::oms::kalshi::RejectedIntent>(&decision.data)) {
            release_tracked_intent(rejected->intent.origin.local_intent_id);
            return true;
        }

        if (const auto* modified =
                std::get_if<predex::core::oms::kalshi::ModifiedIntent>(&decision.data)) {
            auto* tracked = find_tracked_intent(modified->modified_intent.origin,
                                                modified->oms_request_id);
            if (tracked != nullptr) {
                tracked->origin = modified->modified_intent.origin;
                tracked->oms_request_id = modified->oms_request_id;
                reconcile_open_qty(*tracked, modified->modified_intent.qty_lots);
                local_intent_id_by_request_id_[modified->oms_request_id] =
                    modified->modified_intent.origin.local_intent_id;
            }
            return true;
        }

        return true;
    }

    [[nodiscard]] bool process_one_lifecycle_event() noexcept {
        if (lifecycle_queue_ == nullptr) {
            return false;
        }

        predex::core::oms::kalshi::OrderLifecycleEvent event{};
        if (!lifecycle_queue_->try_pop(event)) {
            return false;
        }

        TrackedIntentState* tracked = find_tracked_intent(event.origin, event.oms_request_id);
        if (tracked == nullptr) {
            return true;
        }

        if (const auto* fill =
                std::get_if<predex::core::oms::kalshi::OrderFill>(&event.data)) {
            reduce_open_qty(*tracked, fill->fill_qty_lots);
        }
        if (const auto* replace_ack =
                std::get_if<predex::core::oms::kalshi::ReplaceAck>(&event.data)) {
            reconcile_open_qty(*tracked, replace_ack->replaced_qty_lots);
        }

        switch (event.kind) {
            case predex::core::oms::kalshi::OrderLifecycleEventKind::kReject:
            case predex::core::oms::kalshi::OrderLifecycleEventKind::kFill:
            case predex::core::oms::kalshi::OrderLifecycleEventKind::kCanceled:
                release_tracked_intent(event.origin.local_intent_id);
                break;
            default:
                break;
        }
        return true;
    }

    [[nodiscard]] TrackedIntentState* find_tracked_intent(
        const predex::core::oms::kalshi::IntentOrigin& origin,
        predex::core::oms::kalshi::OmsRequestId oms_request_id = 0) noexcept {
        auto tracked = tracked_intents_.find(origin.local_intent_id);
        if (tracked != tracked_intents_.end()) {
            return &tracked->second;
        }
        if (oms_request_id != 0) {
            const auto local_id = local_intent_id_by_request_id_.find(oms_request_id);
            if (local_id != local_intent_id_by_request_id_.end()) {
                auto tracked_by_request = tracked_intents_.find(local_id->second);
                if (tracked_by_request != tracked_intents_.end()) {
                    return &tracked_by_request->second;
                }
            }
        }
        return nullptr;
    }

    void reconcile_open_qty(TrackedIntentState& tracked, internal::QtyLots new_open_qty) noexcept {
        if (new_open_qty < 0) {
            new_open_qty = 0;
        }
        if (new_open_qty == tracked.open_qty_lots) {
            return;
        }
        if (new_open_qty > tracked.open_qty_lots) {
            const internal::QtyLots delta = new_open_qty - tracked.open_qty_lots;
            risk_state_.event_exposure_lots += delta;
            risk_state_.market_exposure_lots += delta;
        } else {
            const internal::QtyLots delta = tracked.open_qty_lots - new_open_qty;
            decrement_exposure(delta);
        }
        tracked.open_qty_lots = new_open_qty;
    }

    void reduce_open_qty(TrackedIntentState& tracked, internal::QtyLots delta_qty) noexcept {
        if (delta_qty <= 0) {
            return;
        }
        const internal::QtyLots reduction =
            delta_qty > tracked.open_qty_lots ? tracked.open_qty_lots : delta_qty;
        tracked.open_qty_lots -= reduction;
        decrement_exposure(reduction);
    }

    void release_tracked_intent(
        predex::core::oms::kalshi::LocalIntentId local_intent_id) noexcept {
        auto tracked = tracked_intents_.find(local_intent_id);
        if (tracked == tracked_intents_.end()) {
            return;
        }

        if (risk_state_.open_intents_for_event > 0) {
            --risk_state_.open_intents_for_event;
        }
        if (risk_state_.open_intents_for_market > 0) {
            --risk_state_.open_intents_for_market;
        }
        decrement_exposure(tracked->second.open_qty_lots);
        if (tracked->second.oms_request_id.has_value()) {
            local_intent_id_by_request_id_.erase(*tracked->second.oms_request_id);
        }
        tracked_intents_.erase(tracked);
    }

    void decrement_exposure(internal::QtyLots delta_qty) noexcept {
        if (delta_qty <= 0) {
            return;
        }
        risk_state_.event_exposure_lots =
            risk_state_.event_exposure_lots > delta_qty
                ? risk_state_.event_exposure_lots - delta_qty
                : 0;
        risk_state_.market_exposure_lots =
            risk_state_.market_exposure_lots > delta_qty
                ? risk_state_.market_exposure_lots - delta_qty
                : 0;
    }
};

} // namespace predex::core::shards::kalshi
