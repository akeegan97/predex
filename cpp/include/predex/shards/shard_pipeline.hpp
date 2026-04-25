#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>
#include <limits>

#include "predex/audit/audit_types.hpp"
#include "predex/shards/applied_event_update.hpp"
#include "predex/shards/local_risk.hpp"
#include "predex/shards/signal_types.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::shards::kalshi {
namespace oms = predex::core::oms::kalshi;

constexpr std::size_t kMaxSignalsPerEvent = 16;
constexpr std::uint64_t kPipelineProbeSampleRate = 64;

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
    //NOLINTNEXTLINE(readability-convert-member-functions-to-static) 
    [[nodiscard]] PipelineResult on_event(const AppliedEventUpdate& /*update*/) noexcept {
        return {.code = PipelineDecisionCode::kDeclined};
    }
    //NOLINTNEXTLINE(readability-convert-member-functions-to-static) 
    [[nodiscard]] std::size_t drain_oms_updates(std::size_t /*max_batch_size*/) noexcept {
        return 0;
    }
    void on_event_apply_result(const internal::NormalizedEvent& /*event*/,
                               const Event& /*stored_event*/,
                               const EventApplyResult& /*result*/) noexcept {}
};

template <typename LocalRisk, typename... Strategies>
class DefaultShardPipeline {
  public:
    explicit DefaultShardPipeline(
        std::uint16_t shard_id,
        LocalRisk risk,
        utils::SPSCQueue<OmsSubmission>* intent_queue,
        utils::SPSCQueue<oms::OmsToShardDecision>* decision_queue,
        utils::SPSCQueue<oms::OmsToShardLifecycleEvent>* lifecycle_queue,
        utils::SPSCQueue<predex::core::audit::AuditEvent>* audit_queue,
        Strategies... strategies)
        : shard_id_(shard_id),
          risk_(std::move(risk)),
          intent_queue_(intent_queue),
          decision_queue_(decision_queue),
          lifecycle_queue_(lifecycle_queue),
          audit_queue_(audit_queue),
          strategies_(std::move(strategies)...) {}
//NOLINTNEXTLINE
    [[nodiscard]] PipelineResult on_event(const AppliedEventUpdate& update) noexcept {
        reset_buffers();
        current_tick_recv_ns_ = update.update.meta.recv_ns;
        StrategySignalSink signal_sink{*this};
        fan_out_to_strategies(update, signal_sink);
        const internal::TimestampNs strategy_eval_done_ts_ns = monotonic_now_ns();
        if (should_emit_pipeline_probe()) {
            emit_pipeline_probe(update, strategy_eval_done_ts_ns);
        }

        for (std::size_t signal_index = 0; signal_index < signal_count_; ++signal_index) {
            const Signal& signal = signals_buffer_[signal_index];
            auto candidate_intent = build_candidate_intent(update, signal);
            if (!candidate_intent.has_value()) {
                return error_result();
            }

            const RiskDecision risk_decision =
                risk_.evaluate(update, *candidate_intent, risk_state_);
            emit_local_risk_audit(update, *candidate_intent, risk_decision);
            if (risk_decision.code == RiskDecisionCode::kError) {
                return error_result();
            }
            if (!risk_decision.accepted_intent.has_value()) {
                continue;
            }
            if (!try_push_submission(OmsSubmission{*risk_decision.accepted_intent})) {
                return error_result();
            }
        }

        for (std::size_t group_signal_index = 0; group_signal_index < group_signal_count_;
            ++group_signal_index) {
            const GroupSignal& group_signal = group_signals_buffer_[group_signal_index];
            auto candidate_group = build_candidate_group_intent(update, group_signal);
            if (!candidate_group) {
                return error_result();
            }

            const auto& group = *candidate_group;

            auto preview_risk_state = risk_state_;
            bool group_ok = true;
            for (std::size_t leg_index = 0; leg_index < group.leg_count; ++leg_index) {
                const OmsOrderIntent& leg = group.legs[leg_index];
                const RiskDecision leg_decision =
                    risk_.evaluate(update, leg, preview_risk_state);
                emit_local_risk_audit(update, leg, leg_decision);
                if (leg_decision.code == RiskDecisionCode::kError) {
                    return error_result();
                }
                if (!leg_decision.accepted_intent) {
                    group_ok = false;
                    break;
                }
                preview_risk_state = preview_state_after_intent(preview_risk_state, leg);
            }

            if (!group_ok) {
                continue;
            }
            if (!try_push_submission(OmsSubmission{group})) {
                return error_result();
            }
        }

        for (std::size_t submission_index = 0; submission_index < submission_count_;
             ++submission_index) {
            OmsSubmission& submission = submissions_buffer_[submission_index];
            stamp_submission_enqueued_ts(submission, monotonic_now_ns());
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

    void on_event_apply_result(const internal::NormalizedEvent& event,
                               const Event& /*stored_event*/,
                               const EventApplyResult& result) noexcept {
        if (result.desync_reason == ShardDesyncReason::kNone) {
            return;
        }
        emit_shard_desync_audit(event, result);
    }

  private:
    [[nodiscard]] static std::int64_t latency_delta_ns(
        internal::TimestampNs end_ts_ns,
        internal::TimestampNs start_ts_ns) noexcept {
        if (end_ts_ns <= start_ts_ns) {
            return 0;
        }
        const auto raw_delta = end_ts_ns - start_ts_ns;
        constexpr auto max_i64 =
            static_cast<internal::TimestampNs>(std::numeric_limits<std::int64_t>::max());
        if (raw_delta > max_i64) {
            return std::numeric_limits<std::int64_t>::max();
        }
        return static_cast<std::int64_t>(raw_delta);
    }

    [[nodiscard]] static internal::TimestampNs monotonic_now_ns() noexcept {
        return static_cast<internal::TimestampNs>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count());
    }

    struct TrackedIntentState {
        oms::IntentContext context{};
        internal::Side side{internal::Side::kUnknown};
        internal::QtyLots open_qty_lots{0};
        std::optional<oms::OmsRequestId> oms_request_id;
        internal::TimestampNs tick_recv_ts_ns{0};
        internal::TimestampNs signal_ts_ns{0};
        internal::TimestampNs submission_enqueued_ts_ns{0};
        internal::TimestampNs first_decision_ts_ns{0};
        internal::TimestampNs first_lifecycle_ts_ns{0};
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
    utils::SPSCQueue<oms::OmsToShardDecision>* decision_queue_{nullptr};
    utils::SPSCQueue<oms::OmsToShardLifecycleEvent>* lifecycle_queue_{nullptr};
    utils::SPSCQueue<predex::core::audit::AuditEvent>* audit_queue_{nullptr};
    std::tuple<Strategies...> strategies_;

    std::array<Signal, kMaxSignalsPerEvent> signals_buffer_{};
    std::size_t signal_count_{0};
    std::array<GroupSignal, kMaxSignalsPerEvent> group_signals_buffer_{};
    std::size_t group_signal_count_{0};
    std::array<OmsSubmission, kMaxSignalsPerEvent> submissions_buffer_{};
    std::size_t submission_count_{0};

    oms::LocalIntentId next_local_intent_id_{1};
    oms::GroupIntentId next_group_intent_id_{1};

    std::unordered_map<oms::LocalIntentId, TrackedIntentState>
        tracked_intents_;
    std::unordered_map<oms::OmsRequestId, oms::LocalIntentId>
        local_intent_id_by_request_id_;
    internal::TimestampNs current_tick_recv_ns_{0};
    std::uint64_t event_counter_{0};

    [[nodiscard]] PipelineResult error_result() const noexcept {
        return {
            .code = PipelineDecisionCode::kError,
            .signal_count = static_cast<std::uint32_t>(signal_count_ + group_signal_count_),
            .accepted_intent_count = static_cast<std::uint32_t>(accepted_submission_leg_count()),
        };
    }

    void reset_buffers() noexcept {
        signal_count_ = 0;
        group_signal_count_ = 0;
        submission_count_ = 0;
    }

    [[nodiscard]] bool should_emit_pipeline_probe() noexcept {
        if (kPipelineProbeSampleRate == 0) {
            return false;
        }
        const bool emit = (event_counter_ % kPipelineProbeSampleRate) == 0;
        ++event_counter_;
        return emit;
    }

    [[nodiscard]] bool try_push_signal(const Signal& signal) noexcept {
        if (signal_count_ >= kMaxSignalsPerEvent) {
            return false;
        }
        signals_buffer_[signal_count_++] = signal;
        emit_audit(predex::core::audit::AuditEvent{
            .kind = predex::core::audit::AuditKind::kSignal,
            .ts_ns = signal.signal_ts_ns,
            .shard_id = shard_id_,
            .signal_id = signal.signal_id,
            .exchange = signal.exchange,
            .event_id = signal.event_id,
            .market_id = signal.market_id,
            .side = signal.side,
            .qty_lots = signal.target_qty_lots,
            .price_ticks = signal.reference_price_ticks.value_or(0),
            .edge_ticks = signal.edge_ticks,
            .score = signal.score,
        });
        return true;
    }

    [[nodiscard]] bool try_push_group_signal(const GroupSignal& group_signal) noexcept {
        if (group_signal_count_ >= kMaxSignalsPerEvent) {
            return false;
        }
        group_signals_buffer_[group_signal_count_++] = group_signal;
        emit_audit(predex::core::audit::AuditEvent{
            .kind = predex::core::audit::AuditKind::kGroupSignal,
            .ts_ns = group_signal.signal_ts_ns,
            .shard_id = shard_id_,
            .signal_id = group_signal.signal_id,
            .exchange = group_signal.exchange,
            .event_id = group_signal.event_id,
            .leg_count = static_cast<std::uint16_t>(group_signal.leg_count),
            .edge_ticks = group_signal.edge_ticks,
            .score = group_signal.score,
        });
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
            .context = oms::IntentContext{
                .shard_id = shard_id_,
                .affinity_key = update.update.meta.affinity_key,
                .group_intent_id = 0,
                .leg_index = 0,
                .leg_count = 1,
                .signal_id = signal.signal_id,
                .local_intent_id = next_local_intent_id_++,
                .signal_ts_ns = signal.signal_ts_ns == 0 ? update.update.meta.recv_ns
                                                         : signal.signal_ts_ns,
                .tick_recv_ns = update.update.meta.recv_ns,
                .submission_enqueued_ns = 0,
                .event_id = signal.event_id == 0 ? update.event.event_id : signal.event_id,
                .market_id = signal.market_id,
            },
            .exchange = signal.exchange == internal::ExchangeId::kUnknown
                ? update.update.meta.exchange
                : signal.exchange,
            .side = signal.side,
            .outcome = signal.outcome,
            .qty_lots = signal.target_qty_lots,
            .limit_price_ticks = signal.reference_price_ticks,
            .time_in_force = oms::OmsTimeInForce::kGtc,
            .intent_ts_ns = signal.signal_ts_ns == 0 ? update.update.meta.recv_ns
                                                     : signal.signal_ts_ns,
        };
    }

    [[nodiscard]] std::optional<oms::GroupOrderIntent>
    build_candidate_group_intent(const AppliedEventUpdate& update,
                                 const GroupSignal& group_signal) noexcept {
        if (group_signal.kind == SignalKind::kUnknown || group_signal.leg_count == 0 ||
            group_signal.leg_count > oms::kMaxGroupOrderLegs) {
            return std::nullopt;
        }

        oms::GroupOrderIntent group_intent{};
        group_intent.context = oms::IntentContext{
            .shard_id = shard_id_,
            .affinity_key = update.update.meta.affinity_key,
            .group_intent_id = next_group_intent_id_++,
            .leg_index = 0,
            .leg_count = static_cast<std::uint16_t>(group_signal.leg_count),
            .signal_id = group_signal.signal_id,
            .local_intent_id = 0,
            .signal_ts_ns = group_signal.signal_ts_ns == 0
                ? update.update.meta.recv_ns
                : group_signal.signal_ts_ns,
            .tick_recv_ns = update.update.meta.recv_ns,
            .submission_enqueued_ns = 0,
            .event_id = group_signal.event_id == 0 ? update.event.event_id : group_signal.event_id,
            .market_id = 0,
        };
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
            if (leg.market_id == 0 || leg.qty_lots <= 0 ||
                leg.side == internal::Side::kUnknown ||
                leg.outcome == oms::Outcome::kUnknown) {
                return std::nullopt;
            }

            group_intent.legs[leg_index] = OmsOrderIntent{
                .context = oms::IntentContext{
                    .shard_id = shard_id_,
                    .affinity_key = update.update.meta.affinity_key,
                    .group_intent_id = group_intent.context.group_intent_id,
                    .leg_index = static_cast<std::uint16_t>(leg_index),
                    .leg_count = static_cast<std::uint16_t>(group_signal.leg_count),
                    .signal_id = group_signal.signal_id,
                    .local_intent_id = next_local_intent_id_++,
                    .signal_ts_ns = group_intent.intent_ts_ns,
                    .tick_recv_ns = update.update.meta.recv_ns,
                    .submission_enqueued_ns = 0,
                    .event_id = event_id,
                    .market_id = leg.market_id,
                },
                .exchange = exchange,
                .side = leg.side,
                .outcome = leg.outcome,
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
            emit_submission_audit(*intent);
            return;
        }
        if (const auto* group = std::get_if<oms::GroupOrderIntent>(&submission)) {
            for (std::size_t leg_index = 0; leg_index < group->leg_count; ++leg_index) {
                on_intent_enqueued(group->legs[leg_index]);
                emit_submission_audit(group->legs[leg_index]);
            }
        }
    }

    void on_intent_enqueued(const OmsOrderIntent& intent) noexcept {
        ++risk_state_.open_intents_for_event;
        ++risk_state_.open_intents_for_market;
        risk_state_.event_exposure_lots += intent.qty_lots;
        risk_state_.market_exposure_lots += intent.qty_lots;
        const internal::TimestampNs tick_recv_ts =
            intent.context.tick_recv_ns != 0 ? intent.context.tick_recv_ns : current_tick_recv_ns_;
        internal::TimestampNs signal_ts = tick_recv_ts;
        if (intent.context.signal_ts_ns != 0) {
            signal_ts = intent.context.signal_ts_ns;
        } else if (intent.intent_ts_ns != 0) {
            signal_ts = intent.intent_ts_ns;
        }
        const internal::TimestampNs submission_enqueued_ts =
            intent.context.submission_enqueued_ns != 0
                ? intent.context.submission_enqueued_ns
                : current_tick_recv_ns_;
        tracked_intents_[intent.context.local_intent_id] = TrackedIntentState{
            .context = intent.context,
            .side = intent.side,
            .open_qty_lots = intent.qty_lots,
            .oms_request_id = std::nullopt,
            .tick_recv_ts_ns = tick_recv_ts,
            .signal_ts_ns = signal_ts,
            .submission_enqueued_ts_ns = submission_enqueued_ts,
            .first_decision_ts_ns = 0,
            .first_lifecycle_ts_ns = 0,
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

            const auto* group =
                std::get_if<oms::GroupOrderIntent>(&submission);
            if (group == nullptr) {
                continue; // or assert(false) if this should be impossible
            }
            total += group->leg_count;
        }
        return total;
    }

    static void stamp_submission_enqueued_ts(
        OmsSubmission& submission,
        internal::TimestampNs enqueued_ts_ns) noexcept {
        if (auto* intent = std::get_if<OmsOrderIntent>(&submission)) {
            intent->context.submission_enqueued_ns = enqueued_ts_ns;
            return;
        }

        auto* group = std::get_if<oms::GroupOrderIntent>(&submission);
        if (group == nullptr) {
            return;
        }
        for (std::size_t leg_index = 0; leg_index < group->leg_count; ++leg_index) {
            group->legs[leg_index].context.submission_enqueued_ns = enqueued_ts_ns;
        }
    }

    [[nodiscard]] bool process_one_intent_decision() noexcept {
        if (decision_queue_ == nullptr) {
            return false;
        }

        oms::OmsToShardDecision decision{};
        if (!decision_queue_->try_pop(decision)) {
            return false;
        }
        const internal::TimestampNs decision_ts_ns = monotonic_now_ns();

        if (const auto* accepted = std::get_if<oms::IntentAccepted>(&decision)) {
            auto* tracked = find_tracked_intent(
                accepted->corr.context, accepted->corr.order.oms_request_id);
            internal::TimestampNs tick_recv_ts_ns = 0;
            internal::TimestampNs signal_ts_ns = 0;
            internal::TimestampNs submission_ts_ns = 0;
            if (tracked != nullptr) {
                tracked->oms_request_id = accepted->corr.order.oms_request_id;
                if (tracked->first_decision_ts_ns == 0) {
                    tracked->first_decision_ts_ns = decision_ts_ns;
                }
                local_intent_id_by_request_id_[accepted->corr.order.oms_request_id] =
                    accepted->corr.context.local_intent_id;
                tick_recv_ts_ns = tracked->tick_recv_ts_ns;
                signal_ts_ns = tracked->signal_ts_ns;
                submission_ts_ns = tracked->submission_enqueued_ts_ns;
            }
            emit_reconcile_audit(
                accepted->corr.context,
                accepted->corr.order.oms_request_id,
                tracked != nullptr ? tracked->open_qty_lots : 0,
                decision_ts_ns,
                tick_recv_ts_ns,
                signal_ts_ns,
                submission_ts_ns,
                decision_ts_ns,
                0,
                0);
            return true;
        }

        if (const auto* rejected = std::get_if<oms::IntentRejected>(&decision)) {
            auto* tracked = find_tracked_intent(rejected->context, std::nullopt);
            const internal::TimestampNs tick_recv_ts_ns =
                tracked != nullptr ? tracked->tick_recv_ts_ns : 0;
            const internal::TimestampNs signal_ts_ns =
                tracked != nullptr ? tracked->signal_ts_ns : 0;
            const internal::TimestampNs submission_ts_ns =
                tracked != nullptr ? tracked->submission_enqueued_ts_ns : 0;
            emit_reconcile_audit(
                rejected->context,
                0,
                0,
                decision_ts_ns,
                tick_recv_ts_ns,
                signal_ts_ns,
                submission_ts_ns,
                decision_ts_ns,
                0,
                decision_ts_ns);
            release_tracked_intent(rejected->context.local_intent_id);
            return true;
        }

        if (const auto* modified = std::get_if<oms::IntentModified>(&decision)) {
            auto* tracked = find_tracked_intent(
                modified->corr.context, modified->corr.order.oms_request_id);
            internal::TimestampNs tick_recv_ts_ns = 0;
            internal::TimestampNs signal_ts_ns = 0;
            internal::TimestampNs submission_ts_ns = 0;
            if (tracked != nullptr) {
                tracked->context = modified->corr.context;
                tracked->oms_request_id = modified->corr.order.oms_request_id;
                if (tracked->first_decision_ts_ns == 0) {
                    tracked->first_decision_ts_ns = decision_ts_ns;
                }
                reconcile_open_qty(*tracked, modified->replacement.qty_lots);
                local_intent_id_by_request_id_[modified->corr.order.oms_request_id] =
                    modified->corr.context.local_intent_id;
                tick_recv_ts_ns = tracked->tick_recv_ts_ns;
                signal_ts_ns = tracked->signal_ts_ns;
                submission_ts_ns = tracked->submission_enqueued_ts_ns;
            }
            emit_reconcile_audit(
                modified->corr.context,
                modified->corr.order.oms_request_id,
                modified->replacement.qty_lots,
                decision_ts_ns,
                tick_recv_ts_ns,
                signal_ts_ns,
                submission_ts_ns,
                decision_ts_ns,
                0,
                0);
            return true;
        }

        return true;
    }

    [[nodiscard]] bool process_one_lifecycle_event() noexcept {
        if (lifecycle_queue_ == nullptr) {
            return false;
        }

        oms::OmsToShardLifecycleEvent event{};
        if (!lifecycle_queue_->try_pop(event)) {
            return false;
        }
        const internal::TimestampNs event_ts_ns = monotonic_now_ns();
        const auto* corr = std::visit(
            [](const auto& typed_event) -> const oms::ShardOrderCorrelation* {
                return &typed_event.corr;
            },
            event);

        TrackedIntentState* tracked = find_tracked_intent(corr->context, corr->order.oms_request_id);
        if (tracked == nullptr) {
            return true;
        }

        std::visit(
            [&](const auto& typed_event) {
                using T = std::decay_t<decltype(typed_event)>;
                if constexpr (std::is_same_v<T, oms::OrderPartiallyFilled>) {
                    reduce_open_qty(*tracked, typed_event.filled_qty_lots);
                    const std::int64_t fill_delta =
                        static_cast<std::int64_t>(typed_event.filled_qty_lots);
                    if (tracked->side == internal::Side::kBuy ||
                        tracked->side == internal::Side::kBid) {
                        risk_state_.net_position_lots_by_market[corr->context.market_id] += fill_delta;
                    } else if (tracked->side == internal::Side::kSell ||
                               tracked->side == internal::Side::kAsk) {
                        risk_state_.net_position_lots_by_market[corr->context.market_id] -= fill_delta;
                    }
                } else if constexpr (std::is_same_v<T, oms::OrderFilled>) {
                    reduce_open_qty(*tracked, typed_event.filled_qty_lots);
                    const std::int64_t fill_delta =
                        static_cast<std::int64_t>(typed_event.filled_qty_lots);
                    if (tracked->side == internal::Side::kBuy ||
                        tracked->side == internal::Side::kBid) {
                        risk_state_.net_position_lots_by_market[corr->context.market_id] += fill_delta;
                    } else if (tracked->side == internal::Side::kSell ||
                               tracked->side == internal::Side::kAsk) {
                        risk_state_.net_position_lots_by_market[corr->context.market_id] -= fill_delta;
                    }
                } else if constexpr (std::is_same_v<T, oms::OrderWorking>) {
                    reconcile_open_qty(*tracked, typed_event.working_qty_lots);
                }

                if (tracked->first_lifecycle_ts_ns == 0) {
                    tracked->first_lifecycle_ts_ns = event_ts_ns;
                }

                const bool terminal =
                    std::is_same_v<T, oms::OrderFilled> ||
                    std::is_same_v<T, oms::OrderCanceled> ||
                    std::is_same_v<T, oms::OrderVenueRejected>;
                emit_reconcile_audit(
                    corr->context,
                    corr->order.oms_request_id,
                    terminal ? 0 : tracked->open_qty_lots,
                    event_ts_ns,
                    tracked->tick_recv_ts_ns,
                    tracked->signal_ts_ns,
                    tracked->submission_enqueued_ts_ns,
                    tracked->first_decision_ts_ns,
                    tracked->first_lifecycle_ts_ns,
                    terminal ? event_ts_ns : 0);
                if (terminal) {
                    release_tracked_intent(corr->context.local_intent_id);
                }
            },
            event);
        return true;
    }

    [[nodiscard]] TrackedIntentState* find_tracked_intent(
        const oms::IntentContext& context,
        std::optional<oms::OmsRequestId> oms_request_id = std::nullopt) noexcept {
        auto tracked = tracked_intents_.find(context.local_intent_id);
        if (tracked != tracked_intents_.end()) {
            return &tracked->second;
        }
        if (oms_request_id.has_value()) {
            const auto local_id = local_intent_id_by_request_id_.find(*oms_request_id);
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
        oms::LocalIntentId local_intent_id) noexcept {
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

        if (const auto& oms_request_id = tracked->second.oms_request_id; oms_request_id) {
            local_intent_id_by_request_id_.erase(*oms_request_id);
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

    void emit_local_risk_audit(const AppliedEventUpdate& update,
                               const OmsOrderIntent& intent,
                               const RiskDecision& decision) noexcept {
        emit_audit(predex::core::audit::AuditEvent{
            .kind = predex::core::audit::AuditKind::kLocalRisk,
            .ts_ns = update.update.meta.recv_ns,
            .shard_id = shard_id_,
            .signal_id = intent.context.signal_id,
            .group_id = intent.context.group_intent_id,
            .local_intent_id = intent.context.local_intent_id,
            .exchange = intent.exchange,
            .event_id = intent.context.event_id,
            .market_id = intent.context.market_id,
            .side = intent.side,
            .leg_index = intent.context.leg_index,
            .leg_count = intent.context.leg_count,
            .qty_lots = intent.qty_lots,
            .price_ticks = intent.limit_price_ticks.value_or(0),
            .decision_code = static_cast<std::uint8_t>(decision.code),
            .reject_reason = static_cast<std::uint8_t>(decision.reason),
            .event_exposure_lots = risk_state_.event_exposure_lots,
            .market_exposure_lots = risk_state_.market_exposure_lots,
        });
    }

    void emit_submission_audit(const OmsOrderIntent& intent) noexcept {
        const auto tracked_it = tracked_intents_.find(intent.context.local_intent_id);
        const internal::TimestampNs enqueue_ts =
            tracked_it != tracked_intents_.end()
                ? tracked_it->second.submission_enqueued_ts_ns
                : current_tick_recv_ns_;

        internal::TimestampNs signal_ts = 0;
        if (tracked_it != tracked_intents_.end()) {
            signal_ts = tracked_it->second.signal_ts_ns;
        } else if (intent.context.signal_ts_ns != 0) {
            signal_ts = intent.context.signal_ts_ns;
        } else {
            signal_ts = intent.intent_ts_ns;
        }

        internal::TimestampNs tick_recv_ts = 0;
        if (tracked_it != tracked_intents_.end()) {
            tick_recv_ts = tracked_it->second.tick_recv_ts_ns;
        } else if (intent.context.tick_recv_ns != 0) {
            tick_recv_ts = intent.context.tick_recv_ns;
        } else {
            tick_recv_ts = current_tick_recv_ns_;
        }
        emit_audit(predex::core::audit::AuditEvent{
            .kind = predex::core::audit::AuditKind::kSubmission,
            .ts_ns = enqueue_ts,
            .shard_id = shard_id_,
            .signal_id = intent.context.signal_id,
            .group_id = intent.context.group_intent_id,
            .local_intent_id = intent.context.local_intent_id,
            .tick_recv_ns = tick_recv_ts,
            .signal_ts_ns = signal_ts,
            .submission_enqueued_ns = enqueue_ts,
            .signal_to_submission_ns = latency_delta_ns(enqueue_ts, signal_ts),
            .exchange = intent.exchange,
            .event_id = intent.context.event_id,
            .market_id = intent.context.market_id,
            .side = intent.side,
            .leg_index = intent.context.leg_index,
            .leg_count = intent.context.leg_count,
            .qty_lots = intent.qty_lots,
            .price_ticks = intent.limit_price_ticks.value_or(0),
            .event_exposure_lots = risk_state_.event_exposure_lots,
            .market_exposure_lots = risk_state_.market_exposure_lots,
        });
    }

    void emit_pipeline_probe(const AppliedEventUpdate& update,
                             internal::TimestampNs strategy_eval_done_ts_ns) noexcept {
        emit_audit(predex::core::audit::AuditEvent{
            .kind = predex::core::audit::AuditKind::kPipelineProbe,
            .ts_ns = strategy_eval_done_ts_ns,
            .shard_id = shard_id_,
            .tick_recv_ns = update.update.meta.recv_ns,
            .signal_ts_ns = strategy_eval_done_ts_ns,
            .tick_to_signal_ns =
                latency_delta_ns(strategy_eval_done_ts_ns, update.update.meta.recv_ns),
            .exchange = update.update.meta.exchange,
            .event_id = update.event.event_id,
            .market_id = update.update.meta.market_id,
            .qty_lots = static_cast<internal::QtyLots>(signal_count_),
            .aux_qty_lots = static_cast<internal::QtyLots>(group_signal_count_),
        });
    }

    void emit_reconcile_audit(const oms::IntentContext& context,
                              oms::OmsRequestId oms_request_id,
                              internal::QtyLots open_qty_lots,
                              internal::TimestampNs ts_ns,
                              internal::TimestampNs tick_recv_ts_ns,
                              internal::TimestampNs signal_ts_ns,
                              internal::TimestampNs submission_enqueued_ns,
                              internal::TimestampNs oms_decision_ts_ns,
                              internal::TimestampNs first_fill_recv_ts_ns,
                              internal::TimestampNs terminal_recv_ts_ns) noexcept {
        emit_audit(predex::core::audit::AuditEvent{
            .kind = predex::core::audit::AuditKind::kShardReconcile,
            .ts_ns = ts_ns,
            .shard_id = shard_id_,
            .signal_id = context.signal_id,
            .group_id = context.group_intent_id,
            .local_intent_id = context.local_intent_id,
            .oms_request_id = oms_request_id,
            .tick_recv_ns = tick_recv_ts_ns,
            .signal_ts_ns = signal_ts_ns,
            .submission_enqueued_ns = submission_enqueued_ns,
            .oms_decision_ts_ns = oms_decision_ts_ns,
            .first_fill_recv_ns = first_fill_recv_ts_ns,
            .terminal_recv_ns = terminal_recv_ts_ns,
            .signal_to_submission_ns = latency_delta_ns(submission_enqueued_ns, signal_ts_ns),
            .submission_to_decision_ns =
                latency_delta_ns(oms_decision_ts_ns, submission_enqueued_ns),
            .tick_to_first_fill_ns =
                latency_delta_ns(first_fill_recv_ts_ns, tick_recv_ts_ns),
            .tick_to_terminal_ns =
                latency_delta_ns(terminal_recv_ts_ns, tick_recv_ts_ns),
            .event_id = context.event_id,
            .market_id = context.market_id,
            .leg_index = context.leg_index,
            .leg_count = context.leg_count,
            .qty_lots = open_qty_lots,
            .event_exposure_lots = risk_state_.event_exposure_lots,
            .market_exposure_lots = risk_state_.market_exposure_lots,
        });
    }

    void emit_shard_desync_audit(const internal::NormalizedEvent& event,
                                 const EventApplyResult& result) noexcept {
        internal::Side event_side = internal::Side::kUnknown;
        if (const auto* delta = std::get_if<internal::DeltaData>(&event.data)) {
            event_side = delta->side;
        } else if (const auto* trade = std::get_if<internal::TradeData>(&event.data)) {
            event_side = trade->book_side;
        }
        emit_audit(predex::core::audit::AuditEvent{
            .kind = predex::core::audit::AuditKind::kShardDesync,
            .ts_ns = event.meta.recv_ns,
            .shard_id = shard_id_,
            .tick_recv_ns = event.meta.recv_ns,
            .exchange = event.meta.exchange,
            .event_id = event.meta.event_id,
            .market_id = event.meta.market_id,
            .side = event_side,
            .decision_code = static_cast<std::uint8_t>(result.code),
            .reject_reason = static_cast<std::uint8_t>(result.desync_reason),
        });
    }

    void emit_audit(const predex::core::audit::AuditEvent& event) noexcept {
        if (audit_queue_ == nullptr) {
            return;
        }
        static_cast<void>(audit_queue_->try_push(event));
    }
};

} // namespace predex::core::shards::kalshi
