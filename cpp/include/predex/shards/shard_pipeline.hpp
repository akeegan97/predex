#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <tuple>
#include <utility>

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
    [[nodiscard]] PipelineResult on_event(const AppliedEventUpdate& update) noexcept {
        return {.code = PipelineDecisionCode::kDeclined};
    }
};

template <typename LocalRisk, typename... Strategies>
class DefaultShardPipeline {
  public:
    explicit DefaultShardPipeline(
        LocalRisk risk,
        utils::SPSCQueue<ShardOrderIntent>* intent_queue,
        Strategies... strategies)
        : risk_(std::move(risk)),
          intent_queue_(intent_queue),
          strategies_(std::move(strategies)...) {}

    [[nodiscard]] PipelineResult on_event(const AppliedEventUpdate& update) noexcept{
        reset_buffers();
        StrategySignalSink signal_sink{*this};
        fan_out_to_strategies(update, signal_sink);

        for (std::size_t signal_index = 0; signal_index < signal_count_; ++signal_index) {
            const Signal& signal = signals_buffer_[signal_index];
            auto candidate_intent = build_candidate_intent(update, signal);
            if (!candidate_intent.has_value()) {
                return {
                    .code = PipelineDecisionCode::kError,
                    .signal_count = static_cast<std::uint32_t>(signal_count_),
                    .accepted_intent_count = static_cast<std::uint32_t>(intent_count_),
                };
            }

            const RiskDecision risk_decision =
                risk_.evaluate(update, signal, *candidate_intent, risk_state_);
            if (risk_decision.code == RiskDecisionCode::kError) {
                return {
                    .code = PipelineDecisionCode::kError,
                    .signal_count = static_cast<std::uint32_t>(signal_count_),
                    .accepted_intent_count = static_cast<std::uint32_t>(intent_count_),
                };
            }
            if (!risk_decision.accepted_intent.has_value()) {
                continue;
            }
            if (!try_push_intent(*risk_decision.accepted_intent)) {
                return {
                    .code = PipelineDecisionCode::kError,
                    .signal_count = static_cast<std::uint32_t>(signal_count_),
                    .accepted_intent_count = static_cast<std::uint32_t>(intent_count_),
                };
            }
        }

        for (std::size_t intent_index = 0; intent_index < intent_count_; ++intent_index) {
            const ShardOrderIntent& intent = intents_buffer_[intent_index];
            if (intent_queue_ == nullptr || !intent_queue_->try_push(intent)) {
                return {
                    .code = PipelineDecisionCode::kBackpressure,
                    .signal_count = static_cast<std::uint32_t>(signal_count_),
                    .accepted_intent_count = static_cast<std::uint32_t>(intent_count_),
                };
            }
            on_intent_enqueued(intent);
        }

        return {
            .code = intent_count_ > 0 ? PipelineDecisionCode::kAccepted
                                      : PipelineDecisionCode::kDeclined,
            .signal_count = static_cast<std::uint32_t>(signal_count_),
            .accepted_intent_count = static_cast<std::uint32_t>(intent_count_),
        };

    }

  private:
    struct StrategySignalSink {
        explicit StrategySignalSink(DefaultShardPipeline& pipeline) : pipeline_(pipeline) {}

        [[nodiscard]] bool try_push_signal(const Signal& signal) noexcept {
            return pipeline_.try_push_signal(signal);
        }

      private:
        DefaultShardPipeline& pipeline_;
    };

    LocalRisk risk_;
    LocalRiskState risk_state_{};
    utils::SPSCQueue<ShardOrderIntent>* intent_queue_{nullptr};
    std::tuple<Strategies...> strategies_;
    std::array<Signal,kMaxSignalsPerEvent> signals_buffer_{};
    std::size_t signal_count_{0};
    std::array<ShardOrderIntent, kMaxSignalsPerEvent> intents_buffer_{};
    std::size_t intent_count_{0};

    void reset_buffers() noexcept{
        signal_count_ = 0;
        intent_count_ = 0;
    }

    [[nodiscard]] bool try_push_signal(const Signal& signal) noexcept{
        if (signal_count_ >= kMaxSignalsPerEvent) {
            return false;
        }
        signals_buffer_[signal_count_++] = signal;
        return true;
    }
    [[nodiscard]] bool try_push_intent(const ShardOrderIntent& intent) noexcept{
        if (intent_count_ >= kMaxSignalsPerEvent) {
            return false;
        }
        intents_buffer_[intent_count_++] = intent;
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

    [[nodiscard]] std::optional<ShardOrderIntent> build_candidate_intent(
        const AppliedEventUpdate& update,
        const Signal& signal) const noexcept {
        if (signal.kind == SignalKind::kUnknown || signal.market_id == 0 ||
            signal.target_qty_lots <= 0) {
            return std::nullopt;
        }

        return ShardOrderIntent{
            .signal_id = signal.signal_id,
            .exchange = signal.exchange,
            .event_id = signal.event_id == 0 ? update.event.event_id : signal.event_id,
            .market_id = signal.market_id,
            .side = signal.side,
            .qty_lots = signal.target_qty_lots,
            .limit_price_ticks = signal.reference_price_ticks,
            .intent_ts_ns = signal.signal_ts_ns == 0 ? update.update.meta.recv_ns
                                                     : signal.signal_ts_ns,
        };
    }

    void on_intent_enqueued(const ShardOrderIntent& intent) noexcept {
        ++risk_state_.open_intents_for_event;
        ++risk_state_.open_intents_for_market;
        risk_state_.event_exposure_lots += intent.qty_lots;
        risk_state_.market_exposure_lots += intent.qty_lots;
    }
};

} // namespace predex::core::shards::kalshi
