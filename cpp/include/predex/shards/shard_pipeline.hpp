#pragma once

#include <tuple>
#include <array>
#include <cstddef>
#include <cstdint>

#include "predex/shards/applied_event_update.hpp"
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
    Strategies... strategies);

    [[nodiscard]] PipelineResult on_event(const AppliedEventUpdate& update) noexcept{
        reset_buffers();
        

    }

  private:
    LocalRisk risk_;
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
};

} // namespace predex::core::shards::kalshi
