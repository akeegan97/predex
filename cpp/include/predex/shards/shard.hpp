#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>

#include "predex/ingest/frame_pool.hpp"
#include "predex/parsers/kalshi/parser.hpp"
#include "predex/shards/applied_event_update.hpp"
#include "predex/shards/event_store.hpp"
#include "predex/shards/shard_pipeline.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::shards::kalshi {

enum class ProcessCode : std::uint8_t {
    kIdle = 0,
    kProcessed = 1,
    kParseFail = 2,
    kApplyFail = 3,
    kLoggerBackpressure = 4,
    kFrameMissing = 5,
    kPipelineError = 6,
};

struct ProcessOneResult {
    ProcessCode code{ProcessCode::kIdle};
    EventApplyCode apply_code{EventApplyCode::kRejected};
    PipelineDecisionCode pipeline_code{PipelineDecisionCode::kDeclined};
};

template <typename Bundle>
class Shard {
  public:
    explicit Shard(utils::SPSCQueue<ingest::kalshi::FrameHandle>& input_queue,
                   ingest::kalshi::FramePool& frame_pool,
                   utils::SPSCQueue<ingest::kalshi::FrameHandle>& logger_queue,
                   parsers::kalshi::Parser parser,
                   EventStore& event_store,
                   Bundle bundle)
        : input_queue_(input_queue),
          frame_pool_(frame_pool),
          logger_queue_(logger_queue),
          //NOLINTNEXTLINE
          parser_(std::move(parser)),
          event_store_(event_store),
          bundle_(std::move(bundle)) {}

    [[nodiscard]] std::size_t pump(std::size_t max_batch_size) noexcept {
        std::size_t processed = bundle_.drain_oms_updates(max_batch_size);
        while (processed < max_batch_size) {
            const ProcessOneResult result = process_one();
            switch (result.code) {
                case ProcessCode::kIdle:
                    return processed;
                case ProcessCode::kProcessed:
                    ++processed;
                    break;
                case ProcessCode::kParseFail:
                case ProcessCode::kApplyFail:
                case ProcessCode::kFrameMissing:
                case ProcessCode::kPipelineError:
                    break;
                case ProcessCode::kLoggerBackpressure:
                    return processed;
                default:
                    ++failed_count_;
                    return processed;
            }
        }
        return processed;
    }

  private:
    utils::SPSCQueue<ingest::kalshi::FrameHandle>& input_queue_;
    ingest::kalshi::FramePool& frame_pool_;
    utils::SPSCQueue<ingest::kalshi::FrameHandle>& logger_queue_;
    parsers::kalshi::Parser parser_;
    EventStore& event_store_;
    Bundle bundle_;

    std::uint64_t processed_count_{0};
    std::uint64_t failed_count_{0};
    std::uint64_t parse_fail_count_{0};
    std::uint64_t apply_fail_count_{0};
    std::uint64_t logger_fail_count_{0};

    [[nodiscard]] ProcessOneResult process_one() noexcept {
        ingest::kalshi::FrameHandle handle{};
        if (!input_queue_.try_pop(handle)) {
            return ProcessOneResult{};
        }

        const ingest::kalshi::KalshiFrame* frame = get_frame(handle);
        if (frame == nullptr) {
            ++failed_count_;
            return ProcessOneResult{
                .code = ProcessCode::kFrameMissing,
                .apply_code = EventApplyCode::kRejected,
                .pipeline_code = PipelineDecisionCode::kDeclined,
            };
        }

        ProcessOneResult result = apply_event(handle, *frame);
        if (!forward_to_logger(handle)) {
            ++logger_fail_count_;
            result.code = ProcessCode::kLoggerBackpressure;
            return result;
        }

        if (result.code == ProcessCode::kProcessed) {
            ++processed_count_;
        }
        return result;
    }

    [[nodiscard]] bool forward_to_logger(const ingest::kalshi::FrameHandle& handle) noexcept {
        return logger_queue_.try_push(handle);
    }

    [[nodiscard]] const ingest::kalshi::KalshiFrame* get_frame(
        const ingest::kalshi::FrameHandle& handle) noexcept {
        return frame_pool_.frame(handle);
    }

    [[nodiscard]] ProcessOneResult apply_event(const ingest::kalshi::FrameHandle& handle,
                                               const ingest::kalshi::KalshiFrame& frame) noexcept {
        auto parse_result = parser_.parse(handle, frame);
        if (!parse_result.ok()) {
            ++parse_fail_count_;
            return ProcessOneResult{
                .code = ProcessCode::kParseFail,
                .apply_code = EventApplyCode::kParseFail,
                .pipeline_code = PipelineDecisionCode::kDeclined,
            };
        }

        const auto& parsed_value = parse_result.value();
        if (!parsed_value.has_value()) {
            ++parse_fail_count_;
            return ProcessOneResult{
                .code = ProcessCode::kParseFail,
                .apply_code = EventApplyCode::kParseFail,
                .pipeline_code = PipelineDecisionCode::kDeclined,
            };
        }

        const auto& event = *parsed_value;
        
        auto* stored_event = event_store_.find(event.meta.event_id);
        if (stored_event == nullptr) {
            ++apply_fail_count_;
            return ProcessOneResult{
                .code = ProcessCode::kApplyFail,
                .apply_code = EventApplyCode::kRejected,
                .pipeline_code = PipelineDecisionCode::kDeclined,
            };
        }

        const EventApplyCode apply_result = stored_event->apply_market_update(event);
        if (apply_result != EventApplyCode::kApplied) {
            ++apply_fail_count_;
            return ProcessOneResult{
                .code = ProcessCode::kApplyFail,
                .apply_code = apply_result,
                .pipeline_code = PipelineDecisionCode::kDeclined,
            };
        }

        const AppliedEventUpdate update{event, *stored_event};
        const PipelineResult pipeline_result = bundle_.on_event(update);
        if (pipeline_result.code == PipelineDecisionCode::kError) {
            ++failed_count_;
            return ProcessOneResult{
                .code = ProcessCode::kPipelineError,
                .apply_code = apply_result,
                .pipeline_code = pipeline_result.code,
            };
        }

        return ProcessOneResult{
            .code = ProcessCode::kProcessed,
            .apply_code = apply_result,
            .pipeline_code = pipeline_result.code,
        };
    }
};

} // namespace predex::core::shards::kalshi
