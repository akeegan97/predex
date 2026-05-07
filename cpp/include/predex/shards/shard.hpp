#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <unordered_set>
#include <utility>

#include "predex/ingest/frame_pool.hpp"
#include "predex/parsers/kalshi/parser.hpp"
#include "predex/shards/applied_event_update.hpp"
#include "predex/shards/event_store.hpp"
#include "predex/shards/shard_pipeline.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::shards::kalshi {

namespace detail {

inline void format_utc_timestamp(char* buffer, std::size_t buffer_size) noexcept {
    const std::time_t now = std::time(nullptr);
    std::tm utc_tm{};
#if defined(_WIN32)
    gmtime_s(&utc_tm, &now);
#else
    gmtime_r(&now, &utc_tm);
#endif
    if (std::strftime(buffer, buffer_size, "%Y-%m-%d %H:%M:%S UTC", &utc_tm) == 0U) {
        std::snprintf(buffer, buffer_size, "unknown-time");
    }
}

inline const char* raw_event_type_name(ingest::kalshi::KalshiEventType type) noexcept {
    switch (type) {
        case ingest::kalshi::KalshiEventType::kTrade:
            return "trade";
        case ingest::kalshi::KalshiEventType::kDelta:
            return "delta";
        case ingest::kalshi::KalshiEventType::kSnapshot:
            return "snapshot";
        case ingest::kalshi::KalshiEventType::kSubscribed:
            return "subscribed";
        case ingest::kalshi::KalshiEventType::kLifecycle:
            return "lifecycle";
        case ingest::kalshi::KalshiEventType::kUnknown:
        default:
            return "unknown";
    }
}

inline const char* parsed_event_type_name(internal::EventType type) noexcept {
    switch (type) {
        case internal::EventType::kSnapshot:
            return "snapshot";
        case internal::EventType::kDelta:
            return "delta";
        case internal::EventType::kTrade:
            return "trade";
        case internal::EventType::kHeartbeat:
            return "heartbeat";
        case internal::EventType::kStatus:
            return "status";
        case internal::EventType::kLifecycle:
            return "lifecycle";
        case internal::EventType::kUnknown:
        default:
            return "unknown";
    }
}

inline const char* side_name(internal::Side side) noexcept {
    switch (side) {
        case internal::Side::kBid:
            return "bid";
        case internal::Side::kAsk:
            return "ask";
        case internal::Side::kBuy:
            return "buy";
        case internal::Side::kSell:
            return "sell";
        case internal::Side::kUnknown:
        default:
            return "unknown";
    }
}

inline const char* desync_reason_name(ShardDesyncReason reason) noexcept {
    switch (reason) {
        case ShardDesyncReason::kUnexpectedSnapshotAfterInit:
            return "UnexpectedSnapshotAfterInit";
        case ShardDesyncReason::kDeltaWhileDesynced:
            return "DeltaWhileDesynced";
        case ShardDesyncReason::kTradeInvalidOrDesynced:
            return "TradeInvalidOrDesynced";
        case ShardDesyncReason::kDeltaSequence:
            return "DeltaSequence";
        case ShardDesyncReason::kInvalidSide:
            return "InvalidSide";
        case ShardDesyncReason::kNegativeQuantity:
            return "NegativeQuantity";
        case ShardDesyncReason::kInvalidSeq:
            return "InvalidSeq";
        case ShardDesyncReason::kPendingDeltaOverflow:
            return "PendingDeltaOverflow";
        case ShardDesyncReason::kReplayPendingDeltaFailure:
            return "ReplayPendingDeltaFailure";
        case ShardDesyncReason::kInvalidSnapshotData:
            return "InvalidSnapshotData";
        case ShardDesyncReason::kTopologyRecomputeFailure:
            return "TopologyRecomputeFailure";
        case ShardDesyncReason::kNone:
        default:
            return "None";
    }
}

struct PreApplyBookSnapshot{
    bool has_snapshot{false};
    bool desynced{false};
    std::optional<internal::SequenceId> last_seq_id{};
    std::uint64_t corrected_negative_level_count{0};
};

[[gnu::always_inline]]
inline PreApplyBookSnapshot capture_snapshot(const BookState* pre_book_state) noexcept {
    if (pre_book_state == nullptr) {
        return PreApplyBookSnapshot{};
    }
    return PreApplyBookSnapshot{
        .has_snapshot = pre_book_state->has_snapshot,
        .desynced = pre_book_state->desynced,
        .last_seq_id = pre_book_state->last_seq_id,
        .corrected_negative_level_count = pre_book_state->corrected_negative_level_count,
    };
}

[[gnu::noinline, gnu::cold]]
inline void log_correction(
    const predex::core::ingest::kalshi::FrameHandle& handle,
    const BookState& post_book,
    const predex::internal::NormalizedEvent& event
)noexcept{
    char time_buf[32];
    format_utc_timestamp(time_buf, sizeof(time_buf));
    std::fprintf(stdout,
        "[%s] SHARD | phase=correction | event_id=%u | market_id=%u"
        " | sid=%u | seq=%llu | side=%s | price_ticks=%lld | qty_lots=%lld"
        " | existing_qty_lots=%lld | updated_qty_lots=%lld"
        " | correction_count=%llu\n",
        time_buf,
        event.meta.event_id,
        event.meta.market_id,
        handle.sid_,
        static_cast<unsigned long long>(
            event.effective_sequence_id().value_or(0)),
        detail::side_name(post_book.last_failure.side),
        static_cast<long long>(post_book.last_failure.price_ticks),
        static_cast<long long>(post_book.last_failure.delta_qty_lots),
        static_cast<long long>(post_book.last_failure.existing_qty_lots),
        static_cast<long long>(post_book.last_failure.updated_qty_lots),
        static_cast<unsigned long long>(
        post_book.corrected_negative_level_count));
    std::fflush(stdout);
}

[[gnu::noinline, gnu::cold]]
inline void log_desync(
    const predex::core::ingest::kalshi::FrameHandle& handle,
    const PreApplyBookSnapshot& snapshot,
    const BookState* post_book,
    const predex::internal::NormalizedEvent& event,
    std::unordered_set<std::uint64_t>& logged_keys,
    ShardDesyncReason reason
)noexcept{
    //assumes EventId and MarketId are both at most 32 bits, catching any potential widening here
    static_assert(sizeof(event.meta.event_id) <= 4U, "EventId must fit within 32 bits");
    static_assert(sizeof(event.meta.market_id) <= 4U, "MarketId must fit within 32 bits");
    const std::uint64_t desync_key = 
        (static_cast<std::uint64_t>(event.meta.event_id) << 32U) | static_cast<std::uint64_t>(event.meta.market_id);
    const bool first_instance = logged_keys.insert(desync_key).second;
    if(!first_instance){
        return;
    }
    internal::Side side = internal::Side::kUnknown;
    internal::PriceTicks price_ticks = 0;
    internal::QtyLots qty_lots = 0;
    if(const auto *delta = std::get_if<internal::DeltaData>(&event.data)){
        side = delta->side;
        price_ticks = delta->price_ticks;
        qty_lots = delta->delta_qty_lots;
    } else if(const auto *trade = std::get_if<internal::TradeData>(&event.data)){
        side = trade->book_side;
        price_ticks = trade->price_ticks;
        qty_lots = trade->qty_lots;
    }
    const auto existing_qty_lots = post_book != nullptr ? post_book->last_failure.existing_qty_lots : 0;
    const auto updated_qty_lots = post_book != nullptr ? post_book->last_failure.updated_qty_lots : 0;
    const auto pending_delta_count = post_book != nullptr ? post_book->last_failure.pending_delta_count : 0;
    const auto recovery_snapshot_count = post_book != nullptr ? post_book->recovery_snapshot_count : 0;
    
    char time_buf[32];  
    format_utc_timestamp(time_buf, sizeof(time_buf));

    std::fprintf(stdout,
        "[%s] SHARD | phase=first_desync | event_id=%u | market_id=%u"
        " | sid=%u | seq=%llu | raw_type=%s | parsed_type=%s"
        " | reason=%s | pre_has_snapshot=%s | pre_book_desynced=%s"
        " | pre_last_seq=%llu | side=%s | price_ticks=%lld | qty_lots=%lld"
        " | existing_qty_lots=%lld | updated_qty_lots=%lld"
        " | pending_deltas=%zu | recovery_snapshots=%llu\n",
        time_buf,
        event.meta.event_id,
        event.meta.market_id,
        handle.sid_,
        static_cast<unsigned long long>(event.effective_sequence_id().value_or(0)),
        raw_event_type_name(handle.event_type_),
        parsed_event_type_name(event.type),
        desync_reason_name(reason),
        snapshot.has_snapshot ? "true" : "false",
        snapshot.desynced ? "true" : "false",
        static_cast<unsigned long long>(snapshot.last_seq_id.value_or(0)),
        side_name(side),
        static_cast<long long>(price_ticks),
        static_cast<long long>(qty_lots),
        static_cast<long long>(existing_qty_lots),
        static_cast<long long>(updated_qty_lots),
        pending_delta_count,
        static_cast<unsigned long long>(recovery_snapshot_count));
    if(post_book != nullptr && !post_book->recent_updates.empty()){
        std::fprintf(stdout,
            "[%s] SHARD | phase=recent_updates | event_id=%u | market_id=%u",
            time_buf,
            event.meta.event_id,
            event.meta.market_id);
        for(const auto& update : post_book->recent_updates){
            std::fprintf(stdout,
                " | type=%s,seq=%llu,side=%s,price=%lld,qty=%lld",
                parsed_event_type_name(update.type),
                static_cast<unsigned long long>(update.seq_id.value_or(0)),
                side_name(update.side),
                static_cast<long long>(update.price_ticks),
                static_cast<long long>(update.qty_lots));
        }
        std::fputc('\n', stdout);
    }
    std::fflush(stdout);
}


} // namespace detail

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
                   utils::SPSCQueue<ingest::kalshi::FrameHandle>& recycle_queue,
                   EventStore& event_store,
                   Bundle bundle)
        : input_queue_(input_queue),
          frame_pool_(frame_pool),
          logger_queue_(logger_queue),
          recycle_queue_(recycle_queue),
          event_store_(event_store),
          bundle_(std::move(bundle)) {}

    // Thread-safe: may be called from any thread (e.g. io_loop on reconnect).
    void request_reset() noexcept {
        reset_requested_.store(true, std::memory_order_release);
    }

    [[nodiscard]] std::size_t pump(std::size_t max_batch_size) noexcept {
        if (reset_requested_.exchange(false, std::memory_order_acq_rel)) {
            event_store_.reset_all_books();
        }
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
            }
        }
        return processed;
    }

  private:
    utils::SPSCQueue<ingest::kalshi::FrameHandle>& input_queue_;
    ingest::kalshi::FramePool& frame_pool_;
    utils::SPSCQueue<ingest::kalshi::FrameHandle>& logger_queue_;
    utils::SPSCQueue<ingest::kalshi::FrameHandle>& recycle_queue_;
    parsers::kalshi::Parser parser_;
    EventStore& event_store_;
    Bundle bundle_;

    std::atomic<bool> reset_requested_{false};

    std::uint64_t processed_count_{0};
    std::uint64_t failed_count_{0};
    std::uint64_t parse_fail_count_{0};
    std::uint64_t apply_fail_count_{0};
    std::uint64_t logger_fail_count_{0};
    std::unordered_set<std::uint64_t> logged_desync_keys_;

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
            // Logger queue full — recycle the handle so the frame pool is not bled. Any book
            // state mutations performed by apply_event already succeeded; losing the tape entry
            // is the only consequence here.
            (void)recycle_queue_.try_push(handle);
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

        const BookState* pre_book = stored_event->book_store.find(event.meta.market_id);
        const auto pre_snap = detail::capture_snapshot(pre_book);

        const EventApplyResult apply_result = stored_event->apply_market_update(event);
        const BookState* post_book = stored_event->book_store.find(event.meta.market_id);

        if(post_book != nullptr && post_book->corrected_negative_level_count > pre_snap.corrected_negative_level_count) [[unlikely]] {
            detail::log_correction(handle, *post_book, event);
        }

        if(apply_result.desync_reason != ShardDesyncReason::kNone) [[unlikely]] {
            detail::log_desync(handle, pre_snap, post_book, event, logged_desync_keys_, apply_result.desync_reason);
        }
        
        bundle_.on_event_apply_result(event, *stored_event, apply_result);
        if (apply_result.code != EventApplyCode::kApplied) {
            ++apply_fail_count_;
            return ProcessOneResult{
                .code = ProcessCode::kApplyFail,
                .apply_code = apply_result.code,
                .pipeline_code = PipelineDecisionCode::kDeclined,
            };
        }

        if (event.type == internal::EventType::kLifecycle) {
            return ProcessOneResult{
                .code = ProcessCode::kProcessed,
                .apply_code = apply_result.code,
                .pipeline_code = PipelineDecisionCode::kDeclined,
            };
        }

        const AppliedEventUpdate update{event, *stored_event};
        const PipelineResult pipeline_result = bundle_.on_event(update);
        if (pipeline_result.code == PipelineDecisionCode::kError) [[unlikely]] {
            ++failed_count_;
            return ProcessOneResult{
                .code = ProcessCode::kPipelineError,
                .apply_code = apply_result.code,
                .pipeline_code = pipeline_result.code,
            };
        }   

        return ProcessOneResult{
            .code = ProcessCode::kProcessed,
            .apply_code = apply_result.code,
            .pipeline_code = pipeline_result.code,
        };
    }
};
} // namespace predex::core::shards::kalshi



