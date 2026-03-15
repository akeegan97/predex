#include "trading/shards/shard.hpp"

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <string_view>
#include <thread>

namespace trading::shards {
namespace {

constexpr std::uint64_t kMaxParseRejectLogs = 8U;
constexpr std::uint64_t kMaxApplyRejectLogs = 8U;
std::atomic<std::uint64_t> g_parse_reject_logs{0};
std::atomic<std::uint64_t> g_apply_reject_logs{0};

std::string_view parse_error_name(parsers::ParseError error) noexcept {
    switch (error) {
    case parsers::ParseError::kNone:
        return "none";
    case parsers::ParseError::kInvalidJson:
        return "invalid_json";
    case parsers::ParseError::kMissingField:
        return "missing_field";
    case parsers::ParseError::kInvalidField:
        return "invalid_field";
    case parsers::ParseError::kUnsupportedMessageType:
        return "unsupported_type";
    }
    return "unknown";
}

std::string_view apply_reject_name(BookApplyRejectReason reason) noexcept {
    switch (reason) {
    case BookApplyRejectReason::kNone:
        return "none";
    case BookApplyRejectReason::kMissingMarketOrUnknownEvent:
        return "missing_market_or_unknown_event";
    case BookApplyRejectReason::kInvalidSnapshotData:
        return "invalid_snapshot_data";
    case BookApplyRejectReason::kReplayPendingDeltaFailure:
        return "replay_pending_delta_failure";
    case BookApplyRejectReason::kMissingDeltaData:
        return "missing_delta_data";
    case BookApplyRejectReason::kDeltaWhileDesynced:
        return "delta_while_desynced";
    case BookApplyRejectReason::kPendingDeltaOverflowDesync:
        return "pending_delta_overflow_desync";
    case BookApplyRejectReason::kDeltaSequenceOrLevelInvalid:
        return "delta_sequence_or_level_invalid";
    case BookApplyRejectReason::kTradeInvalidOrDesynced:
        return "trade_invalid_or_desynced";
    case BookApplyRejectReason::kUnsupportedEventType:
        return "unsupported_event_type";
    }
    return "unknown";
}

} // namespace

Shard::Shard(ShardConfig config, router::ShardedEventDispatch& dispatch,
             IExchangeMessageParser& parser, BookStore& books, IShardEventHandler* event_handler)
    : config_(config), dispatch_(dispatch), parser_(parser), books_(books),
      event_handler_(event_handler) {}

Shard::~Shard() { stop(); }

bool Shard::start() {
    if (worker_.joinable()) {
        return false;
    }

    consumed_.store(0, std::memory_order_relaxed);
    parsed_.store(0, std::memory_order_relaxed);
    parse_errors_.store(0, std::memory_order_relaxed);
    parser_rejects_.store(0, std::memory_order_relaxed);
    apply_rejects_.store(0, std::memory_order_relaxed);
    parsed_snapshots_.store(0, std::memory_order_relaxed);
    parsed_deltas_.store(0, std::memory_order_relaxed);
    parsed_trades_.store(0, std::memory_order_relaxed);
    parsed_other_.store(0, std::memory_order_relaxed);
    parse_error_invalid_json_.store(0, std::memory_order_relaxed);
    parse_error_missing_field_.store(0, std::memory_order_relaxed);
    parse_error_invalid_field_.store(0, std::memory_order_relaxed);
    parse_error_unsupported_type_.store(0, std::memory_order_relaxed);
    applied_.store(0, std::memory_order_relaxed);
    handler_invoked_.store(0, std::memory_order_relaxed);
    handler_errors_.store(0, std::memory_order_relaxed);
    // Publish started state to readers of running().
    running_.store(true, std::memory_order_release);

    worker_ = std::jthread([this](const std::stop_token& stop_token) { run(stop_token); });
    return true;
}

void Shard::stop() {
    if (!worker_.joinable()) {
        running_.store(false, std::memory_order_release);
        return;
    }

    worker_.request_stop();
    worker_.join();
    running_.store(false, std::memory_order_release);
}

// Acquire pairs with start/stop release stores.
bool Shard::running() const { return running_.load(std::memory_order_acquire); }

ShardStats Shard::stats() const {
    return ShardStats{
        .consumed = consumed_.load(std::memory_order_relaxed),
        .parsed = parsed_.load(std::memory_order_relaxed),
        .parse_errors = parse_errors_.load(std::memory_order_relaxed),
        .parser_rejects = parser_rejects_.load(std::memory_order_relaxed),
        .apply_rejects = apply_rejects_.load(std::memory_order_relaxed),
        .parsed_snapshots = parsed_snapshots_.load(std::memory_order_relaxed),
        .parsed_deltas = parsed_deltas_.load(std::memory_order_relaxed),
        .parsed_trades = parsed_trades_.load(std::memory_order_relaxed),
        .parsed_other = parsed_other_.load(std::memory_order_relaxed),
        .parse_error_invalid_json = parse_error_invalid_json_.load(std::memory_order_relaxed),
        .parse_error_missing_field = parse_error_missing_field_.load(std::memory_order_relaxed),
        .parse_error_invalid_field = parse_error_invalid_field_.load(std::memory_order_relaxed),
        .parse_error_unsupported_type =
            parse_error_unsupported_type_.load(std::memory_order_relaxed),
        .applied = applied_.load(std::memory_order_relaxed),
        .handler_invoked = handler_invoked_.load(std::memory_order_relaxed),
        .handler_errors = handler_errors_.load(std::memory_order_relaxed),
    };
}

void Shard::run(const std::stop_token& stop_token) {
    while (!stop_token.stop_requested()) {
        router::RoutedEvent routed{};
        if (!dispatch_.try_pop(config_.shard_id, routed)) {
            std::this_thread::sleep_for(config_.idle_sleep);
            continue;
        }

        consumed_.fetch_add(1, std::memory_order_relaxed);
        const auto parsed = parser_.parse(routed);
        if (!parsed.ok()) {
            const auto prior_logged = g_parse_reject_logs.fetch_add(1, std::memory_order_relaxed);
            if (prior_logged < kMaxParseRejectLogs) {
                constexpr std::size_t kMaxPayloadPreview = 320U;
                const auto& payload = routed.frame.raw_payload;
                const std::size_t preview_len = std::min(payload.size(), kMaxPayloadPreview);
                const std::string_view preview{payload.data(), preview_len};
                (void)std::fprintf(stderr,
                                   "shard.parse_reject shard_id=%zu exchange=%d market=%s error=%.*s payload_preview=%.*s\n",
                                   config_.shard_id, static_cast<int>(routed.frame.exchange),
                                   routed.frame.market_ticker.c_str(),
                                   static_cast<int>(parse_error_name(parsed.error()).size()),
                                   parse_error_name(parsed.error()).data(),
                                   static_cast<int>(preview.size()), preview.data());
            }
            switch (parsed.error()) {
            case parsers::ParseError::kInvalidJson:
                parse_error_invalid_json_.fetch_add(1, std::memory_order_relaxed);
                break;
            case parsers::ParseError::kMissingField:
                parse_error_missing_field_.fetch_add(1, std::memory_order_relaxed);
                break;
            case parsers::ParseError::kInvalidField:
                parse_error_invalid_field_.fetch_add(1, std::memory_order_relaxed);
                break;
            case parsers::ParseError::kUnsupportedMessageType:
                parse_error_unsupported_type_.fetch_add(1, std::memory_order_relaxed);
                break;
            case parsers::ParseError::kNone:
                break;
            }
            parser_rejects_.fetch_add(1, std::memory_order_relaxed);
            parse_errors_.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        const auto& parsed_event = parsed.value();
        if (!parsed_event.has_value()) {
            parser_rejects_.fetch_add(1, std::memory_order_relaxed);
            parse_errors_.fetch_add(1, std::memory_order_relaxed);
            continue;
        }

        parsed_.fetch_add(1, std::memory_order_relaxed);
        switch (parsed_event->type) {
        case internal::EventType::kSnapshot:
            parsed_snapshots_.fetch_add(1, std::memory_order_relaxed);
            break;
        case internal::EventType::kDelta:
            parsed_deltas_.fetch_add(1, std::memory_order_relaxed);
            break;
        case internal::EventType::kTrade:
            parsed_trades_.fetch_add(1, std::memory_order_relaxed);
            break;
        case internal::EventType::kHeartbeat:
        case internal::EventType::kStatus:
        case internal::EventType::kUnknown:
            parsed_other_.fetch_add(1, std::memory_order_relaxed);
            break;
        }
        const auto apply_result = books_.apply_with_result(*parsed_event);
        if (apply_result.applied) {
            applied_.fetch_add(1, std::memory_order_relaxed);
            if (event_handler_ != nullptr) {
                handler_invoked_.fetch_add(1, std::memory_order_relaxed);
                if (!event_handler_->on_event(*parsed_event)) {
                    handler_errors_.fetch_add(1, std::memory_order_relaxed);
                }
            }
        } else {
            const auto prior_logged = g_apply_reject_logs.fetch_add(1, std::memory_order_relaxed);
            if (prior_logged < kMaxApplyRejectLogs) {
                constexpr std::size_t kMaxPayloadPreview = 320U;
                const auto& payload = parsed_event->raw_payload;
                const std::size_t preview_len = std::min(payload.size(), kMaxPayloadPreview);
                const std::string_view preview{payload.data(), preview_len};
                const auto sequence_id = parsed_event->effective_sequence_id().value_or(0U);
                (void)std::fprintf(
                    stderr,
                    "shard.book_apply_reject shard_id=%zu exchange=%d market=%s seq=%llu reason=%.*s payload_preview=%.*s\n",
                    config_.shard_id, static_cast<int>(parsed_event->meta.exchange),
                    parsed_event->market_ticker.c_str(), static_cast<unsigned long long>(sequence_id),
                    static_cast<int>(apply_reject_name(apply_result.reason).size()),
                    apply_reject_name(apply_result.reason).data(), static_cast<int>(preview.size()),
                    preview.data());
            }
            apply_rejects_.fetch_add(1, std::memory_order_relaxed);
            parse_errors_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    running_.store(false, std::memory_order_release);
}

} // namespace trading::shards
