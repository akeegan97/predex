#include "predex/oms/transport/rest_worker.hpp"

#include <chrono>
#include <thread>
#include <utility>
#include <variant>

namespace predex::core::oms::kalshi::transport {
namespace {

constexpr auto kIdleSleep = std::chrono::milliseconds{1};
constexpr std::size_t kMaxPendingRestEvents = 4096;

[[nodiscard]] internal::TimestampNs monotonic_now_ns() {
    return static_cast<internal::TimestampNs>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

[[nodiscard]] std::optional<internal::PriceTicks> parse_limit_price_ticks(
    const OpenOrderSnapshot& snapshot) {
    if (snapshot.side == "yes" && !snapshot.yes_price_dollars.empty()) {
        try {
            const double dollars = std::stod(snapshot.yes_price_dollars);
            return static_cast<internal::PriceTicks>(dollars * 10000.0);
        } catch (...) {
            return std::nullopt;
        }
    }
    if (snapshot.side == "no" && !snapshot.no_price_dollars.empty()) {
        try {
            const double dollars = std::stod(snapshot.no_price_dollars);
            return static_cast<internal::PriceTicks>(dollars * 10000.0);
        } catch (...) {
            return std::nullopt;
        }
    }
    return std::nullopt;
}

[[nodiscard]] internal::Side parse_order_action_side(const OpenOrderSnapshot& snapshot) {
    if (snapshot.action == "buy") {
        return internal::Side::kBuy;
    }
    if (snapshot.action == "sell") {
        return internal::Side::kSell;
    }
    return internal::Side::kUnknown;
}

[[nodiscard]] Outcome parse_order_outcome(const OpenOrderSnapshot& snapshot) {
    if (snapshot.side == "yes") {
        return Outcome::kYes;
    }
    if (snapshot.side == "no") {
        return Outcome::kNo;
    }
    return Outcome::kYes;
}

[[nodiscard]] internal::QtyLots parse_count_fp_to_lots(std::string_view value) {
    if (value.empty()) {
        return 0;
    }

    const std::size_t dot_pos = value.find('.');
    const auto integer_part =
        dot_pos == std::string_view::npos ? value : value.substr(0, dot_pos);
    if (integer_part.empty()) {
        return 0;
    }

    std::int64_t parsed = 0;
    const auto [ptr, ec] = std::from_chars(
        integer_part.data(), integer_part.data() + integer_part.size(), parsed);
    if (ec != std::errc() || ptr != integer_part.data() + integer_part.size() || parsed < 0) {
        return 0;
    }

    return static_cast<internal::QtyLots>(parsed);
}

[[nodiscard]] KalshiToOmsEvent make_submit_reject_event(const SubmitOrderCmd& command) {
    return VenueOrderReject{
        .order = command.order,
        .recv_ts_ns = monotonic_now_ns(),
        .reason = VenueRejectReason::kUnknown,
        .raw_reason_code = "rest_submit_failed",
        .raw_reason_message = "REST submit failed",
    };
}

[[nodiscard]] KalshiToOmsEvent make_cancel_reject_event(const CancelOrderCmd& command) {
    return VenueCancelReject{
        .order = command.corr.order,
        .recv_ts_ns = monotonic_now_ns(),
        .reason = VenueRejectReason::kUnknown,
        .raw_reason_code = "rest_cancel_failed",
        .raw_reason_message = "REST cancel failed",
    };
}

[[nodiscard]] KalshiToOmsEvent make_modify_reject_event(const ModifyOrderCmd& command) {
    return VenueModifyReject{
        .order = command.corr.order,
        .recv_ts_ns = monotonic_now_ns(),
        .reason = VenueRejectReason::kUnknown,
        .raw_reason_code = "rest_modify_failed",
        .raw_reason_message = "REST modify failed",
    };
}

[[nodiscard]] ReconcileOpenOrderSnapshot make_reconcile_event(const OpenOrderSnapshot& snapshot) {
    return ReconcileOpenOrderSnapshot{
        .order = snapshot.order,
        .context = {},
        .exchange = internal::ExchangeId::kKalshi,
        .side = parse_order_action_side(snapshot),
        .outcome = parse_order_outcome(snapshot),
        .initial_qty_lots = parse_count_fp_to_lots(snapshot.initial_count_fp),
        .working_qty_lots = parse_count_fp_to_lots(snapshot.remaining_count_fp),
        .cumulative_filled_qty_lots = parse_count_fp_to_lots(snapshot.fill_count_fp),
        .working_limit_price_ticks = parse_limit_price_ticks(snapshot),
        .recv_ts_ns = monotonic_now_ns(),
    };
}

} // namespace

RestWorker::RestWorker(RestWorkerQueues queues,
                       KalshiRestAdapter adapter,
                       RestWorkerConfig config)
    : queues_(queues), adapter_(std::move(adapter)), config_(std::move(config)) {}

void RestWorker::run(const std::stop_token& stop_token) {
    while (!stop_token.stop_requested()) {
        if (reconcile_requested_.exchange(false, std::memory_order_acq_rel)) {
            if (!reconcile_open_orders()) {
                std::this_thread::sleep_for(kIdleSleep);
            }
            continue;
        }
        if (!flush_pending_events()) {
            std::this_thread::sleep_for(kIdleSleep);
            continue;
        }
        if (process_one_command()) {
            continue;
        }
        std::this_thread::sleep_for(kIdleSleep);
    }
}

void RestWorker::request_reconcile() noexcept {
    reconcile_requested_.store(true, std::memory_order_release);
}

bool RestWorker::reconcile_open_orders() {
    while (!flush_pending_events()) {
        std::this_thread::sleep_for(kIdleSleep);
    }

    std::optional<std::string> cursor = std::nullopt;
    while (true) {
        OpenOrdersPage page = adapter_.fetch_open_orders(kDefaultOpenOrderFetchLimit, cursor);
        if (!page.ok) {
            return false;
        }

        for (const auto& snapshot : page.orders) {
            auto reconcile_event = make_reconcile_event(snapshot);
            if (config_.ticker_seed_resolver != nullptr) {
                if (auto seed = config_.ticker_seed_resolver(snapshot.ticker);
                    seed.has_value()) {
                    reconcile_event.context = std::move(seed->context);
                    reconcile_event.exchange = seed->exchange;
                    reconcile_event.side = seed->side;
                    reconcile_event.outcome = seed->outcome;
                }
            }
            if (!enqueue_event(KalshiToOmsEvent{std::move(reconcile_event)})) {
                return false;
            }
        }

        while (!flush_pending_events()) {
            std::this_thread::sleep_for(kIdleSleep);
        }

        if (!page.next_cursor.has_value() || page.next_cursor->empty()) {
            return true;
        }
        cursor = std::move(page.next_cursor);
    }
}

bool RestWorker::flush_pending_events() {
    if (queues_.event_queue == nullptr) {
        return pending_events_.empty();
    }

    while (!pending_events_.empty()) {
        if (!queues_.event_queue->try_push(std::move(pending_events_.front()))) {
            return false;
        }
        pending_events_.pop_front();
    }
    return true;
}

bool RestWorker::enqueue_event(KalshiToOmsEvent event) {
    if (pending_events_.size() >= kMaxPendingRestEvents) {
        return false;
    }

    pending_events_.push_back(std::move(event));
    return true;
}

bool RestWorker::process_one_command() {
    if (queues_.command_queue == nullptr) {
        return false;
    }

    OmsToKalshiCommand command{};
    if (!queues_.command_queue->try_pop(command)) {
        return false;
    }

    return handle_command(command);
}

bool RestWorker::handle_command(const OmsToKalshiCommand& command) {
    return std::visit(
        [this](const auto& typed_command) -> bool {
            auto result = [&]() {
                using T = std::decay_t<decltype(typed_command)>;
                if constexpr (std::is_same_v<T, SubmitOrderCmd>) {
                    return adapter_.submit_order(typed_command);
                } else if constexpr (std::is_same_v<T, CancelOrderCmd>) {
                    return adapter_.cancel_order(typed_command);
                } else {
                    return adapter_.modify_order(typed_command);
                }
            }();

            if (result.event.has_value()) {
                return enqueue_event(std::move(*result.event));
            }

            if (result.ok) {
                return true;
            }

            KalshiToOmsEvent reject_event = [&]() -> KalshiToOmsEvent {
                using T = std::decay_t<decltype(typed_command)>;
                if constexpr (std::is_same_v<T, SubmitOrderCmd>) {
                    auto event = make_submit_reject_event(typed_command);
                    auto& reject = std::get<VenueOrderReject>(event);
                    reject.raw_reason_message = result.error_message;
                    return event;
                } else if constexpr (std::is_same_v<T, CancelOrderCmd>) {
                    auto event = make_cancel_reject_event(typed_command);
                    auto& reject = std::get<VenueCancelReject>(event);
                    reject.raw_reason_message = result.error_message;
                    return event;
                } else {
                    auto event = make_modify_reject_event(typed_command);
                    auto& reject = std::get<VenueModifyReject>(event);
                    reject.raw_reason_message = result.error_message;
                    return event;
                }
            }();

            return enqueue_event(std::move(reject_event));
        },
        command);
}

bool RestWorker::emit_event(const KalshiToOmsEvent& event) {
    return enqueue_event(event);
}

} // namespace predex::core::oms::kalshi::transport
