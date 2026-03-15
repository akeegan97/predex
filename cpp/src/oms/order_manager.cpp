#include "trading/oms/order_manager.hpp"

#include <chrono>
#include <exception>
#include <thread>
#include <utility>

namespace trading::oms {

namespace {
internal::TimestampNs to_timestamp_ns(std::chrono::steady_clock::time_point time_point) {
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::nanoseconds>(time_point.time_since_epoch());
    return elapsed.count() > 0 ? static_cast<internal::TimestampNs>(elapsed.count()) : 0U;
}
} // namespace

OrderManager::OrderManager(const IExchangeOmsAdapter& adapter, IOrderTransport& transport,
                           IOrderEventSink& event_sink, OrderManagerConfig config)
    : adapter_(adapter), transport_(transport), event_sink_(event_sink), config_(std::move(config)),
      core_(config_.global_risk, config_.portfolio_risk) {}

OrderManager::~OrderManager() { stop(); }

bool OrderManager::start() {
    if (worker_.joinable()) {
        return false;
    }

    set_error("");
    submitted_count_.store(0, std::memory_order_relaxed);
    sent_count_.store(0, std::memory_order_relaxed);
    send_failed_count_.store(0, std::memory_order_relaxed);
    receive_count_.store(0, std::memory_order_relaxed);
    parse_failed_count_.store(0, std::memory_order_relaxed);
    update_drop_count_.store(0, std::memory_order_relaxed);
    unsupported_intent_count_.store(0, std::memory_order_relaxed);
    pending_intent_high_watermark_.store(0, std::memory_order_relaxed);
    submit_to_send_latency_hist_.reset();
    {
        std::scoped_lock core_lock{core_mutex_};
        core_.reset();
    }

    if (!transport_.connect(config_.transport)) {
        set_error(transport_.last_error());
        return false;
    }

    // Publish started state before worker loop.
    running_.store(true, std::memory_order_release);
    worker_ = std::jthread([this](const std::stop_token& stop_token) { run(stop_token); });
    return true;
}

void OrderManager::stop() {
    if (!worker_.joinable()) {
        running_.store(false, std::memory_order_release);
        return;
    }

    worker_.request_stop();
    transport_.close();
    worker_.join();
    running_.store(false, std::memory_order_release);
}

// Acquire pairs with start/stop release stores.
bool OrderManager::running() const { return running_.load(std::memory_order_acquire); }

std::optional<internal::OrderRequestId> OrderManager::submit(internal::OrderIntent intent) {
    if (!running()) {
        set_error("OrderManager is not running");
        return std::nullopt;
    }
    if (!adapter_.supports(intent)) {
        unsupported_intent_count_.fetch_add(1, std::memory_order_relaxed);
        set_error("Order intent is not supported by adapter");
        return std::nullopt;
    }

    if (intent.intent_ts_ns == 0) {
        intent.intent_ts_ns = monotonic_now_ns();
    }
    const internal::OrderRequestId request_id =
        next_request_id_.fetch_add(1, std::memory_order_relaxed);

    std::string core_error;
    {
        std::scoped_lock core_lock{core_mutex_};
        if (!core_.validate_and_track_submission(request_id, intent, core_error)) {
            set_error(core_error);
            return std::nullopt;
        }
    }

    {
        std::scoped_lock queue_lock{queue_mutex_};
        outbound_queue_.push_back(PendingIntent{
            .request_id = request_id,
            .queued_ts_ns = monotonic_now_ns(),
            .intent = std::move(intent),
        });
        const std::size_t queue_size = outbound_queue_.size();
        std::size_t previous = pending_intent_high_watermark_.load(std::memory_order_relaxed);
        while (queue_size > previous &&
               !pending_intent_high_watermark_.compare_exchange_weak(
                   previous, queue_size, std::memory_order_relaxed, std::memory_order_relaxed)) {
        }
    }
    submitted_count_.fetch_add(1, std::memory_order_relaxed);
    return request_id;
}

OrderManagerStats OrderManager::stats() const {
    std::size_t pending_intent_count = 0;
    {
        std::scoped_lock queue_lock{queue_mutex_};
        pending_intent_count = outbound_queue_.size();
    }

    OrderManagerCoreStats core_stats{};
    {
        std::scoped_lock core_lock{core_mutex_};
        core_stats = core_.stats();
    }

    return OrderManagerStats{
        .submitted_count = submitted_count_.load(std::memory_order_relaxed),
        .sent_count = sent_count_.load(std::memory_order_relaxed),
        .send_failed_count = send_failed_count_.load(std::memory_order_relaxed),
        .risk_reject_count = core_stats.risk_reject_count,
        .portfolio_risk_reject_count = core_stats.portfolio_risk_reject_count,
        .receive_count = receive_count_.load(std::memory_order_relaxed),
        .parse_failed_count = parse_failed_count_.load(std::memory_order_relaxed),
        .update_drop_count = update_drop_count_.load(std::memory_order_relaxed),
        .transition_applied_count = core_stats.transition_applied_count,
        .transition_reject_count = core_stats.transition_reject_count,
        .unknown_order_update_count = core_stats.unknown_order_update_count,
        .policy_reject_count = core_stats.policy_reject_count,
        .unsupported_intent_count = unsupported_intent_count_.load(std::memory_order_relaxed),
        .pending_intent_count = pending_intent_count,
        .pending_intent_high_watermark =
            pending_intent_high_watermark_.load(std::memory_order_relaxed),
        .tracked_order_count = core_stats.tracked_order_count,
        .active_order_count = core_stats.active_order_count,
        .submit_to_send_latency = submit_to_send_latency_hist_.snapshot(),
    };
}

std::string OrderManager::last_error() const {
    std::scoped_lock lock{error_mutex_};
    return last_error_;
}

std::optional<InFlightOrderSnapshot>
OrderManager::in_flight_order(std::string_view client_order_id) const {
    std::scoped_lock core_lock{core_mutex_};
    return core_.in_flight_order(client_order_id);
}

internal::TimestampNs OrderManager::monotonic_now_ns() {
    return to_timestamp_ns(std::chrono::steady_clock::now());
}

void OrderManager::run(const std::stop_token& stop_token) {
    while (!stop_token.stop_requested()) {
        const std::size_t drained_intents = drain_outbound_intents();
        const bool processed_update = pump_incoming_update();
        if (drained_intents == 0 && !processed_update) {
            std::this_thread::sleep_for(config_.loop_idle_sleep);
        }
    }

    transport_.close();
    running_.store(false, std::memory_order_release);
}

// NOLINTBEGIN(readability-function-cognitive-complexity)
std::size_t OrderManager::drain_outbound_intents() {
    std::deque<PendingIntent> local_intents;
    {
        std::scoped_lock queue_lock{queue_mutex_};
        if (outbound_queue_.empty()) {
            return 0;
        }
        local_intents.swap(outbound_queue_);
    }

    std::size_t processed_count = 0;
    for (auto& pending_intent : local_intents) {
        ++processed_count;

        std::optional<internal::OrderStateUpdate> risk_reject_update;
        std::string core_error;
        {
            std::scoped_lock core_lock{core_mutex_};
            const auto decision = core_.evaluate_global_risk(pending_intent.intent, core_error);
            if (decision.has_value()) {
                risk_reject_update =
                    OrderManagerCore::make_global_risk_reject_update(pending_intent.intent, *decision);
                if (!core_.apply_update_transition(*risk_reject_update, core_error)) {
                    update_drop_count_.fetch_add(1, std::memory_order_relaxed);
                    set_error(core_error);
                    continue;
                }
            }
        }

        if (risk_reject_update.has_value()) {
            set_error(core_error);
            if (!event_sink_.on_order_update(*risk_reject_update)) {
                update_drop_count_.fetch_add(1, std::memory_order_relaxed);
            }
            continue;
        }

        if (config_.portfolio_snapshot_provider != nullptr) {
            const auto snapshot = config_.portfolio_snapshot_provider->snapshot_for(
                pending_intent.intent.exchange, pending_intent.intent.market_ticker);
            {
                std::scoped_lock core_lock{core_mutex_};
                const auto decision =
                    core_.evaluate_portfolio_risk(pending_intent.intent, snapshot, core_error);
                if (decision.has_value()) {
                    risk_reject_update =
                        OrderManagerCore::make_portfolio_risk_reject_update(pending_intent.intent,
                                                                             *decision);
                    if (!core_.apply_update_transition(*risk_reject_update, core_error)) {
                        update_drop_count_.fetch_add(1, std::memory_order_relaxed);
                        set_error(core_error);
                        continue;
                    }
                }
            }
        }

        if (risk_reject_update.has_value()) {
            set_error(core_error);
            if (!event_sink_.on_order_update(*risk_reject_update)) {
                update_drop_count_.fetch_add(1, std::memory_order_relaxed);
            }
            continue;
        }

        try {
            const std::string payload = adapter_.build_request(pending_intent.intent);
            if (!transport_.send_text(payload)) {
                send_failed_count_.fetch_add(1, std::memory_order_relaxed);
                set_error(transport_.last_error());
                continue;
            }
            sent_count_.fetch_add(1, std::memory_order_relaxed);
            if (pending_intent.queued_ts_ns > 0) {
                const auto now_ns = monotonic_now_ns();
                if (now_ns > pending_intent.queued_ts_ns) {
                    submit_to_send_latency_hist_.observe(now_ns - pending_intent.queued_ts_ns);
                }
            }
        } catch (const std::exception& exception) {
            send_failed_count_.fetch_add(1, std::memory_order_relaxed);
            set_error(exception.what());
        } catch (...) {
            send_failed_count_.fetch_add(1, std::memory_order_relaxed);
            set_error("Unknown error building OMS request");
        }
    }

    return processed_count;
}
// NOLINTEND(readability-function-cognitive-complexity)

bool OrderManager::pump_incoming_update() {
    auto payload = transport_.recv_text();
    if (!payload.has_value()) {
        return false;
    }

    receive_count_.fetch_add(1, std::memory_order_relaxed);
    const auto parse_result = adapter_.parse_update(*payload);
    const auto& parsed_update = parse_result.value();
    if (!parse_result.ok() || !parsed_update.has_value()) {
        parse_failed_count_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    internal::OrderStateUpdate update = *parsed_update;
    std::string core_error;
    {
        std::scoped_lock core_lock{core_mutex_};
        if (!core_.apply_update_transition(update, core_error)) {
            update_drop_count_.fetch_add(1, std::memory_order_relaxed);
            set_error(core_error);
            return true;
        }
    }

    if (!event_sink_.on_order_update(update)) {
        update_drop_count_.fetch_add(1, std::memory_order_relaxed);
    }
    return true;
}

void OrderManager::set_error(std::string_view error_message) {
    std::scoped_lock lock{error_mutex_};
    last_error_.assign(error_message);
}

} // namespace trading::oms
