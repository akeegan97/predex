#include "trading/strategy/strategy_runner.hpp"

#include <chrono>
#include <exception>
#include <utility>

namespace trading::strategy {
namespace {

internal::TimestampNs monotonic_now_ns() {
    const auto now = std::chrono::steady_clock::now();
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
    return elapsed.count() > 0 ? static_cast<internal::TimestampNs>(elapsed.count()) : 0U;
}

} // namespace

StrategyRunner::StrategyRunner(IStrategy& strategy, IOrderIntentSink& intent_sink,
                               StrategyRunnerConfig config)
    : strategy_(strategy), intent_sink_(intent_sink), config_(config),
      shard_risk_gate_(config_.shard_risk) {}

bool StrategyRunner::on_event(const internal::NormalizedEvent& event) {
    ++stats_.events_processed_count;

    StrategyDecision decision{};
    try {
        decision = strategy_.on_event(event);
    } catch (const std::exception& exception) {
        ++stats_.strategy_error_count;
        set_error(exception.what());
        return false;
    } catch (...) {
        ++stats_.strategy_error_count;
        set_error("strategy error: unknown exception");
        return false;
    }

    for (auto intent : decision.intents) {
        if (intent.exchange == internal::ExchangeId::kUnknown) {
            intent.exchange = event.meta.exchange;
        }
        if (intent.market_ticker.empty()) {
            intent.market_ticker = event.market_ticker;
        }
        if (intent.intent_ts_ns == 0) {
            intent.intent_ts_ns = event.meta.recv_ns;
        }

        ++stats_.intents_emitted_count;

        if (config_.risk_snapshot_provider != nullptr) {
            const auto snapshot =
                config_.risk_snapshot_provider->snapshot_for(intent.exchange, intent.market_ticker);
            const auto risk_decision = shard_risk_gate_.evaluate(intent, snapshot);
            if (!risk_decision.allow) {
                ++stats_.risk_reject_count;
                set_error(risk_decision.reason_message);
                continue;
            }
        }

        if (!intent_sink_.submit_intent(std::move(intent))) {
            ++stats_.sink_reject_count;
            set_error(std::string{intent_sink_.last_error()});
            continue;
        }
        ++stats_.intents_submitted_count;
        if (event.meta.recv_ns > 0) {
            const auto now_ns = monotonic_now_ns();
            if (now_ns > event.meta.recv_ns) {
                event_to_submit_latency_hist_.observe(now_ns - event.meta.recv_ns);
            }
        }
    }

    return true;
}

StrategyRunnerStats StrategyRunner::stats() const {
    auto stats = stats_;
    stats.event_to_submit_latency = event_to_submit_latency_hist_.snapshot();
    return stats;
}

std::string StrategyRunner::last_error() const { return last_error_; }

void StrategyRunner::set_error(std::string error_message) { last_error_ = std::move(error_message); }

} // namespace trading::strategy
