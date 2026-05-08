#pragma once

#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <thread>

#include "predex/oms/oms_types.hpp"
#include "predex/oms/transport/kalshi_rest_adapter.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::oms::kalshi::transport {

struct RestWorkerQueues {
    utils::SPSCQueue<OmsToKalshiCommand>* command_queue{nullptr};
    utils::SPSCQueue<KalshiToOmsEvent>* event_queue{nullptr};
};

struct RestWorkerConfig {
    struct ReconcileOrderSeed {
        IntentContext context{};
        internal::ExchangeId exchange{internal::ExchangeId::kUnknown};
        internal::Side side{internal::Side::kUnknown};
        Outcome outcome{Outcome::kYes};
    };

    std::function<std::optional<ReconcileOrderSeed>(std::string_view)> ticker_seed_resolver;
    std::string trace_output_path{"predex_rest_trace.jsonl"};
};

// Temporary compatibility stub while the new gateway runtime replaces the old
// worker-based transport implementation.
class RestWorker {
  public:
    explicit RestWorker(RestWorkerQueues queues, KalshiRestAdapter adapter,
                        RestWorkerConfig config = {})
        : queues_(queues), adapter_(std::move(adapter)), config_(std::move(config)) {}

    void run(const std::stop_token& stop_token) {
        while (!stop_token.stop_requested()) {
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
        }
    }

    [[nodiscard]] bool reconcile_open_orders() { return true; }

    void request_reconcile() noexcept {}

  private:
    RestWorkerQueues queues_{};
    KalshiRestAdapter adapter_;
    RestWorkerConfig config_{};
};

} // namespace predex::core::oms::kalshi::transport
