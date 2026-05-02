#pragma once

#include <chrono>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "predex/oms/oms_types.hpp"
#include "predex/oms/transport/kalshi_private_ws_adapter.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::oms::kalshi::transport {

struct PrivateWsWorkerQueues {
    utils::SPSCQueue<KalshiToOmsEvent>* event_queue{nullptr};
};

struct PrivateWsWorkerConfig {
    std::vector<std::string> channels;
    std::chrono::milliseconds recv_timeout{50};
};

struct ReconciliationRequest {};

// Temporary compatibility stub while the new gateway runtime replaces the old
// worker-based private websocket path.
class PrivateWsWorker {
  public:
    explicit PrivateWsWorker(PrivateWsWorkerQueues queues,
                             PrivateWsWorkerConfig config,
                             KalshiPrivateWsAdapter adapter)
        : queues_(queues), config_(std::move(config)), adapter_(std::move(adapter)) {}

    void run(const std::stop_token& stop_token) {
        while (!stop_token.stop_requested()) {
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
        }
    }

    [[nodiscard]] std::optional<ReconciliationRequest> take_reconciliation_request() noexcept {
        return std::nullopt;
    }

    void close() noexcept {}

  private:
    PrivateWsWorkerQueues queues_{};
    PrivateWsWorkerConfig config_{};
    KalshiPrivateWsAdapter adapter_;
};

} // namespace predex::core::oms::kalshi::transport
