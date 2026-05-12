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

// SCAFFOLD: future private-WS worker for fill / lifecycle ingestion.
//
// Not constructed by `App` today. `ws_event_queue` is `nullptr` in production
// and the OMS coordinator only reads `oms_rest_event_queue`. This class will
// be wired when the private-WS transport lands (see docs/oms_design.md and
// open_backlog.md "Post-reconnect fill recovery"). The current method bodies
// are intentional no-ops — wiring this in without implementing real receive
// and parse logic would silently break the kill-switch invariant.
class PrivateWsWorker {
  public:
    explicit PrivateWsWorker(PrivateWsWorkerQueues queues, PrivateWsWorkerConfig config,
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
