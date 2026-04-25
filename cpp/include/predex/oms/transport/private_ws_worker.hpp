#pragma once

#include <chrono>
#include <stop_token>
#include <vector>

#include "predex/oms/oms_types.hpp"
#include "predex/oms/transport/kalshi_private_ws_adapter.hpp"
#include "predex/utils/spsc_queue.hpp"
#include "predex/websocket/client.hpp"
#include "predex/websocket/session.hpp"

namespace predex::core::oms::kalshi::transport {

struct PrivateWsWorkerQueues {
    utils::SPSCQueue<KalshiToOmsEvent>* event_queue{nullptr};
};

struct PrivateWsWorkerConfig {
    std::vector<std::string> channels;
    std::chrono::milliseconds recv_timeout{50};
};

// Owns the blocking private-WS thread loop. Reads authenticated OMS/private
// order traffic from Kalshi, normalizes messages into OMS2 venue events, and
// emits them toward OMS through a single SPSC queue.
class PrivateWsWorker {
  public:
    PrivateWsWorker(PrivateWsWorkerQueues queues,
                    PrivateWsWorkerConfig config,
                    KalshiPrivateWsAdapter adapter,
                    predex::websocket::BoostBeastWsTransport transport = {});

    void run(const std::stop_token& stop_token);
    void close();

    [[nodiscard]] std::optional<PrivateWsReconcileRequest> reconciliation_request() const noexcept;
    [[nodiscard]] std::optional<PrivateWsReconcileRequest> take_reconciliation_request() noexcept;

  private:
    PrivateWsWorkerQueues queues_{};
    PrivateWsWorkerConfig config_{};
    KalshiPrivateWsAdapter adapter_;
    predex::websocket::BoostBeastWsTransport transport_;
    predex::websocket::WsSession session_;
    std::optional<PrivateWsReconcileRequest> reconcile_request_;
    bool connected_{false};
    bool awaiting_seq_gap_reconcile_{false};

    [[nodiscard]] bool ensure_connected_and_subscribed();
    [[nodiscard]] bool process_one_message();
    [[nodiscard]] bool emit_event(const KalshiToOmsEvent& event);
    void mark_reconnect_needed_(PrivateWsReconcileReason reason) noexcept;
};

} // namespace predex::core::oms::kalshi::transport
