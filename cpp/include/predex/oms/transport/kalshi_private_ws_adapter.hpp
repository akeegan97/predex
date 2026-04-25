#pragma once

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <nlohmann/json_fwd.hpp>

#include "predex/oms/oms_types.hpp"
#include "predex/websocket/kalshi/ws_adapter.hpp"

namespace predex::core::oms::kalshi::transport {

enum class PrivateWsReconcileReason : std::uint8_t {
    kReconnect = 1,
    kSeqGap = 2,
};

struct PrivateWsReconcileRequest {
    PrivateWsReconcileReason reason{PrivateWsReconcileReason::kReconnect};
};

struct PrivateWsParseResult {
    std::vector<KalshiToOmsEvent> events;
    std::optional<PrivateWsReconcileRequest> reconcile_request;
    std::string error_message;
};

// Kalshi-specific private-WS translation layer. Owns payload parsing and
// normalization into OMS2 venue events, but delegates the actual websocket
// session mechanics to the lower-level websocket stack.
class KalshiPrivateWsAdapter {
  public:
    explicit KalshiPrivateWsAdapter(predex::websocket::kalshi::WsAdapter ws_adapter);

    [[nodiscard]] const predex::websocket::kalshi::WsAdapter& ws_adapter() const noexcept;
    void reset_sequence_tracking() noexcept;

    // Parses one raw websocket payload into zero or more normalized OMS venue
    // events. Reconciliation intent is surfaced separately so the worker can
    // trigger repair logic without overloading the event stream itself.
    [[nodiscard]] PrivateWsParseResult parse_message(std::string_view payload);

  private:
    predex::websocket::kalshi::WsAdapter ws_adapter_;
    std::unordered_map<std::uint64_t, std::uint64_t> last_seq_by_sid_;

    [[nodiscard]] static std::optional<KalshiToOmsEvent> parse_single_event_(
        const std::string& message_type,
        const OmsOrderRef& order,
        internal::TimestampNs recv_ts_ns,
        const nlohmann::json& payload);
};

} // namespace predex::core::oms::kalshi::transport
