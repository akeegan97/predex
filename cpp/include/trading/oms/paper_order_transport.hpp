#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

#include "trading/oms/transport.hpp"

namespace trading::oms {

struct PaperOrderTransportConfig {
    bool auto_fill_on_place{true};
    std::size_t fill_parts{1};
    std::size_t place_reject_bps{0};
    std::chrono::milliseconds ack_delay{std::chrono::milliseconds{0}};
    std::chrono::milliseconds fill_delay{std::chrono::milliseconds{0}};
    std::chrono::milliseconds fill_interval{std::chrono::milliseconds{0}};
    std::chrono::milliseconds reject_delay{std::chrono::milliseconds{0}};
};

class PaperOrderTransport final : public IOrderTransport {
  public:
    explicit PaperOrderTransport(PaperOrderTransportConfig config = {});

    [[nodiscard]] bool connect(const OrderTransportConfig& config) override;
    [[nodiscard]] bool send_text(std::string_view payload) override;
    [[nodiscard]] std::optional<std::string> recv_text() override;
    void close() override;
    [[nodiscard]] std::string_view last_error() const override;

  private:
    enum class ErrorCode : std::uint8_t {
        kNone = 0,
        kNotConnected,
        kInvalidPayload,
        kInvalidRequest,
        kUnknownAction,
    };

    void set_error(ErrorCode error_code);

    struct ScheduledUpdate {
        std::uint64_t due_ts_ns{0};
        std::string payload;
    };

    [[nodiscard]] bool should_reject_place(std::string_view client_order_id) const;
    [[nodiscard]] static std::uint64_t monotonic_now_ns();
    void enqueue_update(std::uint64_t due_ts_ns, std::string payload);

    PaperOrderTransportConfig config_;
    mutable std::mutex mutex_;
    bool connected_{false};
    std::uint64_t next_recv_ts_ns_{1};
    std::uint64_t next_exchange_order_id_{1};
    std::deque<ScheduledUpdate> inbound_updates_;
    std::atomic<ErrorCode> last_error_code_{ErrorCode::kNone};
};

} // namespace trading::oms
