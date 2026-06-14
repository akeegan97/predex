#pragma once

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

#include "predex/oms/gateway/gateway_types.hpp"
#include "predex/oms/transport/kalshi_rest_adapter.hpp"

namespace predex::core::oms::kalshi::gateway {
constexpr std::uint64_t kDefaultKeepWarmThresholdSeconds = 10;
// One warm async REST execution resource.
//
// Ownership:
// - owned by SessionPool
// - scheduled by Gateway through SessionPool
//
// Responsibilities:
// - owns one persistent HTTPS session
// - executes at most one DispatchRequest at a time
// - tracks low-level write/read completion and transport confidence milestones
// - reports request completion back to SessionPool
//
// Non-responsibilities:
// - batching policy
// - retry/recovery policy
// - uncertainty escalation to OMS
// - rate-limit admission
struct AsyncRestConnectionConfig {
    std::size_t connection_index{0};
    std::uint64_t keep_warm_threshold_seconds{kDefaultKeepWarmThresholdSeconds};
    std::string trace_output_path;
};

struct AsyncRestConnectionTelemetry {
    std::uint64_t started_requests{0};
    std::uint64_t completed_requests{0};
    std::uint64_t uncertain_requests{0};
    std::uint64_t failed_starts{0};
    std::uint64_t total_completion_latency_ns{0};
    std::uint64_t last_completion_latency_ns{0};
};

class AsyncRestConnection {
  public:
    explicit AsyncRestConnection(transport::KalshiRestAdapter adapter,
                                 AsyncRestConnectionConfig config = {});

    [[nodiscard]] bool idle() const noexcept;
    [[nodiscard]] bool has_inflight() const noexcept;
    [[nodiscard]] std::optional<DispatchRequestId> inflight_request_id() const noexcept;
    [[nodiscard]] const AsyncRestConnectionTelemetry& telemetry() const noexcept;

    // Preflight check used by SessionPool before handing off ownership.
    [[nodiscard]] bool can_start(const DispatchRequest& request) const noexcept;
    // Consumes one already-planned dispatch request and begins execution.
    void start(DispatchRequest&& request);

    // Non-blocking progress/completion check for the current request.
    // `kIdle` means nothing is in flight, `kInFlight` means still working,
    // and `kCompleted` returns a completion payload to SessionPool.
    [[nodiscard]] ConnectionPollResult poll() noexcept;

    // Performs connection warming/keepalive work while idle.
    [[nodiscard]] bool warm_up() noexcept;
    void keep_warm() noexcept;

    // Closes the underlying session. Any unresolved in-flight request is left
    // for SessionPool/Gateway to recover once surfaced through polling/teardown.
    void close() noexcept;

  private:
    transport::KalshiRestAdapter adapter_;
    AsyncRestConnectionConfig config_{};
    std::optional<DispatchRequest> inflight_request_;
    std::optional<DispatchCompletion> pending_completion_;
    std::size_t inflight_item_index_{0};
    std::optional<transport::PreparedCommandRequest> inflight_prepared_request_;
    std::optional<transport::RestTraceInfo> last_trace_;
    std::vector<KalshiToOmsEvent> emitted_events_;
    AsyncRestConnectionTelemetry telemetry_{};
    std::ofstream trace_output_file_;

    [[nodiscard]] static bool request_is_executable(const DispatchRequest& request) noexcept;
    void reset_inflight_state_() noexcept;
    void finalize_pending_completion_(DispatchRequestState terminal_state,
                                      std::string error_message = {}) noexcept;
    [[nodiscard]] bool start_current_item_();
    [[nodiscard]] transport::PreparedCommandRequest
    prepare_item_(const DispatchItem& item) const;
    [[nodiscard]] transport::PreparedCommandRequest
    prepare_batched_submit_request_() const noexcept;
    [[nodiscard]] transport::CommandResult
    complete_item_(const DispatchItem& item, const transport::HttpResponse& response);
    [[nodiscard]] transport::CommandResult
    complete_batched_submit_request_(const transport::HttpResponse& response) noexcept;
    void append_trace_row_(const DispatchCompletion& completion) noexcept;
};

} // namespace predex::core::oms::kalshi::gateway
