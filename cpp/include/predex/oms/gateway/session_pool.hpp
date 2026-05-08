#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <utility>
#include <vector>

#include "predex/oms/gateway/async_rest_connection.hpp"
#include "predex/oms/gateway/gateway_types.hpp"

namespace predex::core::oms::kalshi::gateway {

// Pool of warm async REST execution resources.
//
// Ownership:
// - owned by Gateway
//
// Responsibilities:
// - owns AsyncRestConnection instances
// - tracks free/busy connection state
// - maintains keepalive/warmth for reusable sessions
// - accepts already-planned DispatchRequests from Gateway
// - returns low-level completion results back to Gateway
//
// Non-responsibilities:
// - OMS command intake
// - sequencing
// - batching
// - rate limiting
// - retry/recovery policy
struct SessionPoolConfig {
    std::size_t hot_connection_count{0};
    std::size_t recovery_connection_count{0};
    std::size_t reconcile_connection_count{0};
};

struct SessionPoolTelemetry {
    std::uint64_t accepted_requests{0};
    std::uint64_t completed_requests{0};
    std::uint64_t rejected_invalid_requests{0};
    std::uint64_t rejected_no_idle_connection{0};
    std::uint64_t total_completion_latency_ns{0};
    std::uint64_t last_completion_latency_ns{0};
};

class SessionPool {
  public:
    explicit SessionPool(std::vector<AsyncRestConnection> hot_connections = {},
                         std::vector<AsyncRestConnection> recovery_connections = {},
                         std::vector<AsyncRestConnection> reconcile_connections = {},
                         SessionPoolConfig config = {})
        : hot_connections_(std::move(hot_connections)),
          recovery_connections_(std::move(recovery_connections)),
          reconcile_connections_(std::move(reconcile_connections)), config_(config) {}

    // Attempts to assign one admitted dispatch request onto an idle connection
    // in the corresponding scheduling class.
    [[nodiscard]] SessionPoolSubmitResult submit(DispatchRequest& request) noexcept {
        if (request.empty()) {
            ++telemetry_.rejected_invalid_requests;
            return SessionPoolSubmitResult::kInvalidRequest;
        }

        request.session_submit_ts_ns = gateway_now_ns();

        auto* connection = find_idle_connection(request.dispatch_class);
        if (connection == nullptr) {
            ++telemetry_.rejected_no_idle_connection;
            return SessionPoolSubmitResult::kNoIdleConnection;
        }

        switch (connection->try_start(std::move(request))) {
        case ConnectionStartResult::kStarted:
            ++telemetry_.accepted_requests;
            return SessionPoolSubmitResult::kAccepted;
        case ConnectionStartResult::kBusy:
            ++telemetry_.rejected_no_idle_connection;
            return SessionPoolSubmitResult::kNoIdleConnection;
        case ConnectionStartResult::kInvalidRequest:
            ++telemetry_.rejected_invalid_requests;
            return SessionPoolSubmitResult::kInvalidRequest;
        case ConnectionStartResult::kConnectionUnavailable:
            return SessionPoolSubmitResult::kPoolUnavailable;
        }
        return SessionPoolSubmitResult::kPoolUnavailable;
    }

    // Polls all owned connections and buffers any completions that are ready.
    // Returns true if any progress was made.
    [[nodiscard]] bool drain_completions() {
        bool made_progress = false;
        drain_connection_group(hot_connections_, made_progress);
        drain_connection_group(recovery_connections_, made_progress);
        drain_connection_group(reconcile_connections_, made_progress);
        return made_progress;
    }

    // Pops one buffered completion previously harvested from owned connections.
    [[nodiscard]] std::optional<SessionPoolCompletion> poll_completion() {
        if (pending_completions_.empty()) {
            return std::nullopt;
        }
        auto completion = std::move(pending_completions_.front());
        pending_completions_.pop_front();
        return completion;
    }

    // Applies keepalive/warm-up work to currently idle owned connections.
    [[nodiscard]] std::size_t warm_up() noexcept {
        return warm_up_group(hot_connections_) + warm_up_group(recovery_connections_) +
               warm_up_group(reconcile_connections_);
    }

    void keep_warm() noexcept {
        keep_warm_group(hot_connections_);
        keep_warm_group(recovery_connections_);
        keep_warm_group(reconcile_connections_);
    }

    void close() noexcept {
        close_group(hot_connections_);
        close_group(recovery_connections_);
        close_group(reconcile_connections_);
    }

    [[nodiscard]] std::size_t idle_connection_count(DispatchClass dispatch_class) const noexcept {
        const auto* connections = connections_for_class(dispatch_class);
        if (connections == nullptr) {
            return 0;
        }

        std::size_t idle_count = 0;
        for (const auto& connection : *connections) {
            if (connection.idle()) {
                ++idle_count;
            }
        }
        return idle_count;
    }

    [[nodiscard]] std::size_t
    inflight_connection_count(DispatchClass dispatch_class) const noexcept {
        const auto* connections = connections_for_class(dispatch_class);
        if (connections == nullptr) {
            return 0;
        }

        std::size_t inflight_count = 0;
        for (const auto& connection : *connections) {
            if (connection.has_inflight()) {
                ++inflight_count;
            }
        }
        return inflight_count;
    }

    [[nodiscard]] const SessionPoolTelemetry& telemetry() const noexcept { return telemetry_; }

  private:
    std::vector<AsyncRestConnection> hot_connections_;
    std::vector<AsyncRestConnection> recovery_connections_;
    std::vector<AsyncRestConnection> reconcile_connections_;
    SessionPoolConfig config_{};
    SessionPoolTelemetry telemetry_{};
    std::deque<SessionPoolCompletion> pending_completions_;

    [[nodiscard]] AsyncRestConnection* find_idle_connection(DispatchClass dispatch_class) noexcept {
        auto* connections = mutable_connections_for_class(dispatch_class);
        if (connections == nullptr) {
            return nullptr;
        }
        for (auto& connection : *connections) {
            if (connection.idle()) {
                return &connection;
            }
        }
        return nullptr;
    }

    void drain_connection_group(std::vector<AsyncRestConnection>& connections,
                                bool& made_progress) {
        for (std::size_t index = 0; index < connections.size(); ++index) {
            auto result = connections[index].poll();
            if (result.status == ConnectionPollStatus::kCompleted &&
                result.completion.has_value()) {
                const auto& completion = *result.completion;
                const auto completion_latency_ns =
                    completion.completed_ts_ns >= completion.request.queued_ts_ns
                        ? completion.completed_ts_ns - completion.request.queued_ts_ns
                        : 0;
                pending_completions_.push_back(SessionPoolCompletion{
                    .connection_index = index,
                    .completion = std::move(*result.completion),
                });
                ++telemetry_.completed_requests;
                telemetry_.last_completion_latency_ns = completion_latency_ns;
                telemetry_.total_completion_latency_ns += completion_latency_ns;
                made_progress = true;
            } else if (result.status == ConnectionPollStatus::kInFlight) {
                made_progress = true;
            }
        }
    }

    [[nodiscard]] static std::size_t
    warm_up_group(std::vector<AsyncRestConnection>& connections) noexcept {
        std::size_t warmed_count = 0;
        for (auto& connection : connections) {
            if (connection.warm_up()) {
                ++warmed_count;
            }
        }
        return warmed_count;
    }

    void keep_warm_group(std::vector<AsyncRestConnection>& connections) noexcept {
        for (auto& connection : connections) {
            if (connection.idle()) {
                connection.keep_warm();
            }
        }
    }

    void close_group(std::vector<AsyncRestConnection>& connections) noexcept {
        for (auto& connection : connections) {
            connection.close();
        }
    }

    [[nodiscard]] std::vector<AsyncRestConnection>*
    mutable_connections_for_class(DispatchClass dispatch_class) noexcept {
        switch (dispatch_class) {
        case DispatchClass::kHot:
            return &hot_connections_;
        case DispatchClass::kRecovery:
            return &recovery_connections_;
        case DispatchClass::kReconcile:
            return &reconcile_connections_;
        }
        return nullptr;
    }

    [[nodiscard]] const std::vector<AsyncRestConnection>*
    connections_for_class(DispatchClass dispatch_class) const noexcept {
        switch (dispatch_class) {
        case DispatchClass::kHot:
            return &hot_connections_;
        case DispatchClass::kRecovery:
            return &recovery_connections_;
        case DispatchClass::kReconcile:
            return &reconcile_connections_;
        }
        return nullptr;
    }
};

} // namespace predex::core::oms::kalshi::gateway
