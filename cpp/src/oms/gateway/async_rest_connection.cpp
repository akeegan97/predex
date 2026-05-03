#include "predex/oms/gateway/async_rest_connection.hpp"

#include <ostream>
#include <string_view>
#include <type_traits>
#include <utility>

namespace predex::core::oms::kalshi::gateway {
namespace {

[[nodiscard]] internal::TimestampNs completion_timestamp_for(
    const transport::HttpResponse& response,
    internal::TimestampNs fallback_ts) noexcept {
    if (response.response_recv_ts_ns != 0) {
        return response.response_recv_ts_ns;
    }
    if (response.request_sent_ts_ns != 0) {
        return response.request_sent_ts_ns;
    }
    return fallback_ts;
}

[[nodiscard]] const char* request_state_to_string(DispatchRequestState state) noexcept {
    switch (state) {
        case DispatchRequestState::kQueued:
            return "queued";
        case DispatchRequestState::kAdmitted:
            return "admitted";
        case DispatchRequestState::kDispatchedPreWrite:
            return "dispatched_pre_write";
        case DispatchRequestState::kPostWriteUnknown:
            return "post_write_unknown";
        case DispatchRequestState::kCompleted:
            return "completed";
        case DispatchRequestState::kAbandonedToRecovery:
            return "abandoned_to_recovery";
    }
    return "unknown";
}

void append_json_escaped(std::ostream& out, std::string_view value) {
    out.put('"');
    for (const char ch : value) {
        switch (ch) {
            case '\\':
                out << "\\\\";
                break;
            case '"':
                out << "\\\"";
                break;
            case '\n':
                out << "\\n";
                break;
            case '\r':
                out << "\\r";
                break;
            case '\t':
                out << "\\t";
                break;
            default:
                out.put(ch);
                break;
        }
    }
    out.put('"');
}

} // namespace

AsyncRestConnection::AsyncRestConnection(transport::KalshiRestAdapter adapter,
                                         AsyncRestConnectionConfig config)
    : adapter_(std::move(adapter)), config_(std::move(config)) {
    if (!config_.trace_output_path.empty()) {
        trace_output_file_.open(config_.trace_output_path, std::ios::out | std::ios::trunc);
    }
}

bool AsyncRestConnection::idle() const noexcept {
    return !inflight_request_.has_value() && !pending_completion_.has_value();
}

bool AsyncRestConnection::has_inflight() const noexcept {
    return inflight_request_.has_value();
}

std::optional<DispatchRequestId> AsyncRestConnection::inflight_request_id() const noexcept {
    if (!inflight_request_.has_value()) {
        return std::nullopt;
    }
    return inflight_request_->dispatch_request_id;
}

const AsyncRestConnectionTelemetry& AsyncRestConnection::telemetry() const noexcept {
    return telemetry_;
}

ConnectionStartResult AsyncRestConnection::try_start(DispatchRequest request) noexcept {
    if (!idle()) {
        ++telemetry_.failed_starts;
        return ConnectionStartResult::kBusy;
    }
    if (!request_is_executable(request)) {
        ++telemetry_.failed_starts;
        return ConnectionStartResult::kInvalidRequest;
    }

    inflight_request_ = std::move(request);
    inflight_request_->connection_start_ts_ns = gateway_now_ns();
    inflight_request_->state = DispatchRequestState::kDispatchedPreWrite;
    inflight_item_index_ = 0;
    emitted_events_.clear();
    last_trace_.reset();
    inflight_prepared_request_.reset();
    pending_completion_.reset();
    ++telemetry_.started_requests;

    if (!start_current_item_()) {
        if (!pending_completion_.has_value()) {
            reset_inflight_state_();
            ++telemetry_.failed_starts;
            return ConnectionStartResult::kConnectionUnavailable;
        }
    }
    return ConnectionStartResult::kStarted;
}

ConnectionPollResult AsyncRestConnection::poll() noexcept {
    if (pending_completion_.has_value()) {
        DispatchCompletion completion = std::move(*pending_completion_);
        reset_inflight_state_();
        return {
            .status = ConnectionPollStatus::kCompleted,
            .completion = std::move(completion),
        };
    }

    if (!inflight_request_.has_value()) {
        return {.status = ConnectionPollStatus::kIdle};
    }

    auto poll_result = adapter_.poll_active_request();
    if (poll_result.status == transport::AsyncHttpRequestStatus::kIdle) {
        finalize_pending_completion_(DispatchRequestState::kPostWriteUnknown,
                                     "transport_disconnected");
    } else if (poll_result.status == transport::AsyncHttpRequestStatus::kCompleted &&
               poll_result.response.has_value()) {
        auto command_result =
            inflight_request_->batch_kind == DispatchBatchKind::kGroupedSubmit
                ? complete_batched_submit_request_(*poll_result.response)
                : complete_item_(inflight_request_->items[inflight_item_index_],
                                 *poll_result.response);
        if (command_result.trace.request_sent_ts_ns != 0 ||
            command_result.trace.response_recv_ts_ns != 0 ||
            !command_result.trace.request_target.empty() ||
            !command_result.trace.error_message.empty()) {
            last_trace_ = command_result.trace;
        }
        if (!command_result.events.empty()) {
            for (auto& event : command_result.events) {
                emitted_events_.push_back(std::move(event));
            }
        } else if (command_result.event.has_value()) {
            emitted_events_.push_back(std::move(*command_result.event));
        }

        if (inflight_request_->batch_kind == DispatchBatchKind::kGroupedSubmit) {
            finalize_pending_completion_(
                command_result.ok || !command_result.events.empty()
                    ? DispatchRequestState::kCompleted
                    : DispatchRequestState::kPostWriteUnknown,
                command_result.error_message);
        } else if (!command_result.ok) {
            if (command_result.event.has_value()) {
                ++inflight_item_index_;
                inflight_prepared_request_.reset();
                if (inflight_item_index_ >= inflight_request_->items.size()) {
                    finalize_pending_completion_(DispatchRequestState::kCompleted,
                                                 command_result.error_message);
                } else if (!start_current_item_() && !pending_completion_.has_value()) {
                    finalize_pending_completion_(DispatchRequestState::kPostWriteUnknown,
                                                 "transport_unavailable");
                }
            } else {
                finalize_pending_completion_(DispatchRequestState::kPostWriteUnknown,
                                             command_result.error_message);
            }
        } else {
            ++inflight_item_index_;
            inflight_prepared_request_.reset();
            if (inflight_item_index_ >= inflight_request_->items.size()) {
                finalize_pending_completion_(DispatchRequestState::kCompleted);
            } else if (!start_current_item_() && !pending_completion_.has_value()) {
                finalize_pending_completion_(DispatchRequestState::kPostWriteUnknown,
                                             "transport_unavailable");
            }
        }
    }

    if (pending_completion_.has_value()) {
        DispatchCompletion completion = std::move(*pending_completion_);
        reset_inflight_state_();
        return {
            .status = ConnectionPollStatus::kCompleted,
            .completion = std::move(completion),
        };
    }

    return {.status = ConnectionPollStatus::kInFlight};
}

bool AsyncRestConnection::warm_up() noexcept {
    if (!idle()) {
        return false;
    }
    return adapter_.warm_up();
}

void AsyncRestConnection::keep_warm() noexcept {
    if (!idle()) {
        return;
    }
    adapter_.check_and_keep_warm(config_.keep_warm_threshold_seconds);
}

void AsyncRestConnection::close() noexcept {
    adapter_.close();
    if (inflight_request_.has_value() && !pending_completion_.has_value()) {
        finalize_pending_completion_(DispatchRequestState::kPostWriteUnknown, "transport_closed");
    }
    reset_inflight_state_();
}

bool AsyncRestConnection::request_is_executable(const DispatchRequest& request) noexcept {
    return !request.empty();
}

void AsyncRestConnection::reset_inflight_state_() noexcept {
    inflight_request_.reset();
    pending_completion_.reset();
    inflight_item_index_ = 0;
    inflight_prepared_request_.reset();
    last_trace_.reset();
    emitted_events_.clear();
}

void AsyncRestConnection::finalize_pending_completion_(DispatchRequestState terminal_state,
                                                       std::string error_message) noexcept {
    if (!inflight_request_.has_value()) {
        return;
    }
    const internal::TimestampNs completed_ts_ns = last_trace_.has_value()
        ? completion_timestamp_for(
              transport::HttpResponse{
                  .request_sent_ts_ns = last_trace_->request_sent_ts_ns,
                  .response_recv_ts_ns = last_trace_->response_recv_ts_ns,
              },
              inflight_request_->queued_ts_ns)
        : inflight_request_->queued_ts_ns;
    pending_completion_ = DispatchCompletion{
        .request = *inflight_request_,
        .emitted_events = emitted_events_,
        .trace = last_trace_,
        .terminal_state = terminal_state,
        .completed_ts_ns = completed_ts_ns,
        .error_message = std::move(error_message),
    };
    append_trace_row_(*pending_completion_);
    ++telemetry_.completed_requests;
    if (terminal_state == DispatchRequestState::kPostWriteUnknown) {
        ++telemetry_.uncertain_requests;
    }
    if (completed_ts_ns >= inflight_request_->queued_ts_ns) {
        const auto latency_ns = completed_ts_ns - inflight_request_->queued_ts_ns;
        telemetry_.last_completion_latency_ns = latency_ns;
        telemetry_.total_completion_latency_ns += latency_ns;
    } else {
        telemetry_.last_completion_latency_ns = 0;
    }
}

bool AsyncRestConnection::start_current_item_() noexcept {
    if (!inflight_request_.has_value()) {
        return false;
    }

    transport::PreparedCommandRequest prepared =
        inflight_request_->batch_kind == DispatchBatchKind::kGroupedSubmit
            ? prepare_batched_submit_request_()
            : prepare_item_(inflight_request_->items[inflight_item_index_]);
    if (!prepared.ok) {
        last_trace_ = prepared.trace;
        finalize_pending_completion_(DispatchRequestState::kCompleted, prepared.error_message);
        return false;
    }

    if (!adapter_.start_prepared_request(prepared)) {
        last_trace_ = prepared.trace;
        finalize_pending_completion_(DispatchRequestState::kPostWriteUnknown,
                                     "transport_unavailable");
        return false;
    }

    last_trace_ = prepared.trace;
    inflight_prepared_request_ = std::move(prepared);
    return true;
}

transport::PreparedCommandRequest AsyncRestConnection::prepare_item_(
    const DispatchItem& item) const noexcept {
    return std::visit(
        [this](const auto& typed_command) -> transport::PreparedCommandRequest {
            using T = std::decay_t<decltype(typed_command)>;
            if constexpr (std::is_same_v<T, SubmitOrderCmd>) {
                return adapter_.prepare_submit_order(typed_command);
            } else if constexpr (std::is_same_v<T, CancelOrderCmd>) {
                return adapter_.prepare_cancel_order(typed_command);
            } else {
                return adapter_.prepare_modify_order(typed_command);
            }
        },
        item.command);
}

transport::PreparedCommandRequest AsyncRestConnection::prepare_batched_submit_request_() const noexcept {
    if (!inflight_request_.has_value()) {
        return {.ok = false, .error_message = "missing inflight request"};
    }
    std::vector<SubmitOrderCmd> commands;
    commands.reserve(inflight_request_->items.size());
    for (const auto& item : inflight_request_->items) {
        const auto* submit = std::get_if<SubmitOrderCmd>(&item.command);
        if (submit == nullptr) {
            return {.ok = false, .error_message = "grouped submit contained non-submit item"};
        }
        commands.push_back(*submit);
    }
    return adapter_.prepare_batched_submit_orders(commands);
}

transport::CommandResult AsyncRestConnection::complete_item_(
    const DispatchItem& item,
    const transport::HttpResponse& response) noexcept {
    return std::visit(
        [&response, this](const auto& typed_command) -> transport::CommandResult {
            using T = std::decay_t<decltype(typed_command)>;
            transport::RestTraceInfo trace =
                inflight_prepared_request_.has_value() ? inflight_prepared_request_->trace
                                                       : transport::RestTraceInfo{};
            trace.http_status_code = response.status_code;
            trace.retry_count = response.retry_count;
            trace.reused_connection = response.reused_connection;
            trace.resolve_start_ts_ns = response.resolve_start_ts_ns;
            trace.resolve_end_ts_ns = response.resolve_end_ts_ns;
            trace.connect_start_ts_ns = response.connect_start_ts_ns;
            trace.connect_end_ts_ns = response.connect_end_ts_ns;
            trace.handshake_start_ts_ns = response.handshake_start_ts_ns;
            trace.handshake_end_ts_ns = response.handshake_end_ts_ns;
            trace.write_start_ts_ns = response.write_start_ts_ns;
            trace.request_sent_ts_ns = response.request_sent_ts_ns;
            trace.response_recv_ts_ns = response.response_recv_ts_ns;
            trace.response_body = response.body;
            trace.error_message = response.error_message;

            if constexpr (std::is_same_v<T, SubmitOrderCmd>) {
                return adapter_.complete_submit_order(typed_command, response, std::move(trace));
            } else if constexpr (std::is_same_v<T, CancelOrderCmd>) {
                return adapter_.complete_cancel_order(typed_command, response, std::move(trace));
            } else {
                return adapter_.complete_modify_order(typed_command, response, std::move(trace));
            }
        },
        item.command);
}

transport::CommandResult AsyncRestConnection::complete_batched_submit_request_(
    const transport::HttpResponse& response) noexcept {
    std::vector<SubmitOrderCmd> commands;
    if (!inflight_request_.has_value()) {
        return {.ok = false, .error_message = "missing inflight request"};
    }
    commands.reserve(inflight_request_->items.size());
    for (const auto& item : inflight_request_->items) {
        const auto* submit = std::get_if<SubmitOrderCmd>(&item.command);
        if (submit == nullptr) {
            return {.ok = false, .error_message = "grouped submit contained non-submit item"};
        }
        commands.push_back(*submit);
    }

    transport::RestTraceInfo trace =
        inflight_prepared_request_.has_value() ? inflight_prepared_request_->trace
                                               : transport::RestTraceInfo{};
    trace.http_status_code = response.status_code;
    trace.retry_count = response.retry_count;
    trace.reused_connection = response.reused_connection;
    trace.resolve_start_ts_ns = response.resolve_start_ts_ns;
    trace.resolve_end_ts_ns = response.resolve_end_ts_ns;
    trace.connect_start_ts_ns = response.connect_start_ts_ns;
    trace.connect_end_ts_ns = response.connect_end_ts_ns;
    trace.handshake_start_ts_ns = response.handshake_start_ts_ns;
    trace.handshake_end_ts_ns = response.handshake_end_ts_ns;
    trace.write_start_ts_ns = response.write_start_ts_ns;
    trace.request_sent_ts_ns = response.request_sent_ts_ns;
    trace.response_recv_ts_ns = response.response_recv_ts_ns;
    trace.response_body = response.body;
    trace.error_message = response.error_message;
    return adapter_.complete_batched_submit_orders(commands, response, std::move(trace));
}

void AsyncRestConnection::append_trace_row_(const DispatchCompletion& completion) noexcept {
    if (!trace_output_file_.is_open()) {
        return;
    }

    trace_output_file_
        << '{'
        << "\"connection_index\":" << config_.connection_index << ','
        << "\"dispatch_request_id\":" << completion.request.dispatch_request_id << ','
        << "\"terminal_state\":";
    append_json_escaped(trace_output_file_, request_state_to_string(completion.terminal_state));
    trace_output_file_
        << ",\"item_count\":" << completion.request.item_count()
        << ",\"group_intent_id\":"
        << (completion.request.group_key.has_value()
                ? completion.request.group_key->group_intent_id
                : static_cast<GroupIntentId>(0))
        << ",\"group_leg_count\":"
        << (completion.request.group_key.has_value()
                ? completion.request.group_key->expected_leg_count
                : static_cast<std::uint16_t>(0))
        << ",\"queued_ts_ns\":" << completion.request.queued_ts_ns
        << ",\"first_item_sequenced_ts_ns\":"
        << (!completion.request.items.empty() ? completion.request.items.front().sequenced_ts_ns : 0)
        << ",\"planned_ts_ns\":" << completion.request.planned_ts_ns
        << ",\"admitted_ts_ns\":" << completion.request.admitted_ts_ns
        << ",\"session_submit_ts_ns\":" << completion.request.session_submit_ts_ns
        << ",\"connection_start_ts_ns\":" << completion.request.connection_start_ts_ns
        << ",\"completed_ts_ns\":" << completion.completed_ts_ns
        << ",\"latency_ns\":"
        << (completion.completed_ts_ns >= completion.request.queued_ts_ns
                ? completion.completed_ts_ns - completion.request.queued_ts_ns
                : 0)
        << ",\"queued_to_planned_ns\":"
        << (completion.request.planned_ts_ns >= completion.request.queued_ts_ns
                ? completion.request.planned_ts_ns - completion.request.queued_ts_ns
                : 0)
        << ",\"ingress_to_sequence_ns\":"
        << ((!completion.request.items.empty() &&
             completion.request.items.front().sequenced_ts_ns >= completion.request.queued_ts_ns)
                ? completion.request.items.front().sequenced_ts_ns -
                      completion.request.queued_ts_ns
                : 0)
        << ",\"sequence_to_plan_ns\":"
        << ((!completion.request.items.empty() &&
             completion.request.planned_ts_ns >= completion.request.items.front().sequenced_ts_ns)
                ? completion.request.planned_ts_ns -
                      completion.request.items.front().sequenced_ts_ns
                : 0)
        << ",\"planned_to_admitted_ns\":"
        << (completion.request.admitted_ts_ns >= completion.request.planned_ts_ns
                ? completion.request.admitted_ts_ns - completion.request.planned_ts_ns
                : 0)
        << ",\"admitted_to_session_submit_ns\":"
        << (completion.request.session_submit_ts_ns >= completion.request.admitted_ts_ns
                ? completion.request.session_submit_ts_ns - completion.request.admitted_ts_ns
                : 0)
        << ",\"session_submit_to_connection_start_ns\":"
        << (completion.request.connection_start_ts_ns >= completion.request.session_submit_ts_ns
                ? completion.request.connection_start_ts_ns -
                      completion.request.session_submit_ts_ns
                : 0)
        << ",\"emitted_event_count\":" << completion.emitted_events.size()
        << ",\"http_status_code\":"
        << (completion.trace.has_value() ? completion.trace->http_status_code : 0)
        << ",\"retry_count\":"
        << (completion.trace.has_value() ? completion.trace->retry_count : 0)
        << ",\"reused_connection\":"
        << ((completion.trace.has_value() && completion.trace->reused_connection) ? "true" : "false")
        << ",\"resolve_start_ts_ns\":"
        << (completion.trace.has_value() ? completion.trace->resolve_start_ts_ns : 0)
        << ",\"resolve_end_ts_ns\":"
        << (completion.trace.has_value() ? completion.trace->resolve_end_ts_ns : 0)
        << ",\"connect_start_ts_ns\":"
        << (completion.trace.has_value() ? completion.trace->connect_start_ts_ns : 0)
        << ",\"connect_end_ts_ns\":"
        << (completion.trace.has_value() ? completion.trace->connect_end_ts_ns : 0)
        << ",\"handshake_start_ts_ns\":"
        << (completion.trace.has_value() ? completion.trace->handshake_start_ts_ns : 0)
        << ",\"handshake_end_ts_ns\":"
        << (completion.trace.has_value() ? completion.trace->handshake_end_ts_ns : 0)
        << ",\"write_start_ts_ns\":"
        << (completion.trace.has_value() ? completion.trace->write_start_ts_ns : 0)
        << ",\"request_sent_ts_ns\":"
        << (completion.trace.has_value() ? completion.trace->request_sent_ts_ns : 0)
        << ",\"resolve_ns\":"
        << ((completion.trace.has_value() &&
             completion.trace->resolve_end_ts_ns >= completion.trace->resolve_start_ts_ns)
                ? completion.trace->resolve_end_ts_ns - completion.trace->resolve_start_ts_ns
                : 0)
        << ",\"connect_ns\":"
        << ((completion.trace.has_value() &&
             completion.trace->connect_end_ts_ns >= completion.trace->connect_start_ts_ns)
                ? completion.trace->connect_end_ts_ns - completion.trace->connect_start_ts_ns
                : 0)
        << ",\"handshake_ns\":"
        << ((completion.trace.has_value() &&
             completion.trace->handshake_end_ts_ns >= completion.trace->handshake_start_ts_ns)
                ? completion.trace->handshake_end_ts_ns -
                      completion.trace->handshake_start_ts_ns
                : 0)
        << ",\"write_queue_ns\":"
        << ((completion.trace.has_value() &&
             completion.trace->write_start_ts_ns >= completion.request.connection_start_ts_ns)
                ? completion.trace->write_start_ts_ns -
                      completion.request.connection_start_ts_ns
                : 0)
        << ",\"connection_start_to_request_sent_ns\":"
        << ((completion.trace.has_value() &&
             completion.trace->request_sent_ts_ns >= completion.request.connection_start_ts_ns)
                ? completion.trace->request_sent_ts_ns -
                      completion.request.connection_start_ts_ns
                : 0)
        << ",\"write_start_to_request_sent_ns\":"
        << ((completion.trace.has_value() &&
             completion.trace->request_sent_ts_ns >= completion.trace->write_start_ts_ns)
                ? completion.trace->request_sent_ts_ns -
                      completion.trace->write_start_ts_ns
                : 0)
        << ",\"response_recv_ts_ns\":"
        << (completion.trace.has_value() ? completion.trace->response_recv_ts_ns : 0)
        << ",\"request_target\":";
    append_json_escaped(
        trace_output_file_,
        completion.trace.has_value() ? std::string_view{completion.trace->request_target}
                                     : std::string_view{});
    trace_output_file_ << ",\"trace_error_message\":";
    append_json_escaped(
        trace_output_file_,
        completion.trace.has_value() ? std::string_view{completion.trace->error_message}
                                     : std::string_view{});
    trace_output_file_ << ",\"completion_error_message\":";
    append_json_escaped(trace_output_file_, completion.error_message);
    trace_output_file_ << ",\"request_body\":";
    append_json_escaped(
        trace_output_file_,
        completion.trace.has_value() ? std::string_view{completion.trace->request_body}
                                     : std::string_view{});
    trace_output_file_ << ",\"response_body\":";
    append_json_escaped(
        trace_output_file_,
        completion.trace.has_value() ? std::string_view{completion.trace->response_body}
                                     : std::string_view{});
    trace_output_file_ << "}\n";
    trace_output_file_.flush();
}

} // namespace predex::core::oms::kalshi::gateway
