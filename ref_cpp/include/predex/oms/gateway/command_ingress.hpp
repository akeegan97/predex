#pragma once
#include <cstdint>
#include <type_traits>


#include "predex/oms/gateway/gateway_types.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::oms::kalshi::gateway {

// Internal queue fanout into the future gateway/session execution layer.
// This remains gateway-internal plumbing, not an OMS-facing abstraction.
struct SessionClassQueues {
    utils::SPSCQueue<CommandEnvelope>* hot_queue{nullptr};
    utils::SPSCQueue<CommandEnvelope>* recovery_queue{nullptr};
    utils::SPSCQueue<CommandEnvelope>* reconcile_queue{nullptr};
};

struct CommandIngressQueues {
    utils::SPSCQueue<OmsToKalshiCommand>* oms_command_queue{nullptr};
    SessionClassQueues session_class_queues{};
};

struct CommandIngressTelemetry {
    std::uint64_t accepted_commands{0};
    std::uint64_t emitted_hot_requests{0};
    std::uint64_t emitted_recovery_requests{0};
    std::uint64_t emitted_reconcile_requests{0};
    std::uint64_t rejected_no_queue{0};
    std::uint64_t backpressured_emits{0};
};

struct CommandIngressConfig {
    DispatchClass default_dispatch_class{DispatchClass::kHot};
};

class CommandIngress {
  public:
    explicit CommandIngress(CommandIngressQueues queues = {}, CommandIngressConfig config = {})
        : queues_(queues), config_(config) {}

    [[nodiscard]] bool drain_one() {
        if (queues_.oms_command_queue == nullptr) {
            ++telemetry_.rejected_no_queue;
            return false;
        }

        OmsToKalshiCommand command;
        if (!queues_.oms_command_queue->try_pop(command)) {
            return false;
        }

        ++telemetry_.accepted_commands;
        CommandEnvelope envelope{
            .dispatch_item_id = next_dispatch_item_id_++,
            .lineage_id = next_lineage_id_++,
            .lineage_key = lineage_key_for_command(command),
            .group_key = group_key_for_command(command),
            .batch_group = batch_group_metadata_for_command(command),
            .operation = operation_for_command(command),
            .dispatch_class = config_.default_dispatch_class,
            .ingress_ts_ns = ingress_timestamp_for(command),
            .command = command,
        };

        return emit_envelope(envelope);
    }

    [[nodiscard]] const CommandIngressTelemetry& telemetry() const noexcept { return telemetry_; }

  private:
    CommandIngressQueues queues_{};
    CommandIngressConfig config_{};
    CommandIngressTelemetry telemetry_{};
    DispatchItemId next_dispatch_item_id_{1};
    LineageId next_lineage_id_{1};

    [[nodiscard]] static internal::TimestampNs
    ingress_timestamp_for(const OmsToKalshiCommand& command) {
        return std::visit(
            [](const auto& typed_command) noexcept -> internal::TimestampNs {
                using T = std::decay_t<decltype(typed_command)>;
                return typed_command.transport_enqueue_ts_ns != 0
                           ? typed_command.transport_enqueue_ts_ns
                           : ([](const auto& fallback_command) noexcept -> internal::TimestampNs {
                                 using U = std::decay_t<decltype(fallback_command)>;
                                 if constexpr (std::is_same_v<U, SubmitOrderCmd>) {
                                     return fallback_command.intent.intent_ts_ns;
                                 } else {
                                     return fallback_command.cmd_ts_ns;
                                 }
                             })(typed_command);
            },
            command);
    }

    [[nodiscard]] bool emit_envelope(CommandEnvelope envelope) {
        const DispatchClass dispatch_class = envelope.dispatch_class;
        auto* target_queue = queue_for_class(dispatch_class);
        if (target_queue == nullptr) {
            ++telemetry_.rejected_no_queue;
            return false;
        }
        if (!target_queue->try_push(envelope)) {
            ++telemetry_.backpressured_emits;
            return false;
        }

        switch (dispatch_class) {
        case DispatchClass::kHot:
            ++telemetry_.emitted_hot_requests;
            break;
        case DispatchClass::kRecovery:
            ++telemetry_.emitted_recovery_requests;
            break;
        case DispatchClass::kReconcile:
            ++telemetry_.emitted_reconcile_requests;
            break;
        }
        return true;
    }

    [[nodiscard]] utils::SPSCQueue<CommandEnvelope>*
    queue_for_class(DispatchClass dispatch_class) noexcept { //NOLINT -- could be const but keeping non-const since it's closely tied to the queues_ member
        switch (dispatch_class) {
        case DispatchClass::kHot:
            return queues_.session_class_queues.hot_queue;
        case DispatchClass::kRecovery:
            return queues_.session_class_queues.recovery_queue;
        case DispatchClass::kReconcile:
            return queues_.session_class_queues.reconcile_queue;
        }
        return nullptr;
    }
};

} // namespace predex::core::oms::kalshi::gateway
