#include "predex/audit/audit_logger.hpp"

#include <stdexcept>
#include <string>

namespace predex::core::audit {
namespace {

const char* kind_to_string(AuditKind kind) {
    switch (kind) {
        case AuditKind::kSignal:
            return "signal";
        case AuditKind::kGroupSignal:
            return "group_signal";
        case AuditKind::kLocalRisk:
            return "local_risk";
        case AuditKind::kSubmission:
            return "submission";
        case AuditKind::kOmsDecision:
            return "oms_decision";
        case AuditKind::kOmsTransport:
            return "oms_transport";
        case AuditKind::kOmsLifecycle:
            return "oms_lifecycle";
        case AuditKind::kShardReconcile:
            return "shard_reconcile";
        default:
            return "unknown";
    }
}

void write_event_json(std::ofstream& output, const AuditEvent& event) {
    output << '{'
           << "\"kind\":\"" << kind_to_string(event.kind) << "\","
           << "\"ts_ns\":" << event.ts_ns << ','
           << "\"shard_id\":" << event.shard_id << ','
           << "\"signal_id\":" << event.signal_id << ','
           << "\"group_id\":" << event.group_id << ','
           << "\"local_intent_id\":" << event.local_intent_id << ','
           << "\"oms_request_id\":" << event.oms_request_id << ','
           << "\"exchange\":" << static_cast<unsigned>(event.exchange) << ','
           << "\"event_id\":" << event.event_id << ','
           << "\"market_id\":" << event.market_id << ','
           << "\"side\":" << static_cast<unsigned>(event.side) << ','
           << "\"leg_index\":" << event.leg_index << ','
           << "\"leg_count\":" << event.leg_count << ','
           << "\"qty_lots\":" << event.qty_lots << ','
           << "\"aux_qty_lots\":" << event.aux_qty_lots << ','
           << "\"price_ticks\":" << event.price_ticks << ','
           << "\"aux_price_ticks\":" << event.aux_price_ticks << ','
           << "\"edge_ticks\":" << event.edge_ticks << ','
           << "\"score\":" << event.score << ','
           << "\"decision_code\":" << static_cast<unsigned>(event.decision_code) << ','
           << "\"reject_reason\":" << static_cast<unsigned>(event.reject_reason) << ','
           << "\"lifecycle_kind\":" << static_cast<unsigned>(event.lifecycle_kind) << ','
           << "\"order_status\":" << static_cast<unsigned>(event.order_status) << ','
           << "\"event_exposure_lots\":" << event.event_exposure_lots << ','
           << "\"market_exposure_lots\":" << event.market_exposure_lots
           << "}\n";
}

} // namespace

AuditLogger::AuditLogger(
    std::vector<predex::utils::SPSCQueue<AuditEvent>*> input_queues,
    std::string_view output_file_path)
    : input_queues_(std::move(input_queues)),
      output_file_(output_file_path.data(), std::ios::out | std::ios::trunc) {
    if (!output_file_.is_open()) {
        throw std::runtime_error("Failed to open audit log file: " +
                                 std::string(output_file_path));
    }
}

std::size_t AuditLogger::pump(std::size_t max_batch_size) noexcept {
    std::size_t logged = 0;
    for (std::size_t i = 0; i < max_batch_size && !input_queues_.empty(); ++i) {
        auto* queue = input_queues_[next_input_queue_];
        if (queue == nullptr) {
            next_input_queue_ = (next_input_queue_ + 1) % input_queues_.size();
            continue;
        }

        AuditEvent event{};
        if (queue->try_pop(event)) {
            write_event_json(output_file_, event);
            ++logged;
        }

        next_input_queue_ = (next_input_queue_ + 1) % input_queues_.size();
    }
    output_file_.flush();
    return logged;
}

} // namespace predex::core::audit
