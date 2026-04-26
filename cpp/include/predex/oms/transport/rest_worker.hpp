#pragma once

#include <atomic>
#include <deque>
#include <fstream>
#include <functional>
#include <stop_token>
#include <string>
#include <string_view>

#include "predex/oms/oms_types.hpp"
#include "predex/oms/transport/kalshi_rest_adapter.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::oms::kalshi::transport {

struct RestWorkerQueues {
    utils::SPSCQueue<OmsToKalshiCommand>* command_queue{nullptr};
    utils::SPSCQueue<KalshiToOmsEvent>* event_queue{nullptr};
};

struct RestWorkerConfig {
    struct ReconcileOrderSeed {
        IntentContext context{};
        internal::ExchangeId exchange{internal::ExchangeId::kUnknown};
        internal::Side side{internal::Side::kUnknown};
        Outcome outcome{Outcome::kYes};
    };

    std::function<std::optional<ReconcileOrderSeed>(std::string_view ticker)>
        ticker_seed_resolver;
  std::string trace_output_path{"predex_rest_trace.jsonl"};
};

// Owns the blocking REST thread loop. Consumes OMS commands, calls the Kalshi
// REST adapter, and emits normalized venue events back toward OMS.
class RestWorker {
  public:
    explicit RestWorker(RestWorkerQueues queues,
                        KalshiRestAdapter adapter,
                        RestWorkerConfig config = {});

    void run(const std::stop_token& stop_token);
    void request_reconcile() noexcept;

    // REST-side reconciliation hook. OMS/transport control can trigger this
    // when reconnect or sequence repair requires a fresh open-order snapshot.
    // Intended to be invoked only by the control path that owns this worker's
    // lifecycle / thread coordination; not safe as a general concurrent API.
    [[nodiscard]] bool reconcile_open_orders();

  private:
    RestWorkerQueues queues_{};
    KalshiRestAdapter adapter_;
    RestWorkerConfig config_{};
    std::ofstream trace_output_;
    std::deque<KalshiToOmsEvent> pending_events_;
    std::atomic<bool> reconcile_requested_{false};

    [[nodiscard]] bool flush_pending_events();
    [[nodiscard]] bool enqueue_event(KalshiToOmsEvent event);
    [[nodiscard]] bool process_one_command();
    [[nodiscard]] bool handle_command(const OmsToKalshiCommand& command);
    [[nodiscard]] bool emit_event(const KalshiToOmsEvent& event);
    void write_trace_record(const OmsToKalshiCommand& command,
                const CommandResult& result) noexcept;
};

} // namespace predex::core::oms::kalshi::transport
