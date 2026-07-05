#pragma once
#include <cstdint>
#include <optional>
#include <vector>

#include <unordered_map>
#include "predex/oms/order_intents.hpp"
#include "predex/oms/oms_types.hpp"

#include "predex/control/control_types.hpp"
#include "predex/utils/spsc.hpp"
namespace predex::oms{

    enum class OmsPumpResult : std::uint8_t{
        kOK = 0,
        kNoWork = 1,
        kError = 2,
    };
    struct OrderRecord{
        OmsContext context{};
        ClientOrderId client_order_id{};
        std::optional<ExchangeOrderId> exchange_order_id;

        OrderState order_state{OrderState::kUNKNOWN};
        OrderState previous_order_state{OrderState::kUNKNOWN};
        intent::Outcome outcome{intent::Outcome::kUNKNOWN};
        intent::MarketId market_id{};

        std::int64_t ordered_qty_lots{0};
        std::int64_t working_price_ticks{0};
        std::int64_t cumulative_filled_qty_lots{0};
        std::int64_t leaves_qty_lots{0};

        intent::OmsRequestId pending_command_oms_request_id{};
        RestCommandKind pending_command_kind{RestCommandKind::kUNKNOWN};

    };
    struct OmsQueues{
        std::vector<utils::SPSCQueue<intent::StrategyIntent>*> strategy_intent_queues;
        std::vector<utils::SPSCQueue<OmsToStrategyMessage>*> strategy_response_queues;
        utils::SPSCQueue<core::control::ControlToOmsCommand>& control_command_queue;
        utils::SPSCQueue<core::control::OmsToControlStatus>& oms_status_queue;
        utils::SPSCQueue<OmsToKalshiCommand>& kalshi_command_queue;
        utils::SPSCQueue<KalshiToOmsEvent>& venue_event_queue;

    };


    class Oms{
        public:
            Oms(OmsQueues queues) : queues_(std::move(queues)) {};

            [[nodiscard]] OmsPumpResult pump_once() noexcept;
            [[nodiscard]] std::size_t drain_strategy_intents(std::size_t max) noexcept;
            [[nodiscard]] std::size_t drain_venue_events(std::size_t max) noexcept;
            [[nodiscard]] std::size_t drain_control_commands(std::size_t max) noexcept;

        private:
            void handle_strategy_intent(const intent::NewOrderIntent& intent) noexcept;
            void handle_strategy_intent(const intent::CancelOrderIntent& intent) noexcept;
            void handle_strategy_intent(const intent::ModifyOrderIntent& intent) noexcept;
            void handle_strategy_intent(const intent::GroupOrderIntent& intent) noexcept;

            void handle_venue_event(const RestOrderResponse& response) noexcept;
            void handle_venue_event(const PrivateWsOrderEvent& event) noexcept;
            void handle_venue_event(const ReconciledOrderSnapshot& snapshot) noexcept;
            
            void handle_control_command(const core::control::AllowTrading& command) noexcept;
            void handle_control_command(const core::control::DisableTrading& command) noexcept;
            void handle_control_command(const core::control::FlattenAllOrders& command) noexcept;

            [[nodiscard]] static bool is_terminal(OrderState state) noexcept;
            [[nodiscard]] static bool is_live(OrderState state) noexcept;
            [[nodiscard]] static bool is_cancelable(OrderState state) noexcept;

            [[nodiscard]] ClientOrderId make_client_order_id(intent::OmsRequestId oms_request_id) noexcept;
            [[nodiscard]] OmsContext make_context(intent::OmsRequestId oms_request_id, intent::IntentContext context) noexcept;
            [[nodiscard]] OrderStateUpdate make_order_state_update(const OrderRecord& record, VenueEventSource source, std::uint64_t update_ts_ns) const noexcept;
            [[nodiscard]] OrderRecord* find_order(intent::OmsRequestId oms_request_id) noexcept;
            [[nodiscard]] OrderRecord* find_order(const ClientOrderId& client_order_id) noexcept;
            [[nodiscard]] OrderRecord* find_order(const ExchangeOrderId& exchange_order_id) noexcept;
            [[nodiscard]] OrderRecord* find_order_for_rest_response(const RestOrderResponse& response) noexcept;
            [[nodiscard]] bool send_strategy_message(std::uint16_t strategy_index, OmsToStrategyMessage message) noexcept;
            [[nodiscard]] bool send_kalshi_command(OmsToKalshiCommand command) noexcept;
            [[nodiscard]] bool send_control_status(core::control::OmsToControlStatus status) noexcept;
            [[nodiscard]] bool emit_cancel_command_for_record(OrderRecord& record, intent::IntentContext context, intent::OmsRequestId command_oms_request_id, std::uint64_t submission_ts_ns) noexcept;
            void emit_order_state_update(const OrderRecord& record, VenueEventSource source, std::uint64_t update_ts_ns) noexcept;
            void emit_trading_enabled_changed() noexcept;
            void emit_telemetry() noexcept;
            void clear_pending_command(OrderRecord& record) noexcept;
            void restore_previous_state(OrderRecord& record) noexcept;
            [[nodiscard]] std::uint64_t live_order_count() const noexcept;
            [[nodiscard]] std::uint64_t pending_submit_order_count() const noexcept;
            [[nodiscard]] std::uint64_t uncertain_order_count() const noexcept;

            std::unordered_map<ClientOrderId, intent::OmsRequestId> client_order_id_to_oms_request_id_map_;
            std::unordered_map<ExchangeOrderId, intent::OmsRequestId> exchange_order_id_to_oms_request_id_map_;
            std::unordered_map<intent::OmsRequestId, OrderRecord> oms_request_id_to_order_record_map_;

            OmsQueues queues_;
            core::control::OmsTelemetrySnapshot telemetry_;
            bool trading_enabled_{false};
            bool flatten_requested_{false};
            intent::OmsRequestId next_oms_request_id_{1};

    };


}
