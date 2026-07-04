#pragma once
#include <cstdint>
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
        intent::Outcome outcome{intent::Outcome::kUNKNOWN};
        intent::MarketId market_id{};

        std::int64_t ordered_qty_lots{0};
        std::int64_t working_price_ticks{0};
        std::int64_t cumulative_filled_qty_lots{0};
        std::int64_t leaves_qty_lots{0};

    };
    struct OmsQueues{
        std::vector<utils::SPSCQueue<intent::StrategyIntent>*> strategy_intent_queues;
        std::vector<utils::SPSCQueue<OmsToStrategyMessage>*> strategy_response_queues;
        utils::SPSCQueue<core::control::ControlToOmsCommand>& control_command_queue;
        utils::SPSCQueue<OmsToKalshiCommand>& kalshi_command_queue;
        utils::SPSCQueue<PrivateWsOrderEvent>& venue_event_queue;

    };


    class Oms{
        public:
            Oms(OmsQueues queues);

            [[nodiscard]] OmsPumpResult pump_once() noexcept;
            [[nodiscard]] std::size_t drain_strategy_intents(std::size_t max) noexcept;
            [[nodiscard]] std::size_t drain_venue_events(std::size_t max) noexcept;
            [[nodiscard]] std::size_t drain_control_commands(std::size_t max) noexcept;

        private:
            void handle_strategy_intent(const intent::NewOrderIntent& intent) noexcept;
            void handle_strategy_intent(const intent::CancelOrderIntent& intent) noexcept;
            void handle_strategy_intent(const intent::ModifyOrderIntent& intent) noexcept;

            void handle_venue_event(const RestOrderResponse& response) noexcept;
            void handle_control_command(const PrivateWsOrderEvent& event) noexcept;

            std::unordered_map<ClientOrderId, intent::OmsRequestId> client_order_id_to_oms_request_id_map_;
            std::unordered_map<ExchangeOrderId, intent::OmsRequestId> exchange_order_id_to_oms_request_id_map_;
            std::unordered_map<intent::OmsRequestId, OrderRecord> oms_request_id_to_order_record_map_;

    };


}