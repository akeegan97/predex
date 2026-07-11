#include <gtest/gtest.h>

#include "predex/control/control_types.hpp"
#include "predex/oms/oms.hpp"
#include "predex/oms/oms_types.hpp"
#include "predex/oms/order_intents.hpp"
#include "predex/utils/spsc.hpp"

namespace {

namespace control = predex::core::control;
namespace oms = predex::oms;
namespace intent = predex::oms::intent;
namespace utils = predex::utils;

struct OmsHarness {
    utils::SPSCQueue<intent::StrategyIntent> strategy_to_oms{1024};
    utils::SPSCQueue<oms::OmsToStrategyMessage> oms_to_strategy{1024};
    utils::SPSCQueue<control::ControlToOmsCommand> control_to_oms{1024};
    utils::SPSCQueue<control::OmsToControlStatus> oms_to_control{1024};
    utils::SPSCQueue<oms::OmsToKalshiCommand> oms_to_kalshi{1024};
    utils::SPSCQueue<oms::KalshiToOmsEvent> kalshi_to_oms{1024};

    oms::OmsQueues queues{
        .strategy_intent_queues = {&strategy_to_oms},
        .strategy_response_queues = {&oms_to_strategy},
        .control_command_queue = control_to_oms,
        .oms_status_queue = oms_to_control,
        .kalshi_command_queue = oms_to_kalshi,
        .venue_event_queue = kalshi_to_oms,
    };

    oms::Oms uut{queues};
};

[[nodiscard]] intent::NewOrderIntent make_valid_new_order() {
    return intent::NewOrderIntent{
        .context = intent::IntentContext{
            .strategy_index = 0,
            .market_id = 101,
            .event_id = 202,
            .strategy_id = 303,
            .strategy_intent_id = 404,
            .signal_id = 505,
        },
        .outcome = intent::Outcome::kYES,
        .action = intent::OrderAction::kBUY,
        .liquidity_intent = intent::LiquidityIntent::kMAKER,
        .order_type = intent::OrderType::kLIMIT,
        .time_in_force = intent::TimeInForce::kGTC,
        .price_ticks = 4200,
        .quantity_lots = 7,
    };
}

[[nodiscard]] intent::CancelOrderIntent make_valid_cancel_order(oms::ClientOrderId client_order_id) {
    return intent::CancelOrderIntent{
        .context = intent::IntentContext{
            .strategy_index = 0,
            .market_id = 101,
            .event_id = 202,
            .strategy_id = 303,
            .strategy_intent_id = 404,
            .signal_id = 505,
        },
        .target_oms_request_id = 1,
    };
}

void enable_trading(OmsHarness& harness) {
    ASSERT_TRUE(harness.control_to_oms.try_push(control::ControlToOmsCommand{control::AllowTrading{}}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    control::OmsToControlStatus status{};
    ASSERT_TRUE(harness.oms_to_control.try_pop(status));
    const auto* changed = std::get_if<control::OmsTradingEnabledChanged>(&status);
    ASSERT_NE(changed, nullptr);
    EXPECT_TRUE(changed->trading_enabled);
}

[[nodiscard]] oms::SubmitOrderCmd submit_order(OmsHarness& harness) {
    EXPECT_TRUE(harness.strategy_to_oms.try_push(intent::StrategyIntent{make_valid_new_order()}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    oms::OmsToStrategyMessage response_msg{};
    if(!harness.oms_to_strategy.try_pop(response_msg)){
        ADD_FAILURE() << "expected OMS response";
        return {};
    }
    const auto* response = std::get_if<oms::OmsResponse>(&response_msg);
    if(response == nullptr){
        ADD_FAILURE() << "expected OmsResponse";
        return {};
    }
    EXPECT_EQ(response->response_type, oms::OmsResponseType::kACCEPTED);
    EXPECT_EQ(response->reject_reason, oms::RejectReason::kNONE);

    oms::OmsToKalshiCommand command_msg{};
    if(!harness.oms_to_kalshi.try_pop(command_msg)){
        ADD_FAILURE() << "expected Kalshi command";
        return {};
    }
    const auto* submit = std::get_if<oms::SubmitOrderCmd>(&command_msg);
    if(submit == nullptr){
        ADD_FAILURE() << "expected SubmitOrderCmd";
        return {};
    }
    return *submit;
}

TEST(OmsTest, RejectsNewOrdersWhenTradingDisabled) {
    OmsHarness harness{};

    ASSERT_TRUE(harness.strategy_to_oms.try_push(intent::StrategyIntent{make_valid_new_order()}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    oms::OmsToStrategyMessage message{};
    ASSERT_TRUE(harness.oms_to_strategy.try_pop(message));
    const auto* response = std::get_if<oms::OmsResponse>(&message);
    ASSERT_NE(response, nullptr);
    EXPECT_EQ(response->response_type, oms::OmsResponseType::kREJECTED);
    EXPECT_EQ(response->reject_reason, oms::RejectReason::kTRADING_DISABLED);

    oms::OmsToKalshiCommand command{};
    EXPECT_FALSE(harness.oms_to_kalshi.try_pop(command));
}

TEST(OmsTest, AcceptsNewOrderWhenTradingEnabledAndEmitsSubmitCommand) {
    OmsHarness harness{};
    enable_trading(harness);

    const oms::SubmitOrderCmd submit = submit_order(harness);

    EXPECT_EQ(submit.oms_request_id, 1U);
    EXPECT_FALSE(submit.client_order_id.empty());
    EXPECT_EQ(submit.new_order_intent.context.market_id, 101U);
    EXPECT_EQ(submit.new_order_intent.price_ticks, 4200);
    EXPECT_EQ(submit.new_order_intent.quantity_lots, 7);
}

TEST(OmsTest, RestSubmitAckMovesOrderToWorking) {
    OmsHarness harness{};
    enable_trading(harness);
    const oms::SubmitOrderCmd submit = submit_order(harness);

    oms::ExchangeOrderId exchange_order_id{};
    ASSERT_TRUE(exchange_order_id.assign_from("venue-order-1"));

    oms::RestOrderResponse ack{
        .context = oms::OmsContext{
            .oms_request_id = submit.oms_request_id,
            .context = submit.new_order_intent.context,
        },
        .command_kind = oms::RestCommandKind::kSUBMIT_ORDER,
        .result_code = oms::RestResultCode::kACKED,
        .client_order_id = submit.client_order_id,
        .exchange_order_id = exchange_order_id,
        .transport_submit_ts_ns = submit.submission_ts_ns,
        .transport_recv_ts_ns = submit.submission_ts_ns + 1000,
        .http_status_code = 200,
    };

    ASSERT_TRUE(harness.kalshi_to_oms.try_push(oms::KalshiToOmsEvent{ack}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    oms::OmsToStrategyMessage update_msg{};
    ASSERT_TRUE(harness.oms_to_strategy.try_pop(update_msg));
    const auto* update = std::get_if<oms::OrderStateUpdate>(&update_msg);
    ASSERT_NE(update, nullptr);
    EXPECT_EQ(update->context.oms_request_id, submit.oms_request_id);
    EXPECT_EQ(update->order_state, oms::OrderState::kWORKING);
    EXPECT_EQ(update->update_source, oms::VenueEventSource::kREST_RESPONSE);
    EXPECT_EQ(update->exchange_order_id, exchange_order_id);
    EXPECT_EQ(update->ordered_qty_lots, 7);
    EXPECT_EQ(update->leaves_qty_lots, 7);
}

TEST(OmsTest, PrivateWsEventUpdatesCanonicalLifecycle) {
    OmsHarness harness{};
    enable_trading(harness);
    const oms::SubmitOrderCmd submit = submit_order(harness);

    oms::ExchangeOrderId exchange_order_id{};
    ASSERT_TRUE(exchange_order_id.assign_from("venue-order-1"));

    oms::PrivateWsOrderEvent event{
        .event_kind = oms::PrivateWsOrderEventKind::kUSER_ORDER,
        .client_order_id = submit.client_order_id,
        .exchange_order_id = exchange_order_id,
        .market_id = submit.new_order_intent.context.market_id,
        .outcome = intent::Outcome::kYES,
        .recv_ts_ns = 100,
        .venue_ts_ns = 90,
        .ws_sequence = 1,
        .order_state = oms::OrderState::kPARTIALLY_FILLED,
        .ordered_qty_lots = 7,
        .cumulative_filled_qty_lots = 3,
        .leaves_qty_lots = 4,
        .last_fill_qty_lots = 3,
        .last_fill_price_ticks = 4200,
    };

    ASSERT_TRUE(harness.kalshi_to_oms.try_push(oms::KalshiToOmsEvent{event}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    oms::OmsToStrategyMessage update_msg{};
    ASSERT_TRUE(harness.oms_to_strategy.try_pop(update_msg));
    const auto* update = std::get_if<oms::OrderStateUpdate>(&update_msg);
    ASSERT_NE(update, nullptr);
    EXPECT_EQ(update->context.oms_request_id, submit.oms_request_id);
    EXPECT_EQ(update->order_state, oms::OrderState::kPARTIALLY_FILLED);
    EXPECT_EQ(update->update_source, oms::VenueEventSource::kWEBSOCKET_FEED);
    EXPECT_EQ(update->ordered_qty_lots, 7);
    EXPECT_EQ(update->cumulative_filled_qty_lots, 3);
    EXPECT_EQ(update->leaves_qty_lots, 4);
}

TEST(OmsTest, CancelOrderUnknownTarget){
    OmsHarness harness{};
    enable_trading(harness);

    intent::CancelOrderIntent cancel{
        .context = intent::IntentContext{
            .strategy_index = 0,
            .market_id = 101,
            .event_id = 202,
            .strategy_id = 303,
            .strategy_intent_id = 404,
            .signal_id = 505,
        },
        .target_oms_request_id = 9999, // unknown
    };

    ASSERT_TRUE(harness.strategy_to_oms.try_push(intent::StrategyIntent{cancel}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    oms::OmsToStrategyMessage response_msg{};
    ASSERT_TRUE(harness.oms_to_strategy.try_pop(response_msg));
    const auto* response = std::get_if<oms::OmsResponse>(&response_msg);
    ASSERT_NE(response, nullptr);
    EXPECT_EQ(response->response_type, oms::OmsResponseType::kREJECTED);
    EXPECT_EQ(response->reject_reason, oms::RejectReason::kUNKNOWN_TARGET_ORDER);
}


} // namespace
