#include <iostream>
#include <string>
#include <variant>

#include "predex/oms/transport/kalshi_rest_adapter.hpp"

namespace {

using predex::core::oms::kalshi::ClientOrderId;
using predex::core::oms::kalshi::IntentContext;
using predex::core::oms::kalshi::NewOrderIntent;
using predex::core::oms::kalshi::OmsTimeInForce;
using predex::core::oms::kalshi::Outcome;
using predex::core::oms::kalshi::SubmitOrderCmd;
using predex::core::oms::kalshi::VenueOrderAck;
using predex::core::oms::kalshi::VenueOrderCanceled;
using predex::core::oms::kalshi::VenueOrderFill;
using predex::core::oms::kalshi::transport::HttpResponse;
using predex::core::oms::kalshi::transport::KalshiRestAdapter;
using predex::core::oms::kalshi::transport::MarketTickerMap;
using predex::core::oms::kalshi::transport::PersistentHttpSession;
using predex::core::oms::kalshi::transport::RestTraceInfo;
using predex::internal::ExchangeId;
using predex::internal::MarketId;
using predex::internal::Side;
using predex::websocket::kalshi::AuthSigner;
using predex::websocket::kalshi::Credentials;

int fail(std::string_view message) {
    std::cerr << "kalshi_rest_adapter_boundary_test: " << message << '\n';
    return 1;
}

SubmitOrderCmd make_submit_command(MarketId market_id, Side side, Outcome outcome,
                                   std::int64_t price_ticks, std::string client_order_id) {
    SubmitOrderCmd command{};
    command.intent.context = IntentContext{
        .shard_id = 1,
        .affinity_key = 1,
        .group_intent_id = 1,
        .leg_index = 0,
        .leg_count = 1,
        .signal_id = 1,
        .local_intent_id = 1,
        .signal_ts_ns = 1,
        .tick_recv_ns = 1,
        .submission_enqueued_ns = 1,
        .event_id = 1,
        .market_id = market_id,
    };
    static_cast<void>(command.order.client_order_id.assign_from(client_order_id));
    command.intent = NewOrderIntent{
        .context = command.intent.context,
        .exchange = ExchangeId::kKalshi,
        .side = side,
        .outcome = outcome,
        .qty_lots = 100,
        .limit_price_ticks = price_ticks,
        .time_in_force = OmsTimeInForce::kIoc,
        .intent_ts_ns = 1,
    };
    return command;
}

} // namespace

int main() {
    AuthSigner signer{Credentials{.key_id = "test", .private_key_pem = "test"}};
    const MarketTickerMap tickers{
        {MarketId{1}, "TEST-YES"},
        {MarketId{2}, "TEST-NO"},
        {MarketId{3}, "TEST-NO"},
    };
    KalshiRestAdapter adapter{
        PersistentHttpSession{std::move(signer), "https://api.elections.kalshi.com"},
        &tickers};

    const auto good = adapter.prepare_submit_order(
        make_submit_command(1, Side::kBuy, Outcome::kYes, 460, "cid-good"));
    if (!good.ok) {
        std::cerr << good.error_message << '\n';
        return fail("expected 460 internal ticks to serialize successfully");
    }
    if (good.request.body.find("\"yes_price\":46") == std::string::npos) {
        return fail("expected 460 internal ticks to convert to yes_price 46");
    }

    const auto converted = adapter.prepare_submit_order(
        make_submit_command(2, Side::kSell, Outcome::kYes, 160, "cid-converted"));
    if (!converted.ok) {
        std::cerr << converted.error_message << '\n';
        return fail("expected 160 internal ticks to serialize successfully as 16 cents");
    }
    if (converted.request.body.find("\"yes_price\":16") == std::string::npos) {
        return fail("expected 160 internal ticks to convert to yes_price 16");
    }

    const auto bad = adapter.prepare_submit_order(
        make_submit_command(3, Side::kSell, Outcome::kYes, 165, "cid-bad"));
    if (bad.ok) {
        return fail(
            "expected non-cent-aligned internal ticks to be rejected before hitting the wire");
    }
    if (bad.error_message.find("Kalshi integer cents") == std::string::npos) {
        std::cerr << bad.error_message << '\n';
        return fail("expected explicit Kalshi cents conversion error");
    }

    const std::vector<SubmitOrderCmd> batched_commands{
        make_submit_command(1, Side::kBuy, Outcome::kYes, 10, "cid-fill"),
        make_submit_command(2, Side::kSell, Outcome::kYes, 80, "cid-cancel"),
    };
    HttpResponse batched_response{
        .ok = true,
        .status_code = 201,
        .body = "{\"orders\":["
                "{\"order\":{\"order_id\":\"ex-fill\",\"status\":\"executed\",\"fill_count_fp\":"
                "\"1.00\","
                "\"initial_count_fp\":\"1.00\",\"remaining_count_fp\":\"0.00\",\"yes_price_"
                "dollars\":\"0.0100\"}},"
                "{\"order\":{\"order_id\":\"ex-cancel\",\"status\":\"canceled\",\"fill_count_fp\":"
                "\"0.00\","
                "\"initial_count_fp\":\"1.00\",\"remaining_count_fp\":\"0.00\",\"yes_price_"
                "dollars\":\"0.0800\"}}]}",
    };
    auto completed = adapter.complete_batched_submit_orders(
        batched_commands, batched_response,
        RestTraceInfo{.request_sent_ts_ns = 11, .response_recv_ts_ns = 22});
    if (!completed.ok) {
        std::cerr << completed.error_message << '\n';
        return fail("expected completed batched submit response to parse");
    }
    if (completed.events.size() != 4) {
        return fail("expected ack/fill for executed leg and ack/canceled for canceled leg");
    }
    if (!std::holds_alternative<VenueOrderAck>(completed.events[0]) ||
        !std::holds_alternative<VenueOrderFill>(completed.events[1]) ||
        !std::holds_alternative<VenueOrderAck>(completed.events[2]) ||
        !std::holds_alternative<VenueOrderCanceled>(completed.events[3])) {
        return fail("expected terminal batched submit statuses to emit fill/canceled events");
    }

    return 0;
}
