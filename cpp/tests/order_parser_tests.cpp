#include <gtest/gtest.h>

#include <cstddef>
#include <span>
#include <string_view>

#include "predex/ingest/kalshi/order_data/order_parser.hpp"

namespace {

namespace order_data = predex::ingest::kalshi::order_data;
namespace oms = predex::oms;
namespace intent = predex::oms::intent;

[[nodiscard]] std::span<const std::byte> as_bytes(
    std::string_view text) noexcept {
    return {
        reinterpret_cast<const std::byte*>(text.data()),
        text.size()
    };
}

TEST(OrderParserTest, ParsesCompleteMarketPositionUpdate) {
    constexpr std::string_view payload = R"json({
        "type":"market_position",
        "sid":3,
        "seq":17,
        "msg":{
            "market_ticker":"TEST-MARKET",
            "position_fp":"-2.50",
            "position_cost_dollars":"1.1250",
            "realized_pnl_dollars":"-0.2500",
            "fees_paid_dollars":"0.0312",
            "position_fee_cost_dollars":"0.0156",
            "volume_fp":"7.25"
        }
    })json";

    order_data::OrderParser parser;
    order_data::ParsedOrderMessage parsed;
    EXPECT_EQ(
        parser.parse_message(as_bytes(payload), parsed),
        order_data::OrderParseCode::kOK);
    EXPECT_EQ(
        parsed.order_event.event_kind,
        oms::PrivateWsOrderEventKind::kMARKET_POSITION);
    EXPECT_EQ(parsed.market_ticker(), "TEST-MARKET");
    EXPECT_EQ(parsed.order_event.ws_sequence, 17U);
    EXPECT_EQ(parsed.order_event.net_position_lots, -250);
    EXPECT_EQ(parsed.order_event.position_cost_ticks, 11'250);
    EXPECT_EQ(parsed.order_event.realized_pnl_ticks, -2'500);
    EXPECT_EQ(parsed.order_event.fees_paid_ticks, 312);
    EXPECT_EQ(parsed.order_event.position_fee_cost_ticks, 156);
    EXPECT_EQ(parsed.order_event.volume_lots, 725);
}

TEST(OrderParserTest, ConservativelyRoundsSixPlacePositionMoney) {
    constexpr std::string_view payload = R"json({
        "type":"market_position",
        "sid":3,
        "seq":18,
        "msg":{
            "market_ticker":"TEST-MARKET",
            "position_fp":"1.00",
            "position_cost_dollars":"1.125001",
            "realized_pnl_dollars":"-0.250001",
            "fees_paid_dollars":"0.031201",
            "position_fee_cost_dollars":"0.015600",
            "volume_fp":"7.25"
        }
    })json";

    order_data::OrderParser parser;
    order_data::ParsedOrderMessage parsed;
    EXPECT_EQ(
        parser.parse_message(as_bytes(payload), parsed),
        order_data::OrderParseCode::kOK);
    EXPECT_EQ(parsed.order_event.position_cost_ticks, 11'251);
    EXPECT_EQ(parsed.order_event.realized_pnl_ticks, -2'501);
    EXPECT_EQ(parsed.order_event.fees_paid_ticks, 313);
    EXPECT_EQ(parsed.order_event.position_fee_cost_ticks, 156);
}

TEST(OrderParserTest, RejectsPositionMoneyBeyondSixPlaces) {
    constexpr std::string_view payload = R"json({
        "type":"market_position",
        "sid":3,
        "seq":19,
        "msg":{
            "market_ticker":"TEST-MARKET",
            "position_fp":"1.00",
            "position_cost_dollars":"1.0000001",
            "realized_pnl_dollars":"0.000000",
            "fees_paid_dollars":"0.000000",
            "position_fee_cost_dollars":"0.000000",
            "volume_fp":"1.00"
        }
    })json";

    order_data::OrderParser parser;
    order_data::ParsedOrderMessage parsed;
    EXPECT_EQ(
        parser.parse_message(as_bytes(payload), parsed),
        order_data::OrderParseCode::kMISSING_FIELD);
}

TEST(OrderParserTest, ParsesFillIdentityAndSignedActionInputs) {
    constexpr std::string_view payload = R"json({
        "type":"fill",
        "sid":4,
        "seq":23,
        "msg":{
            "trade_id":"trade-123",
            "order_id":"order-456",
            "market_ticker":"TEST-MARKET",
            "side":"no",
            "action":"buy",
            "yes_price_dollars":"0.3700",
            "count_fp":"1.25",
            "ts_ms":1234
        }
    })json";

    order_data::OrderParser parser;
    order_data::ParsedOrderMessage parsed;
    EXPECT_EQ(
        parser.parse_message(as_bytes(payload), parsed),
        order_data::OrderParseCode::kOK);
    EXPECT_EQ(parsed.order_event.trade_id.view(), "trade-123");
    EXPECT_EQ(parsed.order_event.exchange_order_id.view(), "order-456");
    EXPECT_EQ(parsed.order_event.outcome, intent::Outcome::kNO);
    EXPECT_EQ(parsed.order_event.action, intent::OrderAction::kBUY);
    EXPECT_EQ(parsed.order_event.last_fill_price_ticks, 6'300);
    EXPECT_EQ(parsed.order_event.last_fill_qty_lots, 125);
    EXPECT_EQ(parsed.order_event.venue_ts_ns, 1'234'000'000U);
    EXPECT_EQ(parsed.order_event.ws_sequence, 23U);
}

} // namespace
