#include <gtest/gtest.h>

#include "predex/research/portfolio.hpp"

namespace {

using predex::research::MarketBbo;
using predex::research::Portfolio;
using predex::research::PortfolioConfig;
using predex::research::QuoteSide;
using predex::research::ReplayStamp;
using predex::research::SimulatedFill;

constexpr std::uint32_t kEventId = 1;
constexpr std::uint32_t kMarketId = 2;
constexpr std::int64_t kBuyPriceTicks = 4'500;
constexpr std::int64_t kSellPriceTicks = 5'500;
constexpr std::int64_t kBidMarkTicks = 4'800;
constexpr std::int64_t kAskMarkTicks = 5'200;
constexpr std::int64_t kOneContractLots = 100;
constexpr std::int64_t kTwoContractLots = 200;
constexpr std::int64_t kThreeContractLots = 300;
constexpr std::int64_t kFeeTicksPerContract = 10;

SimulatedFill fill(QuoteSide quote_side,
                   std::int64_t price_ticks,
                   std::int64_t qty_lots) {
    return SimulatedFill{
        .order_id = "fixture",
        .event_id = kEventId,
        .market_id = kMarketId,
        .quote_side = quote_side,
        .stamp = ReplayStamp{.record_index = 1, .recv_ts_ns = 1},
        .yes_price_ticks = price_ticks,
        .filled_qty_lots = static_cast<double>(qty_lots),
    };
}

TEST(ResearchPortfolio, BuyAndSellNetToOneLongContract) {
    Portfolio portfolio;
    portfolio.apply_fill(fill(
        QuoteSide::kBidYes, kBuyPriceTicks, kThreeContractLots));
    portfolio.apply_fill(fill(
        QuoteSide::kAskYes, kSellPriceTicks, kTwoContractLots));

    const auto position = portfolio.position(kMarketId);
    EXPECT_DOUBLE_EQ(position.signed_yes_contracts, 1.0);
    EXPECT_DOUBLE_EQ(position.cash_ticks, kSellPriceTicks * 2 - kBuyPriceTicks * 3);
}

TEST(ResearchPortfolio, SellAndBuyNetToOneShortContract) {
    Portfolio portfolio;
    portfolio.apply_fill(fill(
        QuoteSide::kAskYes, kSellPriceTicks, kThreeContractLots));
    portfolio.apply_fill(fill(
        QuoteSide::kBidYes, kBuyPriceTicks, kTwoContractLots));

    const auto position = portfolio.position(kMarketId);
    EXPECT_DOUBLE_EQ(position.signed_yes_contracts, -1.0);
    EXPECT_DOUBLE_EQ(position.cash_ticks, kSellPriceTicks * 3 - kBuyPriceTicks * 2);
}

TEST(ResearchPortfolio, FeesApplyWithCorrectCashSign) {
    Portfolio portfolio{PortfolioConfig{
        .fees_ticks_per_contract = kFeeTicksPerContract,
    }};
    portfolio.apply_fill(fill(
        QuoteSide::kBidYes, kBuyPriceTicks, kOneContractLots));
    portfolio.apply_fill(fill(
        QuoteSide::kAskYes, kSellPriceTicks, kOneContractLots));

    const auto position = portfolio.position(kMarketId);
    EXPECT_DOUBLE_EQ(position.signed_yes_contracts, 0.0);
    EXPECT_DOUBLE_EQ(
        position.cash_ticks,
        kSellPriceTicks - kBuyPriceTicks - 2 * kFeeTicksPerContract);
    EXPECT_DOUBLE_EQ(position.fees_ticks, 2 * kFeeTicksPerContract);
    EXPECT_DOUBLE_EQ(portfolio.total_fees_ticks(), 2 * kFeeTicksPerContract);
}

TEST(ResearchPortfolio, LongLiquidationMarksAtBid) {
    Portfolio portfolio;
    portfolio.apply_fill(fill(
        QuoteSide::kBidYes, kBuyPriceTicks, kOneContractLots));

    const auto equity = portfolio.liquidation_equity_ticks(
        kMarketId,
        MarketBbo{
            .bid_ticks = kBidMarkTicks,
            .ask_ticks = kAskMarkTicks,
        });
    ASSERT_TRUE(equity.has_value());
    EXPECT_DOUBLE_EQ(*equity, kBidMarkTicks - kBuyPriceTicks);
}

TEST(ResearchPortfolio, ShortLiquidationMarksAtAsk) {
    Portfolio portfolio;
    portfolio.apply_fill(fill(
        QuoteSide::kAskYes, kSellPriceTicks, kOneContractLots));

    const auto equity = portfolio.liquidation_equity_ticks(
        kMarketId,
        MarketBbo{
            .bid_ticks = kBidMarkTicks,
            .ask_ticks = kAskMarkTicks,
        });
    ASSERT_TRUE(equity.has_value());
    EXPECT_DOUBLE_EQ(*equity, kSellPriceTicks - kAskMarkTicks);
}

TEST(ResearchPortfolio, UnavailableMarkLeavesEquityUnknown) {
    Portfolio portfolio;
    portfolio.apply_fill(fill(
        QuoteSide::kBidYes, kBuyPriceTicks, kOneContractLots));

    EXPECT_FALSE(portfolio.midpoint_equity_ticks(kMarketId, std::nullopt).has_value());
    EXPECT_FALSE(portfolio.liquidation_equity_ticks(kMarketId, std::nullopt).has_value());
}

} // namespace
