#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <vector>

#include "support/fake_ws_transport.hpp"
#include "trading/adapters/exchanges/kalshi/auth_signer.hpp"
#include "trading/adapters/exchanges/kalshi/oms_adapter.hpp"
#include "trading/adapters/exchanges/kalshi/ws_adapter.hpp"
#include "trading/adapters/ws/session.hpp"
#include "trading/config/trader_config.hpp"
#include "trading/decode/exchanges/kalshi/extractor.hpp"
#include "trading/decode/extracted_fields.hpp"
#include "trading/engine/runtime.hpp"
#include "trading/ingest/raw_frame.hpp"
#include "trading/internal/normalized_event.hpp"
#include "trading/internal/oms_types.hpp"
#include "trading/internal/router_frame.hpp"
#include "trading/oms/global_risk_gate.hpp"
#include "trading/oms/order_manager.hpp"
#include "trading/oms/paper_order_transport.hpp"
#include "trading/oms/portfolio_risk_gate.hpp"
#include "trading/oms/position_ledger.hpp"
#include "trading/oms/ws_order_transport.hpp"
#include "trading/parsers/exchanges/kalshi/parser.hpp"
#include "trading/pipeline/live_pipeline.hpp"
#include "trading/pipeline/replay_harness.hpp"
#include "trading/router/router.hpp"
#include "trading/router/shard_dispatch.hpp"
#include "trading/shards/book_store.hpp"
#include "trading/shards/event_handler.hpp"
#include "trading/shards/message_parser.hpp"
#include "trading/strategy/order_intent_sink.hpp"
#include "trading/strategy/order_manager_intent_sink.hpp"
#include "trading/strategy/paper_trade_probe_strategy.hpp"
#include "trading/strategy/ledger_shard_risk_snapshot_provider.hpp"
#include "trading/strategy/market_filter_event_handler.hpp"
#include "trading/strategy/shard_risk_gate.hpp"
#include "trading/strategy/shard_risk_snapshot_provider.hpp"
#include "trading/strategy/strategy_event_handler.hpp"
#include "trading/strategy/strategy.hpp"
#include "trading/strategy/strategy_runner.hpp"

namespace {

constexpr const char* kTestPrivateKeyPem = R"(-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDC0wMNDKatICXc
VHJGfoGoMmz8BoF7HwhTl7CCXAZ55lLvCaLY2LhZumiWRuIHaHZDl2DhZMCDNJ3F
eIEawGjpjlWsj7WGXzSsTZUQLS0TVq5Jr5AtFWbvFn/cYySD47/aFB3mqgUPDS7G
0tTx/RpHjsYdp4j/aES5lBArQVzWVjd8zlKP90Q2J39H2soahvUUNW6hxhz2XFPq
N6YgKclwSApt9177ShO6fPQ7wa4RL+1rRbLPTkZKbW4S2JF8xZ1/RYUFZNKwG1WX
Hp4ZY8wIDReDIN9/ksh33DbVGloIjbJfRo8dMqawwIw03ZJol8KbQIGOlxlLEbdP
H4mgFv7ZAgMBAAECggEAA45NQpzq7lTFOH8/Qpz8tw/jpJFPAzDLMfPvqBkVexXG
5fU7RXdaP6kwCXMKV3z0ROgkVxmJpBE8E9xO5mL7sWav/3zocmFKsh5MebihujvK
iD/93SXhSPb9GHyBB65n3QtP/zfZPnhsyO8sClBg1HsdMqB/tAI7Y8tFGUeOmr9O
WOY359vG5xJlCBhhQ1hWaxVn1zb+Bs3Cs4dqpFnRpJP0xUuMKi6TDXHhUnWsw6Rr
cSC3zeLJ/2lDO58X9T5GEuIjvIZjkDNrvkV3OgLiHAt6R2b+scXirZHzYwAd1s3o
W1vmLPkAAXIip2IYndtIBjMEqMp5+6dHaXel83H0FQKBgQDqesT746NvBwDpSmZj
ho6XiyTQKS6SERK3I3hSKRAXgdOfc5OLWCaHpgitnhaCQ4Zl4lusnuBNFkJnLUZ3
v2CTjrGqUCZtOAy+5k2Fr+HwmmDGCqYXfN6xbZfH0y+kzk8lDZAyT/VkMv3gmtTE
A4FQKVb870xra5J5jwtxEerD1QKBgQDUtITDCtr1aWW6dUF8JA5S3u3HiC2XOdTZ
h5dzLxHWCPtIJrTSYINrx0kqvr6uefTOlWel5ckJ8l/qwPtEyZ36SaQvvrxhew5/
monY3kKLisBQLFyfdwAgKjOTBso3Ydfi0OL5GfnVgRdgPSt96OarLHf/Oj252Ef0
/B9AiFFE9QKBgELrZuy8uxgk1429PhuJe4iknY0LM89cMAs0hFJuhbkvxEXtpl5O
ejCcoj1qsOHcg67wO1m4vitB/xGTSllCtT1nrCz/Uvg41upksBtHZyRT6uqaf2yU
prncnUTacO6IMu5QQKcbSt2d7/b6OV734SAPrFPjt9uwh81JlepfQrFBAoGAYrD/
iJ/YyFWG5eTR5Y+1Na5KFXKR9MDYibXoB4GnfO/d57RN4e72C2QEBtlKEBp2BAQ+
ezMK/JqA9qNj8p65tg/FJoIRVgmKyJojq4Q0zBos8QYkU7kbTdFG7MkRunCjgpH5
PitXSEEFX5oCPAF8RZ/0bW/QhBJeEMWKmp6lVqECgYBQOlGbPcpEIDo738T3yADv
dKLBmz0Z/Rgg22dVhjhhUKYTzjc8spVyUFmXjm9/RFrqZRBZ+r52ZZS9mO/D50PR
Hs7GYP2lYjn+hqDzxRLUDyDFxdKcxVtz4ye9iTCiaOFdCBRDQOMiViyG3bzaEWqv
wMev4lw7N9lCKEbZCmutaA==
-----END PRIVATE KEY-----)";

trading::adapters::exchanges::kalshi::WsAdapter make_kalshi_adapter() {
    return trading::adapters::exchanges::kalshi::WsAdapter{
        trading::adapters::exchanges::kalshi::AuthSigner{
            trading::adapters::exchanges::kalshi::Credentials{
                .key_id = "test-key-id",
                .private_key_pem = kTestPrivateKeyPem,
            },
        },
    };
}

trading::decode::ExtractedFields passthrough_decode(const trading::ingest::RawFrame& frame) {
    return trading::decode::ExtractedFields{
        .status = trading::decode::DecodeStatus::kOk,
        .market_ticker = frame.market_ticker,
        .seq_id = frame.seq_id,
        .recv_timestamp = frame.recv_timestamp,
        .source = frame.source,
    };
}

trading::ingest::RawFrame make_raw_frame(std::string ticker, std::string payload,
                                         std::uint64_t seq_id) {
    trading::ingest::RawFrame frame{};
    frame.recv_timestamp = std::chrono::steady_clock::now();
    frame.source = "kalshi";
    frame.payload = std::move(payload);
    frame.market_ticker = std::move(ticker);
    frame.seq_id = seq_id;
    return frame;
}

std::vector<trading::router::RoutedEvent>
drain_all_shards(trading::router::ShardedEventDispatch& shard_dispatch) {
    std::vector<trading::router::RoutedEvent> drained;
    for (std::size_t shard_id = 0; shard_id < shard_dispatch.shard_count(); ++shard_id) {
        trading::router::RoutedEvent routed{};
        while (shard_dispatch.try_pop(shard_id, routed)) {
            drained.push_back(routed);
        }
    }
    return drained;
}

trading::router::RoutedEvent make_routed_event(
    std::string raw_payload, std::string market_ticker, std::optional<std::uint64_t> seq_id,
    trading::internal::ExchangeId exchange_id = trading::internal::ExchangeId::kKalshi) {
    const auto recv_count = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::steady_clock::now().time_since_epoch())
                                .count();
    const auto recv_ns =
        recv_count > 0 ? static_cast<trading::internal::TimestampNs>(recv_count) : 0U;
    return trading::router::RoutedEvent{
        .route_key =
            {
                .market_ticker = market_ticker,
                .shard_id = 0,
            },
        .frame =
            {
                .exchange = exchange_id,
                .market_ticker = std::move(market_ticker),
                .sequence_id = seq_id,
                .recv_ns = recv_ns,
                .raw_payload = std::move(raw_payload),
            },
    };
}

std::optional<std::size_t>
common_shard_id(const std::vector<trading::router::RoutedEvent>& routed_events) {
    if (routed_events.empty()) {
        return std::nullopt;
    }

    const std::size_t first_shard_id = routed_events.front().route_key.shard_id;
    for (const auto& routed : routed_events) {
        if (routed.route_key.shard_id != first_shard_id) {
            return std::nullopt;
        }
    }
    return first_shard_id;
}

void expect_all_market_tickers(const std::vector<trading::router::RoutedEvent>& routed_events,
                               std::string_view expected_market_ticker) {
    for (const auto& routed : routed_events) {
        EXPECT_EQ(routed.frame.market_ticker, expected_market_ticker);
    }
}

const trading::internal::NormalizedEvent& require_parsed(
    const trading::parsers::ParseResult<trading::internal::NormalizedEvent>& parsed_result) {
    EXPECT_TRUE(parsed_result.ok()) << " parse_error=" << static_cast<int>(parsed_result.error());
    static const trading::internal::NormalizedEvent kEmpty{};
    const auto& parsed_value = parsed_result.value();
    if (!parsed_result.ok() || !parsed_value.has_value()) {
        return kEmpty;
    }
    return *parsed_value;
}

const trading::internal::OrderStateUpdate& require_order_update(
    const trading::oms::ParseResult<trading::internal::OrderStateUpdate>& parsed_result) {
    EXPECT_TRUE(parsed_result.ok()) << " parse_error=" << static_cast<int>(parsed_result.error());
    static const trading::internal::OrderStateUpdate kEmpty{};
    const auto& parsed_value = parsed_result.value();
    if (!parsed_result.ok() || !parsed_value.has_value()) {
        return kEmpty;
    }
    return *parsed_value;
}

trading::internal::OrderStateUpdate make_fill_update(
    trading::internal::ExchangeId exchange, std::string market_ticker,
    std::string client_order_id, trading::internal::Side side,
    trading::internal::QtyLots fill_qty_lots, trading::internal::PriceTicks fill_price_ticks,
    trading::internal::TimestampNs recv_ts_ns = 1) {
    const std::string fill_client_order_id = client_order_id;
    return trading::internal::OrderStateUpdate{
        .exchange = exchange,
        .status = trading::internal::OmsOrderStatus::kFilled,
        .client_order_id = std::move(client_order_id),
        .exchange_order_id = std::nullopt,
        .market_ticker = market_ticker,
        .recv_ts_ns = recv_ts_ns,
        .data =
            trading::internal::OrderFill{
                .client_order_id = fill_client_order_id,
                .exchange_order_id = std::nullopt,
                .market_ticker = std::move(market_ticker),
                .fill_qty_lots = fill_qty_lots,
                .fill_price_ticks = fill_price_ticks,
                .side = side,
                .liquidity = trading::internal::OmsLiquidity::kUnknown,
            },
        .raw_payload = {},
    };
}

trading::internal::NormalizedEvent
make_snapshot_event(std::string ticker, std::optional<trading::internal::SequenceId> sequence_id,
                    std::vector<trading::internal::Level> bids,
                    std::vector<trading::internal::Level> asks) {
    trading::internal::NormalizedEvent event{};
    event.type = trading::internal::EventType::kSnapshot;
    event.market_ticker = std::move(ticker);
    event.raw_sequence_id = sequence_id;
    if (sequence_id.has_value()) {
        event.meta.sequence_id = sequence_id.value();
    }
    event.data = trading::internal::SnapshotData{
        .bids = std::move(bids),
        .asks = std::move(asks),
    };
    return event;
}

trading::internal::NormalizedEvent
make_delta_event(std::string ticker, std::optional<trading::internal::SequenceId> sequence_id,
                 trading::internal::Side side, trading::internal::PriceTicks price_ticks,
                 trading::internal::QtyLots delta_qty_lots) {
    trading::internal::NormalizedEvent event{};
    event.type = trading::internal::EventType::kDelta;
    event.market_ticker = std::move(ticker);
    event.raw_sequence_id = sequence_id;
    if (sequence_id.has_value()) {
        event.meta.sequence_id = sequence_id.value();
    }
    event.data = trading::internal::DeltaData{
        .side = side,
        .price_ticks = price_ticks,
        .delta_qty_lots = delta_qty_lots,
    };
    return event;
}

trading::internal::NormalizedEvent
make_trade_event(std::string ticker, std::optional<trading::internal::SequenceId> sequence_id,
                 trading::internal::PriceTicks price_ticks, trading::internal::QtyLots qty_lots) {
    trading::internal::NormalizedEvent event{};
    event.type = trading::internal::EventType::kTrade;
    event.market_ticker = std::move(ticker);
    event.raw_sequence_id = sequence_id;
    if (sequence_id.has_value()) {
        event.meta.sequence_id = sequence_id.value();
    }
    event.data = trading::internal::TradeData{
        .price_ticks = price_ticks,
        .qty_lots = qty_lots,
        .aggressor = trading::internal::Side::kBuy,
        .trade_id = std::optional<std::string>{"trade-1"},
    };
    return event;
}

template <typename Predicate>
bool wait_until(Predicate predicate, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    return predicate();
}

class FakeOmsTransport final : public trading::oms::IOrderTransport {
  public:
    bool connect(const trading::oms::OrderTransportConfig& config) override {
        std::scoped_lock lock{mutex_};
        if (!connect_result_) {
            last_error_code_.store(ErrorCode::kConnectFailed, std::memory_order_relaxed);
            return false;
        }
        config_ = config;
        connected_ = true;
        last_error_code_.store(ErrorCode::kNone, std::memory_order_relaxed);
        return true;
    }

    bool send_text(std::string_view payload) override {
        std::scoped_lock lock{mutex_};
        if (!connected_) {
            last_error_code_.store(ErrorCode::kNotConnected, std::memory_order_relaxed);
            return false;
        }
        if (fail_send_) {
            last_error_code_.store(ErrorCode::kSendFailed, std::memory_order_relaxed);
            return false;
        }
        sent_payloads_.emplace_back(payload);
        last_error_code_.store(ErrorCode::kNone, std::memory_order_relaxed);
        return true;
    }

    std::optional<std::string> recv_text() override {
        std::scoped_lock lock{mutex_};
        if (!connected_ || inbound_payloads_.empty()) {
            return std::nullopt;
        }
        std::string payload = std::move(inbound_payloads_.front());
        inbound_payloads_.pop_front();
        return payload;
    }

    void close() override {
        std::scoped_lock lock{mutex_};
        connected_ = false;
    }

    [[nodiscard]] std::string_view last_error() const override {
        switch (last_error_code_.load(std::memory_order_relaxed)) {
        case ErrorCode::kNone:
            return "";
        case ErrorCode::kConnectFailed:
            return "connect failed";
        case ErrorCode::kSendFailed:
            return "send failed";
        case ErrorCode::kNotConnected:
            return "not connected";
        }
        return "unknown";
    }

    void set_connect_result(bool connect_result) {
        std::scoped_lock lock{mutex_};
        connect_result_ = connect_result;
    }

    void set_fail_send(bool fail_send) {
        std::scoped_lock lock{mutex_};
        fail_send_ = fail_send;
    }

    void queue_inbound(std::string payload) {
        std::scoped_lock lock{mutex_};
        inbound_payloads_.push_back(std::move(payload));
    }

    [[nodiscard]] std::vector<std::string> sent_payloads() const {
        std::scoped_lock lock{mutex_};
        return sent_payloads_;
    }

    [[nodiscard]] std::size_t sent_count() const {
        std::scoped_lock lock{mutex_};
        return sent_payloads_.size();
    }

  private:
    enum class ErrorCode : std::uint8_t {
        kNone = 0,
        kConnectFailed,
        kSendFailed,
        kNotConnected,
    };

    mutable std::mutex mutex_;
    bool connect_result_{true};
    bool connected_{false};
    bool fail_send_{false};
    std::atomic<ErrorCode> last_error_code_{ErrorCode::kNone};
    trading::oms::OrderTransportConfig config_{};
    std::deque<std::string> inbound_payloads_;
    std::vector<std::string> sent_payloads_;
};

class CollectingOrderEventSink final : public trading::oms::IOrderEventSink {
  public:
    bool on_order_update(const trading::internal::OrderStateUpdate& update) override {
        std::scoped_lock lock{mutex_};
        updates_.push_back(update);
        return true;
    }

    [[nodiscard]] std::size_t update_count() const {
        std::scoped_lock lock{mutex_};
        return updates_.size();
    }

    [[nodiscard]] std::optional<trading::internal::OrderStateUpdate> last_update() const {
        std::scoped_lock lock{mutex_};
        if (updates_.empty()) {
            return std::nullopt;
        }
        return updates_.back();
    }

  private:
    mutable std::mutex mutex_;
    std::vector<trading::internal::OrderStateUpdate> updates_;
};

class FixedPortfolioRiskSnapshotProvider final
    : public trading::oms::IPortfolioRiskSnapshotProvider {
  public:
    void set_snapshot(const trading::oms::PortfolioRiskSnapshot& snapshot) {
        std::scoped_lock lock{mutex_};
        snapshot_ = snapshot;
    }

    [[nodiscard]] trading::oms::PortfolioRiskSnapshot
    snapshot_for(trading::internal::ExchangeId exchange, std::string_view market_ticker) const override {
        static_cast<void>(exchange);
        static_cast<void>(market_ticker);
        std::scoped_lock lock{mutex_};
        return snapshot_;
    }

  private:
    mutable std::mutex mutex_;
    trading::oms::PortfolioRiskSnapshot snapshot_{};
};

class FixedShardRiskSnapshotProvider final : public trading::strategy::IShardRiskSnapshotProvider {
  public:
    void set_snapshot(const trading::strategy::ShardRiskSnapshot& snapshot) {
        std::scoped_lock lock{mutex_};
        snapshot_ = snapshot;
    }

    [[nodiscard]] trading::strategy::ShardRiskSnapshot
    snapshot_for(trading::internal::ExchangeId exchange, std::string_view market_ticker) const override {
        static_cast<void>(exchange);
        static_cast<void>(market_ticker);
        std::scoped_lock lock{mutex_};
        return snapshot_;
    }

  private:
    mutable std::mutex mutex_;
    trading::strategy::ShardRiskSnapshot snapshot_{};
};

class CollectingOrderIntentSink final : public trading::strategy::IOrderIntentSink {
  public:
    bool submit_intent(trading::internal::OrderIntent intent) override {
        std::scoped_lock lock{mutex_};
        if (fail_submit_) {
            return false;
        }
        intents_.push_back(std::move(intent));
        return true;
    }

    [[nodiscard]] std::string_view last_error() const override { return last_error_; }

    void set_fail_submit(bool fail_submit, std::string error_message = "intent sink rejected intent") {
        std::scoped_lock lock{mutex_};
        fail_submit_ = fail_submit;
        last_error_ = std::move(error_message);
    }

    [[nodiscard]] std::size_t count() const {
        std::scoped_lock lock{mutex_};
        return intents_.size();
    }

    [[nodiscard]] std::optional<trading::internal::OrderIntent> last_intent() const {
        std::scoped_lock lock{mutex_};
        if (intents_.empty()) {
            return std::nullopt;
        }
        return intents_.back();
    }

  private:
    mutable std::mutex mutex_;
    std::vector<trading::internal::OrderIntent> intents_;
    bool fail_submit_{false};
    std::string last_error_{"intent sink rejected intent"};
};

class QueueingWsTransport final : public trading::adapters::ws::IWsTransport {
  public:
    bool connect(const trading::adapters::ws::TransportConfig& config) override {
        config_ = config;
        if (!connect_result_) {
            connected_ = false;
            last_error_ = "connect failed";
            return false;
        }
        connected_ = true;
        last_error_.clear();
        return true;
    }

    bool send_text(std::string_view payload) override {
        if (!connected_) {
            last_error_ = "not connected";
            return false;
        }
        sent_messages_.emplace_back(payload);
        last_error_.clear();
        return true;
    }

    std::optional<std::string> recv_text() override {
        if (inbound_messages_.empty()) {
            return std::nullopt;
        }
        std::string payload = std::move(inbound_messages_.front());
        inbound_messages_.pop_front();
        return payload;
    }

    void close() override { connected_ = false; }

    [[nodiscard]] std::string_view last_error() const override { return last_error_; }

    void set_connect_result(bool connect_result) { connect_result_ = connect_result; }

    void queue_inbound(std::string payload) { inbound_messages_.push_back(std::move(payload)); }

    [[nodiscard]] bool connected() const { return connected_; }
    [[nodiscard]] const trading::adapters::ws::TransportConfig& config() const { return config_; }
    [[nodiscard]] const std::vector<std::string>& sent_messages() const { return sent_messages_; }

  private:
    bool connect_result_{true};
    bool connected_{false};
    std::string last_error_;
    trading::adapters::ws::TransportConfig config_{};
    std::deque<std::string> inbound_messages_;
    std::vector<std::string> sent_messages_;
};

class StaticDecisionStrategy final : public trading::strategy::IStrategy {
  public:
    explicit StaticDecisionStrategy(std::vector<trading::internal::OrderIntent> intents = {})
        : intents_(std::move(intents)) {}

    void set_intents(std::vector<trading::internal::OrderIntent> intents) { intents_ = std::move(intents); }
    void set_throw(bool should_throw) { should_throw_ = should_throw; }

    [[nodiscard]] trading::strategy::StrategyDecision
    on_event(const trading::internal::NormalizedEvent& event) override {
        (void)event;
        if (should_throw_) {
            throw std::runtime_error{"strategy failed"};
        }
        return trading::strategy::StrategyDecision{.intents = intents_};
    }

  private:
    std::vector<trading::internal::OrderIntent> intents_;
    bool should_throw_{false};
};

class CountingShardEventHandler final : public trading::shards::IShardEventHandler {
  public:
    explicit CountingShardEventHandler(std::atomic<std::uint64_t>& counter) : counter_(counter) {}

    [[nodiscard]] bool on_event(const trading::internal::NormalizedEvent& event) override {
        (void)event;
        counter_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

  private:
    std::atomic<std::uint64_t>& counter_;
};

TEST(TradingCoreTest, StartupPayloadHasExpectedFields) {
    const auto startup = trading::engine::build_trader_startup_payload("test");
    const auto startup_json = nlohmann::json::parse(startup);

    EXPECT_EQ(startup_json.at("service"), "trading_engine");
    EXPECT_EQ(startup_json.at("status"), "ok");
    EXPECT_EQ(startup_json.at("mode"), "test");
}

TEST(TraderConfigTest, ParsesDropFileOverrides) {
    constexpr std::string_view kConfig = R"({
        "mode": "paper",
        "kalshi": {
            "endpoint": "wss://api.elections.kalshi.com/trade-api/ws/v2",
            "channels": ["orderbook_delta", "trade"],
            "credentials": {
                "key_id_env": "MY_KEY_ENV",
                "private_key_pem_env": "MY_PEM_ENV"
            }
        },
        "pipeline": {
            "source": "kalshi-live",
            "exchange": "kalshi",
            "frame_pool_capacity": 2048,
            "frame_queue_capacity": 2048,
            "shard_count": 8,
            "per_shard_queue_capacity": 512,
            "shard_idle_sleep_ms": 0
        },
        "market_universe": {
            "tickers": ["KXBTC-YES", "KXETH-YES"]
        },
        "risk": {
            "shard": {
                "max_open_orders_per_market": 5,
                "max_working_qty_per_market": 100,
                "max_abs_net_position_per_market": 25
            },
            "oms_global": {
                "max_active_orders_global": 50,
                "max_active_orders_per_market": 10,
                "max_outstanding_qty_global": 1000,
                "max_outstanding_qty_per_market": 100
            },
            "oms_portfolio": {
                "max_abs_net_position_per_market": 50,
                "max_abs_position_gross_global": 200,
                "min_realized_pnl_ticks": -500,
                "enforce_realized_pnl_floor": true
            }
        },
        "strategy": {
            "mode": "paper-trade-probe",
            "client_order_id_prefix": "probe",
            "side": "buy",
            "qty_lots": 2,
            "max_orders_per_shard": 3,
            "limit_price_ticks_override": 4321
        },
        "paper_oms": {
            "auto_fill_on_place": true,
            "fill_parts": 4,
            "place_reject_bps": 250,
            "ack_delay_ms": 7,
            "fill_delay_ms": 9,
            "fill_interval_ms": 11,
            "reject_delay_ms": 13
        },
        "runtime": {
            "execution_mode": "paper",
            "pump_batch_size": 256,
            "pump_idle_sleep_ms": 2,
            "stats_log_interval_ms": 5
        }
    })";

    const auto loaded = trading::config::load_trader_config_from_json(kConfig);
    ASSERT_TRUE(loaded.ok) << loaded.error;

    EXPECT_EQ(loaded.config.mode, "paper");
    EXPECT_EQ(loaded.config.kalshi.channels.size(), 2U);
    EXPECT_EQ(loaded.config.kalshi.channels[0], "orderbook_delta");
    EXPECT_EQ(loaded.config.kalshi.credentials.key_id_env, "MY_KEY_ENV");
    EXPECT_EQ(loaded.config.kalshi.credentials.private_key_pem_env, "MY_PEM_ENV");
    EXPECT_EQ(loaded.config.pipeline.source, "kalshi-live");
    EXPECT_EQ(loaded.config.pipeline.decode_exchange, trading::decode::ExchangeId::kKalshi);
    EXPECT_EQ(loaded.config.pipeline.router_exchange, trading::internal::ExchangeId::kKalshi);
    EXPECT_EQ(loaded.config.pipeline.frame_pool_capacity, 2048U);
    EXPECT_EQ(loaded.config.pipeline.shard_count, 8U);
    ASSERT_EQ(loaded.config.market_universe.tickers.size(), 2U);
    EXPECT_EQ(loaded.config.market_universe.tickers[0], "KXBTC-YES");
    EXPECT_EQ(loaded.config.market_universe.tickers[1], "KXETH-YES");
    EXPECT_EQ(loaded.config.risk.shard.max_open_orders_per_market, 5U);
    EXPECT_EQ(loaded.config.risk.shard.max_working_qty_per_market, 100);
    EXPECT_EQ(loaded.config.risk.shard.max_abs_net_position_per_market, 25);
    EXPECT_EQ(loaded.config.risk.oms_global.max_active_orders_global, 50U);
    EXPECT_EQ(loaded.config.risk.oms_global.max_active_orders_per_market, 10U);
    EXPECT_EQ(loaded.config.risk.oms_global.max_outstanding_qty_global, 1000);
    EXPECT_EQ(loaded.config.risk.oms_global.max_outstanding_qty_per_market, 100);
    EXPECT_EQ(loaded.config.risk.oms_portfolio.max_abs_net_position_per_market, 50);
    EXPECT_EQ(loaded.config.risk.oms_portfolio.max_abs_position_gross_global, 200);
    EXPECT_EQ(loaded.config.risk.oms_portfolio.min_realized_pnl_ticks, -500);
    EXPECT_TRUE(loaded.config.risk.oms_portfolio.enforce_realized_pnl_floor);
    EXPECT_EQ(loaded.config.strategy.mode, trading::config::TraderStrategyMode::kPaperTradeProbe);
    EXPECT_EQ(loaded.config.strategy.client_order_id_prefix, "probe");
    EXPECT_EQ(loaded.config.strategy.side, trading::internal::Side::kBuy);
    EXPECT_EQ(loaded.config.strategy.qty_lots, 2);
    EXPECT_EQ(loaded.config.strategy.max_orders_per_shard, 3U);
    EXPECT_EQ(loaded.config.strategy.limit_price_ticks_override.value_or(0), 4321);
    EXPECT_TRUE(loaded.config.paper_oms.auto_fill_on_place);
    EXPECT_EQ(loaded.config.paper_oms.fill_parts, 4U);
    EXPECT_EQ(loaded.config.paper_oms.place_reject_bps, 250U);
    EXPECT_EQ(loaded.config.paper_oms.ack_delay, std::chrono::milliseconds{7});
    EXPECT_EQ(loaded.config.paper_oms.fill_delay, std::chrono::milliseconds{9});
    EXPECT_EQ(loaded.config.paper_oms.fill_interval, std::chrono::milliseconds{11});
    EXPECT_EQ(loaded.config.paper_oms.reject_delay, std::chrono::milliseconds{13});
    EXPECT_EQ(loaded.config.execution_mode, trading::config::TraderExecutionMode::kPaper);
    EXPECT_EQ(loaded.config.pump_batch_size, 256U);
    EXPECT_EQ(loaded.config.pump_idle_sleep, std::chrono::milliseconds{2});
    EXPECT_EQ(loaded.config.stats_log_interval, std::chrono::milliseconds{5});
}

TEST(TraderConfigTest, RejectsUnknownExchange) {
    constexpr std::string_view kConfig = R"({
        "pipeline": {
            "exchange": "unknown-x"
        }
    })";

    const auto loaded = trading::config::load_trader_config_from_json(kConfig);
    EXPECT_FALSE(loaded.ok);
    EXPECT_NE(loaded.error.find("pipeline.exchange"), std::string::npos);
}

TEST(TraderConfigTest, RejectsUnknownExecutionMode) {
    constexpr std::string_view kConfig = R"({
        "runtime": {
            "execution_mode": "turbo-live"
        }
    })";

    const auto loaded = trading::config::load_trader_config_from_json(kConfig);
    EXPECT_FALSE(loaded.ok);
    EXPECT_NE(loaded.error.find("runtime.execution_mode"), std::string::npos);
}

TEST(TraderConfigTest, RejectsUnknownStrategyMode) {
    constexpr std::string_view kConfig = R"({
        "strategy": {
            "mode": "signal-factory-v99"
        }
    })";

    const auto loaded = trading::config::load_trader_config_from_json(kConfig);
    EXPECT_FALSE(loaded.ok);
    EXPECT_NE(loaded.error.find("strategy.mode"), std::string::npos);
}

TEST(TraderConfigTest, RejectsInvalidRiskConfig) {
    constexpr std::string_view kConfig = R"({
        "risk": {
            "shard": {
                "max_abs_net_position_per_market": -1
            }
        }
    })";

    const auto loaded = trading::config::load_trader_config_from_json(kConfig);
    EXPECT_FALSE(loaded.ok);
    EXPECT_NE(loaded.error.find("risk.shard.max_abs_net_position_per_market"), std::string::npos);
}

TEST(TraderConfigTest, RejectsOutOfRangePaperOmsRejectRate) {
    constexpr std::string_view kConfig = R"({
        "paper_oms": {
            "place_reject_bps": 10001
        }
    })";

    const auto loaded = trading::config::load_trader_config_from_json(kConfig);
    EXPECT_FALSE(loaded.ok);
    EXPECT_NE(loaded.error.find("paper_oms.place_reject_bps"), std::string::npos);
}

TEST(KalshiOmsAdapterTest, BuildsPlaceRequestPayload) {
    constexpr std::string_view kClientOrderId = "cl-100";
    constexpr std::string_view kTicker = "KXBTC-YES";
    constexpr trading::internal::QtyLots kQty = 7;
    constexpr trading::internal::PriceTicks kPrice = 42;

    const trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    const trading::internal::OrderIntent intent{
        .exchange = trading::internal::ExchangeId::kKalshi,
        .action = trading::internal::OmsAction::kPlace,
        .client_order_id = std::string{kClientOrderId},
        .market_ticker = std::string{kTicker},
        .side = trading::internal::Side::kBuy,
        .qty_lots = kQty,
        .limit_price_ticks = kPrice,
        .time_in_force = trading::internal::OmsTimeInForce::kGtc,
    };

    const auto request_payload = adapter.build_request(intent);
    const auto request_json = nlohmann::json::parse(request_payload);
    EXPECT_EQ(request_json.at("action"), "place_order");
    EXPECT_EQ(request_json.at("client_order_id"), kClientOrderId);
    EXPECT_EQ(request_json.at("market_ticker"), kTicker);
    EXPECT_EQ(request_json.at("side"), "buy");
    EXPECT_EQ(request_json.at("qty"), kQty);
    EXPECT_EQ(request_json.at("limit_price"), kPrice);
    EXPECT_EQ(request_json.at("time_in_force"), "gtc");
}

TEST(KalshiOmsAdapterTest, BuildsCancelRequestPayload) {
    constexpr std::string_view kClientOrderId = "cancel-1";
    constexpr std::string_view kTargetClientOrderId = "open-1";

    const trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    const trading::internal::OrderIntent intent{
        .exchange = trading::internal::ExchangeId::kKalshi,
        .action = trading::internal::OmsAction::kCancel,
        .client_order_id = std::string{kClientOrderId},
        .target_client_order_id = std::string{kTargetClientOrderId},
    };

    const auto request_payload = adapter.build_request(intent);
    const auto request_json = nlohmann::json::parse(request_payload);
    EXPECT_EQ(request_json.at("action"), "cancel_order");
    EXPECT_EQ(request_json.at("client_order_id"), kClientOrderId);
    EXPECT_EQ(request_json.at("target_client_order_id"), kTargetClientOrderId);
}

TEST(KalshiOmsAdapterTest, ParsesAckUpdate) {
    constexpr std::string_view kAckPayload = R"({
        "type": "order_ack",
        "recv_ts_ns": 17,
        "msg": {
            "client_order_id": "cl-101",
            "exchange_order_id": "ex-200",
            "market_ticker": "KXBTC-YES",
            "accepted_qty": 5
        }
    })";

    const trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    const auto ack = adapter.parse_update(kAckPayload);
    const auto& ack_update = require_order_update(ack);
    EXPECT_EQ(ack_update.status, trading::internal::OmsOrderStatus::kAccepted);
    const auto* ack_data = std::get_if<trading::internal::OrderAck>(&ack_update.data);
    ASSERT_NE(ack_data, nullptr);
    EXPECT_EQ(ack_data->accepted_qty_lots, 5);
}

TEST(KalshiOmsAdapterTest, ParsesRejectUpdate) {
    constexpr std::string_view kRejectPayload = R"({
        "type": "order_reject",
        "recv_ts_ns": 19,
        "msg": {
            "client_order_id": "cl-102",
            "exchange_order_id": "ex-201",
            "market_ticker": "KXBTC-YES",
            "reason_code": "invalid_qty",
            "reason_message": "qty must be positive"
        }
    })";

    const trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    const auto reject = adapter.parse_update(kRejectPayload);
    const auto& reject_update = require_order_update(reject);
    EXPECT_EQ(reject_update.status, trading::internal::OmsOrderStatus::kRejected);
    const auto* reject_data = std::get_if<trading::internal::OrderReject>(&reject_update.data);
    ASSERT_NE(reject_data, nullptr);
    EXPECT_EQ(reject_data->reason_code, "invalid_qty");
}

TEST(KalshiOmsAdapterTest, ParsesFillUpdate) {
    constexpr std::string_view kFillPayload = R"({
        "type": "fill",
        "recv_ts_ns": 23,
        "msg": {
            "client_order_id": "cl-103",
            "exchange_order_id": "ex-202",
            "market_ticker": "KXBTC-YES",
            "fill_qty": 3,
            "fill_price": 41
        }
    })";

    const trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    const auto fill = adapter.parse_update(kFillPayload);
    const auto& fill_update = require_order_update(fill);
    EXPECT_EQ(fill_update.status, trading::internal::OmsOrderStatus::kFilled);
    const auto* fill_data = std::get_if<trading::internal::OrderFill>(&fill_update.data);
    ASSERT_NE(fill_data, nullptr);
    EXPECT_EQ(fill_data->fill_qty_lots, 3);
    EXPECT_EQ(fill_data->fill_price_ticks, 4100);
    EXPECT_EQ(fill_data->side, trading::internal::Side::kUnknown);
}

TEST(KalshiOmsAdapterTest, ParsesFillUpdateWithSide) {
    constexpr std::string_view kFillPayload = R"({
        "type": "fill",
        "recv_ts_ns": 24,
        "msg": {
            "client_order_id": "cl-104",
            "exchange_order_id": "ex-203",
            "market_ticker": "KXBTC-YES",
            "fill_qty": 2,
            "fill_price": 49,
            "side": "sell"
        }
    })";

    const trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    const auto fill = adapter.parse_update(kFillPayload);
    const auto& fill_update = require_order_update(fill);
    const auto* fill_data = std::get_if<trading::internal::OrderFill>(&fill_update.data);
    ASSERT_NE(fill_data, nullptr);
    EXPECT_EQ(fill_data->side, trading::internal::Side::kSell);
}

TEST(KalshiOmsAdapterTest, ParsesFillUpdateWithDollarPricePrecision) {
    constexpr std::string_view kFillPayload = R"({
        "type": "fill",
        "recv_ts_ns": 25,
        "msg": {
            "client_order_id": "cl-105",
            "exchange_order_id": "ex-204",
            "market_ticker": "KXBTC-YES",
            "fill_qty": 1,
            "fill_price_dollars": "0.5790"
        }
    })";

    const trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    const auto fill = adapter.parse_update(kFillPayload);
    const auto& fill_update = require_order_update(fill);
    const auto* fill_data = std::get_if<trading::internal::OrderFill>(&fill_update.data);
    ASSERT_NE(fill_data, nullptr);
    EXPECT_EQ(fill_data->fill_price_ticks, 5790);
}

TEST(KalshiOmsAdapterTest, RejectsUnsupportedUpdateType) {
    constexpr std::string_view kPayload = R"({
        "type": "heartbeat",
        "msg": {}
    })";

    const trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    const auto parsed = adapter.parse_update(kPayload);
    EXPECT_FALSE(parsed.ok());
    EXPECT_EQ(parsed.error(), trading::oms::ParseError::kUnsupportedMessageType);
}

TEST(GlobalRiskGateTest, RejectsWhenGlobalActiveOrderLimitExceeded) {
    const trading::oms::GlobalRiskGate gate{
        trading::oms::GlobalRiskConfig{
            .max_active_orders_global = 2,
        },
    };

    const trading::oms::GlobalRiskDecision decision = gate.evaluate(
        trading::internal::OrderIntent{
            .exchange = trading::internal::ExchangeId::kKalshi,
            .action = trading::internal::OmsAction::kPlace,
            .client_order_id = "cl-risk-1",
            .market_ticker = "KXBTC-YES",
            .qty_lots = 1,
        },
        trading::oms::GlobalRiskSnapshot{
            .active_orders_global = 2,
        });

    EXPECT_FALSE(decision.allow);
    EXPECT_EQ(decision.code, trading::oms::GlobalRiskRejectCode::kActiveOrdersGlobalLimit);
}

TEST(GlobalRiskGateTest, AllowsCancelEvenWhenLimitsWouldRejectPlace) {
    const trading::oms::GlobalRiskGate gate{
        trading::oms::GlobalRiskConfig{
            .max_active_orders_global = 1,
            .max_outstanding_qty_global = 1,
        },
    };

    const trading::oms::GlobalRiskDecision decision = gate.evaluate(
        trading::internal::OrderIntent{
            .exchange = trading::internal::ExchangeId::kKalshi,
            .action = trading::internal::OmsAction::kCancel,
            .client_order_id = "cancel-risk-1",
        },
        trading::oms::GlobalRiskSnapshot{
            .active_orders_global = 999,
            .outstanding_qty_global = 999,
        });

    EXPECT_TRUE(decision.allow);
    EXPECT_EQ(decision.code, trading::oms::GlobalRiskRejectCode::kNone);
}

TEST(PortfolioRiskGateTest, RejectsWhenMarketAbsPositionLimitExceeded) {
    const trading::oms::PortfolioRiskGate gate{
        trading::oms::PortfolioRiskConfig{
            .max_abs_net_position_per_market = 10,
        },
    };

    const auto decision = gate.evaluate(
        trading::internal::OrderIntent{
            .exchange = trading::internal::ExchangeId::kKalshi,
            .action = trading::internal::OmsAction::kPlace,
            .side = trading::internal::Side::kBuy,
            .qty_lots = 3,
        },
        trading::oms::PortfolioRiskSnapshot{
            .net_position_market = 9,
            .gross_position_global = 20,
        });

    EXPECT_FALSE(decision.allow);
    EXPECT_EQ(decision.code, trading::oms::PortfolioRiskRejectCode::kAbsNetPositionMarketLimit);
}

TEST(PortfolioRiskGateTest, RejectsWhenGrossGlobalLimitExceeded) {
    const trading::oms::PortfolioRiskGate gate{
        trading::oms::PortfolioRiskConfig{
            .max_abs_position_gross_global = 50,
        },
    };

    const auto decision = gate.evaluate(
        trading::internal::OrderIntent{
            .exchange = trading::internal::ExchangeId::kKalshi,
            .action = trading::internal::OmsAction::kPlace,
            .side = trading::internal::Side::kBuy,
            .qty_lots = 4,
        },
        trading::oms::PortfolioRiskSnapshot{
            .net_position_market = 10,
            .gross_position_global = 49,
        });

    EXPECT_FALSE(decision.allow);
    EXPECT_EQ(decision.code, trading::oms::PortfolioRiskRejectCode::kAbsPositionGrossGlobalLimit);
}

TEST(PortfolioRiskGateTest, RejectsWhenRealizedPnlFloorBreached) {
    const trading::oms::PortfolioRiskGate gate{
        trading::oms::PortfolioRiskConfig{
            .min_realized_pnl_ticks = -100,
            .enforce_realized_pnl_floor = true,
        },
    };

    const auto decision = gate.evaluate(
        trading::internal::OrderIntent{
            .exchange = trading::internal::ExchangeId::kKalshi,
            .action = trading::internal::OmsAction::kPlace,
            .side = trading::internal::Side::kSell,
            .qty_lots = 1,
        },
        trading::oms::PortfolioRiskSnapshot{
            .realized_pnl_ticks_total = -101,
        });

    EXPECT_FALSE(decision.allow);
    EXPECT_EQ(decision.code, trading::oms::PortfolioRiskRejectCode::kRealizedPnlFloor);
}

TEST(PortfolioRiskGateTest, AllowsCancelRegardlessOfLimits) {
    const trading::oms::PortfolioRiskGate gate{
        trading::oms::PortfolioRiskConfig{
            .max_abs_net_position_per_market = 1,
            .max_abs_position_gross_global = 1,
            .min_realized_pnl_ticks = -10,
            .enforce_realized_pnl_floor = true,
        },
    };

    const auto decision = gate.evaluate(
        trading::internal::OrderIntent{
            .exchange = trading::internal::ExchangeId::kKalshi,
            .action = trading::internal::OmsAction::kCancel,
            .side = trading::internal::Side::kUnknown,
            .qty_lots = 0,
        },
        trading::oms::PortfolioRiskSnapshot{
            .net_position_market = 100,
            .gross_position_global = 1'000,
            .realized_pnl_ticks_total = -1'000,
        });

    EXPECT_TRUE(decision.allow);
    EXPECT_EQ(decision.code, trading::oms::PortfolioRiskRejectCode::kNone);
}

TEST(PortfolioRiskGateTest, RejectsInvalidIntentSideOrQty) {
    const trading::oms::PortfolioRiskGate gate{};

    const auto decision = gate.evaluate(
        trading::internal::OrderIntent{
            .exchange = trading::internal::ExchangeId::kKalshi,
            .action = trading::internal::OmsAction::kPlace,
            .side = trading::internal::Side::kUnknown,
            .qty_lots = 3,
        },
        trading::oms::PortfolioRiskSnapshot{});

    EXPECT_FALSE(decision.allow);
    EXPECT_EQ(decision.code, trading::oms::PortfolioRiskRejectCode::kInvalidIntent);
}

TEST(ShardRiskGateTest, RejectsWhenAbsNetPositionLimitExceeded) {
    constexpr trading::internal::QtyLots kMaxAbsNetPositionPerMarket = 10;
    constexpr trading::internal::QtyLots kStartingNetPositionMarket = 9;
    constexpr trading::internal::QtyLots kIntentQtyLots = 2;

    const trading::strategy::ShardRiskGate gate{
        trading::strategy::ShardRiskConfig{
            .max_abs_net_position_per_market = kMaxAbsNetPositionPerMarket,
        },
    };

    const auto decision = gate.evaluate(
        trading::internal::OrderIntent{
            .exchange = trading::internal::ExchangeId::kKalshi,
            .action = trading::internal::OmsAction::kPlace,
            .market_ticker = "KXBTC-YES",
            .side = trading::internal::Side::kBuy,
            .qty_lots = kIntentQtyLots,
        },
        trading::strategy::ShardRiskSnapshot{
            .net_position_market = kStartingNetPositionMarket,
        });

    EXPECT_FALSE(decision.allow);
    EXPECT_EQ(decision.code, trading::strategy::ShardRiskRejectCode::kAbsNetPositionPerMarketLimit);
}

TEST(ShardRiskGateTest, AllowsCancelRegardlessOfLimits) {
    const trading::strategy::ShardRiskGate gate{
        trading::strategy::ShardRiskConfig{
            .max_open_orders_per_market = 1,
            .max_working_qty_per_market = 1,
            .max_abs_net_position_per_market = 1,
        },
    };

    const auto decision = gate.evaluate(
        trading::internal::OrderIntent{
            .exchange = trading::internal::ExchangeId::kKalshi,
            .action = trading::internal::OmsAction::kCancel,
            .market_ticker = "KXBTC-YES",
            .side = trading::internal::Side::kUnknown,
            .qty_lots = 0,
        },
        trading::strategy::ShardRiskSnapshot{
            .open_orders_market = 100,
            .working_qty_market = 1'000,
            .net_position_market = 1'000,
        });

    EXPECT_TRUE(decision.allow);
    EXPECT_EQ(decision.code, trading::strategy::ShardRiskRejectCode::kNone);
}

TEST(StrategyRunnerTest, SubmitsStrategyIntentsWhenRiskAllows) {
    constexpr trading::internal::TimestampNs kEventRecvTsNs = 777;
    constexpr trading::internal::QtyLots kIntentQtyLots = 2;
    constexpr trading::internal::QtyLots kMaxAbsNetPositionPerMarket = 10;

    StaticDecisionStrategy strategy{
        std::vector<trading::internal::OrderIntent>{
            trading::internal::OrderIntent{
                .action = trading::internal::OmsAction::kPlace,
                .client_order_id = "cl-strategy-1",
                .side = trading::internal::Side::kBuy,
                .qty_lots = kIntentQtyLots,
            },
        },
    };
    CollectingOrderIntentSink intent_sink;
    FixedShardRiskSnapshotProvider snapshot_provider;
    snapshot_provider.set_snapshot(trading::strategy::ShardRiskSnapshot{
        .open_orders_market = 0,
        .working_qty_market = 0,
        .net_position_market = 0,
    });
    trading::strategy::StrategyRunner runner{
        strategy,
        intent_sink,
        trading::strategy::StrategyRunnerConfig{
            .shard_risk =
                trading::strategy::ShardRiskConfig{
                    .max_abs_net_position_per_market = kMaxAbsNetPositionPerMarket,
                },
            .risk_snapshot_provider = &snapshot_provider,
        },
    };

    const trading::internal::NormalizedEvent event{
        .type = trading::internal::EventType::kDelta,
        .meta =
            trading::internal::EventMeta{
                .exchange = trading::internal::ExchangeId::kKalshi,
                .recv_ns = kEventRecvTsNs,
            },
        .market_ticker = "KXBTC-YES",
        .raw_sequence_id = std::nullopt,
        .data =
            trading::internal::DeltaData{
                .side = trading::internal::Side::kBuy,
                .price_ticks = 40,
                .delta_qty_lots = 1,
            },
        .raw_payload = "{}",
    };

    EXPECT_TRUE(runner.on_event(event));
    EXPECT_EQ(intent_sink.count(), 1U);

    const auto submitted_intent = intent_sink.last_intent();
    ASSERT_TRUE(submitted_intent.has_value());
    const auto submitted_intent_value =
        submitted_intent.value_or(trading::internal::OrderIntent{});
    EXPECT_EQ(submitted_intent_value.exchange, trading::internal::ExchangeId::kKalshi);
    EXPECT_EQ(submitted_intent_value.market_ticker, "KXBTC-YES");
    EXPECT_EQ(submitted_intent_value.intent_ts_ns, kEventRecvTsNs);

    const auto stats = runner.stats();
    EXPECT_EQ(stats.events_processed_count, 1U);
    EXPECT_EQ(stats.intents_emitted_count, 1U);
    EXPECT_EQ(stats.intents_submitted_count, 1U);
    EXPECT_EQ(stats.risk_reject_count, 0U);
    EXPECT_EQ(stats.sink_reject_count, 0U);
    EXPECT_EQ(stats.strategy_error_count, 0U);
}

TEST(StrategyRunnerTest, RejectsIntentWhenShardRiskTrips) {
    constexpr trading::internal::QtyLots kStartingNetPositionMarket = 9;
    constexpr trading::internal::QtyLots kMaxAbsNetPositionPerMarket = 10;

    StaticDecisionStrategy strategy{
        std::vector<trading::internal::OrderIntent>{
            trading::internal::OrderIntent{
                .exchange = trading::internal::ExchangeId::kKalshi,
                .action = trading::internal::OmsAction::kPlace,
                .client_order_id = "cl-strategy-risk-1",
                .market_ticker = "KXBTC-YES",
                .side = trading::internal::Side::kBuy,
                .qty_lots = 2,
            },
        },
    };
    CollectingOrderIntentSink intent_sink;
    FixedShardRiskSnapshotProvider snapshot_provider;
    snapshot_provider.set_snapshot(trading::strategy::ShardRiskSnapshot{
        .net_position_market = kStartingNetPositionMarket,
    });
    trading::strategy::StrategyRunner runner{
        strategy,
        intent_sink,
        trading::strategy::StrategyRunnerConfig{
            .shard_risk =
                trading::strategy::ShardRiskConfig{
                    .max_abs_net_position_per_market = kMaxAbsNetPositionPerMarket,
                },
            .risk_snapshot_provider = &snapshot_provider,
        },
    };

    const auto event = make_trade_event("KXBTC-YES", std::nullopt, 40, 1);
    EXPECT_TRUE(runner.on_event(event));
    EXPECT_EQ(intent_sink.count(), 0U);
    EXPECT_EQ(runner.stats().risk_reject_count, 1U);
    EXPECT_EQ(runner.last_error(), "shard risk reject: max_abs_net_position_per_market exceeded");
}

TEST(StrategyRunnerTest, TracksSinkRejectAndStrategyException) {
    StaticDecisionStrategy strategy{
        std::vector<trading::internal::OrderIntent>{
            trading::internal::OrderIntent{
                .exchange = trading::internal::ExchangeId::kKalshi,
                .action = trading::internal::OmsAction::kPlace,
                .client_order_id = "cl-strategy-sink-1",
                .market_ticker = "KXBTC-YES",
                .side = trading::internal::Side::kBuy,
                .qty_lots = 1,
            },
        },
    };
    CollectingOrderIntentSink intent_sink;
    intent_sink.set_fail_submit(true, "sink unavailable");
    trading::strategy::StrategyRunner runner{strategy, intent_sink};

    const auto event = make_trade_event("KXBTC-YES", std::nullopt, 40, 1);
    EXPECT_TRUE(runner.on_event(event));
    EXPECT_EQ(runner.stats().sink_reject_count, 1U);
    EXPECT_EQ(runner.last_error(), "sink unavailable");

    strategy.set_throw(true);
    EXPECT_FALSE(runner.on_event(event));
    EXPECT_EQ(runner.stats().strategy_error_count, 1U);
    EXPECT_EQ(runner.last_error(), "strategy failed");
}

TEST(PaperTradeProbeStrategyTest, EmitsSinglePlaceIntentFromFirstTradeByDefault) {
    trading::strategy::PaperTradeProbeStrategy strategy{
        trading::strategy::PaperTradeProbeStrategyConfig{
            .client_order_id_prefix = "probe",
            .side = trading::internal::Side::kBuy,
            .qty_lots = 2,
            .time_in_force = trading::internal::OmsTimeInForce::kGtc,
            .max_orders = 1,
            .limit_price_ticks_override = std::nullopt,
        },
        3U};

    const auto first_trade = make_trade_event("KXBTC-YES", 101U, 5789, 1);
    const auto first_decision = strategy.on_event(first_trade);
    ASSERT_EQ(first_decision.intents.size(), 1U);

    const auto& first_intent = first_decision.intents.front();
    EXPECT_EQ(first_intent.action, trading::internal::OmsAction::kPlace);
    EXPECT_EQ(first_intent.side, trading::internal::Side::kBuy);
    EXPECT_EQ(first_intent.qty_lots, 2);
    EXPECT_EQ(first_intent.limit_price_ticks.value_or(0), 5789);
    EXPECT_EQ(first_intent.time_in_force, trading::internal::OmsTimeInForce::kGtc);
    EXPECT_NE(first_intent.client_order_id.find("probe-s3-n0"), std::string::npos);

    const auto second_trade = make_trade_event("KXBTC-YES", 102U, 6000, 1);
    const auto second_decision = strategy.on_event(second_trade);
    EXPECT_TRUE(second_decision.intents.empty());
    EXPECT_EQ(strategy.emitted_order_count(), 1U);
}

TEST(PaperTradeProbeStrategyTest, UsesOverridePriceAndSkipsNonTradeEvents) {
    trading::strategy::PaperTradeProbeStrategy strategy{
        trading::strategy::PaperTradeProbeStrategyConfig{
            .client_order_id_prefix = "override",
            .side = trading::internal::Side::kSell,
            .qty_lots = 1,
            .time_in_force = trading::internal::OmsTimeInForce::kGtc,
            .max_orders = 2,
            .limit_price_ticks_override = 4321,
        },
        0U};

    const auto delta_event =
        make_delta_event("KXBTC-YES", 201U, trading::internal::Side::kBid, 5000, 1);
    const auto delta_decision = strategy.on_event(delta_event);
    EXPECT_TRUE(delta_decision.intents.empty());

    const auto first_trade = make_trade_event("KXBTC-YES", 202U, 5000, 1);
    const auto second_trade = make_trade_event("KXBTC-YES", 203U, 7000, 1);
    const auto first_decision = strategy.on_event(first_trade);
    const auto second_decision = strategy.on_event(second_trade);
    ASSERT_EQ(first_decision.intents.size(), 1U);
    ASSERT_EQ(second_decision.intents.size(), 1U);
    EXPECT_EQ(first_decision.intents.front().limit_price_ticks.value_or(0), 4321);
    EXPECT_EQ(second_decision.intents.front().limit_price_ticks.value_or(0), 4321);
    EXPECT_EQ(strategy.emitted_order_count(), 2U);
}

TEST(StrategyEventHandlerTest, DelegatesToRunnerAndIntentSink) {
    StaticDecisionStrategy strategy{
        std::vector<trading::internal::OrderIntent>{
            trading::internal::OrderIntent{
                .action = trading::internal::OmsAction::kPlace,
                .client_order_id = "cl-handler-1",
                .side = trading::internal::Side::kBuy,
                .qty_lots = 1,
            },
        },
    };
    CollectingOrderIntentSink intent_sink;
    trading::strategy::StrategyEventHandler handler{strategy, intent_sink};

    const auto event = make_trade_event("KXBTC-YES", std::nullopt, 42, 1);
    EXPECT_TRUE(handler.on_event(event));
    EXPECT_EQ(intent_sink.count(), 1U);
    EXPECT_EQ(handler.stats().intents_submitted_count, 1U);
}

TEST(LedgerShardRiskSnapshotProviderTest, ReturnsNetPositionFromPortfolioSnapshot) {
    trading::oms::PositionLedger ledger;
    ASSERT_TRUE(ledger.on_order_update(make_fill_update(
        trading::internal::ExchangeId::kKalshi, "KXBTC-YES", "cl-ledger-1",
        trading::internal::Side::kBuy, 7, 41, 1)));
    ASSERT_TRUE(ledger.on_order_update(make_fill_update(
        trading::internal::ExchangeId::kKalshi, "KXBTC-YES", "cl-ledger-2",
        trading::internal::Side::kSell, 2, 43, 2)));

    trading::strategy::LedgerShardRiskSnapshotProvider provider{ledger};
    const auto snapshot =
        provider.snapshot_for(trading::internal::ExchangeId::kKalshi, "KXBTC-YES");
    EXPECT_EQ(snapshot.net_position_market, 5);
    EXPECT_EQ(snapshot.open_orders_market, 0U);
    EXPECT_EQ(snapshot.working_qty_market, 0);
}

TEST(MarketFilterEventHandlerTest, FiltersEventsByConfiguredMarkets) {
    std::atomic<std::uint64_t> handled_count{0};
    auto delegate = std::make_unique<CountingShardEventHandler>(handled_count);
    trading::strategy::MarketFilterEventHandler handler{
        std::unordered_set<std::string>{"KXBTC-YES"},
        std::move(delegate),
    };

    EXPECT_TRUE(handler.on_event(make_trade_event("KXETH-YES", std::nullopt, 40, 1)));
    EXPECT_TRUE(handler.on_event(make_trade_event("KXBTC-YES", std::nullopt, 41, 1)));
    EXPECT_EQ(handled_count.load(std::memory_order_relaxed), 1U);
}

TEST(OrderManagerIntentSinkTest, SubmitsIntentIntoOrderManager) {
    trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    FakeOmsTransport transport;
    CollectingOrderEventSink event_sink;
    trading::oms::OrderManager manager{
        adapter,
        transport,
        event_sink,
        trading::oms::OrderManagerConfig{
            .transport = trading::oms::OrderTransportConfig{
                .endpoint = "wss://example.test/orders",
            },
        },
    };

    ASSERT_TRUE(manager.start());
    trading::strategy::OrderManagerIntentSink sink{manager};

    EXPECT_TRUE(sink.submit_intent(trading::internal::OrderIntent{
        .exchange = trading::internal::ExchangeId::kKalshi,
        .action = trading::internal::OmsAction::kPlace,
        .client_order_id = "cl-strategy-oms-1",
        .market_ticker = "KXBTC-YES",
        .side = trading::internal::Side::kBuy,
        .qty_lots = 2,
        .limit_price_ticks = 45,
    }));

    EXPECT_TRUE(wait_until([&transport] { return transport.sent_count() == 1U; },
                           std::chrono::milliseconds{50}));
    EXPECT_TRUE(sink.last_error().empty());
    manager.stop();
}

TEST(OrderManagerIntentSinkTest, SurfacesOrderManagerErrorOnReject) {
    trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    FakeOmsTransport transport;
    CollectingOrderEventSink event_sink;
    trading::oms::OrderManager manager{
        adapter,
        transport,
        event_sink,
        trading::oms::OrderManagerConfig{
            .transport = trading::oms::OrderTransportConfig{
                .endpoint = "wss://example.test/orders",
            },
        },
    };

    ASSERT_TRUE(manager.start());
    trading::strategy::OrderManagerIntentSink sink{manager};

    EXPECT_FALSE(sink.submit_intent(trading::internal::OrderIntent{
        .exchange = trading::internal::ExchangeId::kPolymarket,
        .action = trading::internal::OmsAction::kPlace,
        .client_order_id = "cl-strategy-oms-bad-1",
        .market_ticker = "POLY-YES",
        .side = trading::internal::Side::kBuy,
        .qty_lots = 1,
        .limit_price_ticks = 55,
    }));
    EXPECT_EQ(sink.last_error(), "Order intent is not supported by adapter");
    manager.stop();
}

TEST(OrderManagerTest, SendsIntentToTransport) {
    trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    FakeOmsTransport transport;
    CollectingOrderEventSink event_sink;
    trading::oms::OrderManager manager{
        adapter,
        transport,
        event_sink,
        trading::oms::OrderManagerConfig{
            .transport =
                trading::oms::OrderTransportConfig{
                    .endpoint = "wss://example.test/orders",
                },
            .loop_idle_sleep = std::chrono::milliseconds{1},
        },
    };
    ASSERT_TRUE(manager.start());

    const auto request_id = manager.submit(trading::internal::OrderIntent{
        .exchange = trading::internal::ExchangeId::kKalshi,
        .action = trading::internal::OmsAction::kPlace,
        .client_order_id = "cl-200",
        .market_ticker = "KXBTC-YES",
        .side = trading::internal::Side::kBuy,
        .qty_lots = 12,
        .limit_price_ticks = 55,
        .time_in_force = trading::internal::OmsTimeInForce::kGtc,
    });
    ASSERT_TRUE(request_id.has_value());

    ASSERT_TRUE(wait_until([&transport] { return transport.sent_count() == 1; },
                           std::chrono::milliseconds{100}));

    const auto sent_payloads = transport.sent_payloads();
    ASSERT_EQ(sent_payloads.size(), 1U);
    const auto sent_json = nlohmann::json::parse(sent_payloads.front());
    EXPECT_EQ(sent_json.at("action"), "place_order");
    EXPECT_EQ(sent_json.at("client_order_id"), "cl-200");

    manager.stop();
    const auto stats = manager.stats();
    EXPECT_EQ(stats.submitted_count, 1U);
    EXPECT_EQ(stats.sent_count, 1U);
    EXPECT_EQ(stats.send_failed_count, 0U);
    EXPECT_EQ(stats.receive_count, 0U);
}

TEST(OrderManagerTest, DispatchesParsedUpdateToSink) {
    constexpr std::string_view kInboundAck = R"({
        "type": "order_ack",
        "recv_ts_ns": 88,
        "msg": {
            "client_order_id": "cl-200",
            "exchange_order_id": "ex-200",
            "market_ticker": "KXBTC-YES",
            "accepted_qty": 12
        }
    })";

    trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    FakeOmsTransport transport;
    CollectingOrderEventSink event_sink;

    trading::oms::OrderManager manager{
        adapter,
        transport,
        event_sink,
        trading::oms::OrderManagerConfig{
            .transport =
                trading::oms::OrderTransportConfig{
                    .endpoint = "wss://example.test/orders",
                },
            .loop_idle_sleep = std::chrono::milliseconds{1},
        },
    };
    ASSERT_TRUE(manager.start());
    ASSERT_TRUE(manager
                    .submit(trading::internal::OrderIntent{
                        .exchange = trading::internal::ExchangeId::kKalshi,
                        .action = trading::internal::OmsAction::kPlace,
                        .client_order_id = "cl-200",
                        .market_ticker = "KXBTC-YES",
                        .side = trading::internal::Side::kBuy,
                        .qty_lots = 12,
                        .limit_price_ticks = 55,
                        .time_in_force = trading::internal::OmsTimeInForce::kGtc,
                    })
                    .has_value());
    transport.queue_inbound(std::string{kInboundAck});
    ASSERT_TRUE(wait_until([&event_sink] { return event_sink.update_count() == 1; },
                           std::chrono::milliseconds{100}));

    const auto update_value =
        event_sink.last_update().value_or(trading::internal::OrderStateUpdate{});
    EXPECT_EQ(update_value.status, trading::internal::OmsOrderStatus::kAccepted);
    EXPECT_EQ(update_value.client_order_id, "cl-200");

    manager.stop();
    const auto stats = manager.stats();
    EXPECT_EQ(stats.receive_count, 1U);
    EXPECT_EQ(stats.parse_failed_count, 0U);
}

TEST(OrderManagerTest, TracksInFlightOrderAcrossAcceptedThenFilledTransitions) {
    constexpr std::string_view kInboundAck = R"({
        "type": "order_ack",
        "recv_ts_ns": 101,
        "msg": {
            "client_order_id": "cl-300",
            "exchange_order_id": "ex-300",
            "market_ticker": "KXBTC-YES",
            "accepted_qty": 10
        }
    })";
    constexpr std::string_view kInboundFill = R"({
        "type": "fill",
        "recv_ts_ns": 102,
        "msg": {
            "client_order_id": "cl-300",
            "exchange_order_id": "ex-300",
            "market_ticker": "KXBTC-YES",
            "fill_qty": 10,
            "fill_price": 44
        }
    })";

    trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    FakeOmsTransport transport;
    CollectingOrderEventSink event_sink;
    trading::oms::OrderManager manager{
        adapter,
        transport,
        event_sink,
        trading::oms::OrderManagerConfig{
            .transport =
                trading::oms::OrderTransportConfig{
                    .endpoint = "wss://example.test/orders",
                },
            .loop_idle_sleep = std::chrono::milliseconds{1},
        },
    };
    ASSERT_TRUE(manager.start());

    const auto request_id = manager.submit(trading::internal::OrderIntent{
        .exchange = trading::internal::ExchangeId::kKalshi,
        .action = trading::internal::OmsAction::kPlace,
        .client_order_id = "cl-300",
        .market_ticker = "KXBTC-YES",
        .side = trading::internal::Side::kBuy,
        .qty_lots = 10,
        .limit_price_ticks = 45,
        .time_in_force = trading::internal::OmsTimeInForce::kGtc,
    });
    ASSERT_TRUE(request_id.has_value());
    ASSERT_TRUE(wait_until([&transport] { return transport.sent_count() == 1; },
                           std::chrono::milliseconds{100}));

    transport.queue_inbound(std::string{kInboundAck});
    transport.queue_inbound(std::string{kInboundFill});
    ASSERT_TRUE(wait_until([&event_sink] { return event_sink.update_count() == 2; },
                           std::chrono::milliseconds{100}));

    manager.stop();
    const auto snapshot = manager.in_flight_order("cl-300");
    ASSERT_TRUE(snapshot.has_value());
    const trading::oms::InFlightOrderSnapshot order =
        snapshot.value_or(trading::oms::InFlightOrderSnapshot{});
    EXPECT_EQ(order.status, trading::oms::InFlightStatus::kFilled);
    EXPECT_EQ(order.requested_qty_lots, 10);
    EXPECT_EQ(order.filled_qty_lots, 10);
    EXPECT_EQ(order.last_update_ts_ns, 102U);
    ASSERT_TRUE(order.exchange_order_id.has_value());
    EXPECT_EQ(order.exchange_order_id.value_or(""), "ex-300");

    const auto stats = manager.stats();
    EXPECT_EQ(stats.transition_applied_count, 2U);
    EXPECT_EQ(stats.transition_reject_count, 0U);
    EXPECT_EQ(stats.unknown_order_update_count, 0U);
    EXPECT_EQ(stats.update_drop_count, 0U);
    EXPECT_EQ(stats.tracked_order_count, 1U);
    EXPECT_EQ(stats.active_order_count, 0U);
}

TEST(OrderManagerTest, EnrichesFillSideFromTrackedIntent) {
    constexpr std::string_view kInboundFill = R"({
        "type": "fill",
        "recv_ts_ns": 210,
        "msg": {
            "client_order_id": "cl-side-1",
            "exchange_order_id": "ex-side-1",
            "market_ticker": "KXBTC-YES",
            "fill_qty": 5,
            "fill_price": 47
        }
    })";

    trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    FakeOmsTransport transport;
    CollectingOrderEventSink event_sink;
    trading::oms::OrderManager manager{
        adapter,
        transport,
        event_sink,
        trading::oms::OrderManagerConfig{
            .transport =
                trading::oms::OrderTransportConfig{
                    .endpoint = "wss://example.test/orders",
                },
            .loop_idle_sleep = std::chrono::milliseconds{1},
        },
    };
    ASSERT_TRUE(manager.start());
    ASSERT_TRUE(manager
                    .submit(trading::internal::OrderIntent{
                        .exchange = trading::internal::ExchangeId::kKalshi,
                        .action = trading::internal::OmsAction::kPlace,
                        .client_order_id = "cl-side-1",
                        .market_ticker = "KXBTC-YES",
                        .side = trading::internal::Side::kBuy,
                        .qty_lots = 5,
                        .limit_price_ticks = 47,
                        .time_in_force = trading::internal::OmsTimeInForce::kGtc,
                    })
                    .has_value());

    transport.queue_inbound(std::string{kInboundFill});
    ASSERT_TRUE(wait_until([&event_sink] { return event_sink.update_count() == 1; },
                           std::chrono::milliseconds{100}));

    const auto update = event_sink.last_update();
    if (!update.has_value()) {
        GTEST_FAIL() << "expected fill update";
        return;
    }
    const auto* fill = std::get_if<trading::internal::OrderFill>(&update.value().data);
    ASSERT_NE(fill, nullptr);
    EXPECT_EQ(fill->side, trading::internal::Side::kBuy);

    manager.stop();
}

TEST(OrderManagerTest, RejectsOutOfOrderTransitionFromFilledBackToAccepted) {
    constexpr std::string_view kInboundFill = R"({
        "type": "fill",
        "recv_ts_ns": 201,
        "msg": {
            "client_order_id": "cl-301",
            "exchange_order_id": "ex-301",
            "market_ticker": "KXBTC-YES",
            "fill_qty": 4,
            "fill_price": 47
        }
    })";
    constexpr std::string_view kInboundAckAfterFill = R"({
        "type": "order_ack",
        "recv_ts_ns": 202,
        "msg": {
            "client_order_id": "cl-301",
            "exchange_order_id": "ex-301",
            "market_ticker": "KXBTC-YES",
            "accepted_qty": 4
        }
    })";

    trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    FakeOmsTransport transport;
    CollectingOrderEventSink event_sink;
    trading::oms::OrderManager manager{
        adapter,
        transport,
        event_sink,
        trading::oms::OrderManagerConfig{
            .transport =
                trading::oms::OrderTransportConfig{
                    .endpoint = "wss://example.test/orders",
                },
            .loop_idle_sleep = std::chrono::milliseconds{1},
        },
    };
    ASSERT_TRUE(manager.start());
    ASSERT_TRUE(manager
                    .submit(trading::internal::OrderIntent{
                        .exchange = trading::internal::ExchangeId::kKalshi,
                        .action = trading::internal::OmsAction::kPlace,
                        .client_order_id = "cl-301",
                        .market_ticker = "KXBTC-YES",
                        .side = trading::internal::Side::kBuy,
                        .qty_lots = 4,
                        .limit_price_ticks = 48,
                        .time_in_force = trading::internal::OmsTimeInForce::kGtc,
                    })
                    .has_value());

    transport.queue_inbound(std::string{kInboundFill});
    transport.queue_inbound(std::string{kInboundAckAfterFill});
    ASSERT_TRUE(wait_until([&manager] { return manager.stats().receive_count == 2U; },
                           std::chrono::milliseconds{100}));

    manager.stop();

    EXPECT_EQ(event_sink.update_count(), 1U);
    const auto snapshot = manager.in_flight_order("cl-301");
    ASSERT_TRUE(snapshot.has_value());
    const trading::oms::InFlightOrderSnapshot order =
        snapshot.value_or(trading::oms::InFlightOrderSnapshot{});
    EXPECT_EQ(order.status, trading::oms::InFlightStatus::kFilled);

    const auto stats = manager.stats();
    EXPECT_EQ(stats.transition_applied_count, 1U);
    EXPECT_EQ(stats.transition_reject_count, 1U);
    EXPECT_EQ(stats.unknown_order_update_count, 0U);
    EXPECT_EQ(stats.update_drop_count, 1U);
}

TEST(OrderManagerTest, DropsParsedUpdateWhenOrderIsUnknown) {
    constexpr std::string_view kInboundAckUnknownOrder = R"({
        "type": "order_ack",
        "recv_ts_ns": 301,
        "msg": {
            "client_order_id": "cl-missing",
            "exchange_order_id": "ex-missing",
            "market_ticker": "KXBTC-YES",
            "accepted_qty": 3
        }
    })";

    trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    FakeOmsTransport transport;
    CollectingOrderEventSink event_sink;
    transport.queue_inbound(std::string{kInboundAckUnknownOrder});

    trading::oms::OrderManager manager{
        adapter,
        transport,
        event_sink,
        trading::oms::OrderManagerConfig{
            .transport =
                trading::oms::OrderTransportConfig{
                    .endpoint = "wss://example.test/orders",
                },
            .loop_idle_sleep = std::chrono::milliseconds{1},
        },
    };
    ASSERT_TRUE(manager.start());
    ASSERT_TRUE(wait_until([&manager] { return manager.stats().receive_count == 1U; },
                           std::chrono::milliseconds{100}));
    manager.stop();

    EXPECT_EQ(event_sink.update_count(), 0U);
    const auto stats = manager.stats();
    EXPECT_EQ(stats.receive_count, 1U);
    EXPECT_EQ(stats.parse_failed_count, 0U);
    EXPECT_EQ(stats.transition_applied_count, 0U);
    EXPECT_EQ(stats.transition_reject_count, 0U);
    EXPECT_EQ(stats.unknown_order_update_count, 1U);
    EXPECT_EQ(stats.update_drop_count, 1U);
    EXPECT_EQ(stats.tracked_order_count, 0U);
    EXPECT_EQ(stats.active_order_count, 0U);
}

TEST(OrderManagerTest, RejectsPlaceIntentThatReusesTrackedClientOrderId) {
    trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    FakeOmsTransport transport;
    CollectingOrderEventSink event_sink;
    trading::oms::OrderManager manager{
        adapter,
        transport,
        event_sink,
        trading::oms::OrderManagerConfig{
            .transport =
                trading::oms::OrderTransportConfig{
                    .endpoint = "wss://example.test/orders",
                },
            .loop_idle_sleep = std::chrono::milliseconds{1},
        },
    };
    ASSERT_TRUE(manager.start());

    ASSERT_TRUE(manager
                    .submit(trading::internal::OrderIntent{
                        .exchange = trading::internal::ExchangeId::kKalshi,
                        .action = trading::internal::OmsAction::kPlace,
                        .client_order_id = "cl-dup-1",
                        .market_ticker = "KXBTC-YES",
                        .side = trading::internal::Side::kBuy,
                        .qty_lots = 2,
                        .limit_price_ticks = 40,
                        .time_in_force = trading::internal::OmsTimeInForce::kGtc,
                    })
                    .has_value());
    EXPECT_FALSE(manager
                     .submit(trading::internal::OrderIntent{
                         .exchange = trading::internal::ExchangeId::kKalshi,
                         .action = trading::internal::OmsAction::kPlace,
                         .client_order_id = "cl-dup-1",
                         .market_ticker = "KXBTC-YES",
                         .side = trading::internal::Side::kBuy,
                         .qty_lots = 2,
                         .limit_price_ticks = 40,
                         .time_in_force = trading::internal::OmsTimeInForce::kGtc,
                     })
                     .has_value());

    manager.stop();
    const auto stats = manager.stats();
    EXPECT_EQ(stats.submitted_count, 1U);
    EXPECT_EQ(stats.policy_reject_count, 1U);
    EXPECT_EQ(stats.tracked_order_count, 1U);
    EXPECT_EQ(stats.active_order_count, 1U);
}

TEST(OrderManagerTest, RejectsCancelWhenTargetOrderIsTerminal) {
    constexpr std::string_view kInboundFill = R"({
        "type": "fill",
        "recv_ts_ns": 401,
        "msg": {
            "client_order_id": "cl-410",
            "exchange_order_id": "ex-410",
            "market_ticker": "KXBTC-YES",
            "fill_qty": 3,
            "fill_price": 49
        }
    })";

    trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    FakeOmsTransport transport;
    CollectingOrderEventSink event_sink;
    trading::oms::OrderManager manager{
        adapter,
        transport,
        event_sink,
        trading::oms::OrderManagerConfig{
            .transport =
                trading::oms::OrderTransportConfig{
                    .endpoint = "wss://example.test/orders",
                },
            .loop_idle_sleep = std::chrono::milliseconds{1},
        },
    };
    ASSERT_TRUE(manager.start());
    ASSERT_TRUE(manager
                    .submit(trading::internal::OrderIntent{
                        .exchange = trading::internal::ExchangeId::kKalshi,
                        .action = trading::internal::OmsAction::kPlace,
                        .client_order_id = "cl-410",
                        .market_ticker = "KXBTC-YES",
                        .side = trading::internal::Side::kBuy,
                        .qty_lots = 3,
                        .limit_price_ticks = 50,
                        .time_in_force = trading::internal::OmsTimeInForce::kGtc,
                    })
                    .has_value());

    transport.queue_inbound(std::string{kInboundFill});
    ASSERT_TRUE(wait_until([&event_sink] { return event_sink.update_count() == 1; },
                           std::chrono::milliseconds{100}));

    EXPECT_FALSE(manager
                     .submit(trading::internal::OrderIntent{
                         .exchange = trading::internal::ExchangeId::kKalshi,
                         .action = trading::internal::OmsAction::kCancel,
                         .client_order_id = "cancel-410",
                         .target_client_order_id = std::string{"cl-410"},
                     })
                     .has_value());

    manager.stop();
    const auto stats = manager.stats();
    EXPECT_EQ(stats.policy_reject_count, 1U);
    EXPECT_EQ(stats.submitted_count, 1U);
    EXPECT_EQ(stats.transition_applied_count, 1U);
}

TEST(OrderManagerTest, RejectsReplaceWhenTargetIdsResolveToDifferentOrders) {
    constexpr std::string_view kInboundAckA = R"({
        "type": "order_ack",
        "recv_ts_ns": 501,
        "msg": {
            "client_order_id": "cl-500",
            "exchange_order_id": "ex-500",
            "market_ticker": "KXBTC-YES",
            "accepted_qty": 4
        }
    })";
    constexpr std::string_view kInboundAckB = R"({
        "type": "order_ack",
        "recv_ts_ns": 502,
        "msg": {
            "client_order_id": "cl-501",
            "exchange_order_id": "ex-501",
            "market_ticker": "KXBTC-YES",
            "accepted_qty": 4
        }
    })";

    trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    FakeOmsTransport transport;
    CollectingOrderEventSink event_sink;
    trading::oms::OrderManager manager{
        adapter,
        transport,
        event_sink,
        trading::oms::OrderManagerConfig{
            .transport =
                trading::oms::OrderTransportConfig{
                    .endpoint = "wss://example.test/orders",
                },
            .loop_idle_sleep = std::chrono::milliseconds{1},
        },
    };
    ASSERT_TRUE(manager.start());
    ASSERT_TRUE(manager
                    .submit(trading::internal::OrderIntent{
                        .exchange = trading::internal::ExchangeId::kKalshi,
                        .action = trading::internal::OmsAction::kPlace,
                        .client_order_id = "cl-500",
                        .market_ticker = "KXBTC-YES",
                        .side = trading::internal::Side::kBuy,
                        .qty_lots = 4,
                        .limit_price_ticks = 48,
                        .time_in_force = trading::internal::OmsTimeInForce::kGtc,
                    })
                    .has_value());
    ASSERT_TRUE(manager
                    .submit(trading::internal::OrderIntent{
                        .exchange = trading::internal::ExchangeId::kKalshi,
                        .action = trading::internal::OmsAction::kPlace,
                        .client_order_id = "cl-501",
                        .market_ticker = "KXBTC-YES",
                        .side = trading::internal::Side::kBuy,
                        .qty_lots = 4,
                        .limit_price_ticks = 48,
                        .time_in_force = trading::internal::OmsTimeInForce::kGtc,
                    })
                    .has_value());

    transport.queue_inbound(std::string{kInboundAckA});
    transport.queue_inbound(std::string{kInboundAckB});
    ASSERT_TRUE(wait_until([&event_sink] { return event_sink.update_count() == 2; },
                           std::chrono::milliseconds{100}));

    EXPECT_FALSE(manager
                     .submit(trading::internal::OrderIntent{
                         .exchange = trading::internal::ExchangeId::kKalshi,
                         .action = trading::internal::OmsAction::kReplace,
                         .client_order_id = "cl-502",
                         .target_client_order_id = std::string{"cl-500"},
                         .target_exchange_order_id = std::string{"ex-501"},
                         .market_ticker = "KXBTC-YES",
                         .qty_lots = 3,
                         .limit_price_ticks = 47,
                     })
                     .has_value());

    manager.stop();
    const auto stats = manager.stats();
    EXPECT_EQ(stats.policy_reject_count, 1U);
    EXPECT_EQ(stats.submitted_count, 2U);
    EXPECT_EQ(stats.transition_applied_count, 2U);
}

TEST(OrderManagerTest, TracksReplaceChildOrderResolvedByExchangeTargetId) {
    constexpr std::string_view kInboundAckTarget = R"({
        "type": "order_ack",
        "recv_ts_ns": 601,
        "msg": {
            "client_order_id": "cl-600",
            "exchange_order_id": "ex-600",
            "market_ticker": "KXBTC-YES",
            "accepted_qty": 5
        }
    })";
    constexpr std::string_view kInboundAckReplace = R"({
        "type": "order_ack",
        "recv_ts_ns": 602,
        "msg": {
            "client_order_id": "cl-601",
            "exchange_order_id": "ex-601",
            "market_ticker": "KXBTC-YES",
            "accepted_qty": 3
        }
    })";

    trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    FakeOmsTransport transport;
    CollectingOrderEventSink event_sink;
    trading::oms::OrderManager manager{
        adapter,
        transport,
        event_sink,
        trading::oms::OrderManagerConfig{
            .transport =
                trading::oms::OrderTransportConfig{
                    .endpoint = "wss://example.test/orders",
                },
            .loop_idle_sleep = std::chrono::milliseconds{1},
        },
    };
    ASSERT_TRUE(manager.start());

    ASSERT_TRUE(manager
                    .submit(trading::internal::OrderIntent{
                        .exchange = trading::internal::ExchangeId::kKalshi,
                        .action = trading::internal::OmsAction::kPlace,
                        .client_order_id = "cl-600",
                        .market_ticker = "KXBTC-YES",
                        .side = trading::internal::Side::kBuy,
                        .qty_lots = 5,
                        .limit_price_ticks = 46,
                        .time_in_force = trading::internal::OmsTimeInForce::kGtc,
                    })
                    .has_value());
    transport.queue_inbound(std::string{kInboundAckTarget});
    ASSERT_TRUE(wait_until([&event_sink] { return event_sink.update_count() == 1; },
                           std::chrono::milliseconds{100}));

    ASSERT_TRUE(manager
                    .submit(trading::internal::OrderIntent{
                        .exchange = trading::internal::ExchangeId::kKalshi,
                        .action = trading::internal::OmsAction::kReplace,
                        .client_order_id = "cl-601",
                        .target_exchange_order_id = std::string{"ex-600"},
                        .market_ticker = "KXBTC-YES",
                        .qty_lots = 3,
                        .limit_price_ticks = 45,
                    })
                    .has_value());

    transport.queue_inbound(std::string{kInboundAckReplace});
    ASSERT_TRUE(wait_until([&event_sink] { return event_sink.update_count() == 2; },
                           std::chrono::milliseconds{100}));

    manager.stop();
    const auto child_snapshot = manager.in_flight_order("cl-601");
    ASSERT_TRUE(child_snapshot.has_value());
    const trading::oms::InFlightOrderSnapshot child =
        child_snapshot.value_or(trading::oms::InFlightOrderSnapshot{});
    EXPECT_EQ(child.last_action, trading::internal::OmsAction::kReplace);
    EXPECT_EQ(child.status, trading::oms::InFlightStatus::kAccepted);
    EXPECT_EQ(child.market_ticker, "KXBTC-YES");
    EXPECT_EQ(child.requested_qty_lots, 3);
    EXPECT_EQ(child.replace_target_client_order_id.value_or(""), "cl-600");

    const auto stats = manager.stats();
    EXPECT_EQ(stats.policy_reject_count, 0U);
    EXPECT_EQ(stats.unknown_order_update_count, 0U);
    EXPECT_EQ(stats.transition_applied_count, 2U);
    EXPECT_EQ(stats.update_drop_count, 0U);
    EXPECT_EQ(stats.tracked_order_count, 2U);
}

TEST(OrderManagerTest, EmitsRiskRejectAndSkipsSendWhenGlobalRiskTrips) {
    trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    FakeOmsTransport transport;
    CollectingOrderEventSink event_sink;
    trading::oms::OrderManager manager{
        adapter,
        transport,
        event_sink,
        trading::oms::OrderManagerConfig{
            .transport =
                trading::oms::OrderTransportConfig{
                    .endpoint = "wss://example.test/orders",
                },
            .global_risk =
                trading::oms::GlobalRiskConfig{
                    .max_active_orders_global = 1,
                },
            .loop_idle_sleep = std::chrono::milliseconds{1},
        },
    };
    ASSERT_TRUE(manager.start());

    ASSERT_TRUE(manager
                    .submit(trading::internal::OrderIntent{
                        .exchange = trading::internal::ExchangeId::kKalshi,
                        .action = trading::internal::OmsAction::kPlace,
                        .client_order_id = "cl-risk-send-1",
                        .market_ticker = "KXBTC-YES",
                        .side = trading::internal::Side::kBuy,
                        .qty_lots = 2,
                        .limit_price_ticks = 50,
                        .time_in_force = trading::internal::OmsTimeInForce::kGtc,
                    })
                    .has_value());
    ASSERT_TRUE(wait_until([&transport] { return transport.sent_count() == 1; },
                           std::chrono::milliseconds{100}));

    ASSERT_TRUE(manager
                    .submit(trading::internal::OrderIntent{
                        .exchange = trading::internal::ExchangeId::kKalshi,
                        .action = trading::internal::OmsAction::kPlace,
                        .client_order_id = "cl-risk-send-2",
                        .market_ticker = "KXBTC-YES",
                        .side = trading::internal::Side::kBuy,
                        .qty_lots = 2,
                        .limit_price_ticks = 49,
                        .time_in_force = trading::internal::OmsTimeInForce::kGtc,
                    })
                    .has_value());

    ASSERT_TRUE(wait_until([&manager] { return manager.stats().risk_reject_count == 1U; },
                           std::chrono::milliseconds{100}));
    ASSERT_TRUE(wait_until([&event_sink] { return event_sink.update_count() == 1U; },
                           std::chrono::milliseconds{100}));

    manager.stop();
    EXPECT_EQ(transport.sent_count(), 1U);

    const auto snapshot = manager.in_flight_order("cl-risk-send-2");
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot.value_or(trading::oms::InFlightOrderSnapshot{}).status,
              trading::oms::InFlightStatus::kRejected);

    const auto update_value =
        event_sink.last_update().value_or(trading::internal::OrderStateUpdate{});
    EXPECT_EQ(update_value.status, trading::internal::OmsOrderStatus::kRejected);
    const auto* reject_data = std::get_if<trading::internal::OrderReject>(&update_value.data);
    ASSERT_NE(reject_data, nullptr);
    EXPECT_EQ(reject_data->reason_code, "active_orders_global_limit");

    const auto stats = manager.stats();
    EXPECT_EQ(stats.submitted_count, 2U);
    EXPECT_EQ(stats.sent_count, 1U);
    EXPECT_EQ(stats.send_failed_count, 0U);
    EXPECT_EQ(stats.risk_reject_count, 1U);
    EXPECT_EQ(stats.transition_applied_count, 1U);
    EXPECT_EQ(stats.update_drop_count, 0U);
}

TEST(OrderManagerTest, CancelBypassesGlobalRiskLimits) {
    trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    FakeOmsTransport transport;
    CollectingOrderEventSink event_sink;
    trading::oms::OrderManager manager{
        adapter,
        transport,
        event_sink,
        trading::oms::OrderManagerConfig{
            .transport =
                trading::oms::OrderTransportConfig{
                    .endpoint = "wss://example.test/orders",
                },
            .global_risk =
                trading::oms::GlobalRiskConfig{
                    .max_active_orders_global = 1,
                },
            .loop_idle_sleep = std::chrono::milliseconds{1},
        },
    };
    ASSERT_TRUE(manager.start());

    ASSERT_TRUE(manager
                    .submit(trading::internal::OrderIntent{
                        .exchange = trading::internal::ExchangeId::kKalshi,
                        .action = trading::internal::OmsAction::kPlace,
                        .client_order_id = "cl-cancel-risk-1",
                        .market_ticker = "KXBTC-YES",
                        .side = trading::internal::Side::kBuy,
                        .qty_lots = 1,
                        .limit_price_ticks = 51,
                        .time_in_force = trading::internal::OmsTimeInForce::kGtc,
                    })
                    .has_value());
    ASSERT_TRUE(wait_until([&transport] { return transport.sent_count() == 1; },
                           std::chrono::milliseconds{100}));

    ASSERT_TRUE(manager
                    .submit(trading::internal::OrderIntent{
                        .exchange = trading::internal::ExchangeId::kKalshi,
                        .action = trading::internal::OmsAction::kCancel,
                        .client_order_id = "cancel-cancel-risk-1",
                        .target_client_order_id = std::string{"cl-cancel-risk-1"},
                    })
                    .has_value());
    ASSERT_TRUE(wait_until([&transport] { return transport.sent_count() == 2; },
                           std::chrono::milliseconds{100}));

    manager.stop();
    const auto stats = manager.stats();
    EXPECT_EQ(stats.submitted_count, 2U);
    EXPECT_EQ(stats.sent_count, 2U);
    EXPECT_EQ(stats.risk_reject_count, 0U);
}

TEST(OrderManagerTest, EmitsRiskRejectAndSkipsSendWhenPortfolioRiskTrips) {
    constexpr trading::internal::QtyLots kMaxAbsNetPositionPerMarket = 5;
    constexpr trading::internal::QtyLots kInitialNetPositionMarket = 5;
    constexpr trading::internal::QtyLots kInitialGrossPositionGlobal = 5;

    trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    FakeOmsTransport transport;
    CollectingOrderEventSink event_sink;
    FixedPortfolioRiskSnapshotProvider snapshot_provider;
    snapshot_provider.set_snapshot(trading::oms::PortfolioRiskSnapshot{
        .net_position_market = kInitialNetPositionMarket,
        .gross_position_global = kInitialGrossPositionGlobal,
        .realized_pnl_ticks_total = 0,
    });
    trading::oms::OrderManager manager{
        adapter,
        transport,
        event_sink,
        trading::oms::OrderManagerConfig{
            .transport =
                trading::oms::OrderTransportConfig{
                    .endpoint = "wss://example.test/orders",
                },
            .portfolio_risk =
                trading::oms::PortfolioRiskConfig{
                    .max_abs_net_position_per_market = kMaxAbsNetPositionPerMarket,
                },
            .portfolio_snapshot_provider = &snapshot_provider,
            .loop_idle_sleep = std::chrono::milliseconds{1},
        },
    };
    ASSERT_TRUE(manager.start());

    ASSERT_TRUE(manager
                    .submit(trading::internal::OrderIntent{
                        .exchange = trading::internal::ExchangeId::kKalshi,
                        .action = trading::internal::OmsAction::kPlace,
                        .client_order_id = "cl-portfolio-risk-1",
                        .market_ticker = "KXBTC-YES",
                        .side = trading::internal::Side::kBuy,
                        .qty_lots = 1,
                        .limit_price_ticks = 40,
                        .time_in_force = trading::internal::OmsTimeInForce::kGtc,
                    })
                    .has_value());

    ASSERT_TRUE(wait_until([&manager] { return manager.stats().portfolio_risk_reject_count == 1U; },
                           std::chrono::milliseconds{100}));
    ASSERT_TRUE(wait_until([&event_sink] { return event_sink.update_count() == 1U; },
                           std::chrono::milliseconds{100}));

    manager.stop();
    EXPECT_EQ(transport.sent_count(), 0U);

    const auto snapshot = manager.in_flight_order("cl-portfolio-risk-1");
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot.value_or(trading::oms::InFlightOrderSnapshot{}).status,
              trading::oms::InFlightStatus::kRejected);

    const auto update_value =
        event_sink.last_update().value_or(trading::internal::OrderStateUpdate{});
    EXPECT_EQ(update_value.status, trading::internal::OmsOrderStatus::kRejected);
    const auto* reject_data = std::get_if<trading::internal::OrderReject>(&update_value.data);
    ASSERT_NE(reject_data, nullptr);
    EXPECT_EQ(reject_data->reason_code, "abs_net_position_market_limit");

    const auto stats = manager.stats();
    EXPECT_EQ(stats.sent_count, 0U);
    EXPECT_EQ(stats.risk_reject_count, 0U);
    EXPECT_EQ(stats.portfolio_risk_reject_count, 1U);
}

TEST(OrderManagerTest, SendsIntentWhenPortfolioRiskAllows) {
    constexpr trading::internal::QtyLots kInitialNetPositionMarket = 2;
    constexpr trading::internal::QtyLots kInitialGrossPositionGlobal = 8;
    constexpr std::int64_t kRealizedPnlTicksTotal = 100;
    constexpr trading::internal::QtyLots kMaxAbsNetPositionPerMarket = 10;
    constexpr trading::internal::QtyLots kMaxAbsPositionGrossGlobal = 20;

    trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    FakeOmsTransport transport;
    CollectingOrderEventSink event_sink;
    FixedPortfolioRiskSnapshotProvider snapshot_provider;
    snapshot_provider.set_snapshot(trading::oms::PortfolioRiskSnapshot{
        .net_position_market = kInitialNetPositionMarket,
        .gross_position_global = kInitialGrossPositionGlobal,
        .realized_pnl_ticks_total = kRealizedPnlTicksTotal,
    });
    trading::oms::OrderManager manager{
        adapter,
        transport,
        event_sink,
        trading::oms::OrderManagerConfig{
            .transport =
                trading::oms::OrderTransportConfig{
                    .endpoint = "wss://example.test/orders",
                },
            .portfolio_risk =
                trading::oms::PortfolioRiskConfig{
                    .max_abs_net_position_per_market = kMaxAbsNetPositionPerMarket,
                    .max_abs_position_gross_global = kMaxAbsPositionGrossGlobal,
                },
            .portfolio_snapshot_provider = &snapshot_provider,
            .loop_idle_sleep = std::chrono::milliseconds{1},
        },
    };
    ASSERT_TRUE(manager.start());

    ASSERT_TRUE(manager
                    .submit(trading::internal::OrderIntent{
                        .exchange = trading::internal::ExchangeId::kKalshi,
                        .action = trading::internal::OmsAction::kPlace,
                        .client_order_id = "cl-portfolio-allow-1",
                        .market_ticker = "KXBTC-YES",
                        .side = trading::internal::Side::kBuy,
                        .qty_lots = 2,
                        .limit_price_ticks = 40,
                        .time_in_force = trading::internal::OmsTimeInForce::kGtc,
                    })
                    .has_value());
    ASSERT_TRUE(wait_until([&transport] { return transport.sent_count() == 1U; },
                           std::chrono::milliseconds{100}));

    manager.stop();
    const auto stats = manager.stats();
    EXPECT_EQ(stats.sent_count, 1U);
    EXPECT_EQ(stats.risk_reject_count, 0U);
    EXPECT_EQ(stats.portfolio_risk_reject_count, 0U);
}

TEST(OrderManagerTest, UsesPositionLedgerSnapshotProviderForPortfolioRisk) {
    constexpr trading::internal::QtyLots kMaxAbsNetPositionPerMarket = 4;

    trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    FakeOmsTransport transport;
    CollectingOrderEventSink event_sink;
    trading::oms::PositionLedger ledger;

    EXPECT_TRUE(ledger.on_order_update(make_fill_update(
        trading::internal::ExchangeId::kKalshi, "KXBTC-YES", "cl-seed-ledger-1",
        trading::internal::Side::kBuy, 4, 40, 500)));

    trading::oms::OrderManager manager{
        adapter,
        transport,
        event_sink,
        trading::oms::OrderManagerConfig{
            .transport =
                trading::oms::OrderTransportConfig{
                    .endpoint = "wss://example.test/orders",
                },
            .portfolio_risk =
                trading::oms::PortfolioRiskConfig{
                    .max_abs_net_position_per_market = kMaxAbsNetPositionPerMarket,
                },
            .portfolio_snapshot_provider = &ledger,
            .loop_idle_sleep = std::chrono::milliseconds{1},
        },
    };
    ASSERT_TRUE(manager.start());

    ASSERT_TRUE(manager
                    .submit(trading::internal::OrderIntent{
                        .exchange = trading::internal::ExchangeId::kKalshi,
                        .action = trading::internal::OmsAction::kPlace,
                        .client_order_id = "cl-portfolio-ledger-1",
                        .market_ticker = "KXBTC-YES",
                        .side = trading::internal::Side::kBuy,
                        .qty_lots = 1,
                        .limit_price_ticks = 40,
                        .time_in_force = trading::internal::OmsTimeInForce::kGtc,
                    })
                    .has_value());

    ASSERT_TRUE(wait_until([&manager] { return manager.stats().portfolio_risk_reject_count == 1U; },
                           std::chrono::milliseconds{100}));
    ASSERT_TRUE(wait_until([&event_sink] { return event_sink.update_count() == 1U; },
                           std::chrono::milliseconds{100}));

    manager.stop();
    EXPECT_EQ(transport.sent_count(), 0U);
}

TEST(OrderManagerTest, RejectsUnsupportedIntentBeforeQueueing) {
    trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    FakeOmsTransport transport;
    CollectingOrderEventSink event_sink;
    trading::oms::OrderManager manager{
        adapter,
        transport,
        event_sink,
        trading::oms::OrderManagerConfig{
            .transport =
                trading::oms::OrderTransportConfig{
                    .endpoint = "wss://example.test/orders",
                },
            .loop_idle_sleep = std::chrono::milliseconds{1},
        },
    };
    ASSERT_TRUE(manager.start());

    const auto request_id = manager.submit(trading::internal::OrderIntent{
        .exchange = trading::internal::ExchangeId::kPolymarket,
        .action = trading::internal::OmsAction::kPlace,
        .client_order_id = "cl-unsupported",
    });
    EXPECT_FALSE(request_id.has_value());

    manager.stop();
    const auto stats = manager.stats();
    EXPECT_EQ(stats.unsupported_intent_count, 1U);
    EXPECT_EQ(stats.submitted_count, 0U);
    EXPECT_EQ(stats.pending_intent_count, 0U);
}

TEST(OrderManagerTest, StartFailsWhenTransportConnectFails) {
    trading::adapters::exchanges::kalshi::OmsAdapter adapter;
    FakeOmsTransport transport;
    transport.set_connect_result(false);
    CollectingOrderEventSink event_sink;
    trading::oms::OrderManager manager{
        adapter,
        transport,
        event_sink,
        trading::oms::OrderManagerConfig{
            .transport =
                trading::oms::OrderTransportConfig{
                    .endpoint = "wss://example.test/orders",
                },
        },
    };

    EXPECT_FALSE(manager.start());
    EXPECT_FALSE(manager.running());
    EXPECT_EQ(manager.last_error(), "connect failed");
}

// NOLINTBEGIN(readability-function-cognitive-complexity)
TEST(PositionLedgerTest, TracksOpenPositionAndAveragePriceForSameDirectionFills) {
    trading::oms::PositionLedger ledger;

    EXPECT_TRUE(ledger.on_order_update(make_fill_update(
        trading::internal::ExchangeId::kKalshi, "KXBTC-YES", "cl-pos-1",
        trading::internal::Side::kBuy, 10, 40, 100)));
    EXPECT_TRUE(ledger.on_order_update(make_fill_update(
        trading::internal::ExchangeId::kKalshi, "KXBTC-YES", "cl-pos-2",
        trading::internal::Side::kBuy, 5, 55, 101)));

    const auto snapshot =
        ledger.market_position(trading::internal::ExchangeId::kKalshi, "KXBTC-YES");
    if (!snapshot.has_value()) {
        GTEST_FAIL() << "expected position snapshot";
        return;
    }
    const trading::oms::PositionSnapshot& position = *snapshot;
    EXPECT_EQ(position.net_qty_lots, 15);
    EXPECT_EQ(position.avg_open_price_ticks.value_or(0), 45);
    EXPECT_EQ(position.bought_qty_lots, 15);
    EXPECT_EQ(position.sold_qty_lots, 0);
    EXPECT_EQ(position.realized_pnl_ticks, 0);

    const auto stats = ledger.stats();
    EXPECT_EQ(stats.processed_fill_count, 2U);
    EXPECT_EQ(stats.open_position_count, 1U);
    EXPECT_EQ(stats.closed_position_count, 0U);
}

TEST(PositionLedgerTest, RealizesPnlOnCloseAndFlipTransitions) {
    trading::oms::PositionLedger ledger;

    EXPECT_TRUE(ledger.on_order_update(make_fill_update(
        trading::internal::ExchangeId::kKalshi, "KXBTC-YES", "cl-r1",
        trading::internal::Side::kBuy, 10, 40, 200)));
    EXPECT_TRUE(ledger.on_order_update(make_fill_update(
        trading::internal::ExchangeId::kKalshi, "KXBTC-YES", "cl-r2",
        trading::internal::Side::kSell, 4, 55, 201)));
    EXPECT_TRUE(ledger.on_order_update(make_fill_update(
        trading::internal::ExchangeId::kKalshi, "KXBTC-YES", "cl-r3",
        trading::internal::Side::kSell, 10, 30, 202)));
    EXPECT_TRUE(ledger.on_order_update(make_fill_update(
        trading::internal::ExchangeId::kKalshi, "KXBTC-YES", "cl-r4",
        trading::internal::Side::kBuy, 4, 20, 203)));

    const auto snapshot =
        ledger.market_position(trading::internal::ExchangeId::kKalshi, "KXBTC-YES");
    if (!snapshot.has_value()) {
        GTEST_FAIL() << "expected position snapshot";
        return;
    }
    const trading::oms::PositionSnapshot& position = *snapshot;
    EXPECT_EQ(position.net_qty_lots, 0);
    EXPECT_FALSE(position.avg_open_price_ticks.has_value());
    EXPECT_EQ(position.realized_pnl_ticks, 40);
    EXPECT_EQ(position.closed_qty_lots, 14);

    const auto stats = ledger.stats();
    EXPECT_EQ(stats.processed_fill_count, 4U);
    EXPECT_EQ(stats.realized_pnl_ticks_total, 40);
    EXPECT_EQ(stats.open_position_count, 0U);
    EXPECT_EQ(stats.closed_position_count, 1U);
}
// NOLINTEND(readability-function-cognitive-complexity)

TEST(PositionLedgerTest, RejectsUnknownSideOnFill) {
    trading::oms::PositionLedger ledger;

    EXPECT_FALSE(ledger.on_order_update(make_fill_update(
        trading::internal::ExchangeId::kKalshi, "KXBTC-YES", "cl-bad-side",
        trading::internal::Side::kUnknown, 1, 45, 300)));

    const auto stats = ledger.stats();
    EXPECT_EQ(stats.processed_fill_count, 0U);
    EXPECT_EQ(stats.rejected_fill_count, 1U);
    EXPECT_EQ(ledger.last_error(), "Fill update has unknown side");
}

TEST(PositionLedgerTest, IgnoresNonFillUpdates) {
    trading::oms::PositionLedger ledger;
    const trading::internal::OrderStateUpdate ack{
        .exchange = trading::internal::ExchangeId::kKalshi,
        .status = trading::internal::OmsOrderStatus::kAccepted,
        .client_order_id = "cl-ack-1",
        .exchange_order_id = std::nullopt,
        .market_ticker = "KXBTC-YES",
        .recv_ts_ns = 400,
        .data =
            trading::internal::OrderAck{
                .client_order_id = "cl-ack-1",
                .exchange_order_id = std::nullopt,
                .market_ticker = "KXBTC-YES",
                .accepted_qty_lots = 1,
            },
        .raw_payload = {},
    };

    EXPECT_TRUE(ledger.on_order_update(ack));
    const auto stats = ledger.stats();
    EXPECT_EQ(stats.processed_fill_count, 0U);
    EXPECT_EQ(stats.ignored_update_count, 1U);
}

TEST(PositionLedgerTest, BuildsPortfolioRiskSnapshotByExchangeAndMarket) {
    constexpr trading::internal::QtyLots kBuyQtyLotsA = 10;
    constexpr trading::internal::QtyLots kSellQtyLotsA = 4;
    constexpr trading::internal::QtyLots kSellQtyLotsB = 3;
    constexpr trading::internal::QtyLots kBuyQtyLotsPoly = 2;
    constexpr std::int64_t kExpectedKalshiRealizedPnl = 60;

    trading::oms::PositionLedger ledger;

    EXPECT_TRUE(ledger.on_order_update(make_fill_update(
        trading::internal::ExchangeId::kKalshi, "KXBTC-YES", "cl-snap-1",
        trading::internal::Side::kBuy, kBuyQtyLotsA, 40, 600)));
    EXPECT_TRUE(ledger.on_order_update(make_fill_update(
        trading::internal::ExchangeId::kKalshi, "KXBTC-YES", "cl-snap-2",
        trading::internal::Side::kSell, kSellQtyLotsA, 55, 601)));
    EXPECT_TRUE(ledger.on_order_update(make_fill_update(
        trading::internal::ExchangeId::kKalshi, "KXETH-YES", "cl-snap-3",
        trading::internal::Side::kSell, kSellQtyLotsB, 30, 602)));
    EXPECT_TRUE(ledger.on_order_update(make_fill_update(
        trading::internal::ExchangeId::kPolymarket, "POLY-YES", "cl-snap-4",
        trading::internal::Side::kBuy, kBuyQtyLotsPoly, 70, 603)));

    const auto kalshi_snapshot =
        ledger.snapshot_for(trading::internal::ExchangeId::kKalshi, "KXBTC-YES");
    EXPECT_EQ(kalshi_snapshot.net_position_market, 6);
    EXPECT_EQ(kalshi_snapshot.gross_position_global, 9);
    EXPECT_EQ(kalshi_snapshot.realized_pnl_ticks_total, kExpectedKalshiRealizedPnl);

    const auto polymarket_snapshot =
        ledger.snapshot_for(trading::internal::ExchangeId::kPolymarket, "POLY-YES");
    EXPECT_EQ(polymarket_snapshot.net_position_market, 2);
    EXPECT_EQ(polymarket_snapshot.gross_position_global, 2);
    EXPECT_EQ(polymarket_snapshot.realized_pnl_ticks_total, 0);
}

TEST(PaperOrderTransportTest, PlaceRequestEmitsAckThenFill) {
    trading::oms::PaperOrderTransport transport;
    ASSERT_TRUE(transport.connect(trading::oms::OrderTransportConfig{
        .endpoint = "paper://oms",
    }));

    const bool sent = transport.send_text(R"({
        "action": "place_order",
        "client_order_id": "cl-paper-1",
        "market_ticker": "KXBTC-YES",
        "side": "buy",
        "qty": 3,
        "limit_price": 47,
        "time_in_force": "gtc"
    })");
    ASSERT_TRUE(sent);

    const auto ack_payload = transport.recv_text();
    ASSERT_TRUE(ack_payload.has_value());
    const auto ack = nlohmann::json::parse(ack_payload.value_or(""));
    EXPECT_EQ(ack.at("type"), "order_ack");
    EXPECT_EQ(ack.at("msg").at("client_order_id"), "cl-paper-1");
    EXPECT_EQ(ack.at("msg").at("market_ticker"), "KXBTC-YES");
    EXPECT_EQ(ack.at("msg").at("accepted_qty"), 3);

    const auto fill_payload = transport.recv_text();
    ASSERT_TRUE(fill_payload.has_value());
    const auto fill = nlohmann::json::parse(fill_payload.value_or(""));
    EXPECT_EQ(fill.at("type"), "fill");
    EXPECT_EQ(fill.at("msg").at("client_order_id"), "cl-paper-1");
    EXPECT_EQ(fill.at("msg").at("fill_qty"), 3);
    EXPECT_EQ(fill.at("msg").at("fill_price"), 47);
    EXPECT_EQ(fill.at("msg").at("side"), "buy");
}

TEST(PaperOrderTransportTest, RejectsUnsupportedActions) {
    trading::oms::PaperOrderTransport transport;
    ASSERT_TRUE(transport.connect(trading::oms::OrderTransportConfig{
        .endpoint = "paper://oms",
    }));

    ASSERT_TRUE(transport.send_text(R"({
        "action": "cancel_order",
        "client_order_id": "cl-paper-cancel-1"
    })"));
    const auto reject_payload = transport.recv_text();
    ASSERT_TRUE(reject_payload.has_value());
    const auto reject = nlohmann::json::parse(reject_payload.value_or(""));
    EXPECT_EQ(reject.at("type"), "order_reject");
    EXPECT_EQ(reject.at("msg").at("reason_code"), "paper_action_unsupported");
}

TEST(PaperOrderTransportTest, SplitsFillAcrossConfiguredParts) {
    trading::oms::PaperOrderTransport transport{trading::oms::PaperOrderTransportConfig{
        .auto_fill_on_place = true,
        .fill_parts = 3,
    }};
    ASSERT_TRUE(transport.connect(trading::oms::OrderTransportConfig{
        .endpoint = "paper://oms",
    }));

    ASSERT_TRUE(transport.send_text(R"({
        "action": "place_order",
        "client_order_id": "cl-paper-split-1",
        "market_ticker": "KXBTC-YES",
        "side": "buy",
        "qty": 7,
        "limit_price": 49
    })"));

    const auto ack_payload = transport.recv_text();
    ASSERT_TRUE(ack_payload.has_value());
    EXPECT_EQ(nlohmann::json::parse(*ack_payload).at("type"), "order_ack");

    std::int64_t total_fill_qty = 0;
    std::size_t fill_count = 0;
    while (fill_count < 3) {
        const auto fill_payload = transport.recv_text();
        ASSERT_TRUE(fill_payload.has_value());
        const auto fill = nlohmann::json::parse(*fill_payload);
        EXPECT_EQ(fill.at("type"), "fill");
        total_fill_qty += fill.at("msg").at("fill_qty").get<std::int64_t>();
        ++fill_count;
    }
    EXPECT_EQ(total_fill_qty, 7);
}

TEST(PaperOrderTransportTest, RejectsPlaceWhenConfiguredRejectRateIsFull) {
    trading::oms::PaperOrderTransport transport{trading::oms::PaperOrderTransportConfig{
        .auto_fill_on_place = true,
        .place_reject_bps = 10'000,
    }};
    ASSERT_TRUE(transport.connect(trading::oms::OrderTransportConfig{
        .endpoint = "paper://oms",
    }));

    ASSERT_TRUE(transport.send_text(R"({
        "action": "place_order",
        "client_order_id": "cl-paper-reject-1",
        "market_ticker": "KXBTC-YES",
        "side": "buy",
        "qty": 1,
        "limit_price": 50
    })"));

    const auto reject_payload = transport.recv_text();
    ASSERT_TRUE(reject_payload.has_value());
    const auto reject = nlohmann::json::parse(*reject_payload);
    EXPECT_EQ(reject.at("type"), "order_reject");
    EXPECT_EQ(reject.at("msg").at("reason_code"), "paper_place_reject");
    EXPECT_FALSE(transport.recv_text().has_value());
}

TEST(WsOrderTransportTest, ProxiesWsTransportConnectSendRecvAndClose) {
    QueueingWsTransport ws_transport;
    trading::oms::WsOrderTransport transport{ws_transport};

    const trading::oms::OrderTransportConfig config{
        .endpoint = "wss://example.test/orders",
        .headers = {
            {"KALSHI-ACCESS-KEY", "key"},
            {"KALSHI-ACCESS-TIMESTAMP", "123"},
            {"KALSHI-ACCESS-SIGNATURE", "sig"},
        },
    };

    ASSERT_TRUE(transport.connect(config));
    EXPECT_TRUE(ws_transport.connected());
    EXPECT_EQ(ws_transport.config().endpoint, config.endpoint);
    EXPECT_EQ(ws_transport.config().headers.at("KALSHI-ACCESS-KEY"), "key");

    EXPECT_TRUE(transport.send_text(R"({"type":"order.place"})"));
    ASSERT_EQ(ws_transport.sent_messages().size(), 1U);
    EXPECT_EQ(ws_transport.sent_messages().front(), R"({"type":"order.place"})");

    ws_transport.queue_inbound(R"({"type":"order_ack"})");
    const auto inbound = transport.recv_text();
    ASSERT_TRUE(inbound.has_value());
    EXPECT_EQ(inbound.value_or(""), R"({"type":"order_ack"})");

    transport.close();
    EXPECT_FALSE(ws_transport.connected());
}

TEST(WsOrderTransportTest, ForwardsLastErrorFromWsTransport) {
    QueueingWsTransport ws_transport;
    ws_transport.set_connect_result(false);
    trading::oms::WsOrderTransport transport{ws_transport};

    EXPECT_FALSE(transport.connect(trading::oms::OrderTransportConfig{
        .endpoint = "wss://example.test/orders",
    }));
    EXPECT_EQ(transport.last_error(), "connect failed");
}

TEST(WsSessionTest, ConnectBuildsKalshiHeaders) {
    const auto kalshi_adapter = make_kalshi_adapter();
    trading::tests::FakeWsTransport transport;
    trading::adapters::ws::WsSession session{transport, kalshi_adapter};

    ASSERT_TRUE(session.connect());
    const auto& connect_request = session.connect_request();

    EXPECT_FALSE(connect_request.endpoint.empty());
    EXPECT_TRUE(connect_request.headers.contains("KALSHI-ACCESS-KEY"));
    EXPECT_TRUE(connect_request.headers.contains("KALSHI-ACCESS-SIGNATURE"));
    EXPECT_TRUE(connect_request.headers.contains("KALSHI-ACCESS-TIMESTAMP"));
    EXPECT_FALSE(connect_request.headers.at("KALSHI-ACCESS-SIGNATURE").empty());
    EXPECT_FALSE(connect_request.headers.at("KALSHI-ACCESS-TIMESTAMP").empty());
    EXPECT_TRUE(transport.connected());
    EXPECT_EQ(transport.config().endpoint, connect_request.endpoint);
}

TEST(WsSessionTest, SubscribeSendsExpectedPayload) {
    const auto kalshi_adapter = make_kalshi_adapter();
    trading::tests::FakeWsTransport transport;
    trading::adapters::ws::WsSession session{transport, kalshi_adapter};
    const std::vector<std::string> market_tickers{
        "KXBTC-YES",
        "KXETH-YES",
    };

    ASSERT_TRUE(session.connect());
    ASSERT_TRUE(session.subscribe("trades", market_tickers));
    ASSERT_EQ(transport.sent_messages().size(), 1U);

    const auto subscribe_json = nlohmann::json::parse(session.last_subscribe_payload());
    EXPECT_EQ(subscribe_json.at("cmd"), "subscribe");
    EXPECT_EQ(subscribe_json.at("params").at("channels").at(0), "trades");
    EXPECT_EQ(subscribe_json.at("params").at("market_tickers").size(), 2U);
    EXPECT_EQ(subscribe_json.at("params").at("market_tickers").at(0), "KXBTC-YES");
    EXPECT_EQ(subscribe_json.at("params").at("market_tickers").at(1), "KXETH-YES");
}

TEST(RouterTest, RoutesEventIntoExactlyOneShardQueue) {
    constexpr std::size_t kShardCount = 4;
    constexpr std::size_t kPerShardQueueCapacity = 64;
    constexpr std::uint64_t kFirstSeqId = 7;

    trading::router::ShardedEventDispatch shard_dispatch{
        trading::router::ShardedEventDispatchConfig{
            .shard_count = kShardCount,
            .per_shard_queue_capacity = kPerShardQueueCapacity,
        },
    };
    trading::router::Router router{passthrough_decode, shard_dispatch, kShardCount};

    const trading::ingest::RawFrame frame =
        make_raw_frame("KXBTC-YES", R"({"type":"trade","id":1})", kFirstSeqId);

    ASSERT_TRUE(router.route(frame));

    const auto routed_events = drain_all_shards(shard_dispatch);
    ASSERT_EQ(routed_events.size(), 1U);
    EXPECT_EQ(routed_events[0].frame.market_ticker, frame.market_ticker);
    ASSERT_TRUE(routed_events[0].frame.sequence_id.has_value());
    EXPECT_EQ(routed_events[0].frame.sequence_id.value_or(0U), kFirstSeqId);
    EXPECT_LT(routed_events[0].route_key.shard_id, kShardCount);
    EXPECT_EQ(shard_dispatch.dropped_count(), 0U);
}

TEST(RouterTest, RoutesSameMarketToSameShard) {
    constexpr std::size_t kShardCount = 8;
    constexpr std::size_t kPerShardQueueCapacity = 64;
    constexpr std::uint64_t kFirstSeqId = 11;
    constexpr std::uint64_t kSecondSeqId = 12;
    constexpr std::size_t kExpectedEvents = 2;

    trading::router::ShardedEventDispatch shard_dispatch{
        trading::router::ShardedEventDispatchConfig{
            .shard_count = kShardCount,
            .per_shard_queue_capacity = kPerShardQueueCapacity,
        },
    };
    trading::router::Router router{passthrough_decode, shard_dispatch, kShardCount};

    const trading::ingest::RawFrame first =
        make_raw_frame("KXBTC-YES", R"({"type":"trade","id":1})", kFirstSeqId);
    const trading::ingest::RawFrame second =
        make_raw_frame("KXBTC-YES", R"({"type":"trade","id":2})", kSecondSeqId);

    ASSERT_TRUE(router.route(first));
    ASSERT_TRUE(router.route(second));

    const auto routed_events = drain_all_shards(shard_dispatch);
    ASSERT_EQ(routed_events.size(), kExpectedEvents);

    const auto seen_shard_id = common_shard_id(routed_events);
    ASSERT_TRUE(seen_shard_id.has_value());
    expect_all_market_tickers(routed_events, first.market_ticker);
    EXPECT_EQ(shard_dispatch.dropped_count(), 0U);
}

TEST(BookStoreTest, SnapshotBuildsBidAskState) {
    trading::shards::BookStore books;
    const auto snapshot_event =
        make_snapshot_event("KXBTC-YES", 10U, {{100, 5}, {99, 2}}, {{101, 3}, {102, 1}});

    ASSERT_TRUE(books.apply(snapshot_event));
    const auto* state = books.find("KXBTC-YES");
    ASSERT_NE(state, nullptr);
    EXPECT_EQ(state->snapshot_count, 1U);
    EXPECT_EQ(state->delta_count, 0U);
    EXPECT_EQ(state->trade_count, 0U);
    ASSERT_TRUE(state->last_seq_id.has_value());
    EXPECT_EQ(state->last_seq_id.value_or(0U), 10U);
    ASSERT_EQ(state->bids.size(), 2U);
    ASSERT_EQ(state->asks.size(), 2U);
    EXPECT_EQ(state->bids.begin()->first, 100);
    EXPECT_EQ(state->bids.begin()->second, 5);
    EXPECT_EQ(state->asks.begin()->first, 101);
    EXPECT_EQ(state->asks.begin()->second, 3);
}

TEST(BookStoreTest, DeltaUpdatesAndRemovesLevels) {
    trading::shards::BookStore books;
    ASSERT_TRUE(books.apply(make_snapshot_event("KXBTC-YES", 1U, {{100, 5}}, {{101, 3}})));

    ASSERT_TRUE(
        books.apply(make_delta_event("KXBTC-YES", 2U, trading::internal::Side::kBid, 100, -2)));
    ASSERT_TRUE(
        books.apply(make_delta_event("KXBTC-YES", 3U, trading::internal::Side::kBid, 100, -3)));
    ASSERT_TRUE(
        books.apply(make_delta_event("KXBTC-YES", 4U, trading::internal::Side::kAsk, 102, 4)));

    const auto* state = books.find("KXBTC-YES");
    ASSERT_NE(state, nullptr);
    EXPECT_EQ(state->delta_count, 3U);
    EXPECT_EQ(state->bids.count(100), 0U);
    ASSERT_EQ(state->asks.count(102), 1U);
    EXPECT_EQ(state->asks.at(102), 4);
    ASSERT_TRUE(state->last_seq_id.has_value());
    EXPECT_EQ(state->last_seq_id.value_or(0U), 4U);
}

TEST(BookStoreMatrixTest, RejectsTopLevelContractViolations) {
    constexpr trading::internal::PriceTicks kBidPrice = 100;
    constexpr trading::internal::PriceTicks kAskPrice = 101;
    trading::shards::BookStore books;

    trading::internal::NormalizedEvent empty_ticker{};
    empty_ticker.type = trading::internal::EventType::kSnapshot;
    empty_ticker.data = trading::internal::SnapshotData{
        .bids = std::vector<trading::internal::Level>{{kBidPrice, 1}},
        .asks = std::vector<trading::internal::Level>{{kAskPrice, 1}}};
    EXPECT_FALSE(books.apply(empty_ticker));

    trading::internal::NormalizedEvent unknown_type{};
    unknown_type.type = trading::internal::EventType::kUnknown;
    unknown_type.market_ticker = "KXBTC-YES";
    unknown_type.data = trading::internal::SnapshotData{
        .bids = std::vector<trading::internal::Level>{{kBidPrice, 1}},
        .asks = std::vector<trading::internal::Level>{{kAskPrice, 1}},
    };
    EXPECT_FALSE(books.apply(unknown_type));

    EXPECT_EQ(books.size(), 0U);
}

TEST(BookStoreMatrixTest, RejectsInvalidSnapshotAndDeltaPayloads) {
    constexpr std::string_view kTicker = "KXBTC-YES";
    constexpr trading::internal::PriceTicks kBidPrice = 100;
    constexpr trading::internal::PriceTicks kAskPrice = 101;
    constexpr trading::internal::PriceTicks kNegativePrice = -5;
    constexpr trading::internal::SequenceId kBaseSnapshotSeq = 1U;
    constexpr trading::internal::SequenceId kInvalidSnapshotSeqA = 2U;
    constexpr trading::internal::SequenceId kInvalidSnapshotSeqB = 3U;
    constexpr trading::internal::SequenceId kInvalidDeltaSeqA = 4U;
    constexpr trading::internal::SequenceId kInvalidDeltaSeqB = 5U;
    constexpr trading::internal::SequenceId kInvalidTradeSeq = 6U;

    struct InvalidCase {
        std::string name;
        trading::internal::NormalizedEvent event;
    };

    const std::vector<InvalidCase> cases = {
        {
            .name = "snapshot_negative_price",
            .event = make_snapshot_event(std::string{kTicker}, kInvalidSnapshotSeqA, {{-1, 2}},
                                         {{kAskPrice, 1}}),
        },
        {
            .name = "snapshot_non_positive_qty",
            .event = make_snapshot_event(std::string{kTicker}, kInvalidSnapshotSeqB,
                                         {{kBidPrice, 0}}, {{kAskPrice, 1}}),
        },
        {
            .name = "delta_unknown_side",
            .event = make_delta_event(std::string{kTicker}, kInvalidDeltaSeqA,
                                      trading::internal::Side::kUnknown, kBidPrice, 1),
        },
        {
            .name = "delta_negative_price",
            .event = make_delta_event(std::string{kTicker}, kInvalidDeltaSeqB,
                                      trading::internal::Side::kBid, kNegativePrice, 1),
        },
        {
            .name = "trade_variant_mismatch",
            .event =
                [kTicker, kBidPrice, kInvalidTradeSeq] {
                    auto malformed =
                        make_trade_event(std::string{kTicker}, kInvalidTradeSeq, kBidPrice, 1);
                    malformed.data = trading::internal::SnapshotData{};
                    return malformed;
                }(),
        },
    };

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        trading::shards::BookStore books;
        ASSERT_TRUE(books.apply(make_snapshot_event(std::string{kTicker}, kBaseSnapshotSeq,
                                                    {{kBidPrice, 2}}, {{kAskPrice, 2}})));

        EXPECT_FALSE(books.apply(test_case.event));
        const auto* state = books.find(kTicker);
        ASSERT_NE(state, nullptr);
        EXPECT_EQ(state->apply_reject_count, 1U);
    }
}

TEST(BookStoreMatrixTest, UsesMetaSequenceWhenRawSequenceMissing) {
    constexpr trading::internal::PriceTicks kBidPrice = 100;
    constexpr trading::internal::PriceTicks kAskPrice = 101;
    constexpr trading::internal::SequenceId kMetaOnlySeq = 41U;
    trading::shards::BookStore books;
    auto snapshot =
        make_snapshot_event("KXBTC-YES", std::nullopt, {{kBidPrice, 1}}, {{kAskPrice, 1}});
    snapshot.meta.sequence_id = kMetaOnlySeq;

    ASSERT_TRUE(books.apply(snapshot));
    const auto* state = books.find("KXBTC-YES");
    ASSERT_NE(state, nullptr);
    ASSERT_TRUE(state->last_seq_id.has_value());
    EXPECT_EQ(state->last_seq_id.value_or(0U), kMetaOnlySeq);
}

TEST(BookStoreMatrixTest, RawSequenceTakesPrecedenceOverMetaSequence) {
    constexpr trading::internal::PriceTicks kBidPrice = 100;
    constexpr trading::internal::PriceTicks kAskPrice = 101;
    constexpr trading::internal::SequenceId kRawSeq = 10U;
    constexpr trading::internal::SequenceId kConflictingMetaSeq = 999U;
    trading::shards::BookStore books;
    auto snapshot = make_snapshot_event("KXBTC-YES", kRawSeq, {{kBidPrice, 1}}, {{kAskPrice, 1}});
    snapshot.meta.sequence_id = kConflictingMetaSeq;

    ASSERT_TRUE(books.apply(snapshot));
    const auto* state = books.find("KXBTC-YES");
    ASSERT_NE(state, nullptr);
    ASSERT_TRUE(state->last_seq_id.has_value());
    EXPECT_EQ(state->last_seq_id.value_or(0U), kRawSeq);
}

TEST(BookStoreMatrixTest, DeltaBeforeSnapshotBuffersThenReplaysAfterSnapshot) {
    constexpr std::string_view kTicker = "KXBTC-YES";
    constexpr trading::internal::PriceTicks kBidPrice = 100;
    constexpr trading::internal::PriceTicks kAskPrice = 101;
    constexpr trading::internal::SequenceId kBufferedDeltaSeq = 2U;
    constexpr trading::internal::SequenceId kSnapshotSeq = 1U;
    trading::shards::BookStore books;

    ASSERT_TRUE(books.apply(make_delta_event(std::string{kTicker}, kBufferedDeltaSeq,
                                             trading::internal::Side::kBid, kBidPrice, 5)));
    const auto* pre_snapshot = books.find(kTicker);
    ASSERT_NE(pre_snapshot, nullptr);
    EXPECT_FALSE(pre_snapshot->has_snapshot);
    EXPECT_FALSE(pre_snapshot->desynced);
    EXPECT_EQ(pre_snapshot->delta_count, 0U);
    EXPECT_EQ(pre_snapshot->buffered_delta_count, 1U);
    EXPECT_EQ(pre_snapshot->pending_deltas.size(), 1U);
    EXPECT_EQ(pre_snapshot->bids.count(kBidPrice), 0U);

    ASSERT_TRUE(books.apply(make_snapshot_event(std::string{kTicker}, kSnapshotSeq,
                                                {{kBidPrice, 3}}, {{kAskPrice, 2}})));

    const auto* state = books.find(kTicker);
    ASSERT_NE(state, nullptr);
    EXPECT_TRUE(state->has_snapshot);
    EXPECT_FALSE(state->desynced);
    EXPECT_EQ(state->snapshot_count, 1U);
    EXPECT_EQ(state->delta_count, 1U);
    EXPECT_EQ(state->replayed_delta_count, 1U);
    EXPECT_EQ(state->pending_deltas.size(), 0U);
    ASSERT_EQ(state->bids.count(kBidPrice), 1U);
    EXPECT_EQ(state->bids.at(kBidPrice), 8);
}

// NOLINTBEGIN(readability-function-cognitive-complexity)
TEST(BookStoreMatrixTest, PendingDeltaOverflowMarksDesyncUntilSnapshot) {
    constexpr std::string_view kTicker = "KXBTC-YES";
    constexpr trading::internal::PriceTicks kBidPrice = 100;
    constexpr trading::internal::PriceTicks kAskPrice = 101;
    constexpr trading::internal::SequenceId kPostDesyncDeltaSeq = 9999U;
    constexpr trading::internal::SequenceId kPostDesyncTradeSeq = 10000U;
    constexpr trading::internal::SequenceId kRecoverySnapshotSeq = 10001U;
    trading::shards::BookStore books;

    for (std::size_t i = 0; i < trading::shards::BookStore::kMaxPendingDeltas; ++i) {
        ASSERT_TRUE(books.apply(make_delta_event(std::string{kTicker},
                                                 static_cast<trading::internal::SequenceId>(i + 1U),
                                                 trading::internal::Side::kBid, kBidPrice, 1)));
    }
    EXPECT_FALSE(
        books.apply(make_delta_event(std::string{kTicker},
                                     static_cast<trading::internal::SequenceId>(
                                         trading::shards::BookStore::kMaxPendingDeltas + 1U),
                                     trading::internal::Side::kBid, kBidPrice, 1)));

    const auto* desynced = books.find(kTicker);
    ASSERT_NE(desynced, nullptr);
    EXPECT_TRUE(desynced->desynced);
    EXPECT_FALSE(desynced->has_snapshot);
    EXPECT_EQ(desynced->pending_deltas.size(), 0U);
    EXPECT_EQ(desynced->desync_count, 1U);
    EXPECT_EQ(desynced->dropped_pending_delta_count,
              trading::shards::BookStore::kMaxPendingDeltas + 1U);

    EXPECT_FALSE(books.apply(make_delta_event(std::string{kTicker}, kPostDesyncDeltaSeq,
                                              trading::internal::Side::kBid, kBidPrice, 1)));
    EXPECT_FALSE(
        books.apply(make_trade_event(std::string{kTicker}, kPostDesyncTradeSeq, kBidPrice, 1)));

    ASSERT_TRUE(books.apply(make_snapshot_event(std::string{kTicker}, kRecoverySnapshotSeq,
                                                {{kBidPrice, 3}}, {{kAskPrice, 2}})));
    const auto* recovered = books.find(kTicker);
    ASSERT_NE(recovered, nullptr);
    EXPECT_FALSE(recovered->desynced);
    EXPECT_TRUE(recovered->has_snapshot);
    EXPECT_EQ(recovered->snapshot_count, 1U);
    EXPECT_EQ(recovered->delta_count, 0U);
    ASSERT_EQ(recovered->bids.count(kBidPrice), 1U);
    EXPECT_EQ(recovered->bids.at(kBidPrice), 3);
}
// NOLINTEND(readability-function-cognitive-complexity)

// NOLINTBEGIN(readability-function-cognitive-complexity)
TEST(BookStoreTest, SequencePolicyRejectsStaleAndAcceptsNonContiguous) {
    trading::shards::BookStore books;
    ASSERT_TRUE(books.apply(make_snapshot_event("KXBTC-YES", 5U, {{100, 5}}, {{101, 5}})));

    EXPECT_FALSE(
        books.apply(make_delta_event("KXBTC-YES", 4U, trading::internal::Side::kBid, 100, 1)));
    ASSERT_TRUE(books.apply(make_trade_event("KXBTC-YES", 8U, 100, 2)));

    const auto* state = books.find("KXBTC-YES");
    ASSERT_NE(state, nullptr);
    EXPECT_EQ(state->stale_sequence_count, 1U);
    EXPECT_EQ(state->trade_count, 1U);
    ASSERT_TRUE(state->last_trade.has_value());
    if (state->last_trade.has_value()) {
        EXPECT_EQ(state->last_trade.value().price_ticks, 100);
    }
    ASSERT_TRUE(state->last_seq_id.has_value());
    EXPECT_EQ(state->last_seq_id.value_or(0U), 5U);
}
// NOLINTEND(readability-function-cognitive-complexity)

TEST(BookStoreTest, TradeSequenceIsIndependentFromBookSequence) {
    trading::shards::BookStore books;
    ASSERT_TRUE(books.apply(make_snapshot_event("KXBTC-YES", 10U, {{100, 5}}, {{101, 5}})));
    ASSERT_TRUE(
        books.apply(make_delta_event("KXBTC-YES", 20U, trading::internal::Side::kBid, 100, 1)));

    ASSERT_TRUE(books.apply(make_trade_event("KXBTC-YES", 1U, 100, 2)));

    const auto* state = books.find("KXBTC-YES");
    ASSERT_NE(state, nullptr);
    EXPECT_EQ(state->trade_count, 1U);
    ASSERT_TRUE(state->last_trade.has_value());
    EXPECT_EQ(state->last_trade->qty_lots, 2);
    ASSERT_TRUE(state->last_seq_id.has_value());
    EXPECT_EQ(state->last_seq_id.value_or(0U), 20U);
}

TEST(RoutedEventParserTest, ParsesKalshiSnapshotFromRoutedEvent) {
    const auto routed_event = make_routed_event(
        R"({
            "type": "orderbook_snapshot",
            "sid": 3,
            "seq": 2,
            "msg": {
                "market_ticker": "FED-23DEC-T3.00",
                "yes": [[95, 54], [94, 1]],
                "no": [[7, 87], [6, 32]]
            }
        })",
        "FED-23DEC-T3.00", std::nullopt);

    const trading::shards::RoutedEventParser parser;
    const auto parsed_result = parser.parse(routed_event);
    const auto& parsed = require_parsed(parsed_result);

    EXPECT_EQ(parsed.type, trading::internal::EventType::kSnapshot);
    ASSERT_TRUE(parsed.raw_sequence_id.has_value());
    EXPECT_EQ(parsed.raw_sequence_id.value_or(0U), 2U);
}

TEST(RoutedEventParserTest, RejectsUnsupportedExchange) {
    const auto routed_event = make_routed_event(R"({"type":"trade"})", "ANY", std::nullopt,
                                                trading::internal::ExchangeId::kUnknown);

    const trading::shards::RoutedEventParser parser;
    const auto parsed_result = parser.parse(routed_event);
    EXPECT_FALSE(parsed_result.ok());
    EXPECT_EQ(parsed_result.error(), trading::parsers::ParseError::kUnsupportedMessageType);
}

TEST(KalshiDecodeExtractorTest, ExtractsTickerAndSequenceFromSnapshotPayload) {
    trading::ingest::RawFrame frame{};
    frame.recv_timestamp = std::chrono::steady_clock::now();
    frame.source = "kalshi";
    frame.payload = R"({
        "type": "orderbook_snapshot",
        "sid": 3,
        "seq": 2,
        "msg": {
            "market_ticker": "FED-23DEC-T3.00",
            "yes": [[95, 54]],
            "no": [[7, 87]]
        }
    })";

    const auto extracted = trading::decode::exchanges::kalshi::extract(frame);
    EXPECT_TRUE(extracted.ok());
    EXPECT_EQ(extracted.market_ticker, "FED-23DEC-T3.00");
    ASSERT_TRUE(extracted.seq_id.has_value());
    EXPECT_EQ(extracted.seq_id.value_or(0U), 2U);
}

// NOLINTBEGIN(readability-function-cognitive-complexity)
TEST(LivePipelineTest, RoutesMessageFromSinkThroughRouterIntoShardBooks) {
    constexpr std::string_view kTicker = "FED-23DEC-T3.00";
    constexpr std::string_view kPayload = R"({
        "type": "orderbook_snapshot",
        "sid": 3,
        "seq": 2,
        "msg": {
            "market_ticker": "FED-23DEC-T3.00",
            "yes": [[95, 54], [94, 1]],
            "no": [[7, 87], [6, 32]]
        }
    })";
    constexpr std::size_t kQueueCapacity = 64;
    constexpr std::size_t kShardCount = 4;
    constexpr int kMaxAttempts = 200;

    trading::pipeline::LivePipeline pipeline{
        trading::pipeline::LivePipelineConfig{
            .source = "kalshi",
            .decode_exchange = trading::decode::ExchangeId::kKalshi,
            .router_exchange = trading::internal::ExchangeId::kKalshi,
            .frame_pool_capacity = kQueueCapacity,
            .frame_queue_capacity = kQueueCapacity,
            .shard_count = kShardCount,
            .per_shard_queue_capacity = kQueueCapacity,
            .shard_idle_sleep = std::chrono::milliseconds{1},
        },
    };
    ASSERT_TRUE(pipeline.start());

    auto& sink = pipeline.message_sink();
    ASSERT_TRUE(sink.push_message(std::string{kPayload}));

    bool found_book = false;
    for (int attempt = 0; attempt < kMaxAttempts && !found_book; ++attempt) {
        (void)pipeline.pump_ingest(kQueueCapacity);
        for (std::size_t shard_id = 0; shard_id < pipeline.shard_count(); ++shard_id) {
            const auto* books = pipeline.book_store(shard_id);
            if (books == nullptr) {
                continue;
            }
            if (books->find(kTicker) != nullptr) {
                found_book = true;
                break;
            }
        }
        if (!found_book) {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }

    const auto pipeline_stats = pipeline.stats();
    pipeline.stop();

    EXPECT_TRUE(found_book);
    EXPECT_GE(pipeline_stats.ingest_frames_pumped, 1U);
    EXPECT_GE(pipeline_stats.route_success, 1U);
    EXPECT_GE(pipeline_stats.shard_parsed, 1U);
    EXPECT_GE(pipeline_stats.parsed_snapshots, 1U);
    EXPECT_EQ(pipeline_stats.shard_parser_rejects, 0U);
    EXPECT_EQ(pipeline_stats.shard_apply_rejects, 0U);
    EXPECT_EQ(pipeline_stats.parse_error_invalid_json, 0U);
    EXPECT_EQ(pipeline_stats.parse_error_missing_field, 0U);
    EXPECT_EQ(pipeline_stats.parse_error_invalid_field, 0U);
    EXPECT_EQ(pipeline_stats.parse_error_unsupported_type, 0U);
}
// NOLINTEND(readability-function-cognitive-complexity)

// NOLINTBEGIN(readability-function-cognitive-complexity)
TEST(LivePipelineTest, InvokesPerShardEventHandlers) {
    constexpr std::string_view kPayload = R"({
        "type": "orderbook_snapshot",
        "sid": 3,
        "seq": 2,
        "msg": {
            "market_ticker": "FED-23DEC-T3.00",
            "yes": [[95, 54], [94, 1]],
            "no": [[7, 87], [6, 32]]
        }
    })";
    constexpr std::size_t kQueueCapacity = 64;
    constexpr std::size_t kShardCount = 4;
    constexpr int kMaxAttempts = 200;

    std::vector<std::unique_ptr<std::atomic<std::uint64_t>>> invocation_counters;
    invocation_counters.reserve(kShardCount);
    for (std::size_t shard_id = 0; shard_id < kShardCount; ++shard_id) {
        invocation_counters.push_back(std::make_unique<std::atomic<std::uint64_t>>(0U));
    }

    trading::pipeline::LivePipeline pipeline{
        trading::pipeline::LivePipelineConfig{
            .source = "kalshi",
            .decode_exchange = trading::decode::ExchangeId::kKalshi,
            .router_exchange = trading::internal::ExchangeId::kKalshi,
            .frame_pool_capacity = kQueueCapacity,
            .frame_queue_capacity = kQueueCapacity,
            .shard_count = kShardCount,
            .per_shard_queue_capacity = kQueueCapacity,
            .shard_idle_sleep = std::chrono::milliseconds{1},
            .shard_event_handler_factory =
                [&invocation_counters](std::size_t shard_id) -> std::unique_ptr<trading::shards::IShardEventHandler> {
                if (shard_id >= invocation_counters.size() || invocation_counters[shard_id] == nullptr) {
                    return nullptr;
                }
                return std::make_unique<CountingShardEventHandler>(*invocation_counters[shard_id]);
            },
        },
    };
    ASSERT_TRUE(pipeline.start());

    auto& sink = pipeline.message_sink();
    ASSERT_TRUE(sink.push_message(std::string{kPayload}));

    bool handler_invoked = false;
    for (int attempt = 0; attempt < kMaxAttempts && !handler_invoked; ++attempt) {
        (void)pipeline.pump_ingest(kQueueCapacity);
        for (const auto& invocation_counter : invocation_counters) {
            if (invocation_counter == nullptr) {
                continue;
            }
            if (invocation_counter->load(std::memory_order_relaxed) > 0U) {
                handler_invoked = true;
                break;
            }
        }
        if (!handler_invoked) {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }

    pipeline.stop();
    EXPECT_TRUE(handler_invoked);
}
// NOLINTEND(readability-function-cognitive-complexity)

TEST(ReplayHarnessTest, ReplaysPayloadBatchThroughLivePipelineAndDrains) {
    constexpr std::size_t kQueueCapacity = 64;
    trading::pipeline::LivePipeline pipeline{
        trading::pipeline::LivePipelineConfig{
            .source = "kalshi",
            .decode_exchange = trading::decode::ExchangeId::kKalshi,
            .router_exchange = trading::internal::ExchangeId::kKalshi,
            .frame_pool_capacity = kQueueCapacity,
            .frame_queue_capacity = kQueueCapacity,
            .shard_count = 1,
            .per_shard_queue_capacity = kQueueCapacity,
            .shard_idle_sleep = std::chrono::milliseconds{0},
        },
    };
    ASSERT_TRUE(pipeline.start());

    const std::vector<std::string> payloads{
        R"({
            "type": "orderbook_snapshot",
            "sid": 3,
            "seq": 1,
            "msg": {
                "market_ticker": "FED-23DEC-T3.00",
                "yes": [[95, 54], [94, 1]],
                "no": [[7, 87], [6, 32]]
            }
        })",
        R"({
            "type": "orderbook_delta",
            "sid": 3,
            "seq": 2,
            "msg": {
                "market_ticker": "FED-23DEC-T3.00",
                "price": 95,
                "delta": -4,
                "side": "yes"
            }
        })",
        R"({
            "type": "trade",
            "sid": 3,
            "seq": 1,
            "msg": {
                "market_ticker": "FED-23DEC-T3.00",
                "yes_price": 95,
                "no_price": 5,
                "count": 11,
                "taker_side": "yes"
            }
        })",
    };

    trading::pipeline::ReplayHarness harness{
        pipeline,
        trading::pipeline::ReplayHarnessConfig{
            .push_batch_size = 1,
            .pump_batch_size = kQueueCapacity,
            .max_drain_iterations = 256,
            .drain_sleep = std::chrono::milliseconds{1},
        },
    };
    const auto replay_stats = harness.replay(payloads);
    const auto pipeline_stats = pipeline.stats();
    pipeline.stop();

    EXPECT_EQ(replay_stats.attempted_messages, payloads.size());
    EXPECT_EQ(replay_stats.pushed_messages, payloads.size());
    EXPECT_EQ(replay_stats.sink_rejected_messages, 0U);
    EXPECT_TRUE(replay_stats.drain_completed);
    EXPECT_EQ(replay_stats.final_ingest_queue_depth, 0U);
    EXPECT_EQ(replay_stats.final_shard_queue_depth, 0U);

    EXPECT_GE(pipeline_stats.ingest_frames_pumped, payloads.size());
    EXPECT_GE(pipeline_stats.shard_parsed, payloads.size());
    EXPECT_EQ(pipeline_stats.shard_parser_rejects, 0U);
    EXPECT_EQ(pipeline_stats.shard_apply_rejects, 0U);
}

TEST(ReplayHarnessTest, LoadsJsonlReplayFile) {
    const auto temp_path = std::filesystem::temp_directory_path() / "trading_replay_harness_test.jsonl";
    {
        std::ofstream output(temp_path);
        ASSERT_TRUE(output.good());
        output << R"({"type":"trade","msg":{"market_ticker":"KXBTC-YES"}})" << '\n';
        output << '\n';
        output << "  " << R"({"type":"trade","msg":{"market_ticker":"KXETH-YES"}})" << " \n";
    }

    std::vector<std::string> payloads;
    std::string error;
    const bool loaded = trading::pipeline::ReplayHarness::load_jsonl_file(
        temp_path.string(), payloads, error);
    std::filesystem::remove(temp_path);

    EXPECT_TRUE(loaded) << error;
    ASSERT_EQ(payloads.size(), 2U);
    EXPECT_NE(payloads[0].find("KXBTC-YES"), std::string::npos);
    EXPECT_NE(payloads[1].find("KXETH-YES"), std::string::npos);
}

TEST(KalshiParserTest, ParsesOrderbookSnapshotDocsExample) {
    constexpr std::string_view kTicker = "FED-23DEC-T3.00";
    constexpr std::string_view kPayload = R"({
        "type": "orderbook_snapshot",
        "sid": 3,
        "seq": 2,
        "msg": {
            "market_ticker": "FED-23DEC-T3.00",
            "yes": [[95, 54], [94, 1]],
            "no": [[7, 87], [6, 32]]
        }
    })";

    const trading::parsers::exchanges::kalshi::Parser parser;
    const trading::internal::RouterFrame frame{
        .exchange = trading::internal::ExchangeId::kKalshi,
        .market_ticker = std::string{kTicker},
        .sequence_id = std::nullopt,
        .recv_ns = 17U,
        .raw_payload_view = kPayload,
    };
    const auto parsed_result = parser.parse(frame);
    const auto& event = require_parsed(parsed_result);

    EXPECT_EQ(event.type, trading::internal::EventType::kSnapshot);
    EXPECT_EQ(event.meta.exchange, trading::internal::ExchangeId::kKalshi);
    ASSERT_TRUE(event.raw_sequence_id.has_value());
    EXPECT_EQ(event.raw_sequence_id.value_or(0U), 2U);
    EXPECT_EQ(event.meta.sequence_id, 2U);

    const auto* snapshot = std::get_if<trading::internal::SnapshotData>(&event.data);
    ASSERT_NE(snapshot, nullptr);
    ASSERT_EQ(snapshot->bids.size(), 2U);
    ASSERT_EQ(snapshot->asks.size(), 2U);
    EXPECT_EQ(snapshot->bids[0].price_ticks, 9500);
    EXPECT_EQ(snapshot->bids[0].qty_lots, 54);
    EXPECT_EQ(snapshot->asks[0].price_ticks, 700);
    EXPECT_EQ(snapshot->asks[0].qty_lots, 87);
}

TEST(KalshiParserTest, ParsesOrderbookSnapshotWithoutLevelsAsEmptyBook) {
    constexpr std::string_view kTicker = "KXBTC-YES";
    constexpr std::string_view kPayload = R"({
        "type": "orderbook_snapshot",
        "sid": 2,
        "seq": 1,
        "msg": {
            "market_ticker": "KXBTC-YES",
            "market_id": ""
        }
    })";

    const trading::parsers::exchanges::kalshi::Parser parser;
    const trading::internal::RouterFrame frame{
        .exchange = trading::internal::ExchangeId::kKalshi,
        .market_ticker = std::string{kTicker},
        .sequence_id = std::nullopt,
        .recv_ns = 35U,
        .raw_payload_view = kPayload,
    };
    const auto parsed_result = parser.parse(frame);
    const auto& event = require_parsed(parsed_result);

    EXPECT_EQ(event.type, trading::internal::EventType::kSnapshot);
    const auto* snapshot = std::get_if<trading::internal::SnapshotData>(&event.data);
    ASSERT_NE(snapshot, nullptr);
    EXPECT_TRUE(snapshot->bids.empty());
    EXPECT_TRUE(snapshot->asks.empty());
}

TEST(KalshiParserTest, ParsesOrderbookDeltaDocsExample) {
    constexpr std::string_view kTicker = "FED-23DEC-T3.00";
    constexpr std::string_view kPayload = R"({
        "type": "orderbook_delta",
        "sid": 3,
        "seq": 3,
        "msg": {
            "market_ticker": "FED-23DEC-T3.00",
            "price": 95,
            "delta": -54,
            "side": "yes"
        }
    })";

    const trading::parsers::exchanges::kalshi::Parser parser;
    const trading::internal::RouterFrame frame{
        .exchange = trading::internal::ExchangeId::kKalshi,
        .market_ticker = std::string{kTicker},
        .sequence_id = std::nullopt,
        .recv_ns = 19U,
        .raw_payload_view = kPayload,
    };
    const auto parsed_result = parser.parse(frame);
    const auto& event = require_parsed(parsed_result);

    EXPECT_EQ(event.type, trading::internal::EventType::kDelta);
    ASSERT_TRUE(event.raw_sequence_id.has_value());
    EXPECT_EQ(event.raw_sequence_id.value_or(0U), 3U);
    EXPECT_EQ(event.meta.sequence_id, 3U);

    const auto* delta = std::get_if<trading::internal::DeltaData>(&event.data);
    ASSERT_NE(delta, nullptr);
    EXPECT_EQ(delta->side, trading::internal::Side::kBid);
    EXPECT_EQ(delta->price_ticks, 9500);
    EXPECT_EQ(delta->delta_qty_lots, -54);
}

TEST(KalshiParserTest, ParsesOrderbookDeltaDollarFpFields) {
    constexpr std::string_view kTicker = "KXBTC-YES";
    constexpr std::string_view kPayload = R"({
        "type": "orderbook_delta",
        "sid": 1528,
        "seq": 346983,
        "msg": {
            "market_ticker": "KXBTC-YES",
            "price_dollars": "0.960",
            "delta_fp": "-54.00",
            "side": "yes"
        }
    })";

    const trading::parsers::exchanges::kalshi::Parser parser;
    const trading::internal::RouterFrame frame{
        .exchange = trading::internal::ExchangeId::kKalshi,
        .market_ticker = std::string{kTicker},
        .sequence_id = std::nullopt,
        .recv_ns = 37U,
        .raw_payload_view = kPayload,
    };
    const auto parsed_result = parser.parse(frame);
    const auto& event = require_parsed(parsed_result);

    EXPECT_EQ(event.type, trading::internal::EventType::kDelta);
    const auto* delta = std::get_if<trading::internal::DeltaData>(&event.data);
    ASSERT_NE(delta, nullptr);
    EXPECT_EQ(delta->price_ticks, 9600);
    EXPECT_EQ(delta->delta_qty_lots, -54);
    EXPECT_EQ(delta->side, trading::internal::Side::kBid);
}

TEST(KalshiParserTest, ParsesOrderbookDeltaYesFieldsWithoutExplicitSide) {
    constexpr std::string_view kTicker = "KXBTC-YES";
    constexpr std::string_view kPayload = R"({
        "type": "orderbook_delta",
        "sid": 1528,
        "seq": 346983,
        "msg": {
            "market_ticker": "KXBTC-YES",
            "yes_price_dollars": "0.960",
            "yes_delta_fp": "-54.00"
        }
    })";

    const trading::parsers::exchanges::kalshi::Parser parser;
    const trading::internal::RouterFrame frame{
        .exchange = trading::internal::ExchangeId::kKalshi,
        .market_ticker = std::string{kTicker},
        .sequence_id = std::nullopt,
        .recv_ns = 39U,
        .raw_payload_view = kPayload,
    };
    const auto parsed_result = parser.parse(frame);
    const auto& event = require_parsed(parsed_result);

    EXPECT_EQ(event.type, trading::internal::EventType::kDelta);
    const auto* delta = std::get_if<trading::internal::DeltaData>(&event.data);
    ASSERT_NE(delta, nullptr);
    EXPECT_EQ(delta->price_ticks, 9600);
    EXPECT_EQ(delta->delta_qty_lots, -54);
    EXPECT_EQ(delta->side, trading::internal::Side::kBid);
}

TEST(KalshiParserTest, ParsesTradeDocsExample) {
    constexpr std::string_view kTicker = "HIGHNY-25MAR11-B57.25";
    constexpr std::string_view kPayload = R"({
        "type": "trade",
        "msg": {
            "market_ticker": "HIGHNY-25MAR11-B57.25",
            "yes_price": 17,
            "no_price": 84,
            "count": 17,
            "taker_side": "yes",
            "ts": 1741098958832
        }
    })";

    const trading::parsers::exchanges::kalshi::Parser parser;
    const trading::internal::RouterFrame frame{
        .exchange = trading::internal::ExchangeId::kKalshi,
        .market_ticker = std::string{kTicker},
        .sequence_id = std::nullopt,
        .recv_ns = 23U,
        .raw_payload_view = kPayload,
    };
    const auto parsed_result = parser.parse(frame);
    const auto& event = require_parsed(parsed_result);

    EXPECT_EQ(event.type, trading::internal::EventType::kTrade);
    EXPECT_FALSE(event.raw_sequence_id.has_value());

    const auto* trade = std::get_if<trading::internal::TradeData>(&event.data);
    ASSERT_NE(trade, nullptr);
    EXPECT_EQ(trade->price_ticks, 1700);
    EXPECT_EQ(trade->qty_lots, 17);
    EXPECT_EQ(trade->aggressor, trading::internal::Side::kBuy);
}

TEST(KalshiParserTest, ParsesTradeDollarFpFields) {
    constexpr std::string_view kTicker = "KXBTC-YES";
    constexpr std::string_view kPayload = R"({
        "type": "trade",
        "msg": {
            "market_ticker": "KXBTC-YES",
            "yes_price_dollars": "0.4400",
            "no_price_dollars": "0.5700",
            "count_fp": "17.00",
            "taker_side": "yes",
            "ts": 1741098958832
        }
    })";

    const trading::parsers::exchanges::kalshi::Parser parser;
    const trading::internal::RouterFrame frame{
        .exchange = trading::internal::ExchangeId::kKalshi,
        .market_ticker = std::string{kTicker},
        .sequence_id = std::nullopt,
        .recv_ns = 41U,
        .raw_payload_view = kPayload,
    };
    const auto parsed_result = parser.parse(frame);
    const auto& event = require_parsed(parsed_result);

    EXPECT_EQ(event.type, trading::internal::EventType::kTrade);
    const auto* trade = std::get_if<trading::internal::TradeData>(&event.data);
    ASSERT_NE(trade, nullptr);
    EXPECT_EQ(trade->price_ticks, 4400);
    EXPECT_EQ(trade->qty_lots, 17);
    EXPECT_EQ(trade->aggressor, trading::internal::Side::kBuy);
}

TEST(KalshiParserTest, ParsesTradeDollarFpWithSubCentPrecision) {
    constexpr std::string_view kTicker = "KXBTC-YES";
    constexpr std::string_view kPayload = R"({
        "type": "trade",
        "msg": {
            "market_ticker": "KXBTC-YES",
            "yes_price_dollars": "0.5790",
            "count_fp": "1.00",
            "taker_side": "yes",
            "ts": 1741098958832
        }
    })";

    const trading::parsers::exchanges::kalshi::Parser parser;
    const trading::internal::RouterFrame frame{
        .exchange = trading::internal::ExchangeId::kKalshi,
        .market_ticker = std::string{kTicker},
        .sequence_id = std::nullopt,
        .recv_ns = 43U,
        .raw_payload_view = kPayload,
    };
    const auto parsed_result = parser.parse(frame);
    const auto& event = require_parsed(parsed_result);

    EXPECT_EQ(event.type, trading::internal::EventType::kTrade);
    const auto* trade = std::get_if<trading::internal::TradeData>(&event.data);
    ASSERT_NE(trade, nullptr);
    EXPECT_EQ(trade->price_ticks, 5790);
    EXPECT_EQ(trade->qty_lots, 1);
    EXPECT_EQ(trade->aggressor, trading::internal::Side::kBuy);
}

} // namespace
