#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <variant>
#include <algorithm>

#include "predex/internal/market_types.hpp"

namespace predex::core::oms::kalshi {
namespace {
    inline constexpr std::size_t kLengthOfClientOrderId = 64;
    //exchange generated from kalshi, soaks show 36 chars stably, extend buffer to 64 for safety 
    inline constexpr std::size_t kMaxExchangeOrderId = 64;
}
using OmsRequestId = std::uint64_t;
using LocalIntentId = std::uint64_t;
using GroupIntentId = std::uint64_t;

struct ClientOrderId {
    static constexpr std::size_t kCapacity = kLengthOfClientOrderId;
    std::array<char, kCapacity> storage{};
    std::uint8_t size{0};

    [[nodiscard]] std::string_view view() const noexcept {
        return {storage.data(), size};
    }

    [[nodiscard]] const char* c_str() const noexcept {
        return storage.data();
    }

    [[nodiscard]] bool operator==(const ClientOrderId& other) const noexcept {
        return view() == other.view();
    }

    [[nodiscard]] bool empty() const noexcept {
        return size == 0;
    }

    void clear() noexcept {
        storage[0] = '\0';
        size = 0;
    }

    [[nodiscard]] bool assign_from(std::string_view str) noexcept {
        if (str.size() >= kCapacity) {
            clear();
            return false;
        }
        std::copy(str.begin(), str.end(), storage.begin());
        size = static_cast<std::uint8_t>(str.size());
        storage[size] = '\0';
        return true;
    }
};

struct ExchangeOrderId {
    static constexpr std::size_t kCapacity = kMaxExchangeOrderId;
    std::array<char, kCapacity> storage{};
    std::uint8_t size{0};
    
    [[nodiscard]] std::string_view view() const noexcept {
        return {storage.data(), size};
    }

    [[nodiscard]] const char* c_str() const noexcept {
        return storage.data();
    }

    [[nodiscard]] bool operator==(const ExchangeOrderId& other) const noexcept {
        return view() == other.view();
    }

    [[nodiscard]] bool empty() const noexcept {
        return size == 0;
    }
    void clear() noexcept {
        storage[0] = '\0';
        size = 0;
    }

    bool assign_from(std::string_view str) noexcept{
        if (str.size() >= kCapacity) {
            clear();
            return false;
        }
        std::copy(str.begin(), str.end(), storage.begin());
        size = static_cast<std::uint8_t>(str.size());
        if(size < kCapacity){
            storage[size] = '\0';
        }
        return true;
    }

};

enum class TimeInForce : std::uint8_t {
    kGtc = 1,
    kIoc = 2,
    kFok = 3,
};

using OmsTimeInForce = TimeInForce;

enum class OrderTypeIntent : std::uint8_t {
    kLimit = 1,
    kMarketableLimit = 2,
};

enum class LiquidityIntent : std::uint8_t {
    kDefault = 1,
    kPostOnly = 2,
};

enum class Outcome : std::uint8_t {
    kUnknown = 0,
    kYes = 1,
    kNo = 2,
};

enum class IntentRejectReason : std::uint8_t {
    kHardHalt = 0,
    kSoftHalt = 1,
    kGlobalRiskExceeded = 2,
    kInvalidParams = 3,
    kMarketClosed = 4,
    kStaleSignal = 5,
    kDuplicateClientOrderId = 6,
    kUnsupported = 7,
    kNone = 8,
};

enum class VenueRejectReason : std::uint8_t {
    kNone = 0,
    kRateLimit = 1,
    kVenueDown = 2,
    kUnknown = 3,
};

enum class KalshiEventSource : std::uint8_t {
    kRest = 1,
    kPrivateWs = 2,
    kReconcile = 3,
};

enum class GroupExecutionPolicy : std::uint8_t {
    kUnwind = 1,
    kAbortRemaining = 2,
    kBestEffort = 3,
    kAbortRemainingOnReject = kAbortRemaining,
};

inline constexpr std::size_t kMaxGroupOrderLegs = 4;

// Shard lineage, routing identity, and latency attribution travel together today.
// We can split this later if the seams need stricter ownership.
struct IntentContext {
    std::uint16_t shard_id{0};
    internal::AffinityKey affinity_key{0};
    GroupIntentId group_intent_id{0};
    std::uint16_t leg_index{0};
    std::uint16_t leg_count{0};
    std::uint64_t signal_id{0};
    LocalIntentId local_intent_id{0};

    internal::TimestampNs signal_ts_ns{0};
    internal::TimestampNs tick_recv_ns{0};
    internal::TimestampNs submission_enqueued_ns{0};

    internal::EventId event_id{0};
    internal::MarketId market_id{0};
};

struct OmsOrderRef {
    OmsRequestId oms_request_id{0};
    ClientOrderId client_order_id{};
    std::optional<ExchangeOrderId> exchange_order_id;
};

struct ShardOrderCorrelation {
    IntentContext context{};
    OmsOrderRef order{};
};

// ---------------------------------------------------------------------------
// 1. Shard -> OMS requests
// ---------------------------------------------------------------------------

struct NewOrderIntent {
    IntentContext context{};
    internal::ExchangeId exchange{internal::ExchangeId::kUnknown};
    internal::Side side{internal::Side::kUnknown};
    Outcome outcome{Outcome::kYes};
    internal::QtyLots qty_lots{0};
    std::optional<internal::PriceTicks> limit_price_ticks;
    TimeInForce time_in_force{TimeInForce::kGtc};
    LiquidityIntent liquidity_intent{LiquidityIntent::kDefault};
    OrderTypeIntent order_type_intent{OrderTypeIntent::kLimit};
    internal::TimestampNs intent_ts_ns{0};
};

struct GroupOrderIntent {
    IntentContext context{};
    GroupExecutionPolicy execution_policy{GroupExecutionPolicy::kAbortRemaining};
    std::array<NewOrderIntent, kMaxGroupOrderLegs> legs{};
    std::size_t leg_count{0};
    internal::TimestampNs intent_ts_ns{0};
};

struct CancelOrderIntent {
    IntentContext context{};
    std::optional<OmsRequestId> target_oms_request_id;
    ClientOrderId target_client_order_id{};
    std::optional<ExchangeOrderId> target_exchange_order_id;
    internal::TimestampNs intent_ts_ns{0};
};

struct ModifyOrderIntent {
    IntentContext context{};
    std::optional<OmsRequestId> target_oms_request_id;
    ClientOrderId target_client_order_id{};
    std::optional<ExchangeOrderId> target_exchange_order_id;
    // OMS2 modify semantics are intentionally narrow: replacement may adjust
    // working qty/price only. OMS must reject attempts to change side, outcome,
    // event, market, or other immutable order identity fields.
    NewOrderIntent replacement{};
    internal::TimestampNs intent_ts_ns{0};
};

using ShardOmsRequest =
    std::variant<NewOrderIntent, GroupOrderIntent, CancelOrderIntent, ModifyOrderIntent>;

// Compatibility aliases while this module is still in-flight.
using OrderIntent = NewOrderIntent;
using CancelIntent = CancelOrderIntent;
using ModifyIntent = ModifyOrderIntent;
using OmsSubmission = ShardOmsRequest;

// ---------------------------------------------------------------------------
// 2. OMS -> risk requests
// ---------------------------------------------------------------------------

struct CheckNewOrderRisk {
    NewOrderIntent intent{};
};

struct CheckModifyRisk {
    OmsOrderRef target_order{};
    NewOrderIntent replacement{};
};

struct RegisterVenueFillRisk {
    ShardOrderCorrelation corr{};
    internal::QtyLots fill_qty_lots{0};
    internal::PriceTicks fill_price_ticks{0};
};

struct ReleaseOrderRisk {
    ShardOrderCorrelation corr{};
    internal::QtyLots remaining_open_qty_lots{0};
};

using OmsToRiskRequest =
    std::variant<CheckNewOrderRisk, CheckModifyRisk, RegisterVenueFillRisk, ReleaseOrderRisk>;

// ---------------------------------------------------------------------------
// 3. Risk -> OMS decisions
// ---------------------------------------------------------------------------

struct RiskApproved {
    std::int64_t capital_reserved_ticks{0};
};

struct RiskRejected {
    IntentRejectReason reason{IntentRejectReason::kNone};
};

using RiskToOmsDecision = std::variant<RiskApproved, RiskRejected>;

// ---------------------------------------------------------------------------
// 4. OMS -> Kalshi commands
// ---------------------------------------------------------------------------

struct SubmitOrderCmd {
    OmsOrderRef order{};
    NewOrderIntent intent{};
    //surfaced that std::string market_ticker is not actually used until the last JSON serialization step in the transport,
    //as a result can remove market_ticker from OMS->Kalshi command and just resolve it at the transport layer, 
    //converted to internal::MarketId so that OMS doesn't need to carry around string tickers
    internal::MarketId market_id{0};
    std::optional<GroupExecutionPolicy> group_execution_policy;
    internal::TimestampNs transport_enqueue_ts_ns{0};
};

struct CancelOrderCmd {
    ShardOrderCorrelation corr{};
    internal::TimestampNs cmd_ts_ns{0};
    internal::TimestampNs transport_enqueue_ts_ns{0};
};

struct ModifyOrderCmd {
    ShardOrderCorrelation corr{};
    ClientOrderId updated_client_order_id{};
    NewOrderIntent replacement{};
    internal::TimestampNs cmd_ts_ns{0};
    internal::TimestampNs transport_enqueue_ts_ns{0};
};

using OmsToKalshiCommand = std::variant<SubmitOrderCmd, CancelOrderCmd, ModifyOrderCmd>;

// ---------------------------------------------------------------------------
// 5. Kalshi -> OMS events
// ---------------------------------------------------------------------------

struct VenueOrderAck {
    OmsOrderRef order{};
    internal::TimestampNs transport_submit_ts_ns{0};
    internal::TimestampNs recv_ts_ns{0};
    std::uint16_t http_status_code{0};
    std::uint16_t retry_count{0};
    internal::QtyLots accepted_qty_lots{0};
};

struct VenueOrderReject {
    OmsOrderRef order{};
    internal::TimestampNs transport_submit_ts_ns{0};
    internal::TimestampNs recv_ts_ns{0};
    std::uint16_t http_status_code{0};
    std::uint16_t retry_count{0};
    VenueRejectReason reason{VenueRejectReason::kNone};
    std::string raw_reason_code;
    std::string raw_reason_message;
};

struct VenueOrderPartialFill {
    OmsOrderRef order{};
    internal::TimestampNs recv_ts_ns{0};
    internal::QtyLots fill_qty_lots{0};
    internal::PriceTicks fill_price_ticks{0};
    internal::Side side{internal::Side::kUnknown};
};

struct VenueOrderFill {
    OmsOrderRef order{};
    internal::TimestampNs recv_ts_ns{0};
    internal::QtyLots fill_qty_lots{0};
    internal::PriceTicks fill_price_ticks{0};
    internal::Side side{internal::Side::kUnknown};
};

struct VenueCancelAck {
    OmsOrderRef order{};
    internal::TimestampNs transport_submit_ts_ns{0};
    internal::TimestampNs recv_ts_ns{0};
    std::uint16_t http_status_code{0};
    std::uint16_t retry_count{0};
};

struct VenueCancelReject {
    OmsOrderRef order{};
    internal::TimestampNs transport_submit_ts_ns{0};
    internal::TimestampNs recv_ts_ns{0};
    std::uint16_t http_status_code{0};
    std::uint16_t retry_count{0};
    VenueRejectReason reason{VenueRejectReason::kNone};
    std::string raw_reason_code;
    std::string raw_reason_message;
};

struct VenueModifyAck {
    OmsOrderRef order{};
    internal::TimestampNs transport_submit_ts_ns{0};
    internal::TimestampNs recv_ts_ns{0};
    std::uint16_t http_status_code{0};
    std::uint16_t retry_count{0};
    internal::QtyLots working_qty_lots{0};
    std::optional<internal::PriceTicks> working_price_ticks;
};

struct VenueModifyReject {
    OmsOrderRef order{};
    internal::TimestampNs transport_submit_ts_ns{0};
    internal::TimestampNs recv_ts_ns{0};
    std::uint16_t http_status_code{0};
    std::uint16_t retry_count{0};
    VenueRejectReason reason{VenueRejectReason::kNone};
    std::string raw_reason_code;
    std::string raw_reason_message;
};

struct VenueOrderCanceled {
    OmsOrderRef order{};
    internal::TimestampNs recv_ts_ns{0};
};

struct VenueOrderUncertain {
    OmsOrderRef order{};
    internal::TimestampNs recv_ts_ns{0};
};

// Reconciliation snapshots should come in as OMS-ingest events, not be applied
// out-of-band, so they participate in the same canonical bookkeeping.
struct ReconcileOpenOrderSnapshot {
    OmsOrderRef order{};
    IntentContext context{};
    internal::ExchangeId exchange{internal::ExchangeId::kUnknown};
    internal::Side side{internal::Side::kUnknown};
    Outcome outcome{Outcome::kYes};
    internal::QtyLots initial_qty_lots{0};
    internal::QtyLots working_qty_lots{0};
    internal::QtyLots cumulative_filled_qty_lots{0};
    std::optional<internal::PriceTicks> working_limit_price_ticks;
    internal::TimestampNs recv_ts_ns{0};
};

using KalshiToOmsEvent =
    std::variant<VenueOrderAck, VenueOrderReject, VenueOrderPartialFill, VenueOrderFill,
                 VenueCancelAck, VenueCancelReject, VenueModifyAck, VenueModifyReject,
                 VenueOrderCanceled, VenueOrderUncertain, ReconcileOpenOrderSnapshot>;

struct SourcedKalshiEvent {
    KalshiEventSource source{KalshiEventSource::kPrivateWs};
    KalshiToOmsEvent event;
};

// ---------------------------------------------------------------------------
// 6. OMS -> shard events
// ---------------------------------------------------------------------------

struct IntentAccepted {
    ShardOrderCorrelation corr{};
};

struct IntentRejected {
    IntentContext context{};
    IntentRejectReason reason{IntentRejectReason::kNone};
};

struct IntentModified {
    ShardOrderCorrelation corr{};
    NewOrderIntent replacement{};
};

using OmsToShardDecision = std::variant<IntentAccepted, IntentRejected, IntentModified>;

struct OrderWorking {
    ShardOrderCorrelation corr{};
    internal::QtyLots working_qty_lots{0};
    internal::PriceTicks working_price_ticks{0};
};

struct OrderPartiallyFilled {
    ShardOrderCorrelation corr{};
    internal::QtyLots filled_qty_lots{0};
    internal::QtyLots remaining_qty_lots{0};
    internal::PriceTicks fill_price_ticks{0};
};

struct OrderFilled {
    ShardOrderCorrelation corr{};
    internal::QtyLots filled_qty_lots{0};
    internal::PriceTicks fill_price_ticks{0};
};

struct OrderCanceled {
    ShardOrderCorrelation corr{};
};

struct OrderUncertain {
    ShardOrderCorrelation corr{};
};

struct OrderVenueRejected {
    ShardOrderCorrelation corr{};
    VenueRejectReason reason{VenueRejectReason::kNone};
};

using OmsToShardLifecycleEvent = std::variant<OrderWorking, OrderPartiallyFilled, OrderFilled,
                                              OrderCanceled, OrderUncertain, OrderVenueRejected>;

// Compatibility alias for the older draft naming.
using ShardOrderEvent = OmsToShardLifecycleEvent;

enum class OrderStatus : std::uint8_t {
    kUncertain,
    kPendingSubmit,
    kWorking,
    kRejected,
    kPartiallyFilled,
    kFilled,
    kPendingCancel,
    kCanceled,
    kPendingModify,
};

} // namespace predex::core::oms::kalshi

template <> struct std::hash<predex::core::oms::kalshi::ClientOrderId> {
    std::size_t
    operator()(const predex::core::oms::kalshi::ClientOrderId& identity) const noexcept {
        return std::hash<std::string_view>{}(identity.view());
    }
};

template <> struct std::hash<predex::core::oms::kalshi::ExchangeOrderId> {
    std::size_t
    operator()(const predex::core::oms::kalshi::ExchangeOrderId& identity) const noexcept {
        return std::hash<std::string_view>{}(identity.view());
    }
};
