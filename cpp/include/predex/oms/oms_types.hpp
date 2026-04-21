#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>

#include "predex/internal/market_types.hpp"

namespace predex::core::oms::kalshi {

using LocalIntentId = std::uint64_t;
using OmsRequestId = std::uint64_t;
using GroupIntentId = std::uint64_t;
using ClientOrderId = std::string;
using ExchangeOrderId = std::string;
inline constexpr std::size_t kMaxGroupOrderLegs = 4;

enum class OmsAction : std::uint8_t {
    kUnknown = 0,
    kSubmit = 1,
    kCancel = 2,
    kModify = 3,
};

enum class OmsTimeInForce : std::uint8_t {
    kUnknown = 0,
    kGtc = 1,
    kIoc = 2,
    kFok = 3,
};

// Binary-contract outcome the order is expressed against. Distinct from internal::Side, which
// carries only the buy/sell direction. On Kalshi a single action can be (buy, yes), (buy, no),
// (sell, yes), or (sell, no); overloading Side for both dimensions silently mistranslates
// sell-YES intents into sell-NO orders, which is the bug this field exists to prevent.
enum class Outcome : std::uint8_t {
    kUnknown = 0,
    kYes = 1,
    kNo = 2,
};

enum class OmsLiquidity : std::uint8_t {
    kUnknown = 0,
    kMaker = 1,
    kTaker = 2,
};

enum class OmsOrderStatus : std::uint8_t {
    kUnknown = 0,
    kPendingSubmit = 1,
    kLive = 2,
    kRejected = 3,
    kPartiallyFilled = 4,
    kFilled = 5,
    kPendingCancel = 6,
    kCanceled = 7,
    kPendingModify = 8,
    kReplaced = 9,
};

struct IntentOrigin {
    std::uint16_t shard_id{0};
    internal::AffinityKey affinity_key{0};
    GroupIntentId group_id{0};
    LocalIntentId local_intent_id{0};
    std::uint16_t leg_index{0};
    std::uint16_t leg_count{1};
    std::uint64_t signal_id{0};
    /*
    latency fields below
    */
    internal::TimestampNs signal_ts_ns{0};
    internal::TimestampNs tick_recv_ns{0};
    internal::TimestampNs submission_enqueued_ns{0};
    /*
    end latency fields
    */
    internal::EventId event_id{0};
    internal::MarketId market_id{0};
};

struct OrderIntent {
    IntentOrigin origin{};
    internal::ExchangeId exchange{internal::ExchangeId::kUnknown};
    internal::Side side{internal::Side::kUnknown};
    Outcome outcome{Outcome::kUnknown};
    internal::QtyLots qty_lots{0};
    std::optional<internal::PriceTicks> limit_price_ticks;
    OmsTimeInForce time_in_force{OmsTimeInForce::kUnknown};
    internal::TimestampNs intent_ts_ns{0};
};

enum class GroupExecutionPolicy : std::uint8_t {
    kAbortRemainingOnReject = 1,
    kBestEffort = 2,
};

struct GroupOrderIntent {
    GroupIntentId group_id{0};
    GroupExecutionPolicy execution_policy{GroupExecutionPolicy::kAbortRemainingOnReject};
    std::array<OrderIntent, kMaxGroupOrderLegs> legs{};
    std::size_t leg_count{0};
    internal::TimestampNs intent_ts_ns{0};
};

struct CancelIntent {
    IntentOrigin origin{};
    std::optional<OmsRequestId> target_oms_request_id;
    ClientOrderId target_client_order_id;
    std::optional<ExchangeOrderId> target_exchange_order_id;
    internal::TimestampNs intent_ts_ns{0};
};

struct ModifyIntent {
    IntentOrigin origin{};
    std::optional<OmsRequestId> target_oms_request_id;
    ClientOrderId target_client_order_id;
    std::optional<ExchangeOrderId> target_exchange_order_id;
    OrderIntent replacement_intent{};
    internal::TimestampNs intent_ts_ns{0};
};

using OmsSubmission =
    std::variant<OrderIntent, GroupOrderIntent, CancelIntent, ModifyIntent>;

enum class IntentDecisionCode : std::uint8_t {
    kAccepted = 1,
    kRejected = 2,
    kModified = 3,
};

enum class IntentRejectReason : std::uint8_t {
    kNone = 0,
    kGlobalRisk = 1,
    kInvalidIntent = 2,
    kRateLimited = 3,
    kOmsDisabled = 4,
    kVenueUnavailable = 5,
    kHalted = 6,
};

struct AcceptedIntent {
    OrderIntent intent{};
    OmsRequestId oms_request_id{0};
    ClientOrderId client_order_id;
};

struct RejectedIntent {
    OrderIntent intent{};
    IntentRejectReason reason{IntentRejectReason::kNone};
};

struct ModifiedIntent {
    OrderIntent original_intent{};
    OrderIntent modified_intent{};
    OmsRequestId oms_request_id{0};
    ClientOrderId client_order_id;
    IntentRejectReason reason{IntentRejectReason::kNone};
};

using IntentDecisionData =
    std::variant<std::monostate, AcceptedIntent, RejectedIntent, ModifiedIntent>;

struct IntentDecision {
    IntentDecisionCode code{IntentDecisionCode::kRejected};
    IntentDecisionData data;
    internal::TimestampNs decision_ts_ns{0};
};

struct SubmitOrderCmd {
    OmsRequestId oms_request_id{0};
    OrderIntent intent{};
    ClientOrderId client_order_id;
};

struct CancelOrderCmd {
    OmsRequestId oms_request_id{0};
    IntentOrigin origin{};
    ClientOrderId client_order_id;
    std::optional<ExchangeOrderId> exchange_order_id;
    internal::TimestampNs cmd_ts_ns{0};
};

struct ModifyOrderCmd {
    OmsRequestId oms_request_id{0};
    OrderIntent replacement_intent{};
    ClientOrderId client_order_id;
    std::optional<ExchangeOrderId> exchange_order_id;
};

enum class OrderLifecycleEventKind : std::uint8_t {
    kAck = 1,
    kReject = 2,
    kPartialFill = 3,
    kFill = 4,
    kCancelAck = 5,
    kCancelReject = 6,
    kReplaceAck = 7,
    kReplaceReject = 8,
    kCanceled = 9,
};

struct OrderAck {
    internal::QtyLots accepted_qty_lots{0};
};

struct OrderReject {
    std::string reason_code;
    std::string reason_message;
};

struct OrderFill {
    internal::QtyLots fill_qty_lots{0};
    internal::PriceTicks fill_price_ticks{0};
    internal::Side side{internal::Side::kUnknown};
    OmsLiquidity liquidity{OmsLiquidity::kUnknown};
    std::string raw_action;
    std::string raw_side;
    std::int8_t raw_is_yes{-1}; // -1 unknown, 0 false, 1 true
};

struct CancelAck {};

struct CancelReject {
    std::string reason_code;
    std::string reason_message;
};

struct ReplaceAck {
    internal::QtyLots replaced_qty_lots{0};
    std::optional<internal::PriceTicks> replaced_limit_price_ticks;
};

struct ReplaceReject {
    std::string reason_code;
    std::string reason_message;
};

using OrderLifecycleData = std::variant<std::monostate,
                                        OrderAck,
                                        OrderReject,
                                        OrderFill,
                                        CancelAck,
                                        CancelReject,
                                        ReplaceAck,
                                        ReplaceReject>;

struct OrderLifecycleEvent {
    IntentOrigin origin{};
    OmsRequestId oms_request_id{0};
    OrderLifecycleEventKind kind{OrderLifecycleEventKind::kReject};
    OmsOrderStatus status{OmsOrderStatus::kUnknown};
    ClientOrderId client_order_id;
    std::optional<ExchangeOrderId> exchange_order_id;
    internal::TimestampNs recv_ts_ns{0};
    OrderLifecycleData data;
};

struct OrderState {
    IntentOrigin origin{};
    OmsRequestId oms_request_id{0};
    OmsOrderStatus status{OmsOrderStatus::kUnknown};
    ClientOrderId client_order_id;
    std::optional<ExchangeOrderId> exchange_order_id;
    internal::QtyLots original_qty_lots{0};
    internal::QtyLots live_qty_lots{0};
    internal::QtyLots cum_fill_qty_lots{0};
    std::optional<internal::PriceTicks> live_limit_price_ticks;
    internal::TimestampNs last_update_ts_ns{0};
    /*
    latency fields below
    */
    internal::TimestampNs oms_decision_ts_ns{0};
    internal::TimestampNs transport_submit_ts_ns{0};
    internal::TimestampNs first_fill_recv_ns{0};
    internal::TimestampNs terminal_recv_ns{0};
};

// Reconcile requests flow from the private-WS thread (producer) to the REST thread
// (consumer) so that the persistent REST connection is owned by a single thread. Issued
// when the WS connection reconnects or a seq-gap is detected; on delivery the REST thread
// pages through GET /portfolio/orders and synthesises OrderLifecycleEvent acks for each
// still-open order. Coalescing is fine — a dropped duplicate reconcile is covered by the
// one already in flight.
enum class ReconcileReason : std::uint8_t {
    kReconnect = 1,
    kSeqGap = 2,
};

struct ReconcileRequest {
    ReconcileReason reason{ReconcileReason::kReconnect};
    internal::TimestampNs requested_ts_ns{0};
};

struct ExecutionRecord {
    IntentOrigin origin{};
    OmsRequestId oms_request_id{0};
    ClientOrderId client_order_id;
    std::optional<ExchangeOrderId> exchange_order_id;
    internal::MarketId market_id{0};
    internal::EventId event_id{0};
    internal::Side side{internal::Side::kUnknown};
    internal::QtyLots requested_qty_lots{0};
    internal::QtyLots filled_qty_lots{0};
    std::optional<internal::PriceTicks> initial_limit_price_ticks;
    std::optional<internal::PriceTicks> avg_fill_price_ticks;
    OmsOrderStatus terminal_status{OmsOrderStatus::kUnknown};
    internal::TimestampNs created_ts_ns{0};
    internal::TimestampNs completed_ts_ns{0};
};

} // namespace predex::core::oms::kalshi
