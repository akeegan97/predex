#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <variant>

#include "trading/internal/market_types.hpp"

namespace trading::internal {

using OrderRequestId = std::uint64_t;
using ClientOrderId = std::string;
using ExchangeOrderId = std::string;

enum class OmsAction : std::uint8_t {
    kUnknown = 0,
    kPlace = 1,
    kCancel = 2,
    kReplace = 3,
};

enum class OmsTimeInForce : std::uint8_t {
    kUnknown = 0,
    kGtc = 1,
    kIoc = 2,
    kFok = 3,
};

enum class OmsOrderStatus : std::uint8_t {
    kUnknown = 0,
    kAccepted = 1,
    kRejected = 2,
    kCanceled = 3,
    kPartiallyFilled = 4,
    kFilled = 5,
    kReplaced = 6,
};

enum class OmsLiquidity : std::uint8_t {
    kUnknown = 0,
    kMaker = 1,
    kTaker = 2,
};

struct OrderIntent {
    ExchangeId exchange{ExchangeId::kUnknown};
    OmsAction action{OmsAction::kUnknown};
    ClientOrderId client_order_id;
    std::optional<ClientOrderId> target_client_order_id;
    std::optional<ExchangeOrderId> target_exchange_order_id;
    std::string market_ticker;
    Side side{Side::kUnknown};
    QtyLots qty_lots{0};
    std::optional<PriceTicks> limit_price_ticks;
    OmsTimeInForce time_in_force{OmsTimeInForce::kUnknown};
    TimestampNs intent_ts_ns{0};
};

struct OrderAck {
    ClientOrderId client_order_id;
    std::optional<ExchangeOrderId> exchange_order_id;
    std::string market_ticker;
    QtyLots accepted_qty_lots{0};
};

struct OrderReject {
    ClientOrderId client_order_id;
    std::optional<ExchangeOrderId> exchange_order_id;
    std::string reason_code;
    std::string reason_message;
};

struct OrderFill {
    ClientOrderId client_order_id;
    std::optional<ExchangeOrderId> exchange_order_id;
    std::string market_ticker;
    QtyLots fill_qty_lots{0};
    PriceTicks fill_price_ticks{0};
    Side side{Side::kUnknown};
    OmsLiquidity liquidity{OmsLiquidity::kUnknown};
};

using OmsUpdateData = std::variant<std::monostate, OrderAck, OrderReject, OrderFill>;

struct OrderStateUpdate {
    ExchangeId exchange{ExchangeId::kUnknown};
    OmsOrderStatus status{OmsOrderStatus::kUnknown};
    ClientOrderId client_order_id;
    std::optional<ExchangeOrderId> exchange_order_id;
    std::string market_ticker;
    TimestampNs recv_ts_ns{0};
    OmsUpdateData data;
    std::string raw_payload;
};

} // namespace trading::internal
