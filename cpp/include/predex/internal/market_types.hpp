#pragma once

#include <cstdint>

namespace predex::internal {

using EventId = std::uint32_t;
using MarketId = std::uint32_t;
using SequenceId = std::uint64_t;
using PriceTicks = std::int64_t;
using QtyLots = std::int64_t;
using TimestampNs = std::uint64_t;
using AffinityKey = std::uint64_t;

enum class ExchangeId : std::uint8_t {
    kUnknown = 0,
    kKalshi = 1,
    kPolymarket = 2,
};

enum class EventType : std::uint8_t {
    kUnknown = 0,
    kSnapshot = 1,
    kDelta = 2,
    kTrade = 3,
    kHeartbeat = 4,
    kStatus = 5,
    kLifecycle = 6,
};

enum class MarketLifecycleStatus : std::uint8_t {
    kUnknown = 0,
    kCreated = 1,
    kActivated = 2,
    kDeactivated = 3,
    kCloseDateUpdated = 4,
    kDetermined = 5,
    kSettled = 6,
    kFractionalTradingUpdated = 7,
    kPriceLevelStructureUpdated = 8,
};

enum class Side : std::uint8_t {
    kUnknown = 0,
    kBid = 1,
    kAsk = 2,
    kBuy = 3,
    kSell = 4,
};

} // namespace predex::internal
