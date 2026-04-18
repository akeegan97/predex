#pragma once

#include <cstdint>
namespace predex::internal {
enum class EventTopologyKind : std::uint8_t {
    kUnknown = 0,
    kMonotonicChain = 1,
    kMutuallyExclusive = 2,
    kUnorderedGroup = 3,
    kSingleMarket = 4,
};

}