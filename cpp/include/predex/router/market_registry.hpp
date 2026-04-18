#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include "predex/internal/event_topology.hpp"

namespace predex::core::routing::kalshi{
    struct MarketRoute {
    std::uint32_t market_id_{0};
    std::uint32_t event_id_{0};
    std::uint16_t affinity_key_{0};
    internal::EventTopologyKind topology_kind_{internal::EventTopologyKind::kUnknown};
    std::int64_t strike_key_{0};
    };
    struct MarketRegistryEntry{
        std::string ticker_;
        std::string event_ticker_;
        std::uint32_t market_id_{0};
        std::uint32_t event_id_{0};
        std::uint16_t affinity_key_{0};
        internal::EventTopologyKind topology_kind_{internal::EventTopologyKind::kUnknown};
        std::int64_t strike_key_{0}; //used for monotonic chain topology to determine ordering of markets within the chain, not used for other topologies
    };

    class MarketRegistry{
        public:
            MarketRegistry() = default;
            explicit MarketRegistry(const std::vector<MarketRegistryEntry>& entries);

            MarketRegistry(const MarketRegistry&) = delete;
            MarketRegistry& operator=(const MarketRegistry&) = delete;
            MarketRegistry(MarketRegistry&&) = delete;
            MarketRegistry& operator=(MarketRegistry&&) = delete;

            [[nodiscard]] bool try_lookup(std::string_view ticker, MarketRoute& out) const noexcept;
            [[nodiscard]] std::size_t size() const noexcept;

        private:
            struct TransparentHash {
                using is_transparent = void;

                [[nodiscard]] std::size_t operator()(std::string_view value) const noexcept {
                    return std::hash<std::string_view>{}(value);
                }

                [[nodiscard]] std::size_t operator()(const std::string& value) const noexcept {
                    return std::hash<std::string_view>{}(value);
                }
            };

            struct TransparentEqual{
                using is_transparent = void;
                [[nodiscard]] bool operator()(std::string_view lhs, std::string_view rhs) const noexcept {
                    return lhs == rhs;
                }
            };
            std::unordered_map<std::string, MarketRoute, TransparentHash, TransparentEqual> routes_;
        };
}