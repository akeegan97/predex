#include "predex/config/universe_builder.hpp"

#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {

using predex::core::control::AffinityKey;
using predex::core::control::EventId;
using predex::core::control::EventTopology;
using predex::core::control::MarketId;
using predex::core::control::PriceLevelStructure;

[[nodiscard]] std::uint64_t parse_uint64(std::string_view value, std::string_view field_name) { //NOLINT
    if (value.empty()) {
        throw std::runtime_error(std::string{field_name} + " must not be empty");
    }

    std::size_t parsed_chars{0};
    const std::uint64_t parsed = std::stoull(std::string{value}, &parsed_chars, 10);
    if (parsed_chars != value.size()) {
        throw std::runtime_error(std::string{field_name} + " must be an unsigned integer");
    }
    return parsed;
}

[[nodiscard]] MarketId parse_market_id(std::string_view value) {
    const std::uint64_t parsed = parse_uint64(value, "market_id");
    if (parsed > std::numeric_limits<MarketId>::max()) {
        throw std::runtime_error("market_id exceeds MarketId range");
    }
    return static_cast<MarketId>(parsed);
}

[[nodiscard]] EventId parse_event_id(std::string_view value) {
    const std::uint64_t parsed = parse_uint64(value, "event_id");
    if (parsed > std::numeric_limits<EventId>::max()) {
        throw std::runtime_error("event_id exceeds EventId range");
    }
    return static_cast<EventId>(parsed);
}

[[nodiscard]] AffinityKey parse_affinity_key(std::string_view value) {
    return parse_uint64(value, "affinity_key");
}

[[nodiscard]] EventTopology parse_event_topology(std::string_view value) {
    if (value == "monotonic_chain") {
        return EventTopology::kMONOTONIC_CHAIN;
    }
    if (value == "mutually_exclusive") {
        return EventTopology::kMUTUALLY_EXCLUSIVE;
    }
    if (value == "unordered_group") {
        return EventTopology::kUNORDERED_GROUP;
    }
    if (value == "single_market") {
        return EventTopology::kSINGLE_MARKET;
    }
    throw std::runtime_error("Unknown event topology: " + std::string{value});
}

[[nodiscard]] PriceLevelStructure parse_price_level_structure(std::string_view value) {
    if (value == "linear_cent") {
        return PriceLevelStructure::kLINEAR_CENT;
    }
    if (value == "tapered_deci_cent") {
        return PriceLevelStructure::kTAPERED_DECI_CENT;
    }
    if (value == "deci_cent") {
        return PriceLevelStructure::kDECI_CENT;
    }
    throw std::runtime_error("Unknown price_level_structure: " + std::string{value});
}

[[nodiscard]] std::uint32_t checked_u32(std::size_t value, std::string_view field_name) {
    if (value > std::numeric_limits<std::uint32_t>::max()) {
        throw std::runtime_error(std::string{field_name} + " exceeds uint32_t range");
    }
    return static_cast<std::uint32_t>(value);
}

}  // namespace

namespace predex::config {

predex::core::control::UniverseSnapshot build_universe_snapshot(
    const AppConfig& config,
    std::uint32_t shard_count
) {
    using namespace predex::core::control;

    if (shard_count == 0) {
        throw std::runtime_error("Cannot build universe snapshot with zero shards");
    }

    UniverseSnapshot snapshot{};
    snapshot.events.reserve(config.universe.events.size());
    std::vector<std::uint32_t> next_shard_event_index(shard_count, 0);

    for (const auto& event_config : config.universe.events) {
        const EventId event_id = parse_event_id(event_config.event_id);
        const AffinityKey affinity_key = parse_affinity_key(event_config.affinity_key);
        const EventTopology topology = parse_event_topology(event_config.topology);
        const auto shard_index = static_cast<std::uint32_t>(affinity_key % shard_count);
        const std::uint32_t shard_event_index = next_shard_event_index[shard_index]++;

        UniverseEvent event{
            .event_id = event_id,
            .affinity_key = affinity_key,
            .topology = topology,
            .markets = {},
        };
        event.markets.reserve(event_config.markets.size());

        for (std::size_t market_index = 0; market_index < event_config.markets.size(); ++market_index) {
            const auto& market_config = event_config.markets[market_index];
            const MarketId market_id = parse_market_id(market_config.market_id);
            const PriceLevelStructure price_level_structure =
                parse_price_level_structure(market_config.price_level_structure);
            const std::uint32_t event_market_index = checked_u32(market_index, "event_market_index");

            UniverseMarket market{
                .market_id = market_id,
                .kalshi_ticker = market_config.kalshi_ticker,
                .tradeable = market_config.tradeable,
                .price_level_structure = price_level_structure,
            };
            event.markets.push_back(std::move(market));

            snapshot.market_routes.push_back(UniverseMarketRoute{
                .kalshi_ticker = market_config.kalshi_ticker,
                .market_id = market_id,
                .event_id = event_id,
                .affinity_key = affinity_key,
                .topology = topology,
                .shard_index = shard_index,
                .shard_event_index = shard_event_index,
                .event_market_index = event_market_index,
                .tradeable = market_config.tradeable,
                .price_level_structure = price_level_structure,
            });
        }

        snapshot.events.push_back(std::move(event));
    }

    return snapshot;
}

}  // namespace predex::config
