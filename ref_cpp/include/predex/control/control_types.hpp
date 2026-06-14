#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <memory>

#include "predex/internal/event_topology.hpp"
#include "predex/internal/market_types.hpp"

namespace predex::core::control {

enum class ProcessStatus : std::uint8_t {
    kBooting,
    kWaitingForIo,
    kIoConnected,
    kReady,
    kLive,
    kShuttingDown,
    kStopped,
    kFaulted,
};

struct ProcessState {
    ProcessStatus status{ProcessStatus::kBooting};
};

struct MarketRoute{
    std::string kalshi_ticker;
    internal::MarketId market_id{};
    internal::EventId event_id{};
    internal::AffinityKey affinity_key{}; // pre-computed shard key for this market, used to route messages to the correct shard
    internal::EventTopologyKind topology_kind{internal::EventTopologyKind::kUnknown};
    std::int64_t strike_key{}; 
    std::uint64_t close_time_s{};
    bool tradeable{false};
};

struct MarketRouteUniverse{
    std::uint64_t version{0};
    std::vector<MarketRoute> routes;
};


struct IoMarketSubscription {
    internal::MarketId id{};
    std::string kalshi_ticker;
};


struct IoSubscriptionUniverse {
    std::uint64_t version{0};
    std::vector<IoMarketSubscription> markets;
};

enum class ControlIoCommandType : std::uint8_t {
    kRefresh,
    kReconnect,
    kDisconnect,
    kRecoverMarket,
    kApplyUniverseSnapshot,
};

struct ControlIoCommand {
    ControlIoCommandType type{ControlIoCommandType::kDisconnect};

    std::optional<internal::MarketId> market_id;
    std::shared_ptr<const IoSubscriptionUniverse> universe_snapshot;

    static ControlIoCommand disconnect() {
        return {.type = ControlIoCommandType::kDisconnect};
    }

    static ControlIoCommand reconnect() {
        return {.type = ControlIoCommandType::kReconnect};
    }

    static ControlIoCommand apply_universe_snapshot(std::shared_ptr<const IoSubscriptionUniverse> snapshot) {
        ControlIoCommand cmd{};
        cmd.type = ControlIoCommandType::kApplyUniverseSnapshot;
        cmd.universe_snapshot = std::move(snapshot);
        return cmd;
    }

    static ControlIoCommand recover_market(internal::MarketId id){ //NOLINT 
        ControlIoCommand cmd{};
        cmd.type = ControlIoCommandType::kRecoverMarket;
        cmd.market_id = id;
        return cmd;
    }
};

enum class IoControlStatusType : std::uint8_t {
    kDisconnected,
    kConnected,
    kUniverseSnapshotReceived,
    kSubscriptionStarted,
    kSubscriptionReady,
    kSubscriptionFailed,
    kUniverseApplied,
};

struct IoControlStatus {
    IoControlStatusType type{IoControlStatusType::kDisconnected};
    std::uint64_t universe_version{0};
};

enum class IoControlStateType : std::uint8_t {
    kDisconnected,
    kConnected,
    kReconnecting
};

struct IoControlState {
    IoControlStateType current_state{IoControlStateType::kDisconnected};
    IoControlStateType target_state{IoControlStateType::kDisconnected};
    ControlIoCommandType last_cmd_sent{ControlIoCommandType::kDisconnect};
    bool transition_in_flight{false};
};

} // namespace predex::core::control