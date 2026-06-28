#pragma once

#include <cstdint>
#include <string>
#include <variant>
#include <vector>
#include "predex/control/control_lifecycle.hpp"

namespace predex::operator_admin {

enum class OperatorCommandType : std::uint8_t {
    kUNKNOWN = 0,
    kSTATUS = 1,
    kSHUTDOWN_GRACEFUL = 2,
    kSHUTDOWN_FORCEFUL = 3,
    kCOUNTERSTATS = 4,
};

using OperatorCommandId = std::uint64_t;

struct OperatorCommand {
    OperatorCommandType type{OperatorCommandType::kUNKNOWN};
    OperatorCommandId request_id{0};
};

enum class OperatorResponseType : std::uint8_t {
    kACK = 0,
    kERROR = 1,
    kSTATUS = 2,
    kCOUNTERSTATS = 3,
};



struct OperatorStatusSnapshot {
    predex::core::control::LifecyclePhase lifecycle{predex::core::control::LifecyclePhase::kBOOTING};
    bool trading_enabled{false};
    bool shutdown_requested{false};
};

struct IoCounterStats{
    bool io_connected{false};
    std::uint64_t installed_universe_version{};
    std::uint64_t subscribed_universe_version{};
    std::uint64_t frames_received{};
    std::uint64_t frames_published{};
    std::uint64_t frames_dropped{};
    std::uint64_t recycle_failures{};
    std::string last_error;
};
struct RouterCounterStats{
    std::uint64_t frames_seen{};
    std::uint64_t frames_to_shards{};
    std::uint64_t frames_to_logger{};
    std::uint64_t frames_recycled{};
    std::uint64_t leaked_handles{};
};
struct LoggerCounterStats{
    std::string output_file_path;
    std::uint64_t records_written{};
    std::uint64_t bytes_written{};
    std::uint64_t write_failures{};
    std::uint64_t recycle_failures{};
};
struct ShardCounterStats{
    std::uint64_t shard_index{};
    std::uint64_t frames_seen{};
    std::uint64_t frames_applied{};
    std::uint64_t parse_rejects{};
    std::uint64_t event_rejects{};
    std::uint64_t event_desyncs{};
    std::uint64_t frames_to_logger{};
    std::uint64_t frames_recycled{};
    std::uint64_t leaked_handles{};
    std::uint64_t missed_frames_to_logger{};
};


struct OperatorCounterStatsSnapshot{
    OperatorStatusSnapshot status_snapshot;
    IoCounterStats io_stats;
    RouterCounterStats router_stats;
    std::vector<ShardCounterStats> shard_stats;
    LoggerCounterStats logger_stats;
};
struct AckPayload {};

struct ErrorPayload {
    std::string message;
};

using OperatorResponsePayload =
    std::variant<AckPayload, ErrorPayload, OperatorStatusSnapshot, OperatorCounterStatsSnapshot>;

struct OperatorResponse {
    OperatorCommandId request_id{0};
    OperatorResponseType type{OperatorResponseType::kACK};
    OperatorResponsePayload payload{AckPayload{}};
};

}  // namespace predex::operator_admin
