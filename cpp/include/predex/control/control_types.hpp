#pragma once 
#include <cstdint>

namespace predex::core::control{
    // types used to communicate to and from control plane. 
/*
    Shard -> ControlPlane
    OMS -> ControlPlane 
    ControlPlane -> OMS 
    ControlPlane -> WS Client 
    Operator -> ControlPlane
    ControlPlane -> Operator 
*/

    enum class ControlPlaneState: std::uint8_t {
        kInitializing,
        kRunning,
        kShuttingDown
    };

    enum class OperatorCommandType: std::uint8_t{
        kStart,
        kRefresh,
        kPause,
        kStop
    };
    struct OperatorCommand{
        OperatorCommandType type{OperatorCommandType::kStart};
    };

    enum class ShardToControlEventType: std::uint8_t{
        kWorking,
        kDesync,
        kStopped
    };

    struct ShardToControlEvent{
        ShardToControlEventType type{ShardToControlEventType::kWorking};
        std::uint32_t shard_id{0};
    };

    enum class OmsToControlEventType: std::uint8_t{
    };




}