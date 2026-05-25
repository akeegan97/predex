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
//process state
    enum class ProcessStatus : std::uint8_t{
        kBooting,
        kWaitingForIo,
        kIoConnected,
        kReady,//ready to start trading but not live yet
        kLive, //live trading
        kShuttingDown,
        kStopped,
        kFaulted, // other transition states later if needed
    };

    struct ProcessState{
        ProcessStatus status{ProcessStatus::kBooting};
    };


//IO types 

    enum class ControlIoCommandType : std::uint8_t{
        kRefresh,
        kReconnect,
        kDisconnect
    };
    struct ControlIoCommand{
        ControlIoCommandType type;
    };

    enum class IoControlStatusType : std::uint8_t{
        kDisconnected,
        kConnected
    };
    struct IoControlStatus{
        IoControlStatusType type;
    };

    enum class IoControlStateType : std::uint8_t{
        kDisconnected,
        kConnected,
        kReconnecting
    };
    struct IoControlState{
        IoControlStateType current_state{IoControlStateType::kDisconnected};
        IoControlStateType target_state{IoControlStateType::kDisconnected};
        ControlIoCommandType last_cmd_sent{ControlIoCommandType::kDisconnect};
        bool transition_in_flight{false};
    };



}