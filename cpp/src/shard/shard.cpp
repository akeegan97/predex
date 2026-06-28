#include "predex/shard/shard.hpp"
#include "predex/shard/shard_types.hpp"

#include <utility>


namespace predex::shard{

    Shard::Shard(
        std::uint32_t shard_index,
        ShardQueues queues,
        predex::ingest::kalshi::FramePool& frame_pool
    ) : shard_index_(shard_index), queues_(queues), frame_pool_(frame_pool){}

    bool Shard::install_universe(std::vector<KalshiEvent> events){
        return event_store_.initialize(std::move(events));
    }

    bool Shard::send_control_message(ShardToControlMessage message) noexcept{
        return queues_.shard_to_control_queue.try_push(std::move(message));
    }

    bool Shard::command_matches_shard(std::uint64_t universe_version, std::uint32_t command_shard_index) noexcept{
        if(command_shard_index != shard_index_){
            ShardFaulted faulted{
                .shard_index = command_shard_index,
                .universe_version = universe_version,
                .reason = "Shard index mismatch"
            };
            send_control_message(ShardToControlMessage{std::move(faulted)});
            return false;
        }
        if(installed_universe_version_ != 0 && universe_version != installed_universe_version_){
            ShardFaulted faulted{
                .shard_index = shard_index_,
                .universe_version = universe_version,
                .reason = "Universe version mismatch"
            };
            send_control_message(ShardToControlMessage{std::move(faulted)});
            return false;
        }
        return true;
    }

    std::uint32_t Shard::shard_index() const noexcept{
        return shard_index_;
    }

    const ShardStats& Shard::stats() const noexcept{
        return stats_;
    }

    bool Shard::terminal_handoff(const predex::ingest::kalshi::FrameHandle& handle) noexcept{
        //TODO: change return from bool to enum to indicate if handle was recycled or sent to logger on true, or if handle was leaked on false
        if(!queues_.shard_to_logger_queue.try_push(handle)){
            if(!queues_.last_resort_recycle_queue.try_push(handle)){
                ++stats_.leaked_handles;
                return false;
            }
            ++stats_.missed_frames_to_logger;
            ++stats_.frames_recycled;
            return true;
        }
        ++stats_.frames_to_logger;
        return true;
    }

    ShardPumpResult Shard::drain_one_market_data_handle() noexcept{
        ShardPumpResult result{};
        predex::ingest::kalshi::FrameHandle handle{};
        if(!queues_.router_to_shard_queue.try_pop(handle)){
            run_state_ = ShardRunState::kDRAINED;
            ShardDrainComplete drained{
                .universe_version = installed_universe_version_,
                .shard_index = shard_index_,
                .stats = stats_
            };
            (void)send_control_message(ShardToControlMessage{drained});
            result.code = ShardPumpCode::kDRAIN_COMPLETE;
            return result;
        }

        ++stats_.frames_seen;
        if(!terminal_handoff(handle)){
            result.code = ShardPumpCode::kHANDLE_LEAK;
            return result;
        }
        result.code = ShardPumpCode::kDRAINED_FRAME;
        return result;
    }

    ShardPumpResult Shard::pump_once() noexcept{
        ShardPumpResult result{};
        predex::ingest::kalshi::FrameHandle handle{};

        if(run_state_ == ShardRunState::kDRAINING){
            return drain_one_market_data_handle();
        }

        if(run_state_ == ShardRunState::kUNINSTALLED ||
           run_state_ == ShardRunState::kDRAINED ||
           run_state_ == ShardRunState::kFAULTED){
            result.code = ShardPumpCode::kIDLE;
            return result;
        }

        if(!queues_.router_to_shard_queue.try_pop(handle)){
            result.code = ShardPumpCode::kIDLE;
            return result;
        }
        ++stats_.frames_seen;

        const predex::ingest::kalshi::KalshiFrame* frame = frame_pool_.frame(handle);

        if(frame == nullptr){
            result.code = ShardPumpCode::kMISSING_FRAME;
            if(!terminal_handoff(handle)){
                result.code = ShardPumpCode::kHANDLE_LEAK;
            }

            return result;
        }

        KalshiParsedEvent parsed_event{};
        ParseResult parse_result = market_parser_.parse(handle, *frame, parsed_event);
        
        if(!parse_result.success){
            ++stats_.parse_rejects;
            result.code = ShardPumpCode::kPARSE_REJECTED;
            result.parse_result = parse_result;
            if(!terminal_handoff(handle)){
                result.code = ShardPumpCode::kHANDLE_LEAK;
            }
            return result;
        }

        EventApplyResult event_result = event_store_.apply(handle, parsed_event);
        switch(event_result.code){
            case EventApplyCode::kAPPLIED:
                ++stats_.frames_applied;
                result.code = ShardPumpCode::kAPPLIED;
                break;
            case EventApplyCode::kREJECTED:
                ++stats_.event_rejects;
                result.code = ShardPumpCode::kEVENT_REJECTED;
                result.event_result = event_result;
                if(!terminal_handoff(handle)){
                    result.code = ShardPumpCode::kHANDLE_LEAK;
                }
                return result;
            case EventApplyCode::kDESYNCED:
                ++stats_.event_desyncs;
                result.code = ShardPumpCode::kEVENT_DESYNCED;
                result.event_result = event_result;
                if(!terminal_handoff(handle)){
                    result.code = ShardPumpCode::kHANDLE_LEAK;
                }
                return result;
        }
        if(!terminal_handoff(handle)){
            result.code = ShardPumpCode::kHANDLE_LEAK;
            return result;
        }
        return result;
    }

    bool Shard::process_one_control_command() noexcept{
        ControlToShardCommand command{};
        if(!queues_.control_to_shard_queue.try_pop(command)){
            return false;
        }
        std::visit([&](auto& cmd){
            handle_operator_command(cmd);
        }, command);
        return true;
    }

    std::size_t Shard::drain_control_commands(std::size_t max_commands) noexcept{
        std::size_t commands_processed{0};
        while(commands_processed < max_commands && process_one_control_command()){
            ++commands_processed;
        }
        return commands_processed;
    }

    void Shard::handle_operator_command(InstallShardUniverse& command){
        if(command.shard_index != shard_index_){
            ShardFaulted faulted{
                .shard_index = command.shard_index,
                .universe_version = command.universe_version,
                .reason = "Shard index mismatch"
            };
            (void)send_control_message(ShardToControlMessage{std::move(faulted)});
            return;
        }
        const bool install_success = install_universe(std::move(command.events));
        if(install_success){
            installed_universe_version_ = command.universe_version;
            run_state_ = ShardRunState::kLIVE;
            ShardUniverseInstalled installed{
                .universe_version = command.universe_version,
                .shard_index = command.shard_index,
                .event_count = event_store_.size()
            };
            (void)send_control_message(ShardToControlMessage{installed});
            return;
        }
        run_state_ = ShardRunState::kFAULTED;
        ShardFaulted faulted{
            .shard_index = command.shard_index,
            .universe_version = command.universe_version,
            .reason = "Failed to install universe"
        };
        (void)send_control_message(ShardToControlMessage{std::move(faulted)}); 
    }

    void Shard::handle_operator_command(PrepareStopUniverse& command){
        if(!command_matches_shard(command.universe_version, command.shard_index)){
            return;
        }
        run_state_ = ShardRunState::kPREPARING_STOP;
//NOTE: next steps when OMS/Strategy is wired to check/handle stopping commands from Control Plane. Currently immediately transitioned to safe to stop state
        run_state_ = ShardRunState::kSAFE_TO_STOP;
        ShardSafeToStopUniverse safe_to_stop{
            .universe_version = command.universe_version,
            .shard_index = command.shard_index,
            .reason = "No OMS or strategy lifecycle attached"
        };
        (void)send_control_message(ShardToControlMessage{std::move(safe_to_stop)});
    }

    void Shard::handle_operator_command(DrainShardUniverse& command){
        if(!command_matches_shard(command.universe_version, command.shard_index)){
            return;
        }
        run_state_ = ShardRunState::kDRAINING;
    }

    void Shard::handle_operator_command(ResumeShardUniverse& command){
        if(!command_matches_shard(command.universe_version, command.shard_index)){
            return;
        }
        if(event_store_.size() == 0){
            run_state_ = ShardRunState::kFAULTED;
            ShardFaulted faulted{
                .shard_index = command.shard_index,
                .universe_version = command.universe_version,
                .reason = "Cannot resume shard without installed universe"
            };
            (void)send_control_message(ShardToControlMessage{std::move(faulted)});
            return;
        }
        run_state_ = ShardRunState::kLIVE;
    }
    void Shard::maybe_send_telemetry() noexcept {
        const auto now = std::chrono::steady_clock::now();
        if(now < next_telemetry_send_) {
            return;
        }

        (void)send_control_message(ShardToControlMessage{
            ShardTelemetry{
                .shard_index = shard_index_,
                .universe_version = installed_universe_version_,
                .stats = stats_,
            }
        });

        next_telemetry_send_ = now + kSHARD_TELEMETRY_INTERVAL;
    }


}
