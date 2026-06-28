#pragma once 

#include <cstddef>
#include <vector>

#include "predex/operator/operator_commands.hpp"
#include "predex/utils/spsc.hpp"
#include "predex/control/control_types.hpp"
#include "predex/router/router_types.hpp"
#include "predex/shard/shard_types.hpp"


namespace predex::core::control{

    struct OperatorQueues{
        utils::SPSCQueue<::predex::operator_admin::OperatorCommand>& operator_command_queue;
        utils::SPSCQueue<::predex::operator_admin::OperatorResponse>& operator_response_queue;
    };

    struct ControlIoQueues{
        utils::SPSCQueue<ControlToIoCommand>& control_to_io_queue;
        utils::SPSCQueue<IoToControlStatus>& io_to_control_status_queue;
    };

    struct RouterQueue{
        utils::SPSCQueue<router::RouterToControl>& router_to_control_queue;
    };

    struct ControlLoggerQueue{
        utils::SPSCQueue<LoggerToControlStatus>* logger_to_control_status_queue{nullptr};
    };

    struct ControlShardQueues{
        std::vector<utils::SPSCQueue<shard::ControlToShardCommand>*> control_to_shard_queues;
        std::vector<utils::SPSCQueue<shard::ShardToControlMessage>*> shard_to_control_queues;
    };

    struct OperatorPumpResult{
        std::size_t commands_processed{0};
        std::size_t responses_pushed_success{0};
        std::size_t responses_pushed_failure{0};
    };

    struct IoPumpResult{
        std::size_t statuses_processed{0};
        std::size_t commands_pushed_success{0};
        std::size_t commands_pushed_failure{0};
    };

    struct ShardPumpResult{
        std::size_t statuses_processed{0};
        std::size_t commands_pushed_success{0};
        std::size_t commands_pushed_failure{0};
    };


    class ControlPlane{
        public: 
            ControlPlane(
                OperatorQueues queues,
                ControlIoQueues io_queues,
                RouterQueue router_queue,
                ControlShardQueues shard_queues = {},
                ControlLoggerQueue logger_queue = {}
            );
        
            [[nodiscard]] OperatorPumpResult process_operator_commands();

            bool push_operator_response(const ::predex::operator_admin::OperatorResponse& response){
                return queues_.operator_response_queue.try_push(response);
            }

            [[nodiscard]] IoPumpResult process_io_status() noexcept;

            bool push_io_command(const ControlToIoCommand& cmd){
                return io_queues_.control_to_io_queue.try_push(cmd);
            }

            [[nodiscard]] ProcessState process_state() const {
                return process_state_;
            }

            [[nodiscard]] std::shared_ptr<const UniverseSnapshot> active_universe() const noexcept{
                return active_universe_;
            }

            [[nodiscard]] std::uint64_t install_universe(UniverseSnapshot snapshot);
            [[nodiscard]] bool send_active_universe_to_io();
            [[nodiscard]] ShardPumpResult send_active_universe_to_shards();
            [[nodiscard]] ShardPumpResult prepare_active_universe_stop_on_shards();
            [[nodiscard]] ShardPumpResult drain_active_universe_on_shards();
            [[nodiscard]] ShardPumpResult resume_active_universe_on_shards();

            [[nodiscard]] bool process_one_router_message() noexcept;

            [[nodiscard]] bool process_router_messages() noexcept;

            [[nodiscard]] bool process_one_shard_message() noexcept;
            [[nodiscard]] bool process_shard_messages() noexcept;

            [[nodiscard]] bool process_one_logger_message() noexcept;
            [[nodiscard]] bool process_logger_messages() noexcept;


        private:
            OperatorQueues queues_;
            ControlIoQueues io_queues_;
            ProcessState process_state_{LifecyclePhase::kBOOTING};

            void recompute_process_state() noexcept;
            void apply_io_status(const IoToControlStatus& status) noexcept;
            void apply_shard_status(const shard::ShardToControlMessage& status) noexcept;
            void apply_logger_status(const LoggerToControlStatus& status) noexcept;
            [[nodiscard]] bool push_shard_command(std::uint32_t shard_index, shard::ControlToShardCommand command);

            std::shared_ptr<const UniverseSnapshot> active_universe_;
            std::uint64_t next_universe_version_{1};
            RouterQueue router_queue_;
            ControlShardQueues shard_queues_;
            ControlLoggerQueue logger_queue_;
    };

}
