#pragma once 

#include <cstddef>
#include <cstdint>
#include <vector>
#include <chrono>

#include "predex/operator/operator_commands.hpp"
#include "predex/utils/spsc.hpp"
#include "predex/control/control_types.hpp"
#include "predex/router/router_types.hpp"
#include "predex/shard/shard_types.hpp"
#include "predex/control/recovery_coordinator.hpp"


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

    struct ControlOmsQueues{
        utils::SPSCQueue<ControlToOmsCommand>* control_to_oms_queue{nullptr};
        utils::SPSCQueue<OmsToControlStatus>* oms_to_control_status_queue{nullptr};
    };

    struct ControlPrivateOrderFeedQueues{
        utils::SPSCQueue<ControlToPrivateOrderFeedCommand>* control_to_private_order_feed_queue{nullptr};
        utils::SPSCQueue<PrivateOrderFeedToControlStatus>* private_order_feed_to_control_status_queue{nullptr};
    };

    struct ControlOrderRestQueues{
        utils::SPSCQueue<ControlToOrderRestCommand>* control_to_order_rest_queue{nullptr};
        utils::SPSCQueue<OrderRestToControlStatus>* order_rest_to_control_status_queue{nullptr};
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

    struct RequiredComponents{
        bool market_data{false};
        bool shards{true};
        bool logger{true};
        bool oms{false};
        bool private_order_feed{false};
        bool order_rest{false};
    };

    struct SyntheticTradingSessionConfig{
        bool enabled{false};
        std::uint64_t reduce_only_after_ns{0};
        std::uint64_t flatten_to_zero_after_ns{0};
        std::uint64_t stopped_after_ns{0};
    };

    enum class RecoveryPumpCode : std::uint8_t{
        kOK,
        kIO_BACKPRESSURE,
        kCOORDINATOR_COMMIT_FAILED,
    };

    struct RecoveryPumpResult{
        RecoveryPumpCode code{RecoveryPumpCode::kOK};
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
                ControlLoggerQueue logger_queue = {},
                ControlOmsQueues oms_queues = {},
                ControlPrivateOrderFeedQueues private_order_feed_queues = {},
                ControlOrderRestQueues order_rest_queues = {},
                RequiredComponents required_components = {},
                SyntheticTradingSessionConfig synthetic_session_config = {}
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

            [[nodiscard]] std::shared_ptr<const OrderRouteUniverse> active_order_universe() const noexcept{
                return active_order_universe_;
            }

            [[nodiscard]] std::uint64_t install_universe(UniverseSnapshot snapshot);
            [[nodiscard]] bool send_active_universe_to_io();
            [[nodiscard]] bool send_active_order_universe_to_oms();
            [[nodiscard]] bool send_active_order_universe_to_private_order_feed();
            [[nodiscard]] bool send_active_order_universe_to_order_rest();
            [[nodiscard]] bool push_private_order_feed_command(const ControlToPrivateOrderFeedCommand& cmd);
            [[nodiscard]] bool push_order_rest_command(const ControlToOrderRestCommand& cmd);
            [[nodiscard]] bool push_oms_command(const ControlToOmsCommand& cmd);
            [[nodiscard]] ShardPumpResult send_active_universe_to_shards();
            [[nodiscard]] ShardPumpResult prepare_active_universe_stop_on_shards();
            [[nodiscard]] ShardPumpResult drain_active_universe_on_shards();
            [[nodiscard]] ShardPumpResult resume_active_universe_on_shards();

            [[nodiscard]] RecoveryPumpResult process_recovery(RecoveryCoordinator::TimePoint now) noexcept;

            [[nodiscard]] bool process_one_router_message() noexcept;

            [[nodiscard]] bool process_router_messages() noexcept;

            [[nodiscard]] bool process_one_shard_message() noexcept;
            [[nodiscard]] bool process_shard_messages() noexcept;

            [[nodiscard]] bool process_one_logger_message() noexcept;
            [[nodiscard]] bool process_logger_messages() noexcept;

            [[nodiscard]] bool process_one_oms_status() noexcept;
            [[nodiscard]] bool process_oms_status() noexcept;

            [[nodiscard]] bool process_one_private_order_feed_status() noexcept;
            [[nodiscard]] bool process_private_order_feed_status() noexcept;

            [[nodiscard]] bool process_one_order_rest_status() noexcept;
            [[nodiscard]] bool process_order_rest_status() noexcept;
            [[nodiscard]] bool update_trading_session_phase() noexcept;


        private:
            OperatorQueues queues_;
            ControlIoQueues io_queues_;
            ProcessState process_state_{LifecyclePhase::kBOOTING};

            void recompute_process_state() noexcept;
            void apply_io_status(const IoToControlStatus& status) noexcept;
            void apply_shard_status(const shard::ShardToControlMessage& status) noexcept;
            void apply_logger_status(const LoggerToControlStatus& status) noexcept;
            void apply_oms_status(const OmsToControlStatus& status) noexcept;
            void apply_private_order_feed_status(const PrivateOrderFeedToControlStatus& status) noexcept;
            void apply_order_rest_status(const OrderRestToControlStatus& status) noexcept;
            [[nodiscard]] bool push_shard_command(std::uint32_t shard_index, shard::ControlToShardCommand command);

            [[nodiscard]] bool required_components_faulted() const;
            [[nodiscard]] bool required_components_ready_for_capture() const;
            [[nodiscard]] bool required_components_ready_for_trading() const;
            [[nodiscard]] TradingSessionPhase compute_trading_session_phase() const noexcept;
            void apply_trading_session_phase(TradingSessionPhase phase) noexcept;

            std::shared_ptr<const UniverseSnapshot> active_universe_;
            std::shared_ptr<const OrderRouteUniverse> active_order_universe_;
            std::uint64_t next_universe_version_{1};
            RouterQueue router_queue_;
            ControlShardQueues shard_queues_;
            ControlLoggerQueue logger_queue_;
            ControlOmsQueues oms_queues_;
            ControlPrivateOrderFeedQueues private_order_feed_queues_;
            ControlOrderRestQueues order_rest_queues_;
            RequiredComponents required_components_;
            SyntheticTradingSessionConfig synthetic_session_config_;
            RecoveryCoordinator recovery_coordinator_;
            std::chrono::steady_clock::time_point session_start_time_{std::chrono::steady_clock::now()};
            bool recovery_orchestration_faulted_{false};
    };

}
