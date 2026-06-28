#pragma once 

#include <cstdint>
#include <vector>

#include "predex/shard/models.hpp"
#include "predex/shard/market_parser.hpp"
#include "predex/shard/event_store.hpp"
#include "predex/ingest/kalshi/market_data/frame_pool.hpp"
#include "predex/utils/spsc.hpp"
#include "predex/shard/shard_types.hpp"

namespace predex::shard{

    struct ShardQueues{
        predex::utils::SPSCQueue<predex::ingest::kalshi::FrameHandle>& router_to_shard_queue;
        predex::utils::SPSCQueue<predex::ingest::kalshi::FrameHandle>& shard_to_logger_queue;
        predex::utils::SPSCQueue<predex::ingest::kalshi::FrameHandle>& last_resort_recycle_queue;

        predex::utils::SPSCQueue<ShardToControlMessage>& shard_to_control_queue;
        predex::utils::SPSCQueue<ControlToShardCommand>& control_to_shard_queue;
    };
    const std::chrono::milliseconds kSHARD_TELEMETRY_INTERVAL{5000};
    enum class ShardPumpCode : std::uint8_t{
        kIDLE = 0,
        kAPPLIED = 1,
        kPARSE_REJECTED = 2,
        kEVENT_REJECTED = 3,
        kEVENT_DESYNCED = 4,
        kMISSING_FRAME = 5,
        kLOGGER_BACKPRESSURE = 6,
        kHANDLE_LEAK = 7,
        kDRAINED_FRAME = 8,
        kDRAIN_COMPLETE = 9,
    };

    struct ShardPumpResult{
        ShardPumpCode code{ShardPumpCode::kIDLE};
        ParseResult parse_result{};
        EventApplyResult event_result{};
    };



    class Shard{
        public:
            Shard(
                std::uint32_t shard_index,
                ShardQueues queues,
                predex::ingest::kalshi::FramePool& frame_pool
            );



            [[nodiscard]] ShardPumpResult pump_once() noexcept;
            [[nodiscard]] std::uint32_t shard_index() const noexcept;
            [[nodiscard]] const ShardStats& stats() const noexcept;

            [[nodiscard]] bool process_one_control_command() noexcept;
            [[nodiscard]] std::size_t drain_control_commands(std::size_t max_commands) noexcept;
            void maybe_send_telemetry() noexcept;

        private:
            [[nodiscard]] bool terminal_handoff(const predex::ingest::kalshi::FrameHandle& handle) noexcept;
            
            [[nodiscard]] bool install_universe(std::vector<KalshiEvent> events);
            bool send_control_message(ShardToControlMessage message) noexcept;
            [[nodiscard]] bool command_matches_shard(std::uint64_t universe_version, std::uint32_t command_shard_index) noexcept;
            [[nodiscard]] ShardPumpResult drain_one_market_data_handle() noexcept;

            void handle_operator_command(InstallShardUniverse& command);
            void handle_operator_command(PrepareStopUniverse& command);
            void handle_operator_command(DrainShardUniverse& command);
            void handle_operator_command(ResumeShardUniverse& command);




            std::uint32_t shard_index_{0};
            std::uint64_t installed_universe_version_{0};
            ShardRunState run_state_{ShardRunState::kUNINSTALLED};
            ShardQueues queues_;
            predex::ingest::kalshi::FramePool& frame_pool_;

            EventStore event_store_;
            MarketParser market_parser_;
            ShardStats stats_;
            std::chrono::steady_clock::time_point next_telemetry_send_;
    };

}
