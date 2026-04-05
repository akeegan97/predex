#pragma once
#include <cstddef>
#include <string_view>
#include <vector>
#include <fstream>
#include "predex/utils/spsc_queue.hpp"
#include "predex/ingest/frame_pool.hpp"



namespace predex::core::tape::kalshi{
    class Logger{
        public:
            explicit Logger(std::vector<predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>*> input_queues,
                    predex::core::ingest::kalshi::FramePool& frame_pool,
                    predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle> &recycle_queue,
                    std::string_view output_file_path);

            [[nodiscard]] std::size_t pump(std::size_t max_batch_size) noexcept;
            
            Logger(const Logger&) = delete;
            Logger& operator=(const Logger&) = delete;
            Logger(Logger&&) = delete;
            Logger& operator=(Logger&&) = delete;

        private:
            std::vector<predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>*> input_queues_; // terminal sink router/shard (eventually OMS queues as well) -> logger
            predex::core::ingest::kalshi::FramePool& frame_pool_; // shared frame pool for zero copy access to frames
            predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle> &recycle_queue_; // logger producer, IOWriter consumer
            std::ofstream output_file_; // file to write frames to, could be rotated based on size or time
            std::uint64_t logged_count_{0};
            std::uint64_t recycle_failed_count_{0};
            std::uint64_t write_failed_count_{0};
            std::size_t next_input_queue_{0}; // for round robin polling of input queues

    };
}// namespace predex::core::tape::kalshi
 