#pragma once

#include <unordered_map>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include "predex/utils/spsc_queue.hpp"
#include "predex/ingest/frame_pool.hpp"
#include "predex/router/market_registry.hpp"
#include "predex/router/shard_dispatch.hpp"
#include <simdjson.h>



namespace predex::core::routing::kalshi{
    struct RouterTelemetry{
        std::size_t processed_frames_{0};
        std::size_t dropped_frames_{0};
    };

    enum class RouteDecision:std::uint8_t{
        kToShard = 1,
        kToLogger = 2,
        kDrop = 3
    };
    class Router{
        public:
            explicit Router(predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& ingress_queue,
                predex::core::ingest::kalshi::FramePool& frame_pool,
                const predex::core::routing::kalshi::MarketRegistry &market_registry,
                predex::core::routing::kalshi::ShardDispatch &shard_dispatch,
                predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& logger_queue) noexcept;

            [[nodiscard]] std::size_t pump(std::size_t max_batch_size) noexcept;

        private:
            predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& ingress_queue_; // IOWriter producer, Router consumer
            predex::core::ingest::kalshi::FramePool& frame_pool_;
            RouterTelemetry telemetry_;
            const predex::core::routing::kalshi::MarketRegistry& market_registry_;
            predex::core::routing::kalshi::ShardDispatch &shard_dispatch_;
            predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& logger_queue_; // Router producer, logger consumer

            std::unordered_map<std::uint32_t, std::uint64_t> last_seq_by_sid_; //global checker for messages

            simdjson::ondemand::parser parser_; //parser instance for reuse to avoid simdjson parser construction overhead

            [[nodiscard]] bool process_one() noexcept;
            [[nodiscard]] RouteDecision classify(predex::core::ingest::kalshi::FrameHandle& handle, const predex::core::ingest::kalshi::KalshiFrame& frame) noexcept; //need to know where to send after classified or failed
            [[nodiscard]] bool lookup_route(predex::core::ingest::kalshi::FrameHandle& handle, std::string_view market_ticker) const noexcept;
            [[nodiscard]] bool check_sequence(const predex::core::ingest::kalshi::FrameHandle& handle) noexcept; //check and uses last_seq_by_sid_ to determine if the message is in order, duplicate, or out of order. Updates last_seq_by_sid_ if in order.
            [[nodiscard]] bool forward_to_logger(const predex::core::ingest::kalshi::FrameHandle& handle) noexcept;
            [[nodiscard]] static std::size_t compute_shard_id(std::uint16_t affinity_key, std::size_t shard_count) noexcept;
            [[nodiscard]] static std::uint64_t monotonic_now_ns() noexcept;
    };
}// namespace predex::core::routing::kalshi