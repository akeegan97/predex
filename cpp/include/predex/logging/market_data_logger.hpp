#pragma once 
#include "predex/control/control_types.hpp"
#include "predex/ingest/kalshi/market_data/frame_pool.hpp"
#include "predex/utils/spsc.hpp"
#include "predex/utils/latency_histogram.hpp"
#include "predex/utils/monotonic_clock.hpp"
#include <cstddef>
#include <chrono>
#include <fstream>
#include <filesystem>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace predex::logging{

    inline constexpr std::chrono::milliseconds kLOGGER_TELEMETRY_INTERVAL{250};

    struct MarketDataLoggerDeps{
        std::vector<predex::utils::SPSCQueue<predex::ingest::kalshi::FrameHandle>*> input_queues;
        predex::ingest::kalshi::FramePool& frame_pool;
        predex::utils::SPSCQueue<predex::ingest::kalshi::FrameHandle>& recycle_queue;
        predex::utils::SPSCQueue<predex::core::control::LoggerToControlStatus>* logger_to_control_status_queue{nullptr};
        std::string_view output_file_path;
    };

    struct BinaryHeader{
        constexpr static char kTAPEMAGIC[4] = {'P', 'D', 'T', '2'}; //NOLINT 
        constexpr static std::uint16_t kTAPEVERSION = 2;
        constexpr static std::uint16_t kTAPEFLAGS = 0;
    };
    struct MarketDataTapeRecordHeader {
        std::uint64_t universe_version{};
        std::uint64_t recv_ts_ns{};
        std::uint64_t sequence{};
        std::uint64_t affinity_key{};
        std::uint32_t sid{};
        std::uint32_t market_id{};
        std::uint32_t event_id{};
        std::uint32_t shard_index{};
        std::uint32_t shard_event_index{};
        std::uint32_t event_market_index{};
        std::uint32_t payload_len{};
        std::uint8_t frame_kind{};
        std::uint8_t topology{};
        std::uint16_t flags{};
    };
    class MarketDataLogger{
        public:
            explicit MarketDataLogger(const MarketDataLoggerDeps& deps)
                : input_queues_(deps.input_queues),
                  frame_pool_(deps.frame_pool),
                  recycle_queue_(deps.recycle_queue),
                  logger_to_control_status_queue_(deps.logger_to_control_status_queue),
                  output_file_path_(deps.output_file_path) {
                    const std::filesystem::path output_path{std::string{deps.output_file_path}};
                    if(output_path.has_parent_path()){
                        std::filesystem::create_directories(output_path.parent_path());
                    }
                    output_file_.open(output_path, std::ios::binary | std::ios::out);
                    if(!output_file_.is_open()){
                        throw std::runtime_error("Failed to open log file: " + std::string(deps.output_file_path));
                    }
                    output_file_.write(BinaryHeader::kTAPEMAGIC, sizeof(BinaryHeader::kTAPEMAGIC));
                    output_file_.write(reinterpret_cast<const char*>(&BinaryHeader::kTAPEVERSION), sizeof(BinaryHeader::kTAPEVERSION));
                    output_file_.write(reinterpret_cast<const char*>(&BinaryHeader::kTAPEFLAGS), sizeof(BinaryHeader::kTAPEFLAGS));
                    bytes_written_ = sizeof(BinaryHeader::kTAPEMAGIC) +
                                     sizeof(BinaryHeader::kTAPEVERSION) +
                                     sizeof(BinaryHeader::kTAPEFLAGS);
                    output_file_.flush();
                    if(output_file_.fail()){
                        throw std::runtime_error("Failed to write tape header: " + std::string(deps.output_file_path));
                    }
                    push_control_status(predex::core::control::LoggerStarted{
                        .output_file_path = output_file_path_,
                    });

                  }
//NOLINTNEXTLINE
            [[nodiscard]] std::size_t pump(std::size_t max_batch_size) noexcept{
                std::size_t logged = 0;

                for(std::size_t i = 0; i < max_batch_size && !input_queues_.empty(); ++i){
                    auto* queue = input_queues_[next_input_queue_];
                    if(queue == nullptr){
                        next_input_queue_ = (next_input_queue_ + 1) % input_queues_.size();
                        continue; // skip null queues
                    }
                    predex::ingest::kalshi::FrameHandle handle{};

                    if(queue->try_pop(handle)){
                        const auto* frame = frame_pool_.frame(handle);
                        if(frame != nullptr){
                            if(frame->len > predex::ingest::kalshi::kMaxFrameBytes){
                                ++write_failed_count_;
                                if(!recycle_queue_.try_push(handle)){
                                    ++recycle_failed_count_;
                                }
                                next_input_queue_ = (next_input_queue_ + 1) % input_queues_.size();
                                continue;
                            }
                            MarketDataTapeRecordHeader record_header{
                                .universe_version = handle.universe_version,
                                .recv_ts_ns = frame->recv_ts_ns,
                                .sequence = handle.sequence,
                                .affinity_key = handle.affinity_key,
                                .sid = handle.sid,
                                .market_id = handle.market_id,
                                .event_id = handle.event_id,
                                .shard_index = handle.shard_index,
                                .shard_event_index = handle.shard_event_index,
                                .event_market_index = handle.event_market_index,
                                .payload_len = frame->len,
                                .frame_kind = static_cast<std::uint8_t>(handle.kind),
                                .topology = static_cast<std::uint8_t>(handle.topology),
                                .flags = static_cast<std::uint16_t>(frame->flags)
                            };
                            output_file_.write(reinterpret_cast<const char*>(&record_header), sizeof(record_header));
                            output_file_.write(reinterpret_cast<const char*>(frame->payload.data()), frame->len);
                            if(output_file_.fail()){
                                ++write_failed_count_;
                                report_fault_once("Failed to write market data tape record");
                                if(!recycle_queue_.try_push(handle)){
                                    ++recycle_failed_count_;
                                }
                                next_input_queue_ = (next_input_queue_ + 1) % input_queues_.size();
                                continue;
                            }
                            ++logged;
                            ++logged_count_;
                            bytes_written_ += sizeof(record_header) + frame->len;
                            const auto logger_write_complete_ts_ns =
                                predex::utils::monotonic_now_ns();
                            const auto channel_index =
                                predex::ingest::kalshi::market_data_channel_index(handle.kind);
                            if(channel_index < predex::core::control::kMarketDataChannelCount){
                                predex::utils::record_elapsed_ns(
                                    ingress_to_logger_write_latency_[channel_index],
                                    handle.ingress_ts_ns,
                                    logger_write_complete_ts_ns);
                                predex::utils::record_elapsed_ns(
                                    shard_to_logger_latency_[channel_index],
                                    handle.shard_publish_ts_ns,
                                    logger_write_complete_ts_ns);
                            }
                        }
                        if(!recycle_queue_.try_push(handle)){
                            ++recycle_failed_count_;
                        }
                    }
                    next_input_queue_ = (next_input_queue_ + 1) % input_queues_.size();
                }
                maybe_send_telemetry();
                return logged;
            }
        
        private:
            bool push_control_status(predex::core::control::LoggerToControlStatus status) noexcept{
                if(logger_to_control_status_queue_ == nullptr){
                    return false;
                }
                return logger_to_control_status_queue_->try_push(std::move(status));
            }

            void maybe_send_telemetry() noexcept{
                const auto now = std::chrono::steady_clock::now();
                if(now < next_telemetry_send_){
                    return;
                }
                (void)push_control_status(predex::core::control::LoggerTelemetry{
                    .telemetry = predex::core::control::LoggerTelemetrySnapshot{
                        .records_written = logged_count_,
                        .bytes_written = bytes_written_,
                        .write_failures = write_failed_count_,
                        .recycle_failures = recycle_failed_count_,
                        .shard_to_logger_latency = shard_to_logger_latency_,
                        .ingress_to_logger_write_latency =
                            ingress_to_logger_write_latency_,
                    },
                });
                next_telemetry_send_ = now + kLOGGER_TELEMETRY_INTERVAL;
            }

            void report_fault_once(std::string error_message) noexcept{
                if(fault_reported_){
                    return;
                }
                fault_reported_ = true;
                (void)push_control_status(predex::core::control::LoggerFaulted{
                    .error_message = std::move(error_message),
                });
            }

            std::vector<predex::utils::SPSCQueue<predex::ingest::kalshi::FrameHandle>*> input_queues_;
            predex::ingest::kalshi::FramePool& frame_pool_;
            predex::utils::SPSCQueue<predex::ingest::kalshi::FrameHandle>& recycle_queue_;
            predex::utils::SPSCQueue<predex::core::control::LoggerToControlStatus>* logger_to_control_status_queue_{nullptr};
            std::string output_file_path_;
            std::ofstream output_file_;
            std::uint64_t logged_count_{0};
            std::uint64_t bytes_written_{0};
            std::uint64_t recycle_failed_count_{0};
            std::uint64_t write_failed_count_{0};
            predex::core::control::MarketDataChannelLatency
                shard_to_logger_latency_{};
            predex::core::control::MarketDataChannelLatency
                ingress_to_logger_write_latency_{};
            bool fault_reported_{false};
            std::size_t next_input_queue_{0}; 
            std::chrono::steady_clock::time_point next_telemetry_send_{std::chrono::steady_clock::now() + kLOGGER_TELEMETRY_INTERVAL};
    };
}
