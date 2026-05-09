#include "predex/tape/logger.hpp"

#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>

namespace predex::core::tape::kalshi {
namespace {
constexpr char kTapeMagic[4] = {'P', 'D', 'T', '2'}; //NOLINT: "PDT" for "Predex Tape", "2" for version 2 of the tape format.
constexpr std::uint16_t kTapeVersion = 2;
constexpr std::uint16_t kTapeFlags = 0;
} // namespace

Logger::Logger(
    std::vector<predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>*> input_queues,
    predex::core::ingest::kalshi::FramePool& frame_pool,
    predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& recycle_queue,
    std::string_view output_file_path)
    : input_queues_(std::move(input_queues)), frame_pool_(frame_pool),
      recycle_queue_(recycle_queue) {
    const std::filesystem::path output_path{output_file_path};
    if (output_path.has_parent_path()) {
        std::filesystem::create_directories(output_path.parent_path());
    }
    output_file_.open(output_path, std::ios::binary | std::ios::out);
    if (!output_file_.is_open()) {
        throw std::runtime_error("Failed to open log file: " + std::string(output_file_path));
    }
    output_file_.write(kTapeMagic, sizeof(kTapeMagic));
    output_file_.write(reinterpret_cast<const char*>(&kTapeVersion), sizeof(kTapeVersion));
    output_file_.write(reinterpret_cast<const char*>(&kTapeFlags), sizeof(kTapeFlags));
    if (output_file_.fail()) {
        throw std::runtime_error("Failed to write tape header: " + std::string(output_file_path));
    }
}

std::size_t Logger::pump(size_t max_batch_size) noexcept {
    size_t logged = 0;
    for (size_t i = 0; i < max_batch_size && !input_queues_.empty(); ++i) {

        auto* queue = input_queues_[next_input_queue_];
        if (queue == nullptr) {
            next_input_queue_ = (next_input_queue_ + 1) % input_queues_.size();
            continue; // skip null queues
        }
        predex::core::ingest::kalshi::FrameHandle handle{};
        if (queue->try_pop(handle)) {
            const auto* frame = frame_pool_.frame(handle);
            if (frame != nullptr) {
                // write frame receive timestamp, then frame length, then payload.
                output_file_.write(reinterpret_cast<const char*>(&frame->recv_ts_ns_),
                                   sizeof(frame->recv_ts_ns_));
                output_file_.write(reinterpret_cast<const char*>(&frame->len_),
                                   sizeof(frame->len_));
                output_file_.write(reinterpret_cast<const char*>(frame->payload.data()),
                                   frame->len_);
                if (output_file_.fail()) {
                    ++write_failed_count_;
                    // still try and recycle handle back to IOWriter even if write failed to avoid
                    // blocking the pipeline, but increment write_failed_count_ for telemetry
                    if (!recycle_queue_.try_push(handle)) {
                        ++recycle_failed_count_;
                    }
                    continue;
                }
                ++logged_count_;
                logged++;
            }
            if (!recycle_queue_.try_push(handle)) {
                ++recycle_failed_count_;
            }
        }
        next_input_queue_ = (next_input_queue_ + 1) % input_queues_.size();
    }
    return logged;
}
} // namespace predex::core::tape::kalshi
