#include "predex/tape/logger.hpp"

#include <stdexcept>
#include <string>

namespace predex::core::tape::kalshi {
    Logger::Logger(std::vector<predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>*> input_queues,
                    predex::core::ingest::kalshi::FramePool& frame_pool,
                    predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle> &recycle_queue,
                    std::string_view output_file_path)
        : input_queues_(std::move(input_queues)),
          frame_pool_(frame_pool),
          recycle_queue_(recycle_queue),
          output_file_(output_file_path.data(), std::ios::binary | std::ios::out) {
              if(!output_file_.is_open()){
                  throw std::runtime_error("Failed to open log file: " + std::string(output_file_path));
              }
          }

    std::size_t Logger::pump(size_t max_batch_size) noexcept{
        size_t logged = 0;
        for(size_t i = 0; i < max_batch_size && !input_queues_.empty(); ++i){
            
            auto* queue = input_queues_[next_input_queue_];
            if(queue == nullptr){
                next_input_queue_ = (next_input_queue_ + 1) % input_queues_.size();
                continue; //skip null queues
            }
            predex::core::ingest::kalshi::FrameHandle handle{};
            if(queue->try_pop(handle)){
                const auto* frame = frame_pool_.frame(handle);
                if(frame != nullptr){
                    //write frame length followed by frame payload for easier parsing during replay
                    output_file_.write(reinterpret_cast<const char*>(&frame->len_), sizeof(frame->len_));
                    output_file_.write(reinterpret_cast<const char*>(frame->payload), frame->len_);
                    if(output_file_.fail()){
                        ++write_failed_count_;
                        //still try and recycle handle back to IOWriter even if write failed to avoid blocking the pipeline, but increment write_failed_count_ for telemetry
                        if(!recycle_queue_.try_push(handle)){
                            ++recycle_failed_count_;
                        }
                        continue; 
                    }
                    ++logged_count_;
                    logged++;
                }
                if(!recycle_queue_.try_push(handle)){
                    ++recycle_failed_count_;
                    
                }
            }
            next_input_queue_ = (next_input_queue_ + 1) % input_queues_.size();
        }
        return logged;
    }
} // namespace predex::core::tape::kalshi
