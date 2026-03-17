#pragma once

#include <chrono>
#include <cstddef>
#include <string>
#include <vector>

#include "trading/pipeline/live_pipeline.hpp"

namespace trading::pipeline {

struct ReplayHarnessConfig {
    std::size_t push_batch_size{1};
    std::size_t pump_batch_size{LivePipeline::kDefaultPumpBatchSize};
    std::size_t max_drain_iterations{4096};
    std::chrono::milliseconds drain_sleep{std::chrono::milliseconds{0}};
};

struct ReplayHarnessStats {
    std::size_t attempted_messages{0};
    std::size_t pushed_messages{0};
    std::size_t sink_rejected_messages{0};
    std::size_t pump_calls{0};
    std::size_t frames_pumped{0};
    bool drain_completed{false};
    std::size_t final_ingest_queue_depth{0};
    std::size_t final_shard_queue_depth{0};
};

class ReplayHarness final {
  public:
    explicit ReplayHarness(LivePipeline& pipeline, ReplayHarnessConfig config = {});

    [[nodiscard]] ReplayHarnessStats replay(const std::vector<std::string>& payloads);
    [[nodiscard]] static bool load_jsonl_file(const std::string& path,
                                              std::vector<std::string>& out_payloads,
                                              std::string& error);

  private:
    LivePipeline& pipeline_;
    ReplayHarnessConfig config_;
};

} // namespace trading::pipeline
