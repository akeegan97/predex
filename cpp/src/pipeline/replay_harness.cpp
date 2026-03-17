#include "trading/pipeline/replay_harness.hpp"

#include <algorithm>
#include <fstream>
#include <thread>

namespace trading::pipeline {
namespace {

std::string trim_ascii_whitespace(std::string line) {
    constexpr const char* kWhitespace = " \t\r\n";
    const auto begin = line.find_first_not_of(kWhitespace);
    if (begin == std::string::npos) {
        return {};
    }
    const auto end = line.find_last_not_of(kWhitespace);
    return line.substr(begin, end - begin + 1);
}

} // namespace

ReplayHarness::ReplayHarness(LivePipeline& pipeline, ReplayHarnessConfig config)
    : pipeline_(pipeline), config_(config) {
    config_.push_batch_size = std::max<std::size_t>(1U, config_.push_batch_size);
    config_.pump_batch_size = std::max<std::size_t>(1U, config_.pump_batch_size);
}

ReplayHarnessStats ReplayHarness::replay(const std::vector<std::string>& payloads) {
    ReplayHarnessStats stats{};
    auto& sink = pipeline_.message_sink();

    const auto pump_once = [this, &stats]() {
        const std::size_t pumped = pipeline_.pump_ingest(config_.pump_batch_size);
        ++stats.pump_calls;
        stats.frames_pumped += pumped;
        return pumped;
    };

    for (const auto& payload : payloads) {
        ++stats.attempted_messages;
        if (sink.push_message(payload)) {
            ++stats.pushed_messages;
        } else {
            ++stats.sink_rejected_messages;
        }
        if (stats.attempted_messages % config_.push_batch_size == 0) {
            (void)pump_once();
        }
    }

    for (std::size_t iteration = 0; iteration < config_.max_drain_iterations; ++iteration) {
        const std::size_t pumped = pump_once();
        const auto pipeline_stats = pipeline_.stats();
        stats.final_ingest_queue_depth = pipeline_stats.ingest_queue_depth;
        stats.final_shard_queue_depth = pipeline_stats.shard_queue_depth;
        if (pumped == 0 && pipeline_stats.ingest_queue_depth == 0 &&
            pipeline_stats.shard_queue_depth == 0) {
            stats.drain_completed = true;
            break;
        }
        if (config_.drain_sleep.count() > 0) {
            std::this_thread::sleep_for(config_.drain_sleep);
        }
    }

    if (!stats.drain_completed) {
        const auto pipeline_stats = pipeline_.stats();
        stats.final_ingest_queue_depth = pipeline_stats.ingest_queue_depth;
        stats.final_shard_queue_depth = pipeline_stats.shard_queue_depth;
    }

    return stats;
}

bool ReplayHarness::load_jsonl_file(const std::string& path, std::vector<std::string>& out_payloads,
                                    std::string& error) {
    std::ifstream input(path);
    if (!input) {
        error = "failed to open replay file: " + path;
        return false;
    }

    std::vector<std::string> parsed_payloads;
    std::string line;
    while (std::getline(input, line)) {
        const auto trimmed = trim_ascii_whitespace(std::move(line));
        if (trimmed.empty()) {
            continue;
        }
        parsed_payloads.push_back(trimmed);
    }

    out_payloads = std::move(parsed_payloads);
    error.clear();
    return true;
}

} // namespace trading::pipeline
