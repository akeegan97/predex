#pragma once

#include <cstddef>
#include <fstream>
#include <string_view>
#include <vector>

#include "predex/audit/audit_types.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::audit {

class AuditLogger {
  public:
    explicit AuditLogger(
        std::vector<predex::utils::SPSCQueue<AuditEvent>*> input_queues,
        std::string_view output_file_path);

    [[nodiscard]] std::size_t pump(std::size_t max_batch_size) noexcept;

    AuditLogger(const AuditLogger&) = delete;
    AuditLogger& operator=(const AuditLogger&) = delete;
    AuditLogger(AuditLogger&&) = delete;
    AuditLogger& operator=(AuditLogger&&) = delete;

  private:
    std::vector<predex::utils::SPSCQueue<AuditEvent>*> input_queues_;
    std::ofstream output_file_;
    std::size_t next_input_queue_{0};
};

} // namespace predex::core::audit
