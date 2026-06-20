// cpp/tests/spine_smoke.cpp
#include <cassert>
#include <cstddef>

#include "predex/control/control_plane.hpp"
#include "predex/control/control_types.hpp"
#include "predex/ingest/kalshi/market_data/frame_pool.hpp"
#include "predex/operator/operator_command_handler.hpp"
#include "predex/operator/operator_commands.hpp"
#include "predex/socket/command_handler.hpp"
#include "predex/socket/unix_command_server.hpp"

int main() {
    predex::ingest::kalshi::FramePool pool{2};

    predex::ingest::kalshi::FrameHandle first{};
    predex::ingest::kalshi::FrameHandle second{};
    predex::ingest::kalshi::FrameHandle overflow{};

    assert(pool.capacity() == 2U);
    assert(pool.available() == 2U);
    assert(pool.try_acquire(first));
    assert(pool.try_acquire(second));
    assert(!pool.try_acquire(overflow));

    auto* frame = pool.writable_frame(first);
    assert(frame != nullptr);
    frame->payload[0] = std::byte{'{'};
    frame->len = 1U;

    const auto* read_frame = pool.frame(first);
    assert(read_frame != nullptr);
    assert(read_frame->len == 1U);
    assert(read_frame->payload[0] == std::byte{'{'});

    assert(pool.recycle(first));
    assert(pool.frame(first) == nullptr);
    assert(!pool.recycle(first));

    predex::ingest::kalshi::FrameHandle reused{};
    assert(pool.try_acquire(reused));
    assert(reused.pool_index == first.pool_index);
    assert(reused.pool_generation != first.pool_generation);

    assert(pool.recycle(second));
    assert(pool.recycle(reused));
    assert(pool.available() == 2U);

    return 0;
}
