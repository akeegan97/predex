#include "predex/shard/shard.hpp"
#include "predex/ingest/kalshi/market_data/frame_pool.hpp"
#include "predex/ingest/kalshi/market_data/integrity_messages.hpp"
#include "predex/shard/shard_types.hpp"

#include <utility>


namespace predex::shard{

    Shard::Shard(
        std::uint32_t shard_index,
        ShardQueues queues,
        predex::ingest::kalshi::FramePool& frame_pool
    ) : shard_index_(shard_index), queues_(queues), frame_pool_(frame_pool){}

    bool Shard::install_universe(std::vector<KalshiEvent> events){
        return event_store_.initialize(std::move(events));
    }

    bool Shard::send_control_message(ShardToControlMessage message) noexcept{
        return queues_.shard_to_control_queue.try_push(std::move(message));
    }

    bool Shard::send_or_defer_recovery_status(
        ShardToControlMessage message) noexcept{
        if(pending_recovery_status_.has_value()){
            return false;
        }
        if(queues_.shard_to_control_queue.try_push(message)){
            return true;
        }
        pending_recovery_status_.emplace(std::move(message));
        return true;
    }

    bool Shard::flush_pending_recovery_status() noexcept{
        if(!pending_recovery_status_.has_value()){
            return true;
        }
        if(!queues_.shard_to_control_queue.try_push(*pending_recovery_status_)){
            return false;
        }
        pending_recovery_status_.reset();
        return true;
    }

    bool Shard::report_market_recovery_required(
        std::uint64_t universe_version,
        const ingest::kalshi::IntegrityIncidentKey& incident,
        std::uint32_t sid,
        std::uint64_t sequence,
        MarketId market_id,
        EventId event_id,
        ingest::kalshi::BookInvalidationReason reason) noexcept{
        return send_or_defer_recovery_status(ShardToControlMessage{
            ShardMarketRecoveryRequired{
                .universe_version = universe_version,
                .shard_index = shard_index_,
                .incident = incident,
                .sid = sid,
                .sequence = sequence,
                .market_id = market_id,
                .event_id = event_id,
                .reason = reason,
            }
        });
    }

    bool Shard::command_matches_shard(std::uint64_t universe_version, std::uint32_t command_shard_index) noexcept{ //NOLINT
        if(command_shard_index != shard_index_){
            fault_shard(universe_version, "Shard index mismatch");
            return false;
        }
        if(installed_universe_version_ == 0 || universe_version != installed_universe_version_){
            fault_shard(universe_version, "Universe version mismatch");
            return false;
        }
        return true;
    }

    std::uint32_t Shard::shard_index() const noexcept{
        return shard_index_;
    }

    const ShardStats& Shard::stats() const noexcept{
        return stats_;
    }

    bool Shard::terminal_handoff(const predex::ingest::kalshi::FrameHandle& handle) noexcept{
        //TODO: change return from bool to enum to indicate if handle was recycled or sent to logger on true, or if handle was leaked on false
        if(!queues_.shard_to_logger_queue.try_push(handle)){
            if(!queues_.last_resort_recycle_queue.try_push(handle)){
                ++stats_.leaked_handles;
                return false;
            }
            ++stats_.missed_frames_to_logger;
            ++stats_.frames_recycled;
            return true;
        }
        ++stats_.frames_to_logger;
        return true;
    }

    ShardPumpResult Shard::drain_one_message() noexcept{
        ShardPumpResult result{};
        predex::ingest::kalshi::MarketDataPathMessage message{};
        if(!queues_.router_to_shard_queue.try_pop(message)){
            run_state_ = ShardRunState::kDRAINED;
            ShardDrainComplete drained{
                .universe_version = installed_universe_version_,
                .shard_index = shard_index_,
                .stats = stats_
            };
            (void)send_control_message(ShardToControlMessage{drained});
            result.code = ShardPumpCode::kDRAIN_COMPLETE;
            return result;
        }
        return dispatch_message(message,MarketDataDispatchMode::kDRAINING);
    }

    ShardPumpResult Shard::pump_once() noexcept{
        if(!flush_pending_recovery_status()){
            return ShardPumpResult{};
        }
        if(run_state_ == ShardRunState::kDRAINING){
            return drain_one_message();
        }
        if(run_state_ == ShardRunState::kUNINSTALLED ||
           run_state_ == ShardRunState::kDRAINED ||
           run_state_ == ShardRunState::kFAULTED){
            ShardPumpResult result{};
            result.code = ShardPumpCode::kIDLE;
            return result;
        }
        predex::ingest::kalshi::MarketDataPathMessage message{};

        if(!queues_.router_to_shard_queue.try_pop(message)){
            ShardPumpResult result{};
            result.code = ShardPumpCode::kIDLE;
            return result;
        }

        return dispatch_message(message, MarketDataDispatchMode::kLIVE);

    }


    ShardPumpResult Shard::dispatch_message(const ingest::kalshi::MarketDataPathMessage& message, MarketDataDispatchMode dispatch_mode) noexcept{
        return std::visit([this, dispatch_mode](const auto& msg)->ShardPumpResult{
            return handle_message(msg, dispatch_mode);
        }, message);
    }

//NOLINTNEXTLINE -- suppressing cognitive complexity, easier to read than nesting suggests.
    ShardPumpResult Shard::handle_message(const ingest::kalshi::FrameHandle& handle, MarketDataDispatchMode dispatch_mode) noexcept{
        ShardPumpResult result{};
        const bool is_order_book = handle.kind == predex::ingest::kalshi::FrameKind::kORDERBOOK_DELTA || handle.kind == predex::ingest::kalshi::FrameKind::kORDERBOOK_SNAPSHOT;
        ++stats_.frames_seen;
        if (dispatch_mode == MarketDataDispatchMode::kDRAINING) {
            ShardPumpResult result{};

            result.code = terminal_handoff(handle)
                ? ShardPumpCode::kDRAINED_FRAME
                : ShardPumpCode::kHANDLE_LEAK;

            return result;
        }

        if(!validate_handle_target(handle)){
            result.code = ShardPumpCode::kFRAME_ROUTE_REJECTED;
            if(!terminal_handoff(handle)){
                result.code = ShardPumpCode::kHANDLE_LEAK;
            }
            return result;
        }

        const predex::ingest::kalshi::KalshiFrame* frame = frame_pool_.frame(handle);

        if(frame == nullptr){
            result.code = ShardPumpCode::kMISSING_FRAME;
            if(is_order_book){
                result.market_invalidation =
                event_store_.invalidate_market(
                    handle.shard_event_index,
                    handle.event_market_index,
                    handle.market_id,
                    ingest::kalshi::BookInvalidationReason::kSHARD_FRAME_MISSING
                );
                if(!account_invalidation_result(result.market_invalidation)){
                    fault_shard(
                        handle.universe_version,
                        result.market_invalidation.target_found
                            ? "Invalid transition returned by missing-frame invalidation"
                            : "Market invalidation target not found for missing frame");
                }else if(
                    result.market_invalidation.book_sync_transition ==
                        BookSyncTransition::kBECAME_UNUSABLE ||
                    result.market_invalidation.book_sync_transition ==
                        BookSyncTransition::kRECOVERY_REQUIRED){
                    result.incident = ingest::kalshi::IntegrityIncidentKey{
                        .origin = ingest::kalshi::IntegrityIncidentOrigin::kSHARD,
                        .producer_index = shard_index_,
                        .incident_id = next_shard_incident_id_++,
                    };
                    if(!report_market_recovery_required(
                        handle.universe_version,
                        result.incident,
                        handle.sid,
                        handle.sequence,
                        handle.market_id,
                        handle.event_id,
                        ingest::kalshi::BookInvalidationReason::kSHARD_FRAME_MISSING)){
                        fault_shard(
                            handle.universe_version,
                            "Could not retain missing-frame recovery status");
                    }
                }
            }
            if(!terminal_handoff(handle)){
                result.code = ShardPumpCode::kHANDLE_LEAK;
            }
            return result;
        }

        KalshiParsedEvent parsed_event{};
        ParseResult parse_result = market_parser_.parse(handle, *frame, parsed_event);
        if(!parse_result.success){
            ++stats_.parse_rejects;
            result.code = ShardPumpCode::kPARSE_REJECTED;
            result.parse_result = parse_result;
            if(is_order_book){
                result.market_invalidation =
                event_store_.invalidate_market(
                    handle.shard_event_index,
                    handle.event_market_index,
                    handle.market_id,
                    ingest::kalshi::BookInvalidationReason::kSHARD_PARSE_FAILURE
                );
                if(!account_invalidation_result(result.market_invalidation)){
                    fault_shard(
                        handle.universe_version,
                        result.market_invalidation.target_found
                            ? "Invalid transition returned by parse-failure invalidation"
                            : "Market invalidation target not found for parse failure");
                }else if(
                    result.market_invalidation.book_sync_transition ==
                        BookSyncTransition::kBECAME_UNUSABLE ||
                    result.market_invalidation.book_sync_transition ==
                        BookSyncTransition::kRECOVERY_REQUIRED){
                    result.incident = ingest::kalshi::IntegrityIncidentKey{
                        .origin = ingest::kalshi::IntegrityIncidentOrigin::kSHARD,
                        .producer_index = shard_index_,
                        .incident_id = next_shard_incident_id_++,
                    };
                    if(!report_market_recovery_required(
                        handle.universe_version,
                        result.incident,
                        handle.sid,
                        handle.sequence,
                        handle.market_id,
                        handle.event_id,
                        ingest::kalshi::BookInvalidationReason::kSHARD_PARSE_FAILURE)){
                        fault_shard(
                            handle.universe_version,
                            "Could not retain parse-failure recovery status");
                    }
                }
            }
            if(!terminal_handoff(handle)){
                result.code = ShardPumpCode::kHANDLE_LEAK;
            }

            return result;
        }

        EventApplyResult event_result = event_store_.apply(handle, parsed_event);
        switch (event_result.book_sync_transition) {
            case BookSyncTransition::kBECAME_UNUSABLE:
                ++stats_.markets_became_unusable;
                ++stats_.event_desyncs;
                break;

            case BookSyncTransition::kRECOVERY_REQUIRED:
                ++stats_.markets_recovery_required;
                break;

            case BookSyncTransition::kNONE:
            case BookSyncTransition::kINITIAL_SNAPSHOT_INSTALLED:
            case BookSyncTransition::kRECOVERED:
                break;
        }
        if(event_result.disposition == ApplyDisposition::kREJECTED){
            ++stats_.event_rejects;
            result.code = ShardPumpCode::kEVENT_REJECTED;
            result.event_result = event_result;
            if(!terminal_handoff(handle)){
                result.code = ShardPumpCode::kHANDLE_LEAK;
            }
            return result;
        }
        if(event_result.disposition == ApplyDisposition::kIGNORED){
            ++stats_.event_ignored;
            result.code = ShardPumpCode::kEVENT_IGNORED;
            result.event_result = event_result;
            if(!terminal_handoff(handle)){
                result.code = ShardPumpCode::kHANDLE_LEAK;
            }
            return result;
        }
        if(event_result.disposition == ApplyDisposition::kAPPLIED){
            ++stats_.frames_applied;
            result.code = ShardPumpCode::kAPPLIED;
            result.event_result = event_result;
            if(handle.recovery_id != 0 &&
               handle.kind == ingest::kalshi::FrameKind::kORDERBOOK_SNAPSHOT &&
               (event_result.book_sync_transition == BookSyncTransition::kRECOVERED ||
                event_result.book_sync_transition == BookSyncTransition::kNONE)){
                if(!send_or_defer_recovery_status(ShardToControlMessage{
                    ShardRecoverySnapshotApplied{
                        .recovery_id = handle.recovery_id,
                        .universe_version = handle.universe_version,
                        .shard_index = shard_index_,
                        .sid = handle.sid,
                        .sequence = handle.sequence,
                        .market_id = handle.market_id,
                        .transition = event_result.book_sync_transition,
                    }
                })){
                    fault_shard(
                        handle.universe_version,
                        "Could not retain recovery-snapshot status");
                }
            }
            if(!terminal_handoff(handle)){
                result.code = ShardPumpCode::kHANDLE_LEAK;
            }
            return result;
        }
        return result;
    }

    ShardPumpResult Shard::handle_message(const ingest::kalshi::MarketInvalidationBarrier& message, MarketDataDispatchMode dispatch_mode) noexcept{
        std::ignore = dispatch_mode;

        ++stats_.market_barriers_seen;

        const auto reject_barrier = [this, &message](const char* reason) {
            ++stats_.barrier_rejects;
            run_state_ = ShardRunState::kFAULTED;

            (void)send_control_message(ShardToControlMessage{
                ShardFaulted{
                    .shard_index = shard_index_,
                    .universe_version = message.universe_version,
                    .reason = reason,
                }
            });

            ShardPumpResult result{};
            result.code = ShardPumpCode::kINTEGRITY_BARRIER_REJECTED;
            result.incident = message.incident;
            return result;
        };

        if (message.reason == ingest::kalshi::BookInvalidationReason::kNONE) {
            return reject_barrier("Market invalidation barrier has no reason");
        }

        if (message.universe_version != installed_universe_version_) {
            return reject_barrier("Market invalidation universe mismatch");
        }

        if (message.shard_index != shard_index_) {
            return reject_barrier("Market invalidation shard mismatch");
        }

        const Event* event = event_store_.get_event(message.shard_event_index);
        if (event == nullptr || event->event_id() != message.event_id) {
            return reject_barrier("Market invalidation event mismatch");
        }

        const BookInvalidationResult invalidation =
            event_store_.invalidate_market(
                message.shard_event_index,
                message.event_market_index,
                message.market_id,
                message.reason);

        if (!invalidation.target_found) {
            auto result = reject_barrier("Market invalidation target not found");
            result.market_invalidation = invalidation;
            return result;
        }

        if (!account_invalidation_result(invalidation)) {
            auto result =
                reject_barrier("Invalid transition returned by market invalidation");
            result.market_invalidation = invalidation;
            return result;
        }

        if(invalidation.book_sync_transition == BookSyncTransition::kBECAME_UNUSABLE ||
           invalidation.book_sync_transition == BookSyncTransition::kRECOVERY_REQUIRED){
            if(!report_market_recovery_required(
                message.universe_version,
                message.incident,
                message.sid,
                message.sequence,
                message.market_id,
                message.event_id,
                message.reason)){
                return reject_barrier(
                    "Could not retain market recovery status");
            }
        }

        ShardPumpResult result{};
        result.code = ShardPumpCode::kMARKET_BARRIER_HANDLED;
        result.incident = message.incident;
        result.market_invalidation = invalidation;
        return result;
    }

    ShardPumpResult Shard::handle_message(const ingest::kalshi::OrderBookSubscriptionInvalidationBarrier& message, MarketDataDispatchMode dispatch_mode) noexcept{
        std::ignore = dispatch_mode;

        ShardPumpResult result{};
        ++stats_.subscription_barriers_seen;
        result.incident = message.incident;

        if (message.reason == ingest::kalshi::BookInvalidationReason::kNONE ||
            message.universe_version != installed_universe_version_) {
            ++stats_.barrier_rejects;
            run_state_ = ShardRunState::kFAULTED;

            (void)send_control_message(ShardToControlMessage{
                ShardFaulted{
                    .shard_index = shard_index_,
                    .universe_version = message.universe_version,
                    .reason = "Invalid order-book subscription barrier",
                }
            });

            result.code = ShardPumpCode::kINTEGRITY_BARRIER_REJECTED;
            return result;
        }

        const BookInvalidationSummary summary = event_store_.invalidate_all_markets(message.reason);

        stats_.markets_became_unusable += summary.targets_became_unusable;
        stats_.markets_recovery_required += summary.targets_recovery_required;
        stats_.markets_already_awaiting_recovery +=summary.targets_already_awaiting_recovery;

        result.code = ShardPumpCode::kSUBSCRIPTION_BARRIER_HANDLED;
        result.subscription_invalidation = summary;
        return result;
    }


    bool Shard::process_one_control_command() noexcept{
        ControlToShardCommand command{};
        if(!queues_.control_to_shard_queue.try_pop(command)){
            return false;
        }
        std::visit([&](auto& cmd){
            handle_operator_command(cmd);
        }, command);
        return true;
    }

    std::size_t Shard::drain_control_commands(std::size_t max_commands) noexcept{
        std::size_t commands_processed{0};
        while(commands_processed < max_commands && process_one_control_command()){
            ++commands_processed;
        }
        return commands_processed;
    }

    void Shard::handle_operator_command(InstallShardUniverse& command){
        if (command.shard_index != shard_index_) {
            fault_shard(command.universe_version, "Install shard index mismatch");
            return;
        }

        if (command.universe_version == 0) {
            fault_shard(command.universe_version, "Cannot install universe version zero");
            return;
        }

        const bool install_allowed =
            run_state_ == ShardRunState::kUNINSTALLED ||
            run_state_ == ShardRunState::kDRAINED;

        if (!install_allowed) {
            fault_shard(
                command.universe_version,
                "Universe install requested from invalid Shard state");
            return;
        }

        if (run_state_ == ShardRunState::kDRAINED &&
            command.universe_version <= installed_universe_version_) {
            fault_shard(
                command.universe_version,
                "Replacement universe version is not newer");
            return;
        }


        if (!install_universe(std::move(command.events))) {
            fault_shard(
                command.universe_version,
                "Failed to install universe events");
            return;
        }
        installed_universe_version_ = command.universe_version;
        stats_ = ShardStats{};//reset stats on new universe install
        run_state_ = ShardRunState::kLIVE;
        ShardUniverseInstalled installed{
            .universe_version = command.universe_version,
            .shard_index = shard_index_,
            .event_count = event_store_.size()
        };
        (void)send_control_message(ShardToControlMessage{(installed)});
    }

    void Shard::handle_operator_command(PrepareStopUniverse& command){
        if(!command_matches_shard(command.universe_version, command.shard_index) || !require_run_state(ShardRunState::kLIVE, command.universe_version, "Prepare stop requested before Shard was live")){
            return;
        }
        run_state_ = ShardRunState::kPREPARING_STOP;
        run_state_ = ShardRunState::kSAFE_TO_STOP;
        ShardSafeToStopUniverse safe_to_stop{
            .universe_version = command.universe_version,
            .shard_index = shard_index_,
            .reason = "No OMS or strategy lifecycle attached"
        };
        (void)send_control_message(ShardToControlMessage{std::move(safe_to_stop)});
    }

    void Shard::handle_operator_command(DrainShardUniverse& command){
        if(!command_matches_shard(command.universe_version, command.shard_index) || !require_run_state(ShardRunState::kSAFE_TO_STOP, command.universe_version, "Drain requested before Shard was safe to stop")){
            return;
        }
        run_state_ = ShardRunState::kDRAINING;
    }

    void Shard::handle_operator_command(ResumeShardUniverse& command){
        if(!command_matches_shard(command.universe_version, command.shard_index) || !require_run_state(ShardRunState::kSAFE_TO_STOP, command.universe_version, "Resume requested before Shard was safe to stop")){
            return;
        }
        run_state_ = ShardRunState::kLIVE;
    }
    void Shard::maybe_send_telemetry() noexcept {
        if(pending_recovery_status_.has_value()){
            return;
        }
        const auto now = std::chrono::steady_clock::now();
        if(now < next_telemetry_send_) {
            return;
        }

        (void)send_control_message(ShardToControlMessage{
            ShardTelemetry{
                .shard_index = shard_index_,
                .universe_version = installed_universe_version_,
                .stats = stats_,
            }
        });

        next_telemetry_send_ = now + kSHARD_TELEMETRY_INTERVAL;
    }

    bool Shard::require_run_state(ShardRunState expected, std::uint64_t universe_version, const char* reason) noexcept{
        if(run_state_ != expected){
            fault_shard(universe_version, reason);
            return false;
        }
        return true;
    }

    void Shard::fault_shard(std::uint64_t universe_version, const char* reason) noexcept{
        run_state_ = ShardRunState::kFAULTED;
        ShardFaulted faulted{
            .shard_index = shard_index_,
            .universe_version = universe_version,
            .reason = reason
        };
        (void)send_control_message(ShardToControlMessage{std::move(faulted)});
    }

    bool Shard::account_invalidation_result(const BookInvalidationResult& invalidation) noexcept{
        if(!invalidation.target_found){
            return false;
        }

        switch(invalidation.book_sync_transition){
            case BookSyncTransition::kBECAME_UNUSABLE:
                ++stats_.markets_became_unusable;
                return true;

            case BookSyncTransition::kRECOVERY_REQUIRED:
                ++stats_.markets_recovery_required;
                return true;

            case BookSyncTransition::kNONE:
                ++stats_.markets_already_awaiting_recovery;
                return true;

            case BookSyncTransition::kINITIAL_SNAPSHOT_INSTALLED:
            case BookSyncTransition::kRECOVERED:
                return false;
        }
        return false;
    }

    bool Shard::validate_handle_target(const ingest::kalshi::FrameHandle& handle) noexcept{
        if(handle.shard_index != shard_index_){
            fault_shard(handle.universe_version, "Frame handle shard index mismatch");
            return false;
        }
        if(handle.universe_version != installed_universe_version_){
            fault_shard(handle.universe_version, "Frame handle universe version mismatch");
            return false;
        }

        const Event* event = event_store_.get_event(handle.shard_event_index);
        if(event == nullptr){
            fault_shard(handle.universe_version, "Frame handle event index mismatch");
            return false;
        }
        if(event->event_id() != handle.event_id){
            fault_shard(handle.universe_version, "Frame handle event ID mismatch");
            return false;
        }
        if(event->shard_event_index() != handle.shard_event_index){
            fault_shard(handle.universe_version, "Frame handle event route index mismatch");
            return false;
        }
        if(event->event_topology() != handle.topology){
            fault_shard(handle.universe_version, "Frame handle event topology mismatch");
            return false;
        }

        const KalshiMarket* market = event->get_market(handle.event_market_index);
        if(market == nullptr){
            fault_shard(handle.universe_version, "Frame handle market index mismatch");
            return false;
        }
        if(market->market_id != handle.market_id){
            fault_shard(handle.universe_version, "Frame handle market ID mismatch");
            return false;
        }
        if(market->event_market_index != handle.event_market_index){
            fault_shard(handle.universe_version, "Frame handle market route index mismatch");
            return false;
        }
        return true;
    }

}
