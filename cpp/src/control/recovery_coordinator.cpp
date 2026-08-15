#include "predex/control/recovery_coordinator.hpp"
#include "predex/control/control_types.hpp"

#include <algorithm>
#include <utility>
#include <limits>
#include <stdexcept>
namespace {

    [[nodiscard]] bool valid_targeted_incident_source(
        const predex::shard::ShardMarketRecoveryRequired& fact) noexcept{
        using predex::ingest::kalshi::BookInvalidationReason;
        using predex::ingest::kalshi::IntegrityIncidentOrigin;

        switch(fact.reason){
            case BookInvalidationReason::kWIRE_POOL_EXHAUSTION:
            case BookInvalidationReason::kWIRE_TO_ROUTER_DELIVERY_LOSS:
                return fact.incident.origin == IntegrityIncidentOrigin::kWIRE_SESSION &&
                       fact.incident.producer_index == 0;

            case BookInvalidationReason::kROUTER_TO_SHARD_DELIVERY_LOSS:
                return fact.incident.origin == IntegrityIncidentOrigin::kROUTER &&
                       fact.incident.producer_index == 0;

            case BookInvalidationReason::kSHARD_FRAME_MISSING:
            case BookInvalidationReason::kSHARD_PARSE_FAILURE:
                return fact.incident.origin == IntegrityIncidentOrigin::kSHARD &&
                       fact.incident.producer_index == fact.shard_index;

            case BookInvalidationReason::kNONE:
            case BookInvalidationReason::kEXCHANGE_SEQUENCE_GAP:
                return false;
        }
        return false;
    }
    

} // namespace

namespace predex::core::control{

    RecoveryObservationResult RecoveryCoordinator::observe(
        const shard::ShardMarketRecoveryRequired& fact,
        const UniverseSnapshot& active_universe,
        TimePoint now){
        const auto reject = [](RecoveryObservationCode code){
            return RecoveryObservationResult{
                .code = code,
                .recovery_id = 0,
                .markets_affected = 0,
            };
        };

        if(active_universe.version == 0 || active_universe.version != fact.universe_version){
            return reject(RecoveryObservationCode::kSTALE_UNIVERSE);
        }

        if(fact.incident.origin == ingest::kalshi::IntegrityIncidentOrigin::kUNKNOWN || fact.incident.incident_id == 0){
            return reject(RecoveryObservationCode::kINVALID_INCIDENT);
        }

        if(!valid_targeted_incident_source(fact)){
            return reject(RecoveryObservationCode::kINVALID_REASON);
        }

        const auto route_it = std::find_if(
            active_universe.market_routes.begin(),
            active_universe.market_routes.end(),
            [&fact](const UniverseMarketRoute& route) {
                return route.market_id == fact.market_id;
            });
        if(route_it == active_universe.market_routes.end()){
            return reject(RecoveryObservationCode::kUNKNOWN_MARKET);
        }

        if(route_it->event_id != fact.event_id || route_it->shard_index != fact.shard_index){
            return reject(RecoveryObservationCode::kROUTE_MISMATCH);
        }

        const RecoverySourceKey source_key{
            .universe_version = fact.universe_version,
            .incident = fact.incident,
        };

        const auto duplicate_it = recovery_by_source_.find(source_key);
        if(duplicate_it != recovery_by_source_.end()){
            return RecoveryObservationResult{
                .code = RecoveryObservationCode::kDUPLICATE,
                .recovery_id = duplicate_it->second,
                .markets_affected = 0,
            };
        }

        const auto active_it = active_recovery_by_market_.find(fact.market_id);
        if(active_it != active_recovery_by_market_.end()){
            return RecoveryObservationResult{
                .code = RecoveryObservationCode::kALREADY_RECOVERING,
                .recovery_id = active_it->second,
                .markets_affected = 0,
            };
        }

        if(next_recovery_id_ == 0){
            return reject(RecoveryObservationCode::kRECOVERY_ID_EXHAUSTED);
        }
        const RecoveryId recovery_id = next_recovery_id_;

        RecoveryIncidentState incident{
            .recovery_id = recovery_id,
            .source = fact.incident,
            .universe_version = fact.universe_version,
            .sid = fact.sid,
            .reason = fact.reason,
            .market_recoveries = {
                MarketRecoveryState{
                    .market_id = fact.market_id,
                    .event_id = fact.event_id,
                    .shard_index = fact.shard_index,
                    .phase = MarketRecoveryPhase::kPENDING_REQUEST,
                    .attempts_sent = 0,
                    .last_attempt_time = TimePoint{},
                    .next_attempt_time = now,
                }
            },
            .started_at = now,
            .last_progress_at = now,
        };

        auto incident_it = incidents_.end();
        auto source_it = recovery_by_source_.end();
        try{
            const auto incident_insert = incidents_.emplace(recovery_id, std::move(incident));
            incident_it = incident_insert.first;
            if(!incident_insert.second){
                return reject(RecoveryObservationCode::kRECOVERY_ID_EXHAUSTED);
            }

            const auto source_insert = recovery_by_source_.emplace(source_key, recovery_id);
            source_it = source_insert.first;
            if(!source_insert.second){
                incidents_.erase(incident_it);
                return RecoveryObservationResult{
                    .code = RecoveryObservationCode::kDUPLICATE,
                    .recovery_id = source_insert.first->second,
                    .markets_affected = 0,
                };
            }

            const auto market_insert = active_recovery_by_market_.emplace(fact.market_id, recovery_id);
            if(!market_insert.second){
                recovery_by_source_.erase(source_it);
                incidents_.erase(incident_it);
                return RecoveryObservationResult{
                    .code = RecoveryObservationCode::kALREADY_RECOVERING,
                    .recovery_id = market_insert.first->second,
                    .markets_affected = 0,
                };
            }
        }catch(...){
            if(source_it != recovery_by_source_.end()){
                recovery_by_source_.erase(source_it);
            }
            if(incident_it != incidents_.end()){
                incidents_.erase(incident_it);
            }
            throw;
        }

        ++next_recovery_id_;
        return RecoveryObservationResult{
            .code = RecoveryObservationCode::kCREATED,
            .recovery_id = recovery_id,
            .markets_affected = 1,
        };
    }

    RecoveryObservationResult RecoveryCoordinator::observe(
        const router::OrderBookSubscriptionBarrierDelivered& fact,
        const UniverseSnapshot& active_universe,
        TimePoint now){
        const auto reject = [](RecoveryObservationCode code){
            return RecoveryObservationResult{
                .code = code,
                .recovery_id = 0,
                .markets_affected = 0,
            };
        };

        if(active_universe.version == 0 ||
           active_universe.version != fact.universe_version){
            return reject(RecoveryObservationCode::kSTALE_UNIVERSE);
        }
        if(fact.incident.origin !=
               ingest::kalshi::IntegrityIncidentOrigin::kWIRE_SESSION ||
           fact.incident.producer_index != 0 ||
           fact.incident.incident_id == 0){
            return reject(RecoveryObservationCode::kINVALID_INCIDENT);
        }
        if(fact.reason !=
           ingest::kalshi::BookInvalidationReason::kEXCHANGE_SEQUENCE_GAP){
            return reject(RecoveryObservationCode::kINVALID_REASON);
        }

        const RecoverySourceKey source_key{
            .universe_version = fact.universe_version,
            .incident = fact.incident,
        };
        const auto duplicate_it = recovery_by_source_.find(source_key);
        if(duplicate_it != recovery_by_source_.end()){
            return RecoveryObservationResult{
                .code = RecoveryObservationCode::kDUPLICATE,
                .recovery_id = duplicate_it->second,
                .markets_affected = 0,
            };
        }
        if(active_universe.market_routes.empty()){
            return reject(RecoveryObservationCode::kUNKNOWN_MARKET);
        }
        if(next_recovery_id_ == 0){
            return reject(RecoveryObservationCode::kRECOVERY_ID_EXHAUSTED);
        }

        std::vector<MarketRecoveryState> market_recoveries;
        market_recoveries.reserve(active_universe.market_routes.size());
        RecoveryId existing_recovery_id{};
        for(const auto& route : active_universe.market_routes){
            const auto active_it = active_recovery_by_market_.find(route.market_id);
            if(active_it != active_recovery_by_market_.end()){
                if(existing_recovery_id == 0){
                    existing_recovery_id = active_it->second;
                }
                continue;
            }
            market_recoveries.push_back(MarketRecoveryState{
                .market_id = route.market_id,
                .event_id = route.event_id,
                .shard_index = route.shard_index,
                .phase = MarketRecoveryPhase::kPENDING_REQUEST,
                .attempts_sent = 0,
                .last_attempt_time = TimePoint{},
                .next_attempt_time = now,
            });
        }

        if(market_recoveries.empty()){
            return RecoveryObservationResult{
                .code = RecoveryObservationCode::kALREADY_RECOVERING,
                .recovery_id = existing_recovery_id,
                .markets_affected = 0,
            };
        }

        const RecoveryId recovery_id = next_recovery_id_;
        const std::size_t market_count = market_recoveries.size();
        RecoveryIncidentState incident{
            .recovery_id = recovery_id,
            .source = fact.incident,
            .universe_version = fact.universe_version,
            .sid = fact.sid,
            .reason = fact.reason,
            .market_recoveries = std::move(market_recoveries),
            .started_at = now,
            .last_progress_at = now,
        };

        auto incident_it = incidents_.end();
        auto source_it = recovery_by_source_.end();
        std::vector<MarketId> latched_markets;
        latched_markets.reserve(market_count);
        try{
            const auto incident_insert = incidents_.emplace(
                recovery_id,
                std::move(incident));
            incident_it = incident_insert.first;
            if(!incident_insert.second){
                return reject(RecoveryObservationCode::kRECOVERY_ID_EXHAUSTED);
            }

            const auto source_insert = recovery_by_source_.emplace(
                source_key,
                recovery_id);
            source_it = source_insert.first;
            if(!source_insert.second){
                incidents_.erase(incident_it);
                return RecoveryObservationResult{
                    .code = RecoveryObservationCode::kDUPLICATE,
                    .recovery_id = source_insert.first->second,
                    .markets_affected = 0,
                };
            }

            for(const auto& market : incident_it->second.market_recoveries){
                const auto inserted = active_recovery_by_market_.emplace(
                    market.market_id,
                    recovery_id);
                if(!inserted.second){
                    throw std::logic_error(
                        "Market recovery latch changed during subscription recovery creation");
                }
                latched_markets.push_back(market.market_id);
            }
        }catch(...){
            for(const auto market_id : latched_markets){
                active_recovery_by_market_.erase(market_id);
            }
            if(source_it != recovery_by_source_.end()){
                recovery_by_source_.erase(source_it);
            }
            if(incident_it != incidents_.end()){
                incidents_.erase(incident_it);
            }
            throw;
        }

        ++next_recovery_id_;
        return RecoveryObservationResult{
            .code = RecoveryObservationCode::kCREATED,
            .recovery_id = recovery_id,
            .markets_affected = market_count,
        };
    }

    RecoverySupersessionSummary RecoveryCoordinator::supersede_before_universe(
        std::uint64_t new_universe_version,
        TimePoint now) noexcept{
        RecoverySupersessionSummary summary{};
        if(new_universe_version == 0){
            return summary;
        }

        for(auto& [recovery_id, incident] : incidents_){
            if(incident.universe_version == new_universe_version){
                continue;
            }

            bool incident_changed = false;
            for(auto& market : incident.market_recoveries){
                const auto active_it =
                    active_recovery_by_market_.find(market.market_id);
                if(active_it != active_recovery_by_market_.end() &&
                   active_it->second == recovery_id){
                    active_recovery_by_market_.erase(active_it);
                    ++summary.active_latches_released;
                    incident_changed = true;
                }

                if(market.phase != MarketRecoveryPhase::kSNAPSHOT_APPLIED &&
                   market.phase != MarketRecoveryPhase::kSUPERSEDED){
                    market.phase = MarketRecoveryPhase::kSUPERSEDED;
                    ++summary.markets_superseded;
                    incident_changed = true;
                }
            }

            if(incident_changed){
                incident.last_progress_at = now;
                ++summary.incidents_superseded;
            }
        }

        return summary;
    }

    std::optional<RecoverMarketIo>
    RecoveryCoordinator::next_pending_command(TimePoint now) const noexcept{
        const RecoveryIncidentState* selected_incident = nullptr;
        const MarketRecoveryState* selected_market = nullptr;

        for(const auto& [recovery_id, incident] : incidents_){
            for(const auto& market : incident.market_recoveries){
                if(market.phase != MarketRecoveryPhase::kPENDING_REQUEST ||
                market.next_attempt_time > now){
                    continue;
                }

                if(market.attempts_sent ==
                std::numeric_limits<std::uint32_t>::max()){
                    continue;
                }

                const bool comes_first =
                    selected_incident == nullptr ||
                    recovery_id < selected_incident->recovery_id ||
                    (recovery_id == selected_incident->recovery_id &&
                    market.market_id < selected_market->market_id);

                if(comes_first){
                    selected_incident = &incident;
                    selected_market = &market;
                }
            }
        }

        if(selected_incident == nullptr){
            return std::nullopt;
        }

        return RecoverMarketIo{
            .recovery_id = selected_incident->recovery_id,
            .universe_version = selected_incident->universe_version,
            .market_id = selected_market->market_id,
            .request_attempt = selected_market->attempts_sent + 1,
        };
    }

    std::vector<RecoverMarketIo> RecoveryCoordinator::next_pending_commands(
        TimePoint now,
        std::size_t maximum_commands) const{
        std::vector<RecoverMarketIo> commands;
        if(maximum_commands == 0){
            return commands;
        }

        const RecoveryIncidentState* selected_incident = nullptr;
        for(const auto& [recovery_id, incident] : incidents_){
            const bool has_ready_market = std::any_of(
                incident.market_recoveries.begin(),
                incident.market_recoveries.end(),
                [now](const MarketRecoveryState& market){
                    return market.phase == MarketRecoveryPhase::kPENDING_REQUEST &&
                           market.next_attempt_time <= now &&
                           market.attempts_sent !=
                               std::numeric_limits<std::uint32_t>::max();
                });
            if(has_ready_market &&
               (selected_incident == nullptr ||
                recovery_id < selected_incident->recovery_id)){
                selected_incident = &incident;
            }
        }
        if(selected_incident == nullptr){
            return commands;
        }

        commands.reserve(std::min(
            maximum_commands,
            selected_incident->market_recoveries.size()));
        for(const auto& market : selected_incident->market_recoveries){
            if(market.phase != MarketRecoveryPhase::kPENDING_REQUEST ||
               market.next_attempt_time > now ||
               market.attempts_sent ==
                   std::numeric_limits<std::uint32_t>::max()){
                continue;
            }
            commands.push_back(RecoverMarketIo{
                .recovery_id = selected_incident->recovery_id,
                .universe_version = selected_incident->universe_version,
                .market_id = market.market_id,
                .request_attempt = market.attempts_sent + 1,
            });
        }

        std::sort(
            commands.begin(),
            commands.end(),
            [](const RecoverMarketIo& lhs, const RecoverMarketIo& rhs){
                return lhs.market_id < rhs.market_id;
            });
        if(commands.size() > maximum_commands){
            commands.resize(maximum_commands);
        }
        return commands;
    }

    bool RecoveryCoordinator::mark_command_enqueued(
        const RecoverMarketIo& command,
        TimePoint now) noexcept{
        const auto incident_it = incidents_.find(command.recovery_id);
        if(incident_it == incidents_.end()){
            return false;
        }

        RecoveryIncidentState& incident = incident_it->second;
        if(command.universe_version != incident.universe_version){
            return false;
        }

        const auto market_it = std::find_if(
            incident.market_recoveries.begin(),
            incident.market_recoveries.end(),
            [&command](const MarketRecoveryState& market){
                return market.market_id == command.market_id;
            });

        if(market_it == incident.market_recoveries.end()){
            return false;
        }

        const auto active_it =
            active_recovery_by_market_.find(command.market_id);

        if(active_it == active_recovery_by_market_.end() ||
        active_it->second != command.recovery_id){
            return false;
        }

        if(market_it->phase != MarketRecoveryPhase::kPENDING_REQUEST ||
        market_it->next_attempt_time > now ||
        market_it->attempts_sent ==
            std::numeric_limits<std::uint32_t>::max() ||
        command.request_attempt != market_it->attempts_sent + 1){
            return false;
        }

        market_it->attempts_sent = command.request_attempt;
        market_it->phase = MarketRecoveryPhase::kREQUEST_ENQUEUED;
        market_it->last_attempt_time = now;
        incident.last_progress_at = now;
        return true;
    }

    RecoveryFactResult RecoveryCoordinator::handle(const IoRecoveryRequestAccepted& fact, TimePoint now) noexcept{
        RecoveryFactResult result{
            .recovery_id = fact.recovery_id,
            .market_id = fact.market_id
        };
        
        const auto incident_iter = incidents_.find(fact.recovery_id);
        if(incident_iter == incidents_.end()){
            return result;
        }

        RecoveryIncidentState& incident = incident_iter->second;
        if(fact.universe_version != incident.universe_version){
            return result;
        }

        const auto market_iter = std::find_if(incident.market_recoveries.begin(), incident.market_recoveries.end(),[&fact](const MarketRecoveryState& market){
            return market.market_id == fact.market_id;
        });
        if(market_iter == incident.market_recoveries.end()){
            return result;
        }

        MarketRecoveryState& market = *market_iter;
        if(fact.request_attempt < market.attempts_sent){
            result.disposition = RecoveryFactDisposition::kIGNORED;
            return result;
        }

        if(fact.request_attempt > market.attempts_sent){
            return result;
        }

        const auto active_iter = active_recovery_by_market_.find(fact.market_id);
        if(active_iter == active_recovery_by_market_.end() || active_iter->second != fact.recovery_id){
            result.disposition = RecoveryFactDisposition::kIGNORED;            
            return result;
        }

        if(market.phase != MarketRecoveryPhase::kREQUEST_ENQUEUED){
            result.disposition = RecoveryFactDisposition::kIGNORED;
            return result;
        }

        market.phase = MarketRecoveryPhase::kREQUEST_ACCEPTED;
        market.last_attempt_time = now;
        incident.last_progress_at = now;
        result.disposition = RecoveryFactDisposition::kAPPLIED;
        result.effect = RecoveryFactEffect::kREQUEST_ACCEPTED;
        return result;
    }

    RecoveryFactResult RecoveryCoordinator::handle(const IoRecoveryRequestFailed& fact, TimePoint now) noexcept {
        RecoveryFactResult result{
            .recovery_id = fact.recovery_id,
            .market_id = fact.market_id,
        };

        const auto incident_it = incidents_.find(fact.recovery_id);
        if(incident_it == incidents_.end()){
            return result;
        }

        RecoveryIncidentState& incident = incident_it->second;
        if(fact.universe_version != incident.universe_version){
            return result;
        }

        const auto market_it = std::find_if(
            incident.market_recoveries.begin(),
            incident.market_recoveries.end(),
            [&fact](const MarketRecoveryState& market){
                return market.market_id == fact.market_id;
            });

        if(market_it == incident.market_recoveries.end()){
            return result;
        }

        MarketRecoveryState& market = *market_it;

        if(fact.request_attempt < market.attempts_sent){
            result.disposition = RecoveryFactDisposition::kIGNORED;
            return result;
        }

        if(fact.request_attempt > market.attempts_sent){
            return result;
        }

        const auto active_it =
            active_recovery_by_market_.find(fact.market_id);

        if(active_it == active_recovery_by_market_.end() ||
        active_it->second != fact.recovery_id){
            result.disposition = RecoveryFactDisposition::kIGNORED;
            return result;
        }

        if(market.phase != MarketRecoveryPhase::kREQUEST_ENQUEUED &&
           market.phase != MarketRecoveryPhase::kREQUEST_ACCEPTED){
            // Especially important: duplicate failures must not extend backoff.
            result.disposition = RecoveryFactDisposition::kIGNORED;
            return result;
        }

        incident.last_progress_at = now;

        if(market.attempts_sent >= config_.max_attempts){
            market.phase = MarketRecoveryPhase::kFAILED;

            // Keep active_recovery_by_market_ latched: book remains unusable.
            result.disposition = RecoveryFactDisposition::kAPPLIED;
            result.effect = RecoveryFactEffect::kMARKET_FAILED;
            return result;
        }

        market.phase = MarketRecoveryPhase::kPENDING_REQUEST;
        market.next_attempt_time =
            now + retry_backoff(market.attempts_sent);

        result.disposition = RecoveryFactDisposition::kAPPLIED;
        result.effect = RecoveryFactEffect::kRETRY_SCHEDULED;
        return result;
    }
    RecoveryFactResult RecoveryCoordinator::handle(const shard::ShardRecoverySnapshotApplied& fact,const UniverseSnapshot& active_universe,TimePoint now) noexcept {
        RecoveryFactResult result{
            .recovery_id = fact.recovery_id,
            .market_id = fact.market_id,
        };

        if(active_universe.version == 0 ||
        fact.universe_version != active_universe.version){
            result.disposition = RecoveryFactDisposition::kIGNORED;
            return result;
        }

        const auto incident_it = incidents_.find(fact.recovery_id);
        if(incident_it == incidents_.end()){
            return result;
        }

        RecoveryIncidentState& incident = incident_it->second;
        if(fact.universe_version != incident.universe_version){
            return result;
        }

        const auto market_it = std::find_if(
            incident.market_recoveries.begin(),
            incident.market_recoveries.end(),
            [&fact](const MarketRecoveryState& market){
                return market.market_id == fact.market_id;
            });

        if(market_it == incident.market_recoveries.end()){
            return result;
        }

        MarketRecoveryState& market = *market_it;

        const auto route_it = std::find_if(
            active_universe.market_routes.begin(),
            active_universe.market_routes.end(),
            [&fact](const UniverseMarketRoute& route){
                return route.market_id == fact.market_id;
            });

        if(route_it == active_universe.market_routes.end() ||
        route_it->shard_index != fact.shard_index ||
        route_it->shard_index != market.shard_index ||
        route_it->event_id != market.event_id){
            return result;
        }

        const auto active_it =
            active_recovery_by_market_.find(fact.market_id);

        if(active_it == active_recovery_by_market_.end() ||
        active_it->second != fact.recovery_id){
            result.disposition = RecoveryFactDisposition::kIGNORED;
            return result;
        }

        if(market.phase == MarketRecoveryPhase::kSNAPSHOT_APPLIED ||
        market.phase == MarketRecoveryPhase::kSUPERSEDED){
            result.disposition = RecoveryFactDisposition::kIGNORED;
            return result;
        }
        if(market.phase == MarketRecoveryPhase::kPENDING_REQUEST &&
        market.attempts_sent == 0){
            return result;
        }

        if(fact.transition != shard::BookSyncTransition::kRECOVERED &&
        fact.transition != shard::BookSyncTransition::kNONE){
            return result;
        }

        market.phase = MarketRecoveryPhase::kSNAPSHOT_APPLIED;
        incident.last_progress_at = now;

        active_recovery_by_market_.erase(active_it);

        result.disposition = RecoveryFactDisposition::kAPPLIED;
        result.effect = RecoveryFactEffect::kMARKET_RECOVERED;

        result.incident_completed = std::all_of(
            incident.market_recoveries.begin(),
            incident.market_recoveries.end(),
            [](const MarketRecoveryState& candidate){
                return candidate.phase ==
                        MarketRecoveryPhase::kSNAPSHOT_APPLIED ||
                    candidate.phase ==
                        MarketRecoveryPhase::kSUPERSEDED;
            });

        if(result.incident_completed){
            result.recovery_duration =
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    now - incident.started_at);
        }

        return result;
    }

    RecoveryTimeoutSummary RecoveryCoordinator::expire_timeouts(
        TimePoint now) noexcept{
        RecoveryTimeoutSummary summary{};

        for(auto& [recovery_id, incident] : incidents_){
            for(auto& market : incident.market_recoveries){
                std::chrono::milliseconds timeout{};
                if(market.phase == MarketRecoveryPhase::kREQUEST_ENQUEUED){
                    timeout = config_.request_ack_timeout;
                }else if(market.phase == MarketRecoveryPhase::kREQUEST_ACCEPTED){
                    timeout = config_.snapshot_timeout;
                }else{
                    continue;
                }

                if(now < market.last_attempt_time + timeout){
                    continue;
                }
                const auto active_it =
                    active_recovery_by_market_.find(market.market_id);
                if(active_it == active_recovery_by_market_.end() ||
                   active_it->second != recovery_id){
                    continue;
                }

                if(market.phase == MarketRecoveryPhase::kREQUEST_ENQUEUED){
                    ++summary.request_ack_timeouts;
                }else{
                    ++summary.snapshot_timeouts;
                }

                incident.last_progress_at = now;
                if(market.attempts_sent >= config_.max_attempts){
                    market.phase = MarketRecoveryPhase::kFAILED;
                    ++summary.markets_failed;
                    continue;
                }

                market.phase = MarketRecoveryPhase::kPENDING_REQUEST;
                market.next_attempt_time =
                    now + retry_backoff(market.attempts_sent);
                ++summary.retries_scheduled;
            }
        }

        return summary;
    }

    std::size_t RecoveryCoordinator::active_incident_count() const noexcept{
        std::size_t count{};
        for(const auto& [recovery_id, incident] : incidents_){
            const bool active = std::any_of(
                incident.market_recoveries.begin(),
                incident.market_recoveries.end(),
                [this, recovery_id](const MarketRecoveryState& market){
                    const auto iterator =
                        active_recovery_by_market_.find(market.market_id);
                    return iterator != active_recovery_by_market_.end() &&
                           iterator->second == recovery_id;
                });
            if(active){
                ++count;
            }
        }
        return count;
    }
}
