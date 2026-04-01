#include "predex/router/market_registry.hpp"

namespace predex::core::routing{
    MarketRegistry::MarketRegistry(const std::vector<MarketRegistryEntry>& entries){
        routes_.reserve(entries.size());
        for(const auto& entry: entries){
            auto iter = routes_.find(entry.ticker_);
            if(iter != routes_.end()){
                //duplicate entry for ticker, could log this or handle differently if needed but for now just skip/keep first entry
                continue;
            }
            routes_.emplace(entry.ticker_, MarketRoute{entry.market_id_, entry.affinity_key_});
        }
    }

    bool MarketRegistry::try_lookup(std::string_view ticker, MarketRoute& out) const noexcept{
        auto registry_iterator = routes_.find(ticker);
        if(registry_iterator == routes_.end()){
            return false;
        }
        out = registry_iterator->second;
        return true;
    }

    std::size_t MarketRegistry::size() const noexcept{
        return routes_.size();
    }
}