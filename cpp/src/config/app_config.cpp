#include "predex/config/app_config.hpp"
#include "predex/exchange/kalshi/kalshi_ws_protocol.hpp"
#include <fstream>
#include <nlohmann/json.hpp>
#include <string>

namespace {
    std::string get_string_or_unsigned(const nlohmann::json& json, const char* field_name){
        if(!json.contains(field_name)){
            throw std::runtime_error(std::string{"Missing required field: "} + field_name);
        }
        const auto& value = json.at(field_name);
        if(value.is_string()){
            return value.get<std::string>();
        }
        if(value.is_number_unsigned()){
            return std::to_string(value.get<std::uint64_t>());
        }
        if(value.is_number_integer()){
            const auto signed_value = value.get<std::int64_t>();
            if(signed_value < 0){
                throw std::runtime_error(std::string{"Field must be non-negative: "} + field_name);
            }
            return std::to_string(static_cast<std::uint64_t>(signed_value));
        }
        throw std::runtime_error(std::string{"Field must be a string or unsigned integer: "} + field_name);
    }

    std::string get_required_string(const nlohmann::json& json, const char* field_name){
        if(!json.contains(field_name) || !json.at(field_name).is_string()){
            throw std::runtime_error(std::string{"Missing or invalid string field: "} + field_name);
        }
        return json.at(field_name).get<std::string>();
    }

    predex::exchange::kalshi::KalshiMarketDataChannel parse_market_data_channel(const std::string& channel){
        if(channel == "orderbook_delta"){
            return predex::exchange::kalshi::KalshiMarketDataChannel::kORDERBOOK_DELTA;
        }
        if(channel == "trade"){
            return predex::exchange::kalshi::KalshiMarketDataChannel::kTRADE;
        }
        if(channel == "market_lifecycle_v2"){
            return predex::exchange::kalshi::KalshiMarketDataChannel::kMARKET_LIFECYCLE;
        }
        throw std::runtime_error("Unknown Kalshi market data channel: " + channel);
    }

    predex::exchange::kalshi::KalshiOrderDataChannel parse_order_data_channel(const std::string& channel){
        if(channel == "fill"){
            return predex::exchange::kalshi::KalshiOrderDataChannel::kFILL;
        }
        if(channel == "market_positions"){
            return predex::exchange::kalshi::KalshiOrderDataChannel::kMARKET_POSITIONS;
        }
        if(channel == "user_orders"){
            return predex::exchange::kalshi::KalshiOrderDataChannel::kUSER_ORDERS;
        }
        throw std::runtime_error("Unknown Kalshi private order feed channel: " + channel);
    }
}
namespace predex::config{

    [[nodiscard]] AppConfig default_app_config(){
        return AppConfig{};
    }
//NOLINTNEXTLINE: json parsing 
    [[nodiscard]] AppConfig load_app_config(std::string_view config_path){
        try{
            std::ifstream config_file(std::string{config_path});
            if (!config_file.is_open()) {
                throw std::runtime_error("Failed to open config file: " + std::string{config_path});
            }

            nlohmann::json json_config;
            config_file >> json_config;
            
            AppConfig config{};

            if(json_config.contains("runtime")){
                const auto& runtime_json = json_config["runtime"];
                RuntimeConfig runtime_config{};
                if(runtime_json.contains("shard_count")){
                    runtime_config.shard_count = runtime_json["shard_count"].get<std::size_t>();
                }
                if(runtime_json.contains("shard_queue_capacity")){
                    runtime_config.shard_queue_capacity = runtime_json["shard_queue_capacity"].get<std::size_t>();
                }
                if(runtime_json.contains("router_queue_capacity")){
                    runtime_config.router_queue_capacity = runtime_json["router_queue_capacity"].get<std::size_t>();
                }
                if(runtime_json.contains("frame_pool_capacity")){
                    runtime_config.frame_pool_capacity = runtime_json["frame_pool_capacity"].get<std::size_t>();
                }
                if(runtime_json.contains("operator_queue_capacity")){
                    runtime_config.operator_queue_capacity = runtime_json["operator_queue_capacity"].get<std::size_t>();
                }
                if(runtime_json.contains("operator_socket_path")){
                    runtime_config.operator_socket_path = runtime_json["operator_socket_path"].get<std::string>();
                }
                if(runtime_json.contains("market_data_tape_path")){
                    runtime_config.market_data_tape_path = runtime_json["market_data_tape_path"].get<std::string>();
                }
                if(runtime_json.contains("synthetic_trading_session_enabled")){
                    runtime_config.synthetic_trading_session_enabled = runtime_json["synthetic_trading_session_enabled"].get<bool>();
                }
                if(runtime_json.contains("reduce_only_after_seconds")){
                    runtime_config.reduce_only_after_seconds = runtime_json["reduce_only_after_seconds"].get<std::uint64_t>();
                }
                if(runtime_json.contains("flatten_to_zero_after_seconds")){
                    runtime_config.flatten_to_zero_after_seconds = runtime_json["flatten_to_zero_after_seconds"].get<std::uint64_t>();
                }
                if(runtime_json.contains("stopped_after_seconds")){
                    runtime_config.stopped_after_seconds = runtime_json["stopped_after_seconds"].get<std::uint64_t>();
                }

                config.runtime = std::move(runtime_config);
            }
            if (json_config.contains("kalshi")){
                const auto& kalshi_json = json_config["kalshi"];
                KalshiConfig kalshi_config{};

                if(kalshi_json.contains("auth")){
                    const auto& auth_json = kalshi_json["auth"];
                    KalshiAuthConfig auth_config{};
                    if(auth_json.contains("key_id_env")){
                        auth_config.key_id_env = auth_json["key_id_env"].get<std::string>();
                    }
                    if(auth_json.contains("private_key_pem_env")){
                        auth_config.private_key_pem_env = auth_json["private_key_pem_env"].get<std::string>();
                    }
                    kalshi_config.auth = std::move(auth_config);
                }

                if(kalshi_json.contains("market_data")){
                    const auto& market_data_json = kalshi_json["market_data"];
                    KalshiMarketDataConfig market_data_config{};
                    if(market_data_json.contains("enable_market_data")){
                        market_data_config.enable_market_data = market_data_json["enable_market_data"].get<bool>();
                    }
                    if(market_data_json.contains("channels")){
                        for(const auto& channel_str : market_data_json["channels"]){
                            if(!channel_str.is_string()){
                                throw std::runtime_error("Invalid Kalshi market data channel: expected string");
                            }
                            const std::string channel_name = channel_str.get<std::string>();
                            market_data_config.channels.push_back(parse_market_data_channel(channel_name));
                        }
                    }
                    kalshi_config.market_data = std::move(market_data_config);
                }

                if(kalshi_json.contains("order_rest")){
                    const auto& order_rest_json = kalshi_json["order_rest"];
                    KalshiOrderRestConfig order_rest_config{};
                    if(order_rest_json.contains("enable_order_rest")){
                        order_rest_config.enable_order_rest = order_rest_json["enable_order_rest"].get<bool>();
                    }
                    if(order_rest_json.contains("endpoint")){
                        order_rest_config.endpoint = order_rest_json["endpoint"].get<std::string>();
                    }
                    if(order_rest_json.contains("max_concurrent_streams")){
                        order_rest_config.max_concurrent_streams = order_rest_json["max_concurrent_streams"].get<std::size_t>();
                    }
                    kalshi_config.order_rest = std::move(order_rest_config);
                }

                if(kalshi_json.contains("private_order_feed")){
                    const auto& private_order_feed_json = kalshi_json["private_order_feed"];
                    KalshiPrivateOrderFeedConfig private_order_feed_config{};
                    if(private_order_feed_json.contains("enable_private_order_feed")){
                        private_order_feed_config.enable_private_order_feed = private_order_feed_json["enable_private_order_feed"].get<bool>();
                    }
                    if(private_order_feed_json.contains("channels")){
                        for(const auto& channel_str : private_order_feed_json["channels"]){
                            if(!channel_str.is_string()){
                                throw std::runtime_error("Invalid Kalshi private order feed channel: expected string");
                            }
                            const std::string channel_name = channel_str.get<std::string>();
                            private_order_feed_config.channels.push_back(parse_order_data_channel(channel_name));
                        }
                    }
                    kalshi_config.private_order_feed = std::move(private_order_feed_config);
                }

                config.kalshi = std::move(kalshi_config);
            }
            if(json_config.contains("universe")){
                const auto& universe_json = json_config["universe"];
                UniverseConfig universe_config{};
                if(universe_json.contains("events")){
                    for(const auto& event_json : universe_json["events"]){
                        EventConfig event_config{
                            .event_id = get_string_or_unsigned(event_json, "event_id"),
                            .affinity_key = get_string_or_unsigned(event_json, "affinity_key"),
                            .topology = get_required_string(event_json, "topology"),
                            .markets = {},
                        };

                        if(!event_json.contains("markets") || !event_json["markets"].is_array()){
                            throw std::runtime_error("Universe event markets must be an array");
                        }

                        for(const auto& market_json : event_json["markets"]){
                            MarketConfig market_config{
                                .market_id = get_string_or_unsigned(market_json, "market_id"),
                                .kalshi_ticker = get_required_string(market_json, "kalshi_ticker"),
                                .tradeable = market_json.value("tradeable", false),
                                .price_level_structure = get_required_string(market_json, "price_level_structure"),
                            };
                            event_config.markets.push_back(std::move(market_config));
                        }

                        universe_config.events.push_back(std::move(event_config));
                    }
                }

                config.universe = std::move(universe_config);
            }
            validate_app_config(config);
            return config;
        } catch (const std::exception& e) {
            throw std::runtime_error("Error loading app config from " + std::string{config_path} + ": " + e.what());
        }
    }
//NOLINTNEXTLINE: heavy config validation logic
    void validate_app_config(const AppConfig& config){
        if(config.runtime.shard_count == 0){
            throw std::runtime_error("Invalid configuration: shard_count must be greater than 0");
        }
        if(config.runtime.shard_queue_capacity == 0 || ((config.runtime.shard_queue_capacity & (config.runtime.shard_queue_capacity - 1)) != 0)){
            throw std::runtime_error("Invalid configuration: shard_queue_capacity must be a positive power of two");
        }
        if(config.runtime.router_queue_capacity == 0 || ((config.runtime.router_queue_capacity & (config.runtime.router_queue_capacity - 1)) != 0)){
            throw std::runtime_error("Invalid configuration: router_queue_capacity must be a positive power of two");
        }
        if(config.runtime.frame_pool_capacity == 0 || ((config.runtime.frame_pool_capacity & (config.runtime.frame_pool_capacity - 1)) != 0)){
            throw std::runtime_error("Invalid configuration: frame_pool_capacity must be a positive power of two");
        }
        if(config.runtime.operator_queue_capacity == 0 || ((config.runtime.operator_queue_capacity & (config.runtime.operator_queue_capacity - 1)) != 0)){
            throw std::runtime_error("Invalid configuration: operator_queue_capacity must be a positive power of two");
        }
        if(config.runtime.operator_socket_path.empty()){
            throw std::runtime_error("Invalid configuration: operator_socket_path must not be empty");
        }
        if(config.runtime.market_data_tape_path.empty()){
            throw std::runtime_error("Invalid configuration: market_data_tape_path must not be empty");
        }
        if(config.runtime.synthetic_trading_session_enabled){
            if(config.runtime.reduce_only_after_seconds == 0){
                throw std::runtime_error("Invalid configuration: reduce_only_after_seconds must be greater than 0 when synthetic trading session is enabled");
            }
            if(config.runtime.flatten_to_zero_after_seconds != 0 &&
               config.runtime.flatten_to_zero_after_seconds < config.runtime.reduce_only_after_seconds){
                throw std::runtime_error("Invalid configuration: flatten_to_zero_after_seconds must be >= reduce_only_after_seconds");
            }
            if(config.runtime.stopped_after_seconds != 0){
                const std::uint64_t lower_bound = config.runtime.flatten_to_zero_after_seconds != 0
                    ? config.runtime.flatten_to_zero_after_seconds
                    : config.runtime.reduce_only_after_seconds;
                if(config.runtime.stopped_after_seconds < lower_bound){
                    throw std::runtime_error("Invalid configuration: stopped_after_seconds must be >= the previous synthetic trading session cutoff");
                }
            }
        }
        if(config.universe.events.empty()){
            throw std::runtime_error("Invalid configuration: universe must contain at least one event");
        }
        for(const auto& event : config.universe.events){
            if(event.event_id.empty()){
                throw std::runtime_error("Invalid configuration: universe event_id must not be empty");
            }
            if(event.affinity_key.empty()){
                throw std::runtime_error("Invalid configuration: universe affinity_key must not be empty");
            }
            if(event.topology.empty()){
                throw std::runtime_error("Invalid configuration: universe topology must not be empty");
            }
            if(event.markets.empty()){
                throw std::runtime_error("Invalid configuration: universe event must contain at least one market");
            }
            for(const auto& market : event.markets){
                if(market.market_id.empty()){
                    throw std::runtime_error("Invalid configuration: universe market_id must not be empty");
                }
                if(market.kalshi_ticker.empty()){
                    throw std::runtime_error("Invalid configuration: universe kalshi_ticker must not be empty");
                }
                if(market.price_level_structure.empty()){
                    throw std::runtime_error("Invalid configuration: universe price_level_structure must not be empty");
                }
            }
        }
        if(config.kalshi.market_data.enable_market_data){
            if(config.kalshi.auth.key_id_env.empty()){
                throw std::runtime_error("Invalid configuration: kalshi.auth.key_id_env must not be empty when market data is enabled");
            }
            if(config.kalshi.auth.private_key_pem_env.empty()){
                throw std::runtime_error("Invalid configuration: kalshi.auth.private_key_pem_env must not be empty when market data is enabled");
            }
            if(config.kalshi.market_data.channels.empty()){
                throw std::runtime_error("Invalid configuration: kalshi.market_data.channels must not be empty when market data is enabled");
            }
        }
        if(config.kalshi.order_rest.enable_order_rest){
            if(config.kalshi.auth.key_id_env.empty()){
                throw std::runtime_error("Invalid configuration: kalshi.auth.key_id_env must not be empty when order REST is enabled");
            }
            if(config.kalshi.auth.private_key_pem_env.empty()){
                throw std::runtime_error("Invalid configuration: kalshi.auth.private_key_pem_env must not be empty when order REST is enabled");
            }
            if(config.kalshi.order_rest.max_concurrent_streams == 0){
                throw std::runtime_error("Invalid configuration: kalshi.order_rest.max_concurrent_streams must be greater than 0 when order REST is enabled");
            }
        }
        if(config.kalshi.private_order_feed.enable_private_order_feed){
            if(config.kalshi.auth.key_id_env.empty()){
                throw std::runtime_error("Invalid configuration: kalshi.auth.key_id_env must not be empty when private order feed is enabled");
            }
            if(config.kalshi.auth.private_key_pem_env.empty()){
                throw std::runtime_error("Invalid configuration: kalshi.auth.private_key_pem_env must not be empty when private order feed is enabled");
            }
            if(config.kalshi.private_order_feed.channels.empty()){
                throw std::runtime_error("Invalid configuration: kalshi.private_order_feed.channels must not be empty when private order feed is enabled");
            }
        }
    }

}
