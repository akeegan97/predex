#include "predex/app.hpp"
#include "predex/ingest/frame_pool.hpp"
#include "predex/ingest/io_writer.hpp"
#include "predex/router/market_registry.hpp"
#include "predex/router/router.hpp"
#include "predex/router/shard_dispatch.hpp"
#include "predex/shards/book_store.hpp"
#include "predex/shards/shard.hpp"
#include "predex/tape/logger.hpp"
#include "predex/parsers/kalshi/parser.hpp"
#include "predex/websocket/client.hpp"
#include "predex/websocket/session.hpp"
#include "predex/websocket/kalshi/auth_signer.hpp"
#include "predex/websocket/kalshi/ws_adapter.hpp"
#include "predex/utils/spsc_queue.hpp"
#include <chrono>
#include <fmt/base.h>
#include <memory>
#include <thread>
#include <atomic>
#include <vector>
#include <mutex>


namespace predex {
    constexpr std::int64_t kDefaultSleepMs = 100;
    struct App::Runtime {
        using FrameHandle = predex::core::ingest::kalshi::FrameHandle;
        using FrameQueue = predex::utils::SPSCQueue<FrameHandle>;

        explicit Runtime(AppConfig config_in);

        AppConfig config;
        mutable std::mutex error_mutex;
        std::string last_error;
        std::atomic<bool> running{false};
        std::vector<core::routing::kalshi::MarketRegistryEntry> market_registry_entries;
        static std::vector<predex::core::routing::kalshi::MarketRegistryEntry>
        build_market_registry_entries(const AppConfig& config) {
            std::vector<predex::core::routing::kalshi::MarketRegistryEntry> entries;
            entries.reserve(config.market_routes.size());
            for (const auto& route : config.market_routes) {
                entries.push_back({
                    .ticker_ = route.market_ticker,
                    .market_id_ = static_cast<uint32_t>(route.market_id),
                    .affinity_key_ = static_cast<uint16_t>(route.affinity_key),
                });
            }
            return entries;
        }

        websocket::kalshi::AuthSigner auth_signer;
        websocket::kalshi::WsAdapter ws_adapter;
        websocket::BoostBeastWsTransport ws_transport;
        websocket::WsSession ws_session;

        core::ingest::kalshi::FramePool frame_pool;
        std::unique_ptr<FrameQueue> io_to_router_queue;
        std::unique_ptr<FrameQueue> router_to_logger_queue;
        std::unique_ptr<FrameQueue> recycle_queue;
        std::vector<std::unique_ptr<FrameQueue>> shard_input_queues;
        std::vector<std::unique_ptr<FrameQueue>> shard_to_logger_queues;

        core::routing::kalshi::MarketRegistry market_registry;
        std::vector<FrameQueue*> shard_input_queue_ptrs;
        std::vector<FrameQueue*> logger_input_queue_ptrs;

        std::unique_ptr<core::routing::kalshi::ShardDispatch> shard_dispatch;
        std::unique_ptr<core::routing::kalshi::Router> router;
        std::unique_ptr<core::ingest::kalshi::IOWriter> io_writer;
        std::unique_ptr<core::tape::kalshi::Logger> tape_logger;

        std::vector<core::shards::kalshi::BookStore> book_stores;
        std::vector<std::unique_ptr<core::shards::kalshi::Shard>> shards;
        std::jthread io_thread;
        std::jthread router_thread;
        std::vector<std::jthread> shard_threads; // will house strategy & risk eventually on the same thread as shard 
        //eventually add thread for OMS/EMS 
        std::jthread logger_thread;

        
        

        bool start();
        void run();
        void stop();

        void io_loop(const std::stop_token& stop_token);
        void router_loop(const std::stop_token& stop_token) const;
        void shard_loop(std::size_t shard_index, const std::stop_token& stop_token) const;
        void logger_loop(const std::stop_token& stop_token) const;
        void set_error(std::string message);
        std::string_view last_error_view() const noexcept;
    };
    App::Runtime::Runtime(AppConfig config_in)
        : config(std::move(config_in)),
        market_registry_entries(build_market_registry_entries(config)),
        auth_signer(websocket::kalshi::Credentials{
            .key_id = config.kalshi_ws.key_id,
            .private_key_pem = config.kalshi_ws.private_key_pem,
        }),
        ws_adapter(auth_signer, config.kalshi_ws.endpoint),
        ws_session(ws_transport, ws_adapter),
        frame_pool(config.pipeline.frame_pool_capacity),
        market_registry(market_registry_entries) {

        io_to_router_queue =
            std::make_unique<FrameQueue>(config.pipeline.io_to_router_capacity);
        router_to_logger_queue =
            std::make_unique<FrameQueue>(config.pipeline.router_to_logger_capacity);
        recycle_queue =
            std::make_unique<FrameQueue>(config.pipeline.frame_pool_capacity);

        shard_input_queues.reserve(config.pipeline.shard_count);
        shard_to_logger_queues.reserve(config.pipeline.shard_count);
        shard_input_queue_ptrs.reserve(config.pipeline.shard_count);
        logger_input_queue_ptrs.reserve(config.pipeline.shard_count + 1);
        book_stores.reserve(config.pipeline.shard_count);
        shards.reserve(config.pipeline.shard_count);

        for (std::size_t i = 0; i < config.pipeline.shard_count; ++i) {
            shard_input_queues.push_back(
                std::make_unique<FrameQueue>(config.pipeline.shard_input_capacity));
            shard_to_logger_queues.push_back(
                std::make_unique<FrameQueue>(config.pipeline.shard_to_logger_capacity));
            book_stores.emplace_back();
        }

        for (const auto& queue : shard_input_queues) {
            shard_input_queue_ptrs.push_back(queue.get());
        }

        logger_input_queue_ptrs.push_back(router_to_logger_queue.get());
        for (const auto& queue : shard_to_logger_queues) {
            logger_input_queue_ptrs.push_back(queue.get());
        }

        shard_dispatch =
            std::make_unique<core::routing::kalshi::ShardDispatch>(shard_input_queue_ptrs);

        router = std::make_unique<core::routing::kalshi::Router>(
            *io_to_router_queue,
            frame_pool,
            market_registry,
            *shard_dispatch,
            *router_to_logger_queue);

        io_writer = std::make_unique<core::ingest::kalshi::IOWriter>(
            frame_pool,
            *io_to_router_queue,
            *recycle_queue);

        tape_logger = std::make_unique<core::tape::kalshi::Logger>(
            logger_input_queue_ptrs,
            frame_pool,
            *recycle_queue,
            config.tape.output_path);

        for (std::size_t i = 0; i < config.pipeline.shard_count; ++i) {
            shards.push_back(std::make_unique<core::shards::kalshi::Shard>(
                *shard_input_queues[i],
                frame_pool,
                *shard_to_logger_queues[i],
                predex::core::parsers::kalshi::Parser{},
                book_stores[i],
                nullptr));
        }
    }
    void App::Runtime::set_error(std::string message){
        std::lock_guard lock(error_mutex);
        last_error = std::move(message);
    }

    bool App::Runtime::start(){
        if(running.load(std::memory_order_acquire)){
            return true;
        }
        if(!ws_session.connect()){
            ws_session.close();
            set_error(ws_session.last_error());
            return false;
        }

        for(const auto& channel : config.kalshi_ws.channels){
            if(!ws_session.subscribe(channel, config.kalshi_ws.market_tickers)){
                ws_session.close();
                set_error(ws_session.last_error());
                return false;
            }
        }
        set_error("");
        running.store(true, std::memory_order_release);
        
        io_thread = std::jthread([this](const std::stop_token& stop_token){
            io_loop(stop_token);
        });
        router_thread = std::jthread([this](const std::stop_token& stop_token){
            router_loop(stop_token);
        });
        shard_threads.clear();
        shard_threads.reserve(config.pipeline.shard_count);
        for(std::size_t i = 0; i < config.pipeline.shard_count; ++i){
            shard_threads.emplace_back([this, i](const std::stop_token& stop_token){
                shard_loop(i, stop_token);
            });
        }
        logger_thread = std::jthread([this](const std::stop_token& stop_token){
            logger_loop(stop_token);
        });
        return true;
    }

    void App::Runtime::io_loop(const std::stop_token& stop_token) {
        while (running.load(std::memory_order_acquire) && !stop_token.stop_requested()) {
            const auto recv_result = ws_session.recv_text(std::chrono::milliseconds{50});

            if (recv_result.status == websocket::RecvStatus::kTimeout) {
                continue;
            }

            if (recv_result.status == websocket::RecvStatus::kClosed) {
                if (running.load(std::memory_order_acquire)) {
                    set_error("Websocket connection closed unexpectedly");
                    running.store(false, std::memory_order_release);
                }
                break;
            }

            if (recv_result.status == websocket::RecvStatus::kError) {
                set_error(ws_session.last_error());
                running.store(false, std::memory_order_release);
                break;
            }

            if (!io_writer->on_wire_message(recv_result.payload)) {
                set_error("Failed to enqueue message into IOWriter");
                running.store(false, std::memory_order_release);
                break;
            }
        }

        ws_session.close();
    }

    void App::Runtime::router_loop(const std::stop_token& stop_token) const {
        while(running.load(std::memory_order_acquire) && !stop_token.stop_requested()){
            const auto routed = router->pump(config.pipeline.io_to_router_capacity);
            if(routed == 0U){
                std::this_thread::yield();
            }
        }
    }

    void App::Runtime::shard_loop(std::size_t shard_index, const std::stop_token& stop_token) const {
        const auto& shard = shards[shard_index];
        while(running.load(std::memory_order_acquire) && !stop_token.stop_requested()){
            const auto processed = shard->pump(config.pipeline.shard_input_capacity);
            if(processed == 0U){
                std::this_thread::yield();
            }
        }
    }

    void App::Runtime::logger_loop(const std::stop_token& stop_token) const {
        while(running.load(std::memory_order_acquire) && !stop_token.stop_requested()){
            const auto logged = tape_logger->pump(config.pipeline.router_to_logger_capacity);
            if(logged == 0U){
                std::this_thread::yield();
            }
        }
    }

    void App::Runtime::run(){
        if(!running.load(std::memory_order_acquire)){
            set_error("Attempted to run App that is not started");
            return;
        }
        while(running.load(std::memory_order_acquire)){
            std::this_thread::sleep_for(std::chrono::milliseconds(kDefaultSleepMs));
        }
        // stop();
    }

    void App::Runtime::stop(){
        running.store(false, std::memory_order_release);
        if(io_thread.joinable()){
            io_thread.request_stop();
            io_thread.join();
        }
        if(router_thread.joinable()){
            router_thread.request_stop();
            router_thread.join();
        }
        for(auto& thread : shard_threads){
            if(thread.joinable()){
                thread.request_stop();
                thread.join();
            }
        }
        shard_threads.clear();
        if(logger_thread.joinable()){
            logger_thread.request_stop();
            logger_thread.join();
        }

    }

    App::App(AppConfig config)
        : runtime_(std::make_unique<Runtime>(std::move(config))) {}

    App::~App() = default;
    bool App::start() {
        return runtime_->start(); 
    }


    void App::run() {
        runtime_->run();
    }

    void App::stop() {
        runtime_->stop();
    }

    std::string App::last_error() const noexcept {
        std::scoped_lock lock(runtime_->error_mutex);
        return runtime_->last_error;
    }



}
