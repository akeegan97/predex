cmake_minimum_required(VERSION 3.24)

project(trading_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(spdlog CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(simdjson CONFIG REQUIRED)
find_package(Boost REQUIRED COMPONENTS system)
find_package(OpenSSL REQUIRED)
find_package(GTest CONFIG REQUIRED)

add_library(trading_core src/core/app.cpp)
target_include_directories(trading_core PUBLIC include)
target_link_libraries(trading_core PUBLIC nlohmann_json::nlohmann_json)

add_library(trading_engine src/engine/runtime.cpp src/config/trader_config.cpp)
target_include_directories(trading_engine PUBLIC include)
target_link_libraries(trading_engine PUBLIC trading_core)

add_library(trading_adapters_logging src/adapters/logging/logger.cpp)
target_include_directories(trading_adapters_logging PUBLIC include)
target_link_libraries(trading_adapters_logging PUBLIC spdlog::spdlog)

add_library(trading_adapters_oms src/adapters/exchanges/kalshi/oms_adapter.cpp)
target_include_directories(trading_adapters_oms PUBLIC include)
target_link_libraries(trading_adapters_oms PUBLIC nlohmann_json::nlohmann_json)

add_library(
  trading_oms_runtime
  src/oms/global_risk_gate.cpp
  src/oms/order_manager_core.cpp
  src/oms/order_manager.cpp
  src/oms/paper_order_transport.cpp
  src/oms/portfolio_risk_gate.cpp
  src/oms/position_ledger_core.cpp
  src/oms/position_ledger.cpp
  src/oms/ws_order_transport.cpp
)
target_include_directories(trading_oms_runtime PUBLIC include)
target_link_libraries(trading_oms_runtime PUBLIC trading_adapters_oms)

add_library(
  trading_strategy_runtime
  src/strategy/shard_risk_gate.cpp
  src/strategy/strategy_event_handler.cpp
  src/strategy/strategy_runner.cpp
)
target_include_directories(trading_strategy_runtime PUBLIC include)
target_link_libraries(trading_strategy_runtime PUBLIC trading_oms_runtime)

add_library(
  trading_adapters_ws
  src/adapters/ws/client.cpp
  src/adapters/ws/feed_runner.cpp
  src/adapters/ws/session.cpp
  src/adapters/exchanges/kalshi/auth_signer.cpp
  src/adapters/exchanges/kalshi/ws_adapter.cpp
  src/adapters/exchanges/polymarket/ws_adapter.cpp
)
target_include_directories(trading_adapters_ws PUBLIC include)
target_link_libraries(
  trading_adapters_ws
  PUBLIC nlohmann_json::nlohmann_json
         Boost::system
         OpenSSL::SSL
         OpenSSL::Crypto
)

add_library(
  trading_pipeline
  src/ingest/frame_pool.cpp
  src/ingest/ws_message_sink.cpp
  src/pipeline/live_pipeline.cpp
  src/pipeline/replay_harness.cpp
  src/decode/dispatch.cpp
  src/decode/exchanges/kalshi/extractor.cpp
  src/decode/exchanges/polymarket/extractor.cpp
  src/router/router.cpp
  src/router/shard_dispatch.cpp
  src/shards/book_store.cpp
  src/shards/message_parser.cpp
  src/shards/shard.cpp
  src/parsers/exchanges/kalshi/parser.cpp
  src/parsers/exchanges/polymarket/parser.cpp
)
target_include_directories(trading_pipeline PUBLIC include)
target_link_libraries(
  trading_pipeline
  PUBLIC trading_adapters_ws
         simdjson::simdjson
)

add_executable(trader_app apps/trader/main.cpp)
target_link_libraries(
  trader_app
  PRIVATE trading_engine
          trading_adapters_logging
          trading_strategy_runtime
          trading_adapters_ws
          trading_pipeline
)

add_executable(replay_app apps/replay/main.cpp)
target_link_libraries(
  replay_app
  PRIVATE trading_engine
          trading_adapters_logging
          trading_strategy_runtime
          trading_pipeline
)

add_executable(logger_app apps/logger/main.cpp)
target_link_libraries(
  logger_app
  PRIVATE trading_engine
          trading_adapters_logging
)

if(BUILD_TESTING)
  include(GoogleTest)
  add_executable(trading_tests tests/trading_tests.cpp)
  target_link_libraries(
    trading_tests
    PRIVATE trading_core
            trading_engine
            trading_adapters_oms
            trading_oms_runtime
            trading_strategy_runtime
            trading_adapters_ws
            trading_pipeline
            GTest::gtest_main
  )
  gtest_discover_tests(trading_tests)
endif()
