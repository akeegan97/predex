cmake_minimum_required(VERSION 3.24)

project(predex_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(nlohmann_json CONFIG REQUIRED)
find_package(simdjson CONFIG REQUIRED)
find_package(Boost REQUIRED COMPONENTS system)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

add_library(
  predex_websocket
  src/websocket/client.cpp
  src/websocket/session.cpp
  src/websocket/kalshi/auth_signer.cpp
  src/websocket/kalshi/ws_adapter.cpp
)

add_library(predex::websocket ALIAS predex_websocket)

target_include_directories(predex_websocket PUBLIC include)

target_link_libraries(
  predex_websocket
  PUBLIC nlohmann_json::nlohmann_json
         Boost::system
         OpenSSL::SSL
         OpenSSL::Crypto
)

add_library(
  predex_core_pipeline
  src/ingest/frame_pool.cpp
  src/ingest/io_writer.cpp
  src/router/market_registry.cpp
  src/router/shard_dispatch.cpp
  src/router/router.cpp
  src/shards/book_store.cpp
  src/shards/shard.cpp
  src/parsers/kalshi/parser.cpp
  src/tape/logger.cpp
)

add_library(predex::core_pipeline ALIAS predex_core_pipeline)

target_include_directories(predex_core_pipeline PUBLIC include)

target_link_libraries(
  predex_core_pipeline
  PUBLIC simdjson::simdjson
)

add_library(
  predex_app
  src/app.cpp
)

add_library(predex::app ALIAS predex_app)

target_include_directories(predex_app PUBLIC include)

target_link_libraries(
  predex_app
  PUBLIC predex::websocket
         predex::core_pipeline
         Threads::Threads
)

add_executable(
  trader_app
  apps/trader/main.cpp
)

target_link_libraries(
  trader_app
  PRIVATE predex::app
          nlohmann_json::nlohmann_json
)
