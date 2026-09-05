cmake_minimum_required(VERSION 3.24)
project(predex_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(CTest)

find_package(nlohmann_json CONFIG REQUIRED)
find_package(simdjson CONFIG REQUIRED)
find_package(Boost REQUIRED COMPONENTS system)
find_package(OpenSSL REQUIRED)
find_package(GTest CONFIG REQUIRED)
find_package(CURL REQUIRED)

add_executable(predexctl 
  apps/predexctl/main.cpp
)

add_executable(predex
  apps/predex/main.cpp
  src/control/control_plane.cpp
  src/operator/operator_command_handler.cpp
  src/socket/unix_command_server.cpp
  src/exchange/kalshi/adapters/auth_signer.cpp
  src/exchange/kalshi/adapters/market_data_handler.cpp
  src/exchange/kalshi/adapters/order_rest_adapter.cpp
  src/exchange/kalshi/websocket_session.cpp
  include/predex/ingest/kalshi/market_data/frame_pool.hpp
  src/ingest/kalshi/market_data/wire_session.cpp
  include/predex/router/router_types.hpp
  src/router/router.cpp
  include/predex/shard/event.hpp
  include/predex/shard/event_store.hpp
  include/predex/shard/market_parser.hpp
  include/predex/shard/shard.hpp
  src/shard/event_store.cpp
  src/shard/event.cpp
  src/shard/shard.cpp
  src/shard/market_parser.cpp
  include/predex/shard/shard_types.hpp
  src/config/app_config.cpp
  src/config/universe_builder.cpp
  include/predex/logging/market_data_logger.hpp
  include/predex/strategy/strategy.hpp
  include/predex/oms/oms_types.hpp
  include/predex/oms/order_intents.hpp
  include/predex/oms/oms.hpp
  src/oms/oms.cpp
  include/predex/ingest/kalshi/order_data/order_session.hpp
  include/predex/ingest/kalshi/order_data/order_parser.hpp
  src/ingest/kalshi/order_data/order_parser.cpp
  src/exchange/kalshi/order_rest_session.cpp
  src/exchange/kalshi/http2_session.cpp
  src/ingest/kalshi/order_data/order_session.cpp
  src/exchange/kalshi/adapters/order_data_handler.cpp
  src/control/recovery_coordinator.cpp
  include/predex/socket/unique_fd.hpp
  include/predex/socket/socket_path_guard.hpp
)

add_executable(predex_tests
  tests/app_config_tests.cpp
  tests/control_plane_recovery_tests.cpp
  tests/event_tests.cpp
  tests/frame_pool_tests.cpp
  tests/http2_session_tests.cpp
  tests/idle_backoff_tests.cpp
  tests/latency_histogram_tests.cpp
  tests/oms_tests.cpp
  tests/recovery_coordinator_tests.cpp
  tests/router_tests.cpp
  tests/shard_tests.cpp
  tests/wire_sequence_tests.cpp
  tests/wire_session_tests.cpp
  src/control/recovery_coordinator.cpp
  src/config/app_config.cpp
  src/control/control_plane.cpp
  src/router/router.cpp
  src/ingest/kalshi/market_data/wire_session.cpp
  src/exchange/kalshi/adapters/market_data_handler.cpp
  src/exchange/kalshi/websocket_session.cpp
  src/shard/event.cpp
  src/shard/event_store.cpp
  src/shard/market_parser.cpp
  src/shard/shard.cpp
  src/exchange/kalshi/adapters/auth_signer.cpp
  src/exchange/kalshi/http2_session.cpp
  src/oms/oms.cpp
  tests/websocket_session_tests.cpp
)

add_subdirectory(apps/research)

add_executable(predex_http_smoke
  apps/testing/main.cpp
  src/exchange/kalshi/adapters/auth_signer.cpp
  src/exchange/kalshi/http2_session.cpp
)

target_include_directories(predex_tests PRIVATE include)
target_include_directories(predexctl PRIVATE include)
target_include_directories(predex PRIVATE include)
target_include_directories(predex_http_smoke PRIVATE include)
target_link_libraries(predexctl PRIVATE nlohmann_json::nlohmann_json)
target_link_libraries(predex PRIVATE nlohmann_json::nlohmann_json)
target_link_libraries(predex PRIVATE simdjson::simdjson)
target_link_libraries(predex PRIVATE Boost::system OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(predex PRIVATE CURL::libcurl)
target_link_libraries(predex_tests PRIVATE
  GTest::gtest
  GTest::gtest_main
  simdjson::simdjson
  OpenSSL::SSL
  OpenSSL::Crypto
  CURL::libcurl
  Boost::system
  nlohmann_json::nlohmann_json
)
target_link_libraries(predex_http_smoke PRIVATE OpenSSL::SSL OpenSSL::Crypto CURL::libcurl)
add_test(NAME predex_tests COMMAND predex_tests)
