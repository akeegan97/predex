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
)

add_executable(smoke 
  tests/spine_smoke.cpp
)
target_include_directories(smoke PRIVATE include)
target_include_directories(predexctl PRIVATE include)
target_include_directories(predex PRIVATE include)
target_link_libraries(predexctl PRIVATE nlohmann_json::nlohmann_json)
target_link_libraries(predex PRIVATE nlohmann_json::nlohmann_json)
target_link_libraries(predex PRIVATE simdjson::simdjson)
target_link_libraries(predex PRIVATE Boost::system OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(smoke PRIVATE simdjson::simdjson)
