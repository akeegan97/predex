cmake_minimum_required(VERSION 3.24)
project(predex_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(CTest)

find_package(nlohmann_json CONFIG REQUIRED)

add_executable(predexctl 
  apps/predexctl/main.cpp
)
target_link_libraries(predexctl PRIVATE nlohmann_json::nlohmann_json)