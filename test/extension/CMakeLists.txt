cmake_minimum_required(VERSION 3.5...3.29)

project (test_database_connector_ext C CXX)

# DuckDB headers require C++17 (default on recent GCC, but not on AppleClang)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(MSVC) 
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_CURRENT_BINARY_DIR})
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_CURRENT_BINARY_DIR})
endif()

if(NOT DEFINED ENV{DUCKDB_SRC_PATH})
  message(FATAL_ERROR "'DUCKDB_SRC_PATH' environment variable must be defined")
endif()

add_executable(${PROJECT_NAME}
    ../../src/attached/attached_catalog.cpp
    ../../src/attached/attached_table.cpp
    ../../src/functions/configure_pool.cpp
    ../../src/functions/configure_pool.cpp
    ../../src/optimizer/aggregate_optimizer.cpp
    ../../src/optimizer/optimizer_util.cpp
    ../../src/optimizer/order_by_and_limit_optimizer.cpp
    ../../src/query/query_writer.cpp
    ../../src/table_scan/filter_pushdown.cpp
    ../../src/table_scan/filter_util.cpp
    ../test_common.cpp
    test_configure_pool.cpp
    test_connect.cpp
    test_query.cpp
)

target_include_directories(${PROJECT_NAME} PRIVATE
    $ENV{DUCKDB_SRC_PATH}/src/include
    $ENV{DUCKDB_SRC_PATH}/third_party/fmt/include
    ../../src/include
    ../catch
    ../include
)

# Resolves to libduckdb.so on Linux, libduckdb.dylib on macOS and duckdb.lib on Windows
find_library(DUCKDB_LIBRARY
    NAMES duckdb
    PATHS $ENV{DUCKDB_SRC_PATH}/build/release/src
    NO_DEFAULT_PATH
)
if(NOT DUCKDB_LIBRARY)
  message(FATAL_ERROR "DuckDB shared library not found in '$ENV{DUCKDB_SRC_PATH}/build/release/src'")
endif()

target_link_libraries(${PROJECT_NAME} PRIVATE
    ${DUCKDB_LIBRARY}
)
