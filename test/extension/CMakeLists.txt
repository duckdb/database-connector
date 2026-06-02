cmake_minimum_required(VERSION 3.5...3.29)

project (test_database_connector_ext C CXX)

if(MSVC) 
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_CURRENT_BINARY_DIR})
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_CURRENT_BINARY_DIR})
endif()

if(NOT DEFINED ENV{DUCKDB_SRC_PATH})
  message(FATAL_ERROR "'DUCKDB_SRC_PATH' environment variable must be defined")
endif()

add_executable(${PROJECT_NAME}
    ../../src/optimizer/aggregate_optimizer.cpp
    ../../src/optimizer/optimizer_util.cpp
    ../../src/optimizer/order_by_and_limit_optimizer.cpp
    ../../src/query/query_writer.cpp
    ../../src/table_scan/filter_pushdown.cpp
    ../../src/table_scan/filter_util.cpp
    ../test_common.cpp
    test_query.cpp
)

target_include_directories(${PROJECT_NAME} PRIVATE
    $ENV{DUCKDB_SRC_PATH}/src/include
    $ENV{DUCKDB_SRC_PATH}/third_party/fmt/include
    ../../src/include
    ../catch
    ../include
)

target_link_libraries(${PROJECT_NAME} PRIVATE
    $ENV{DUCKDB_SRC_PATH}/build/release/src/libduckdb.so
)
