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
    ../../src/query/query_writer.cpp
    ../test_common.cpp
    test_query.cpp
)

target_include_directories(${PROJECT_NAME} PRIVATE
    $ENV{DUCKDB_SRC_PATH}/src/include
    ../../src/include
    ../catch
    ../include
)

target_link_libraries(${PROJECT_NAME} PRIVATE
    $ENV{DUCKDB_SRC_PATH}/build/release/src/libduckdb.so
)
