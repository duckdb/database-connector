cmake_minimum_required(VERSION 3.5...3.29)

project (database_connection_test C CXX)

if (MSVC) 
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_CURRENT_BINARY_DIR})
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_CURRENT_BINARY_DIR})
endif ()

add_executable(test_database_connection
    test_common.cpp
    test_defer.cpp
    test_make_unique.cpp
    test_pool.cpp
)

target_include_directories(test_database_connection PRIVATE
    ../src/include
    catch
    # duckdb_capi
    include
)
