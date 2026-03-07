cmake_minimum_required(VERSION 3.5...3.29)

project (database_connection_test C CXX)

if(MSVC) 
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_CURRENT_BINARY_DIR})
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_CURRENT_BINARY_DIR})
endif()

option(ENABLE_THREAD_SANITIZER "Enable thread sanitizer." FALSE)

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

if(${ENABLE_THREAD_SANITIZER})
    message(STATUS "Thread sanitizer is enabled")
    set(SANITIZER_TYPE thread)
else()
    set(SANITIZER_TYPE address)
endif()

if(NOT MSVC)
    target_compile_options(test_database_connection PRIVATE
        -O0
        -Wall
        -Werror
        -Wextra
        -fsanitize=${SANITIZER_TYPE}
    )
    target_link_options(test_database_connection PRIVATE
        -fsanitize=${SANITIZER_TYPE}
    ) 
else()
    target_compile_options(test_database_connection PRIVATE
        /W4
        /WX
    )
    target_compile_definitions(test_database_connection PRIVATE
        -D_CRT_SECURE_NO_WARNINGS
    )
endif()
