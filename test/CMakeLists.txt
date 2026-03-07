cmake_minimum_required(VERSION 3.5...3.29)

project (database_connection_test C CXX)

if(MSVC) 
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_CURRENT_BINARY_DIR})
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_CURRENT_BINARY_DIR})
endif()

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

if(NOT MSVC)
    target_compile_options(test_database_connection PRIVATE
        -O0
        -Wall
        -Werror
        -Wextra
        -fsanitize=address
    )
    if(APPLE)
        target_link_options(test_database_connection PRIVATE
            -fsanitize=address
        ) 
    else()
        target_link_libraries(test_database_connection PRIVATE
            asan
        )
    endif()
else()
    target_compile_options(test_database_connection PRIVATE
        /W4
        /WX
    )
    target_compile_definitions(test_database_connection PRIVATE
        -D_CRT_SECURE_NO_WARNINGS
    )
endif()
