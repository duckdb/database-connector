cmake_minimum_required(VERSION 3.5...3.29)

project (test_database_connector C CXX)

if(MSVC) 
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_CURRENT_BINARY_DIR})
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_CURRENT_BINARY_DIR})
endif()

option(ENABLE_THREAD_SANITIZER "Enable thread sanitizer." FALSE)

add_executable(${PROJECT_NAME}
    test_common.cpp
    test_defer.cpp
    test_make_unique.cpp
    test_pool.cpp
)

target_include_directories(${PROJECT_NAME} PRIVATE
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
    target_compile_options(${PROJECT_NAME} PRIVATE
        -O0
        -Wall
        -Werror
        -Wextra
        -fsanitize=${SANITIZER_TYPE}
    )
    target_link_options(${PROJECT_NAME} PRIVATE
        -fsanitize=${SANITIZER_TYPE}
    ) 
else()
    target_compile_options(${PROJECT_NAME} PRIVATE
        /W4
        /WX
    )
    target_compile_definitions(${PROJECT_NAME} PRIVATE
        -D_CRT_SECURE_NO_WARNINGS
    )
endif()
