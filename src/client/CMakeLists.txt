cmake_minimum_required(VERSION 3.14)
project(DFGProject LANGUAGES CXX)

set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)

find_package(argparse QUIET)

if(NOT argparse_FOUND)
    message(STATUS "argparse not found locally. Fetching from GitHub...")

    FetchContent_Declare(
        argparse
        GIT_REPOSITORY https://github.com/p-ranav/argparse.git
    )

    FetchContent_MakeAvailable(argparse)
endif()

add_executable(dfg_client client.cpp)

target_link_libraries(dfg_client
    PRIVATE
    dfg_common
    pthread
    argparse
)

