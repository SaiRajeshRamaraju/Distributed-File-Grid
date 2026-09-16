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

add_executable(client client.cpp)

target_link_libraries(client
    PRIVATE
    dfg_common
    pthread
    argparse
)

# Provide dfg_client as a symlink to client for backward compatibility
add_custom_command(TARGET client POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E create_symlink $<TARGET_FILE_NAME:client> $<TARGET_FILE_DIR:client>/dfg_client
    COMMENT "Creating symlink dfg_client -> client"
)

add_custom_target(dfg_client ALL DEPENDS client)

