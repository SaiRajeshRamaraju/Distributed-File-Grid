include(FetchContent)

find_package(argparse QUIET)

if(NOT argparse_FOUND)
    message(STATUS "argparse not found locally. Fetching from GitHub...")

    FetchContent_Declare(
        argparse
        GIT_REPOSITORY https://github.com/p-ranav/argparse.git
        GIT_TAG v3.2
    )

    FetchContent_MakeAvailable(argparse)
endif()

add_library(dfg_client_lib STATIC src/client.cpp src/diagnostics.cpp)
target_include_directories(dfg_client_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(dfg_client_lib PUBLIC dfg_common)

add_executable(client src/main.cpp)
target_link_libraries(client PRIVATE dfg_client_lib argparse)

# Provide dfg_client as a symlink to client for backward compatibility
add_custom_command(TARGET client POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E create_symlink $<TARGET_FILE_NAME:client> $<TARGET_FILE_DIR:client>/dfg_client
    COMMENT "Creating symlink dfg_client -> client"
)

add_custom_target(dfg_client ALL DEPENDS client)
