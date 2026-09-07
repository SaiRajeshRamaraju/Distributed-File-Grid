cmake_minimum_required(VERSION 3.16)
project(Distributed-File-Grid VERSION 1.0 LANGUAGES CXX)

# ── C++ standard ──
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ── Build configuration ──
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

# ── Debug flags ──
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-g3 -O0 -DDEBUG -fno-omit-frame-pointer)
    add_compile_options(-fsanitize=address,undefined)
    add_link_options(-fsanitize=address,undefined)
endif()

# Place all binaries in build/ root instead of per-component subdirs
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

# NOTE: Keep this flag enabled for proper LSP support.
# It generates compile_commands.json for accurate project indexing.
set(CMAKE_EXPORT_COMPILE_COMMANDS 1) # Write compile_commands.json
enable_testing()

# ── Required packages ──
find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)

find_package(Protobuf CONFIG)
if(NOT Protobuf_FOUND)
    message(STATUS "Protobuf config not found, trying find_package")
    find_package(Protobuf REQUIRED)
endif()
set(Protobuf_USE_STATIC_LIBS ON)
set(Protobuf_LIBRARIES protobuf::libprotobuf)

find_program(Protobuf_PROTOC_EXECUTABLE NAMES protoc)
if(NOT Protobuf_PROTOC_EXECUTABLE)
    message(FATAL_ERROR "protoc not found")
endif()
message(STATUS "Found Protobuf: ${Protobuf_VERSION}")
message(STATUS "Using protoc: ${Protobuf_PROTOC_EXECUTABLE}")

include_directories(${Protobuf_INCLUDE_DIRS})

# ── Optional Redis ──
option(WITH_REDIS "Build with Redis++ support" OFF)
if(WITH_REDIS)
    find_package(redis++ QUIET)
    find_package(hiredis QUIET)
    if(NOT redis++_FOUND)
        find_path(REDIS_PLUS_PLUS_INCLUDE_DIRS NAMES sw/redis++/redis++.h PATHS /usr/local/include /usr/include)
        find_library(REDIS_PLUS_PLUS_LIB NAMES redis++ PATHS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu)
        find_library(HIREDIS_LIB NAMES hiredis PATHS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu)
        if(REDIS_PLUS_PLUS_INCLUDE_DIRS AND REDIS_PLUS_PLUS_LIB AND HIREDIS_LIB)
            message(STATUS "Found redis++: ${REDIS_PLUS_PLUS_LIB}")
            message(STATUS "Found hiredis: ${HIREDIS_LIB}")
            if(NOT TARGET redis++::redis++)
                add_library(redis++::redis++ UNKNOWN IMPORTED)
                set_target_properties(redis++::redis++ PROPERTIES
                    IMPORTED_LOCATION "${REDIS_PLUS_PLUS_LIB}"
                    INTERFACE_INCLUDE_DIRECTORIES "${REDIS_PLUS_PLUS_INCLUDE_DIRS}"
                    INTERFACE_LINK_LIBRARIES "${HIREDIS_LIB}"
                )
            endif()
        else()
            message(FATAL_ERROR "redis++ or hiredis not found. Please install libredis++-dev and libhiredis-dev")
        endif()
    endif()
    add_compile_definitions(WITH_REDIS)
endif()

# ── Optional tests ──
option(BUILD_TESTS "Build test suite" OFF)

# ── Third-party dependencies (Abseil, Prometheus) ──
include(cmake/Dependencies.cmake)

# ── Protobuf code generation ──
include(cmake/ProtobufGen.cmake)
dfg_generate_proto(
    TARGET_NAME dfg_proto
    PROTO_DIR   ${CMAKE_SOURCE_DIR}/src/proto/v1
)

# ── Source tree ──
add_subdirectory(src)

# ── Tests ──
if(BUILD_TESTS)
    add_subdirectory(tests)
    add_subdirectory(e2e)
endif()
