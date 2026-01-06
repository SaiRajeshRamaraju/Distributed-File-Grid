cmake_minimum_required(VERSION 3.16)
project(Distributed-File-Grid VERSION 1.0 LANGUAGES CXX)

# Set C++ standard and build type
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build configuration
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

# Enable testing
enable_testing()

# Find required packages
find_package(Threads REQUIRED)

# Find Protobuf
find_package(Protobuf REQUIRED CONFIG)
if(NOT Protobuf_FOUND)
    message(STATUS "Protobuf config not found, trying to find package")
    find_package(Protobuf REQUIRED)
endif()

# Set protobuf variables
set(Protobuf_USE_STATIC_LIBS ON)
set(Protobuf_LIBRARIES protobuf::libprotobuf)

# Find protoc
find_program(Protobuf_PROTOC_EXECUTABLE NAMES protoc)
if(NOT Protobuf_PROTOC_EXECUTABLE)
    message(FATAL_ERROR "protoc not found")
endif()

message(STATUS "Found Protobuf: ${Protobuf_VERSION}")
message(STATUS "Using protoc: ${Protobuf_PROTOC_EXECUTABLE}")

include_directories(${Protobuf_INCLUDE_DIRS})

# Find ZLIB
find_package(ZLIB REQUIRED)

# Find or fetch Abseil
include(FetchContent)
FetchContent_Declare(
    abseil-cpp
    GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
    GIT_TAG 20230125.3
)
set(ABSL_PROPAGATE_CXX_STD ON)
set(ABSL_BUILD_TESTING OFF)
set(ABSL_ENABLE_INSTALL ON)
set(ABSL_USE_GOOGLETEST_HEAD OFF)
set(ABSL_PROPAGATE_CXX_STD ON)
FetchContent_MakeAvailable(abseil-cpp)

# Add Abseil logging library
set(ABSEIL_LIBS
    absl::log_internal_check_op
    absl::log_internal_conditions
    absl::log_internal_format
    absl::log_internal_globals
    absl::log_internal_log_sink_set
    absl::log_internal_message
    absl::log_internal_nullguard
    absl::log_internal_proto
    absl::log
)

# Prometheus-cpp (download via FetchContent to avoid external dependency)
FetchContent_Declare(
    prometheus-cpp
    GIT_REPOSITORY https://github.com/jupp0r/prometheus-cpp.git
    GIT_TAG v1.2.4
)
set(_DFG_ENABLE_TESTING_BACKUP ${ENABLE_TESTING})
set(ENABLE_TESTING OFF CACHE BOOL "Build prometheus-cpp tests" FORCE)
FetchContent_MakeAvailable(prometheus-cpp)
if(DEFINED _DFG_ENABLE_TESTING_BACKUP)
    set(ENABLE_TESTING ${_DFG_ENABLE_TESTING_BACKUP} CACHE BOOL "Build tests" FORCE)
else()
    unset(ENABLE_TESTING CACHE)
endif()

set(PROMETHEUS_LIBS
    prometheus-cpp::core
    prometheus-cpp::pull
    prometheus-cpp::push
)

# Make Redis++ optional
option(WITH_REDIS "Build with Redis++ support" OFF)
if(WITH_REDIS)
    find_package(redis++ REQUIRED)
    add_compile_definitions(WITH_REDIS)
endif()

# Common include directories
add_subdirectory(src)

# Generate protobuf files
file(GLOB PROTO_FILES "${CMAKE_SOURCE_DIR}/src/protos/v1/*.proto")

# Create output directory for generated protobuf files
set(PROTO_GEN_DIR ${CMAKE_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

# Generate C++ sources from .proto files
set(PROTO_SRCS)
set(PROTO_HDRS)
foreach(PROTO_FILE ${PROTO_FILES})
    get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
    get_filename_component(PROTO_PATH ${PROTO_FILE} DIRECTORY)
    
    set(PROTO_SRC "${PROTO_GEN_DIR}/${PROTO_NAME}.pb.cc")
    set(PROTO_HDR "${PROTO_GEN_DIR}/${PROTO_NAME}.pb.h")
    
    add_custom_command(
        OUTPUT ${PROTO_SRC} ${PROTO_HDR}
        COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        ARGS --cpp_out=${PROTO_GEN_DIR} -I ${PROTO_PATH} ${PROTO_FILE}
        DEPENDS ${PROTO_FILE} protobuf::protoc
        COMMENT "Generating C++ code for ${PROTO_NAME}.proto"
        VERBATIM
    )
    
    list(APPEND PROTO_SRCS ${PROTO_SRC})
    list(APPEND PROTO_HDRS ${PROTO_HDR})
    
    # Make sure the generated files are cleaned
    set_source_files_properties(${PROTO_SRC} ${PROTO_HDR} PROPERTIES GENERATED TRUE)
    
    # Add the generated directory to include paths
    include_directories(${PROTO_GEN_DIR})
endforeach()

# Common link libraries for all targets
set(COMMON_LIBS 
    ${Protobuf_LIBRARIES}
    Threads::Threads
)

# Head Server
add_executable(head_server 
    src/Head_Server/head_server.cpp
    src/Head_Server/asyc_file_recv_to_chunks.cpp
    src/Head_Server/chunck_read_to_file.cpp
    ${PROTO_SRCS}
)

target_link_libraries(head_server
    PRIVATE
        ${CMAKE_THREAD_LIBS_INIT}
        protobuf::libprotobuf
        ${ABSEIL_LIBS}
        ${PROMETHEUS_LIBS}
        ${ZLIB_LIBRARIES}
)

# Cluster Server
add_executable(cluster_server 
    src/Cluster_Server/cluster_server.cpp
    src/Cluster_Server/async_file_recv.cpp
    src/Cluster_Server/metrics_exporter.cpp
    ${PROTO_SRCS}
)

target_link_libraries(cluster_server
    PRIVATE
        ${CMAKE_THREAD_LIBS_INIT}
        protobuf::libprotobuf
        ${ABSEIL_LIBS}
        ${PROMETHEUS_LIBS}
        ${ZLIB_LIBRARIES}
)

# Health Checker
add_executable(health_checker
    src/Health_Checker/health_checker.cpp
    ${PROTO_SRCS}
)

target_include_directories(health_checker PRIVATE
    ${CMAKE_SOURCE_DIR}/src/include
    ${CMAKE_BINARY_DIR}/generated
)

target_link_libraries(health_checker PRIVATE
    ${COMMON_LIBS}
    absl::log_internal_check_op
)

# ZooKeeper Head Server Monitor (now backed by the embedded coordinator)
add_executable(zk_head_server_monitor
    src/ZooKeeper_HealthChecker/zk_head_server_monitor.cpp
    src/ZooKeeper_HealthChecker/zookeeper_client.cpp
)

target_include_directories(zk_head_server_monitor PRIVATE
    ${CMAKE_SOURCE_DIR}/src/include
)

target_link_libraries(zk_head_server_monitor PRIVATE
    ${CMAKE_THREAD_LIBS_INIT}
)

# Include directories for protobuf
target_include_directories(head_server PRIVATE 
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/include
    ${CMAKE_BINARY_DIR}/generated
    ${CMAKE_SOURCE_DIR}/src/protos/v1/generate
    ${Protobuf_INCLUDE_DIRS}
)

target_include_directories(cluster_server PRIVATE 
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/include
    ${CMAKE_BINARY_DIR}/generated
    ${CMAKE_SOURCE_DIR}/src/protos/v1/generate
    ${Protobuf_INCLUDE_DIRS}
)

target_include_directories(health_checker PRIVATE 
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/include
    ${CMAKE_BINARY_DIR}/generated
    ${CMAKE_SOURCE_DIR}/src/protos/v1/generate
    ${Protobuf_INCLUDE_DIRS}
)

# Include directories and libraries for redis++
if(WITH_REDIS)
    target_include_directories(head_server PRIVATE ${REDIS_PLUS_PLUS_INCLUDE_DIRS})
    target_compile_options(head_server PRIVATE ${REDIS_PLUS_PLUS_CFLAGS_OTHER})
    target_link_libraries(head_server PRIVATE redis++::redis++)
endif()

# Compiler options
target_compile_options(cluster_server PRIVATE -fcoroutines)
target_compile_options(health_checker PRIVATE -fcoroutines)

# Testing
option(BUILD_TESTS "Build test suite" OFF)  # Disable tests by default for now
if(BUILD_TESTS)
    find_package(GTest REQUIRED)
    
    # Add test executables
    add_executable(simple_heartbeat_test
        UnitTesting/simple_heartbeat_test.cpp
    )
    
    # Link test dependencies
    target_link_libraries(simple_heartbeat_test PRIVATE
        GTest::GTest
        GTest::Main
        Threads::Threads
        ${COMMON_LIBS}
    )
    
    # Add tests
    add_test(NAME SimpleHeartbeatTest COMMAND simple_heartbeat_test)
    
    # Add a test target that can be run with 'make test'
    add_custom_target(check
        COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
        DEPENDS simple_heartbeat_test
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    )
endif()
