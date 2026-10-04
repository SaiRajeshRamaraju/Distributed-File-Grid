# tests/CMakeLists.txt

find_package(GTest REQUIRED)
find_package(Python3 REQUIRED COMPONENTS Interpreter)

# ── Unit tests ──

add_executable(test_async_io
    unit/test_async_io.cpp
)
target_link_libraries(test_async_io PRIVATE
    GTest::GTest GTest::Main
    Threads::Threads
    dfg_common
    dfg_proto
)
target_include_directories(test_async_io PRIVATE
    ${CMAKE_BINARY_DIR}/generated
)
add_test(NAME AsyncIoTest COMMAND test_async_io)

add_executable(simple_heartbeat_test
    unit/simple_heartbeat_test.cpp
)
target_link_libraries(simple_heartbeat_test PRIVATE
    GTest::GTest GTest::Main
    Threads::Threads
    dfg_common
)
add_test(NAME SimpleHeartbeatTest COMMAND simple_heartbeat_test)

add_executable(test_heartbeat
    unit/test_heartbeat.cpp
)
target_link_libraries(test_heartbeat PRIVATE
    GTest::GTest
    Threads::Threads
    dfg_common
    dfg_proto
)
target_compile_options(test_heartbeat PRIVATE -fcoroutines)
target_include_directories(test_heartbeat PRIVATE
    ${CMAKE_BINARY_DIR}/generated
)
add_test(NAME HeartbeatTest COMMAND test_heartbeat)

add_executable(test_metadata_replication
    unit/test_metadata_replication.cpp
)
target_link_libraries(test_metadata_replication PRIVATE
    GTest::GTest GTest::Main
    Threads::Threads
    dfg_common
    dfg_proto
)
target_include_directories(test_metadata_replication PRIVATE
    ${CMAKE_BINARY_DIR}/generated
    ${CMAKE_SOURCE_DIR}/src/head_server
)
add_test(NAME MetadataReplicationTest COMMAND test_metadata_replication)

add_executable(test_server_registry unit/test_server_registry.cpp)
target_link_libraries(test_server_registry PRIVATE
    GTest::GTest GTest::Main Threads::Threads dfg_common
)
add_test(NAME ServerRegistryTest COMMAND test_server_registry)

set_tests_properties(
    AsyncIoTest SimpleHeartbeatTest HeartbeatTest MetadataReplicationTest ServerRegistryTest
    PROPERTIES LABELS "unit" TIMEOUT 90
)

add_test(NAME ClusterIntegrationTest
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/integration/test_cluster.py
        --build-dir ${CMAKE_BINARY_DIR}
)
set_tests_properties(ClusterIntegrationTest PROPERTIES
    LABELS "integration" TIMEOUT 180 RUN_SERIAL TRUE
)

# ── Convenience target ──
add_custom_target(run_tests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
    DEPENDS test_async_io simple_heartbeat_test test_heartbeat test_metadata_replication
            test_server_registry test_e2e head_server cluster_server client
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)
