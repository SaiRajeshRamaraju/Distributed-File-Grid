# tests/CMakeLists.txt

find_package(GTest REQUIRED)

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

# ── Convenience target ──
add_custom_target(run_tests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
    DEPENDS test_async_io simple_heartbeat_test test_heartbeat
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)
