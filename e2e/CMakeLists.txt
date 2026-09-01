# e2e/CMakeLists.txt — End-to-End (E2E) Test Suite Configuration

find_package(GTest REQUIRED)
find_package(Threads REQUIRED)
find_package(OpenSSL QUIET)

# Ensure aliases for target naming compatibility
if(TARGET GTest::gtest AND NOT TARGET GTest::GTest)
    add_library(GTest::GTest ALIAS GTest::gtest)
endif()
if(TARGET GTest::gtest_main AND NOT TARGET GTest::Main)
    add_library(GTest::Main ALIAS GTest::gtest_main)
endif()
if(TARGET dfg_common AND NOT TARGET dfg_core)
    add_library(dfg_core ALIAS dfg_common)
endif()

# ── End-to-End (E2E) test executable ──
add_executable(test_e2e
    test_e2e.cpp
)

target_link_libraries(test_e2e PRIVATE
    GTest::GTest
    GTest::Main
    Threads::Threads
    dfg_common
    dfg_proto
)

if(TARGET OpenSSL::Crypto)
    target_link_libraries(test_e2e PRIVATE OpenSSL::Crypto)
endif()

target_include_directories(test_e2e PRIVATE
    ${CMAKE_BINARY_DIR}/generated
    ${CMAKE_SOURCE_DIR}/src/common/include
)

# ── Register CTest Target ──
add_test(NAME E2ETest COMMAND test_e2e)

# ── Convenience custom target to execute full E2E test script ──
add_custom_target(run_e2e
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/e2e_test.sh
    DEPENDS dfg dfg_client head_server cluster_server health_checker test_e2e
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Executing Distributed File Grid End-to-End (E2E) Test Runner"
)
