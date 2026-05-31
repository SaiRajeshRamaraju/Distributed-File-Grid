# ─────────────────── Dependencies.cmake ───────────────────
# FetchContent declarations for third-party dependencies.

include(FetchContent)

# ── Abseil ──
set(ABSL_PROPAGATE_CXX_STD ON)
set(ABSL_BUILD_TESTING OFF)
set(ABSL_ENABLE_INSTALL ON)
set(ABSL_USE_GOOGLETEST_HEAD OFF)

FetchContent_Declare(
    abseil-cpp
    GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
    GIT_TAG 20230125.3
)
FetchContent_MakeAvailable(abseil-cpp)

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

# ── Prometheus-cpp ──
FetchContent_Declare(
    prometheus-cpp
    GIT_REPOSITORY https://github.com/jupp0r/prometheus-cpp.git
    GIT_TAG v1.2.4
)
set(_DFG_ENABLE_TESTING_BACKUP ${ENABLE_TESTING})
set(ENABLE_TESTING OFF CACHE BOOL "Build prometheus-cpp tests" FORCE)
set(ENABLE_PUSH OFF CACHE BOOL "Build prometheus-cpp push (requires CURL)" FORCE)
FetchContent_MakeAvailable(prometheus-cpp)
if(DEFINED _DFG_ENABLE_TESTING_BACKUP)
    set(ENABLE_TESTING ${_DFG_ENABLE_TESTING_BACKUP} CACHE BOOL "Build tests" FORCE)
else()
    unset(ENABLE_TESTING CACHE)
endif()

set(PROMETHEUS_LIBS
    prometheus-cpp::core
    prometheus-cpp::pull
)
