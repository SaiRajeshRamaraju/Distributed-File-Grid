# src/health_checker/CMakeLists.txt

add_executable(health_checker
    health_checker.cpp
    ${CMAKE_SOURCE_DIR}/src/common/src/zookeeper_client.cpp
)

target_include_directories(health_checker PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/src/head_server   # for redis_handler.hpp
)

target_link_libraries(health_checker PRIVATE
    dfg_common
    dfg_proto
    Threads::Threads
    ${ABSEIL_LIBS}
    ${PROMETHEUS_LIBS}
    ${ZLIB_LIBRARIES}
)

target_compile_options(health_checker PRIVATE -fcoroutines)

if(WITH_REDIS)
    target_include_directories(health_checker PRIVATE ${REDIS_PLUS_PLUS_INCLUDE_DIRS})
    target_compile_options(health_checker PRIVATE ${REDIS_PLUS_PLUS_CFLAGS_OTHER})
    target_link_libraries(health_checker PRIVATE redis++::redis++)
endif()
