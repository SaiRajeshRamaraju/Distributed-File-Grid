# src/cli/CMakeLists.txt — Unified dfg binary

add_executable(dfg
    dfg_cli.cpp
    ${CMAKE_SOURCE_DIR}/src/head_server/head_server.cpp
    ${CMAKE_SOURCE_DIR}/src/head_server/file_upload.cpp
    ${CMAKE_SOURCE_DIR}/src/head_server/file_download.cpp
    ${CMAKE_SOURCE_DIR}/src/cluster_server/chunk_service.cpp
    ${CMAKE_SOURCE_DIR}/src/cluster_server/metrics_exporter.cpp
)

target_compile_definitions(dfg PRIVATE DFG_UNIFIED_BINARY)

target_include_directories(dfg PRIVATE
    ${CMAKE_SOURCE_DIR}/src/head_server
    ${CMAKE_SOURCE_DIR}/src/cluster_server
)

target_link_libraries(dfg PRIVATE
    dfg_common
    dfg_proto
    Threads::Threads
    ${ABSEIL_LIBS}
    ${PROMETHEUS_LIBS}
    ${ZLIB_LIBRARIES}
)

target_compile_options(dfg PRIVATE -fcoroutines)

if(WITH_REDIS)
    target_include_directories(dfg PRIVATE ${REDIS_PLUS_PLUS_INCLUDE_DIRS})
    target_compile_options(dfg PRIVATE ${REDIS_PLUS_PLUS_CFLAGS_OTHER})
    target_link_libraries(dfg PRIVATE redis++::redis++)
endif()
