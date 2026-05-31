# src/cluster_server/CMakeLists.txt

add_executable(cluster_server
    cluster_server.cpp
    async_file_recv.cpp
    metrics_exporter.cpp
)

target_include_directories(cluster_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(cluster_server PRIVATE
    dfg_common
    dfg_proto
    Threads::Threads
    ${ABSEIL_LIBS}
    ${PROMETHEUS_LIBS}
    ${ZLIB_LIBRARIES}
)

target_compile_options(cluster_server PRIVATE -fcoroutines)
