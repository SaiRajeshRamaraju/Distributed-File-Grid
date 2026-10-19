add_library(dfg_cluster_server STATIC
    src/cluster_server.cpp
    src/chunk_service.cpp
    src/metrics_exporter.cpp
)
target_include_directories(dfg_cluster_server PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(dfg_cluster_server PUBLIC dfg_common PRIVATE
    dfg_service_dependencies
)

add_executable(cluster_server src/main.cpp)
target_link_libraries(cluster_server PRIVATE dfg_cluster_server)
