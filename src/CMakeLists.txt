add_subdirectory(common)
add_subdirectory(metadata)

# Shared dependencies for the service implementations.
add_library(dfg_service_dependencies INTERFACE)
target_link_libraries(dfg_service_dependencies INTERFACE
    dfg_common dfg_proto ${ABSEIL_LIBS} ${PROMETHEUS_LIBS} ZLIB::ZLIB
)

add_subdirectory(head_server)
add_subdirectory(cluster_server)
add_subdirectory(health_checker)
add_subdirectory(zk_monitor)
add_subdirectory(cli)
add_subdirectory(client)
