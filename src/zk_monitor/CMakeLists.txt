# src/zk_monitor/CMakeLists.txt

add_executable(zk_head_server_monitor
    zk_monitor.cpp
    ${CMAKE_SOURCE_DIR}/src/common/src/zookeeper_client.cpp
)

target_include_directories(zk_head_server_monitor PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(zk_head_server_monitor PRIVATE
    dfg_common
    Threads::Threads
    ${PROMETHEUS_LIBS}
)
