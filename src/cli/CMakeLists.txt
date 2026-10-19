add_executable(dfg src/main.cpp)
target_link_libraries(dfg PRIVATE
    dfg_head_server dfg_cluster_server dfg_health_checker dfg_zk_monitor
)
