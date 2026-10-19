add_library(dfg_zk_monitor STATIC
    src/zk_monitor.cpp
)
target_include_directories(dfg_zk_monitor PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(dfg_zk_monitor PUBLIC dfg_common PRIVATE
    dfg_service_dependencies
)

add_executable(zk_head_server_monitor src/main.cpp)
target_link_libraries(zk_head_server_monitor PRIVATE dfg_zk_monitor)
