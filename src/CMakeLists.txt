# src/CMakeLists.txt — rolls up all component subdirectories

add_subdirectory(common)
add_subdirectory(head_server)
add_subdirectory(cluster_server)
add_subdirectory(health_checker)
add_subdirectory(zk_monitor)
add_subdirectory(cli)
add_subdirectory(client)
