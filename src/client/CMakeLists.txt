add_executable(dfg_client client.cpp)
target_link_libraries(dfg_client
    dfg_common
    pthread
)
