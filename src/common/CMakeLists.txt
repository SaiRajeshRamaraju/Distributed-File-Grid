add_library(dfg_common STATIC src/zookeeper_client.cpp src/atomic_output.cpp)
target_include_directories(dfg_common PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(dfg_common PUBLIC Threads::Threads)
target_compile_features(dfg_common PUBLIC cxx_std_20)
