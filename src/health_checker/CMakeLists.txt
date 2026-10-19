add_library(dfg_health_checker STATIC
    src/health_checker.cpp
)
target_include_directories(dfg_health_checker PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(dfg_health_checker PUBLIC dfg_common PRIVATE
    dfg_service_dependencies dfg_metadata
)

add_executable(health_checker src/main.cpp)
target_link_libraries(health_checker PRIVATE dfg_health_checker)
