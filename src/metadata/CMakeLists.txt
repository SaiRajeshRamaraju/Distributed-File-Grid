# Shared metadata API with optional Redis backend.
add_library(dfg_metadata INTERFACE)
target_include_directories(dfg_metadata INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(dfg_metadata INTERFACE dfg_common)
if(WITH_REDIS)
    target_compile_definitions(dfg_metadata INTERFACE WITH_REDIS)
    target_link_libraries(dfg_metadata INTERFACE redis++::redis++)
endif()
