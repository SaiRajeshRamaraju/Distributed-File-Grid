add_library(dfg_head_server STATIC
    src/client_request.cpp
    src/head_server.cpp
    src/file_upload.cpp
    src/file_download.cpp
)
target_include_directories(dfg_head_server PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(dfg_head_server PUBLIC dfg_common PRIVATE
    dfg_service_dependencies dfg_metadata
)

add_executable(head_server src/main.cpp)
target_link_libraries(head_server PRIVATE dfg_head_server)
