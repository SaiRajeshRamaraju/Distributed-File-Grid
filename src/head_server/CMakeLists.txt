# src/head_server/CMakeLists.txt

# ── Standalone head_server binary ──
add_executable(head_server
    head_server.cpp
    file_upload.cpp
    file_download.cpp
)

target_include_directories(head_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(head_server PRIVATE
    dfg_common
    dfg_proto
    Threads::Threads
    ${ABSEIL_LIBS}
    ${PROMETHEUS_LIBS}
    ${ZLIB_LIBRARIES}
)

target_compile_options(head_server PRIVATE -fcoroutines)

if(WITH_REDIS)
    target_include_directories(head_server PRIVATE ${REDIS_PLUS_PLUS_INCLUDE_DIRS})
    target_compile_options(head_server PRIVATE ${REDIS_PLUS_PLUS_CFLAGS_OTHER})
    target_link_libraries(head_server PRIVATE redis++::redis++)
endif()
