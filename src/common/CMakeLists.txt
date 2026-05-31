# src/common/CMakeLists.txt — shared header-only library (dfg_common)
# Currently header-only; add .cpp files to SOURCES when splitting headers.

add_library(dfg_common INTERFACE)

target_include_directories(dfg_common INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# When we split headers into .hpp + .cpp (Phase 4), change to:
#   add_library(dfg_common STATIC src/config_loader.cpp src/system_info.cpp ...)
#   target_include_directories(dfg_common PUBLIC include)
