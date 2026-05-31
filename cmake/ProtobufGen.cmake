# ─────────────────── ProtobufGen.cmake ───────────────────
# Helper function to generate C++ sources from .proto files.
#
# Usage:
#   dfg_generate_proto(
#       TARGET_NAME dfg_proto
#       PROTO_DIR   ${CMAKE_SOURCE_DIR}/src/proto/v1
#   )
#
# Creates a static library target with the generated .pb.cc/.pb.h files.

function(dfg_generate_proto)
    cmake_parse_arguments(ARG "" "TARGET_NAME;PROTO_DIR" "" ${ARGN})

    file(GLOB PROTO_FILES "${ARG_PROTO_DIR}/*.proto")

    set(PROTO_GEN_DIR ${CMAKE_BINARY_DIR}/generated)
    file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

    set(PROTO_SRCS)
    set(PROTO_HDRS)

    foreach(PROTO_FILE ${PROTO_FILES})
        get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
        get_filename_component(PROTO_PATH ${PROTO_FILE} DIRECTORY)

        set(PROTO_SRC "${PROTO_GEN_DIR}/${PROTO_NAME}.pb.cc")
        set(PROTO_HDR "${PROTO_GEN_DIR}/${PROTO_NAME}.pb.h")

        add_custom_command(
            OUTPUT ${PROTO_SRC} ${PROTO_HDR}
            COMMAND ${Protobuf_PROTOC_EXECUTABLE}
            ARGS --experimental_allow_proto3_optional
                 --cpp_out=${PROTO_GEN_DIR}
                 -I ${PROTO_PATH}
                 ${PROTO_FILE}
            DEPENDS ${PROTO_FILE} protobuf::protoc
            COMMENT "Generating C++ code for ${PROTO_NAME}.proto"
            VERBATIM
        )

        list(APPEND PROTO_SRCS ${PROTO_SRC})
        list(APPEND PROTO_HDRS ${PROTO_HDR})
        set_source_files_properties(${PROTO_SRC} ${PROTO_HDR} PROPERTIES GENERATED TRUE)
    endforeach()

    add_library(${ARG_TARGET_NAME} STATIC ${PROTO_SRCS} ${PROTO_HDRS})
    target_include_directories(${ARG_TARGET_NAME} PUBLIC ${PROTO_GEN_DIR})
    target_link_libraries(${ARG_TARGET_NAME} PUBLIC protobuf::libprotobuf)
endfunction()
