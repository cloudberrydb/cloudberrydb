# paxformat.so

set(pax_comm_src
    comm/bitmap.cc
    comm/guc.cc
    comm/paxc_wrappers.cc
    comm/pax_memory.cc
    comm/cbdb_wrappers.cc
    comm/vec_numeric.cc)

set(pax_exceptions_src
    exceptions/CException.cc)

set(pax_storage_src
    storage/cache/pax_cache.cc
    storage/cache/pax_plasma_cache.cc
    storage/columns/pax_column_cache.cc
    storage/columns/pax_column_traits.cc
    storage/columns/pax_column.cc
    storage/columns/pax_compress.cc
    storage/columns/pax_columns.cc
    storage/columns/pax_encoding_utils.cc
    storage/columns/pax_encoding_non_fixed_column.cc
    storage/columns/pax_encoding_column.cc
    storage/columns/pax_decoding.cc
    storage/columns/pax_encoding.cc
    storage/columns/pax_numeric_column.cc
    storage/columns/pax_rlev2_decoding.cc
    storage/columns/pax_rlev2_encoding.cc
    storage/columns/pax_vec_column.cc
    storage/columns/pax_vec_encoding_column.cc
    storage/oper/pax_oper.cc
    storage/oper/pax_stats.cc
    storage/file_system.cc
    storage/local_file_system.cc
    storage/micro_partition.cc
    storage/micro_partition_file_factory.cc
    storage/micro_partition_metadata.cc
    storage/micro_partition_row_filter_reader.cc
    storage/micro_partition_stats.cc
    storage/orc/orc_format_reader.cc
    storage/orc/orc_group.cc
    storage/orc/orc_vec_group.cc
    storage/orc/orc_reader.cc
    storage/orc/orc_writer.cc
    storage/pax_buffer.cc
    storage/pax_filter.cc
    storage/proto/protobuf_stream.cc
   )

add_library(paxformat SHARED ${PROTO_SRCS} ${pax_storage_src} ${pax_exceptions_src} ${pax_comm_src} )
target_compile_definitions(paxformat PRIVATE BUILD_PAX_FORMAT)
target_include_directories(paxformat PUBLIC ${ZTSD_HEADER} ${CMAKE_CURRENT_SOURCE_DIR} ${CBDB_INCLUDE_DIR})
target_link_libraries(paxformat PUBLIC uuid protobuf zstd z)
set_target_properties(paxformat PROPERTIES
  OUTPUT_NAME paxformat)
add_dependencies(paxformat generate_protobuf)

# export headers
set(PAX_COMM_HEADERS
  comm/bitmap.h
  comm/cbdb_api.h
  comm/log.h
  comm/cbdb_wrappers.h
  comm/pax_rel.h
  comm/pax_memory.h
  comm/guc.h
)

set(PAX_EXCEPTION_HEADERS
  exceptions/CException.h
)

# TODO(gongxun):
# We should explicitly specify the headers
# that need to be exported, and use the syntax of
# install(FILES,...) to install the header files
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/storage
  DESTINATION ${CMAKE_INSTALL_PREFIX}/include/pax
  FILES_MATCHING
  PATTERN "*.h"
)

install(FILES ${PAX_COMM_HEADERS}
  DESTINATION ${CMAKE_INSTALL_PREFIX}/include/pax/comm
)

install(FILES ${PAX_EXCEPTION_HEADERS}
  DESTINATION ${CMAKE_INSTALL_PREFIX}/include/pax/exceptions
)

## install dynamic libraray
install(TARGETS paxformat
  LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX}/lib)

