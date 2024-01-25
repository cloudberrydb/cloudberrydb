cmake_minimum_required (VERSION 3.11.0)

# protobuf
include(FindProtobuf)
find_package(Protobuf 3.6.1 REQUIRED)

set(protobuf_files
  ${CMAKE_CURRENT_SOURCE_DIR}/storage/proto/micro_partition_stats.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/storage/proto/pax.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/storage/proto/orc_proto.proto
)


set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/storage/proto)
set(PROTO_OUTPUT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/storage/proto)

## we generate these files in the cmake -B build phase. 
## when we start the make phase, all dependent files exist, and we can compile with multiple threads.
FOREACH(FIL ${protobuf_files})
    GET_FILENAME_COMPONENT(FIL_WE ${FIL} NAME_WE)
    string(REGEX REPLACE ".+/(.+)\\..*" "\\1" FILE_NAME ${FIL})
    string(REGEX REPLACE "(.+)\\${FILE_NAME}.*" "\\1" FILE_PATH ${FIL})

    set(PROTO_SRCS ${PROTO_SRCS} "${PROTO_OUTPUT_DIR}/${FIL_WE}.pb.cc")
    set(PROTO_HDRS ${PROTO_SRCS} "${PROTO_OUTPUT_DIR}/${FIL_WE}.pb.h")

    EXECUTE_PROCESS(
            COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} -I ${PROTO_DIR} --cpp_out=${PROTO_OUTPUT_DIR} ${FIL}
    )
ENDFOREACH()

add_custom_target(generate_protobuf DEPENDS ${PROTO_SRCS} ${PROTO_HDRS})

# bison
bison_target(paxc_gram access/paxc_gram.y ${CMAKE_CURRENT_BINARY_DIR}/paxc_gram.c)

if(NOT BUILD_PAX_FORMAT)
add_executable(generate_sql_script_program "${CMAKE_CURRENT_SOURCE_DIR}/../../tools/gen_sql.c")
target_include_directories(generate_sql_script_program PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${CBDB_INCLUDE_DIR})
add_custom_command(OUTPUT generate_sql_file
  COMMAND ${CMAKE_CURRENT_BINARY_DIR}/generate_sql_script_program > "${CMAKE_CURRENT_SOURCE_DIR}/../../pax-cdbinit--1.0.sql"
  DEPENDS generate_sql_script_program
  COMMENT "dynamically generate sql script file"
)
add_custom_target(create_sql_script DEPENDS generate_sql_script_program generate_sql_file)
endif(BUILD_PAX_FORMAT)

# ztsd
set(ZSTD_BUILD_PROGRAMS OFF)
set(ZSTD_BUILD_TESTS OFF)
set(ZSTD_BUILD_CONTRIB OFF)
add_subdirectory(contrib/zstd/build/cmake/)
set(ZTSD_HEADER contrib/zstd/lib)

set(pax_comm_src
    comm/bitmap.cc
    comm/guc.cc
    comm/paxc_wrappers.cc
    comm/pax_memory.cc
    comm/cbdb_wrappers.cc)

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
    storage/pax_itemptr.cc
    storage/paxc_block_map_manager.cc
    storage/proto/protobuf_stream.cc
   )

if(NOT BUILD_PAX_FORMAT)
  set(pax_storage_src 
    ${pax_storage_src} 
    storage/pax.cc 
    storage/pax_table_partition_writer.cc  
    storage/strategy.cc
    storage/micro_partition_iterator.cc
  )
endif(NOT BUILD_PAX_FORMAT)

set(pax_access_src
    ${BISON_paxc_gram_OUTPUTS} # BISON output file
    access/paxc_rel_options.cc
    access/paxc_scanner.cc
    access/pax_access_handle.cc
    access/pax_deleter.cc
    access/pax_dml_state.cc
    access/pax_inserter.cc
    access/pax_partition.cc
    access/pax_updater.cc
    access/pax_scanner.cc)

set(pax_catalog_src
    catalog/pax_aux_table.cc
    catalog/pg_pax_tables.cc
    catalog/pax_fastsequence.cc
    )

set(pax_vec_src
  storage/vec/pax_vec_adapter.cc
  storage/vec/pax_vec_reader.cc)

link_directories($ENV{GPHOME}/lib)

if(BUILD_PAX_FORMAT)
  # paxformat.so
  ADD_DEFINITIONS(-DBUILD_PAX_FORMAT)
  add_library(paxformat SHARED ${PROTO_SRCS} ${pax_storage_src} ${pax_exceptions_src} ${pax_comm_src} )
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
  )

  # export headers
  set(PAX_EXCEPTION_HEADERS
    exceptions/CException.h
  )

  ## install dynamic libraray
  install(TARGETS paxformat
    LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX}/lib)

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

else()
  set(pax_target_src  ${PROTO_SRCS} ${pax_storage_src}  ${pax_exceptions_src}
    ${pax_access_src} ${pax_comm_src} ${pax_catalog_src} ${pax_vec_src})
  set(pax_target_include ${ZTSD_HEADER} ${CMAKE_CURRENT_SOURCE_DIR} ${CBDB_INCLUDE_DIR})
  set(pax_target_link_libs protobuf zstd z postgres)
  set(pax_target_link_directories ${PROJECT_SOURCE_DIR}/../../src/backend/)
  set(pax_target_dependencies generate_protobuf create_sql_script)

  # enable local index, not use uuid to generate file name
  if (NOT ENABLE_LOCAL_INDEX OR ENABLE_PLASMA)
    set(pax_target_link_libs ${pax_target_link_libs} uuid)
  endif()

  # enable plasma
  if (ENABLE_PLASMA)
    set(pax_target_link_libs ${pax_target_link_libs} plasma)
  endif()

  add_library(pax SHARED ${pax_target_src})
  set_target_properties(pax PROPERTIES OUTPUT_NAME pax)

  # vec build
  if (VEC_BUILD)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(GLIB REQUIRED glib-2.0)
    set(pax_target_include
        ${pax_target_include}
        ${VEC_HOME}/src/include # for utils/tuptable_vec.h
        ${VEC_HOME}/arrow/include  # for arrow-glib/arrow-glib.h and otehr arrow interface
        ${GLIB_INCLUDE_DIRS} # for glib-object.h
    )
    set(pax_target_link_directories
        ${pax_target_link_directories}
        ${VEC_HOME}/arrow/lib)
    set(pax_target_link_libs
        ${pax_target_link_libs}
        arrow)
  endif(VEC_BUILD)

  target_include_directories(pax PUBLIC ${pax_target_include})
  target_link_directories(pax PUBLIC ${pax_target_link_directories})
  target_link_libraries(pax PUBLIC ${pax_target_link_libs})
  set_target_properties(pax PROPERTIES
    BUILD_RPATH_USE_ORIGIN ON
    BUILD_WITH_INSTALL_RPATH ON
    INSTALL_RPATH "$ORIGIN:$ORIGIN/.."
    LINK_FLAGS "-Wl,--enable-new-dtags"
  )

  add_dependencies(pax ${pax_target_dependencies})
  add_custom_command(TARGET pax POST_BUILD
                  COMMAND ${CMAKE_COMMAND} -E
                  copy_if_different $<TARGET_FILE:pax> ${CMAKE_CURRENT_SOURCE_DIR}/../../pax.so)

  if (BUILD_GTEST)
    add_subdirectory(contrib/googletest)
    ADD_DEFINITIONS(-DRUN_GTEST)
    file(GLOB test_case_sources
      pax_gtest_helper.cc
      pax_gtest.cc
      ${CMAKE_CURRENT_SOURCE_DIR}/*/*_test.cc
      ${CMAKE_CURRENT_SOURCE_DIR}/*/*/*_test.cc)

    add_executable(test_main ${pax_target_src} ${test_case_sources})
    add_dependencies(test_main ${pax_target_dependencies} gtest gmock)
    target_include_directories(test_main PUBLIC ${pax_target_include} ${CMAKE_CURRENT_SOURCE_DIR} ${gtest_SOURCE_DIR}/include contrib/cpp-stub/src/ contrib/cpp-stub/src_linux/)

    target_link_directories(test_main PUBLIC ${pax_target_link_directories})
    target_link_libraries(test_main PUBLIC ${pax_target_link_libs} gtest gmock postgres)
  endif(BUILD_GTEST)

  if(BUILD_GBENCH)
    add_subdirectory(contrib/googlebench)
    ADD_DEFINITIONS(-DRUN_GBENCH)
    file(GLOB bench_sources
        pax_gbench.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/*/*_bench.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/*/*/*_bench.cc)

      add_executable(bench_main ${pax_target_src} ${bench_sources})
      add_dependencies(bench_main ${pax_target_dependencies} gtest gmock)
      target_include_directories(bench_main PUBLIC ${pax_target_include} ${CMAKE_CURRENT_SOURCE_DIR} contrib/googlebench/include contrib/cpp-stub/src/ contrib/cpp-stub/src_linux/)
      link_directories(contrib/googlebench/src)
      target_link_libraries(bench_main PUBLIC ${pax_target_link_libs} gtest gmock benchmark postgres)
      if (VEC_BUILD)
        target_link_libraries(bench_main PRIVATE arrow)
      endif(VEC_BUILD)
  endif(BUILD_GBENCH)
endif(BUILD_PAX_FORMAT)

if (BUILD_TOOLS) 
  add_subdirectory(contrib/tabulate)
  link_directories($ENV{GPHOME}/lib)

  add_executable(pax_dump storage/tools/pax_dump.cpp storage/tools/pax_dump_reader.cpp)
  target_include_directories(pax_dump PUBLIC ${pax_target_include} ${CMAKE_CURRENT_SOURCE_DIR} contrib/tabulate/include)
  add_dependencies(pax_dump ${pax_target_dependencies})
  target_link_libraries(pax_dump PUBLIC pax protobuf)
endif(BUILD_TOOLS)
