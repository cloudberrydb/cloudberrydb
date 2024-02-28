project(interconnect_ext)

cmake_minimum_required (VERSION 3.11.0)
set(CMAKE_CXX_STANDARD 14)

find_program(
  PG_CONFIG pg_config
  HINTS ${PG_PATH}
  PATH_SUFFIXES bin
  DOC "The path to the pg_config of the CBDB version to compile against")

if(NOT PG_CONFIG)
  message(FATAL_ERROR "Unable to find 'pg_config'")
endif()

function(GET_PG_CONFIG var)
  set(_temp)

  # Only call pg_config if the variable didn't already have a value.
  if(NOT ${var})
    execute_process(
      COMMAND ${PG_CONFIG} ${ARGN}
      OUTPUT_VARIABLE _temp
      OUTPUT_STRIP_TRAILING_WHITESPACE)
  endif()

  set(${var}
    ${_temp}
    PARENT_SCOPE)
endfunction()

# Get CBDB configuration from pg_config
get_pg_config(PG_INCLUDEDIR --includedir)

set(TOP_DIR ../../..)
set(UNIT_TEST_DIR ${TOP_DIR}/src/test/unit/cmockery/)
set(UNIT_TEST_MOCK_DIR ${TOP_DIR}/src/test/unit/mock/)
set(IC_MODULE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../)
set(CBDB_INCLUDE_DIR ${PG_INCLUDEDIR}/postgresql/server)

add_definitions(-DENABLE_IC_PROXY)

set(interconnect_src 
    ../ic_common.c
    ../ic_modules.c
    ../tcp/ic_tcp.c
    ../udp/ic_udpifc.c
    ../proxy/ic_proxy_main.c
    ../proxy/ic_proxy_client.c
    ../proxy/ic_proxy_peer.c
    ../proxy/ic_proxy_router.c
    ../proxy/ic_proxy_backend.c
    ../proxy/ic_proxy_addr.c
    ../proxy/ic_proxy_key.c
    ../proxy/ic_proxy_packet.c
    ../proxy/ic_proxy_pkt_cache.c
    ../proxy/ic_proxy_iobuf.c)

set(interconnect_ext_test_src
    ${UNIT_TEST_DIR}/cmockery.c
    ic_test_env.c
    elog_mock.c
    ic_interface_test.c)

set(ic_bench_src
    ${UNIT_TEST_DIR}/cmockery.c
    ic_test_env.c
    elog_mock.c
    ic_bench.c)

SET(CMAKE_BUILD_TYPE "Debug")
SET(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb")

add_compile_definitions(ENABLE_IC_PROXY)
link_directories($ENV{GPHOME}/lib)
add_executable(interconnect_ext_test ${interconnect_src} ${interconnect_ext_test_src})
target_include_directories(interconnect_ext_test PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} "${CBDB_INCLUDE_DIR}" "${IC_MODULE_DIR}" "${UNIT_TEST_DIR}")

target_link_libraries(interconnect_ext_test PUBLIC 
    pthread
    uv 
    postgres)

link_directories($ENV{GPHOME}/lib)
add_executable(ic_bench ${interconnect_src} ${ic_bench_src})
target_include_directories(ic_bench PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} "${CBDB_INCLUDE_DIR}" "${IC_MODULE_DIR}" "${UNIT_TEST_DIR}")
target_link_libraries(ic_bench PUBLIC 
    pthread
    uv 
    postgres)
