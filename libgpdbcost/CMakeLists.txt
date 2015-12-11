# Copyright (c) 2015, Pivotal Software, Inc. All Rights Reserved.

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../libgpopt/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../libnaucrates/include)
add_library(gpdbcost SHARED
            src/CCostModelGPDB.cpp
            src/CCostModelGPDBLegacy.cpp
            src/CCostModelParamsGPDB.cpp
            src/CCostModelParamsGPDBLegacy.cpp
            src/ICostModel.cpp)

target_link_libraries(gpdbcost  
                      ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_DL_LIBS}
                      ${GPOS_LIBRARIES}
                      ${XERCES_LIBRARIES}
                      ${GPORCA_SOLARIS_EXTRA_LIBS})

# Prevent linker errors from undefined symbols on OSX.
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set_target_properties(gpdbcost PROPERTIES
                        LINK_FLAGS "-undefined dynamic_lookup")
endif()

set_target_properties(gpdbcost PROPERTIES
                      SOVERSION ${GPORCA_ABI_VERSION}
                      VERSION ${GPORCA_VERSION_STRING})

install(TARGETS gpdbcost DESTINATION lib)
install(DIRECTORY include/gpdbcost DESTINATION include)
