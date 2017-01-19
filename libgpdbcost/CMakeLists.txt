# Copyright (c) 2015, Pivotal Software, Inc. All Rights Reserved.

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../libgpopt/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../libnaucrates/include)
# for the generated config.h file.
include_directories(${PROJECT_BINARY_DIR}/libgpos/include/)
add_library(gpdbcost
            include/gpdbcost/CCostModelGPDB.h
            src/CCostModelGPDB.cpp
            include/gpdbcost/CCostModelGPDBLegacy.h
            src/CCostModelGPDBLegacy.cpp
            include/gpdbcost/CCostModelParamsGPDB.h
            src/CCostModelParamsGPDB.cpp
            include/gpdbcost/CCostModelParamsGPDBLegacy.h
            src/CCostModelParamsGPDBLegacy.cpp
            src/ICostModel.cpp
        )

target_link_libraries(gpdbcost  
                      ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_DL_LIBS}
                      gpos
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
