# Copyright (c) 2015, Pivotal Software, Inc. All Rights Reserved.

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../libgpcost/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../libnaucrates/include)

# Generate version-number header.
configure_file(version.h.in
        ${PROJECT_BINARY_DIR}/libgpopt/include/libgpopt/version.h)

# for the generated config.h file.
include_directories(${PROJECT_BINARY_DIR}/libgpos/include/)

file(GLOB_RECURSE hdrs ${CMAKE_CURRENT_SOURCE_DIR}/include/*.h)
file(GLOB_RECURSE srcs ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)

# Add headers to make them visible in some IDEs (Clion, VS, Xcode)
list(APPEND srcs ${hdrs})

add_library(gpopt ${srcs})

target_link_libraries(gpopt
                      gpdbcost
                      naucrates
                      ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_DL_LIBS}
                      gpos
                      ${XERCES_LIBRARIES}
                      ${GPORCA_SOLARIS_EXTRA_LIBS})

set_target_properties(gpopt PROPERTIES
                      SOVERSION ${GPORCA_ABI_VERSION}
                      VERSION ${GPORCA_VERSION_STRING})

install(TARGETS gpopt DESTINATION lib)
install(DIRECTORY include/gpopt DESTINATION include)
install(FILES "${PROJECT_BINARY_DIR}/libgpopt/include/libgpopt/version.h"
        DESTINATION include/gpopt)
