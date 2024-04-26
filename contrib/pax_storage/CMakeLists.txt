project(Pax)
cmake_minimum_required (VERSION 3.11.0)
set(CMAKE_CXX_STANDARD 14)

set(TOP_DIR ${PROJECT_SOURCE_DIR}/../..)
set(CBDB_INCLUDE_DIR ${TOP_DIR}/src/include)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-error=ignored-qualifiers -Wuninitialized -Winit-self -Wstrict-aliasing -Wno-missing-field-initializers -Wno-unused-parameter -Wno-clobbered -g")

# Build gtest options
option(BUILD_GTEST "Build with google test" ON)
option(BUILD_GBENCH "Build with google benchmark" OFF)

# Build pax tools
option(BUILD_TOOLS "Build with pax tools" ON)

include(CheckSymbolExists)
SET(PG_CONFIG_HEADER_FILE "${CBDB_INCLUDE_DIR}/pg_config.h")
message(STATUS "pg_config.h => ${PG_CONFIG_HEADER_FILE}")

CHECK_SYMBOL_EXISTS(USE_ASSERT_CHECKING "${PG_CONFIG_HEADER_FILE}" PAX_USE_DEBUG)
if (PAX_USE_DEBUG)
  ADD_DEFINITIONS(-DENABLE_DEBUG)
  # Use to build compile_commands.json
  SET(CMAKE_BUILD_TYPE "Debug")
  SET(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -ggdb")
else()
  SET(CMAKE_BUILD_TYPE "Release")
  SET(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3")
  # no need build gtest in release mode
  SET(BUILD_GTEST OFF)
endif()
message(STATUS "PAX BUILD TYPE: ${CMAKE_BUILD_TYPE}")

if(BUILD_GBENCH)
  SET(BUILD_GTEST ON)
endif(BUILD_GBENCH)

if (BUILD_GTEST)
  SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -no-pie -fno-stack-protector -Wall -Wno-unused-function  -Wno-unused-variable")
  SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-access-control -fno-inline -Wno-pmf-conversions -Wl,--allow-multiple-definition -no-pie -fno-stack-protector")
endif(BUILD_GTEST)

# Plasma options
option (ENABLE_PLASMA "Enable plasma cache" OFF)
if (ENABLE_PLASMA)
ADD_DEFINITIONS(-DENABLE_PLASMA)
# plasma need use CXX_STANDARD 17
set(CMAKE_CXX_STANDARD 17)
endif()

# Vec options
CHECK_SYMBOL_EXISTS(USE_VECTORIZATION "${PG_CONFIG_HEADER_FILE}" VEC_BUILD)
if (VEC_BUILD)
  set(VEC_HOME "${PROJECT_SOURCE_DIR}/../vectorization")
  set(INSTALL_HOME "${CMAKE_INSTALL_PREFIX}")
  ADD_DEFINITIONS(-DVEC_BUILD)
  message(STATUS "Build pax with vectorization support, VEC_HOME=${VEC_HOME} INSTALL_HOME=${INSTALL_HOME}")
else()
  message(STATUS "Build pax without vectorization support")
endif(VEC_BUILD)

## find dependency
## depend on the value of the above option to check dependencies.
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/")
include(FindDependencies)
add_subdirectory(src/cpp)
