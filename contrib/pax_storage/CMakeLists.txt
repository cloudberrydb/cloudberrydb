project(Pax)
cmake_minimum_required (VERSION 3.11.0)
set(CMAKE_CXX_STANDARD 14)

find_package(BISON REQUIRED)

# TODO check exists if this is needed
set(CBDB_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/../../src/include)

# use sequential block number to enable btree index
option(ENABLE_LOCAL_INDEX "Enable local index support" OFF)

# Debug options
option(ENABLE_DEBUG "Enable debug" ON)

# Build gtest options
option(BUILD_GTEST "Build with google test" ON)

# Build pax format lib
option(BUILD_PAX_FORMAT "Build pax format lib" OFF)

if (ENABLE_LOCAL_INDEX)
  ADD_DEFINITIONS(-DENABLE_LOCAL_INDEX)
endif(ENABLE_LOCAL_INDEX)

if (ENABLE_DEBUG)
  ADD_DEFINITIONS(-DENABLE_DEBUG)
  # Use to build compile_commands.json
  set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
  SET(CMAKE_BUILD_TYPE "Debug")
  SET(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb")
else()
  SET(CMAKE_BUILD_TYPE "Release")
  SET(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall")
  # no need build gtest in release mode
  SET(BUILD_GTEST OFF)
endif(ENABLE_DEBUG)

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
option(VEC_BUILD "Build pax vectorization version" OFF)
set(VEC_HOME "" CACHE STRING "Path to vectorization home")
if (VEC_BUILD)

if("${VEC_HOME}" STREQUAL "")
  message(FATAL_ERROR "No found vectorization home setting. Using -DVEC_HOME to spec vectorization home")
endif()

set(CBDB_ROOT_INCLUDE_DIR ${PG_INCLUDEDIR})
ADD_DEFINITIONS(-DVEC_BUILD)

endif(VEC_BUILD)
add_subdirectory(src/cpp)
