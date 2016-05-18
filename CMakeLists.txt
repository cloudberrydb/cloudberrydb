# Copyright (c) 2015, Pivotal Software, Inc.

cmake_minimum_required(VERSION 3.0 FATAL_ERROR)
project(gpopt LANGUAGES CXX C)

set(GPORCA_VERSION_MAJOR 1)
set(GPORCA_VERSION_MINOR 635)
set(GPORCA_VERSION_STRING ${GPORCA_VERSION_MAJOR}.${GPORCA_VERSION_MINOR})

# Whenever an ABI-breaking change is made to GPORCA, this should be incremented.
# ABI changes include removing functions, and adding or removing function
# parameters. Because GPORCA is a C++ library, there are also several other cases
# that might cause ABI changes, including adding or removing class members,
# and things that might change vtables for classes with virtual methods. If in
# doubt, do the safe thing and increment this number.
set(GPORCA_ABI_VERSION 3)

# Configure CCache if available
find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
       set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
       set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)

# Check build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "RelWithDebInfo")
endif(NOT CMAKE_BUILD_TYPE)
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

# Override CMAKE_SYSTEM_PROCESSOR if it has been explicitly set in a toolchain file.
if (FORCED_CMAKE_SYSTEM_PROCESSOR)
  set(CMAKE_SYSTEM_PROCESSOR ${FORCED_CMAKE_SYSTEM_PROCESSOR})
endif()

# Turn on compiler flags for all warnings if available.
include(CheckCXXCompilerFlag)

check_cxx_compiler_flag("-Wall" COMPILER_HAS_WALL)
if (COMPILER_HAS_WALL)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
endif()

check_cxx_compiler_flag("-Werror" COMPILER_HAS_WERROR)
if (COMPILER_HAS_WERROR)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
endif()

check_cxx_compiler_flag("-Wextra" COMPILER_HAS_WEXTRA)
if (COMPILER_HAS_WEXTRA)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wextra")
endif()

check_cxx_compiler_flag("-pedantic-errors" COMPILER_HAS_PEDANTIC_ERRORS)
if (COMPILER_HAS_PEDANTIC_ERRORS)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pedantic-errors")
endif()

# Turn off warnings about variadic macros if for some reason the C++ dialect is
# not compatible with C99.
check_cxx_compiler_flag("-Wno-variadic-macros" COMPILER_HAS_WNO_VARIADIC_MACROS)
if (COMPILER_HAS_WNO_VARIADIC_MACROS)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-variadic-macros")
endif()

# Turn off warning about tautological comparisons with NULL in some ill-formed
# Orca code.
# TODO(chasseur): This warning should be reinstated once the code in question
# has been modified to no longer depend on undefined behavior.
check_cxx_compiler_flag("-Wno-tautological-undefined-compare"
                        COMPILER_HAS_WNO_TAUTOLOGICAL_UNDEFINED_COMPARE)
if (COMPILER_HAS_WNO_TAUTOLOGICAL_UNDEFINED_COMPARE)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-tautological-undefined-compare")
endif()

# Generate maximum detail for DEBUG information with -g3 if available.
check_cxx_compiler_flag("-g3" COMPILER_HAS_G3)
if (COMPILER_HAS_G3)
  SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g3")
  SET(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -g3")
endif()

# Do not omit frame pointer. Even with RELEASE builds, it is used for
# backtracing.
check_cxx_compiler_flag("-fno-omit-frame-pointer"
                        COMPILER_HAS_FNO_OMIT_FRAME_POINTER)
if (COMPILER_HAS_FNO_OMIT_FRAME_POINTER)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
endif()

# Turn on GPOS_DEBUG define for DEBUG builds.
cmake_policy(SET CMP0043 NEW)
set_property(
    DIRECTORY
    APPEND PROPERTY COMPILE_DEFINITIONS $<$<CONFIG:Debug>:GPOS_DEBUG>)

# Turn on platform-specific defines.
set_property(
    DIRECTORY
    APPEND PROPERTY COMPILE_DEFINITIONS
        GPOS_${CMAKE_SYSTEM_NAME}
        GPOS_${CMAKE_SYSTEM_PROCESSOR})

# Autodetect bit-width if not already set by toolchain file.
if (NOT GPOS_ARCH_BITS)
  # Autodetect bit-width.
  if (${CMAKE_SIZEOF_VOID_P} EQUAL 8)
    set(GPOS_ARCH_BITS 64)
  elseif (${CMAKE_SIZEOF_VOID_P} EQUAL 4)
    set(GPOS_ARCH_BITS 32)
  else()
    message(FATAL_ERROR "Could not detect 32-bit OR 64-bit architecture")
  endif()
endif()

set_property(
    DIRECTORY
    APPEND PROPERTY COMPILE_DEFINITIONS
        GPOS_${GPOS_ARCH_BITS}BIT)

# Library dependencies for optimizer.
set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

# POSIX threads.
find_package(Threads REQUIRED)
if (NOT CMAKE_USE_PTHREADS_INIT)
  message(FATAL_ERROR "Found a threading library, but it is not pthreads.")
endif()

# Xerces.
find_package(Xerces REQUIRED)
include_directories(${XERCES_INCLUDE_DIRS})

# GPOS.
find_package(GPOS 1.136 REQUIRED)
include_directories(${GPOS_INCLUDE_DIRS})

# Extra system libs needed on Solaris.
set(GPORCA_SOLARIS_EXTRA_LIBS)
if (${CMAKE_SYSTEM_NAME} MATCHES "SunOS")
  find_library(RT_LIBRARY NAMES rt librt)
  if (${RT_LIBRARY-NOTFOUND})
    message(WARNING "rt library not found. Linking may fail.")
  else()
    set(GPORCA_SOLARIS_EXTRA_LIBS "${GPORCA_SOLARIS_EXTRA_LIBS} ${RT_LIBRARY}")
  endif()

  find_library(SOCKET_LIBRARY NAMES socket libsocket)
  if (${SOCKET_LIBRARY-NOTFOUND})
    message(WARNING "socket library not found. Linking may fail.")
  else()
    set(GPORCA_SOLARIS_EXTRA_LIBS "${GPORCA_SOLARIS_EXTRA_LIBS} ${SOCKET_LIBRARY}")
  endif()
endif()

enable_testing()

# Sub-libraries.
add_subdirectory(libnaucrates)
add_subdirectory(libgpdbcost)
add_subdirectory(libgpopt)

# Tests.
add_subdirectory(server)
