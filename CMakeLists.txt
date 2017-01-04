# Copyright (c) 2015, Pivotal Software, Inc. All Rights Reserved.

cmake_minimum_required(VERSION 3.0 FATAL_ERROR)
project(gpos LANGUAGES CXX C)

set(GPOS_VERSION_MAJOR 1)
set(GPOS_VERSION_MINOR 147)
set(GPOS_VERSION_STRING ${GPOS_VERSION_MAJOR}.${GPOS_VERSION_MINOR})

# Whenever an ABI-breaking change is made to GPOS, this should be incremented.
# ABI changes include removing functions, and adding or removing function
# parameters. Because GPOS is a C++ library, there are also several other cases
# that might cause ABI changes, including adding or removing class members,
# and things that might change vtables for classes with virtual methods. If in
# doubt, do the safe thing and increment this number.
set(GPOS_ABI_VERSION 10)

# Default to shared libraries.
option(BUILD_SHARED_LIBS "build shared libraries" ON)

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
elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "amd64")
  # Use a standardized name for 64-bit x86.
  set(CMAKE_SYSTEM_PROCESSOR "x86_64")
endif()

# Prevent linking errors for builtins if using an old GCC with the i386
# toolchain file.
if (ENABLE_OLD_GCC_32BIT_MARCH
    AND CMAKE_COMPILER_IS_GNUCXX
    AND (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.4.4"))
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=i686")
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

# Turn off warning about tautological compare in CDynamicPtrArray.
# TODO(chasseur): CDynamicPtrArray::UlSafeLength() should be rewritten so that
# it does not trigger this error in Clang.
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
    if (${CMAKE_SYSTEM_PROCESSOR} MATCHES "x86_64")
      message(WARNING "Detected x86-64 processor, but pointer size is 32 bits. "
                      "Compilation may fail because of this inconsistency (using "
                      "one of the toolchain files to explicitly target a specific "
                      "CPU architecture may avoid this problem).")
    endif()
  else()
    message(FATAL_ERROR "Could not detect 32-bit OR 64-bit architecture")
  endif()
endif()

set_property(
    DIRECTORY
    APPEND PROPERTY COMPILE_DEFINITIONS
        GPOS_${GPOS_ARCH_BITS}BIT)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

enable_testing()
add_subdirectory(libgpos)

# Mac OS X handles searching for dynamic libraries differently from other
# unices. It does not merely search for the leaf filename of a .dylib in a list
# of paths known to the linker, but instead looks up libraries based on either
# their full absolute path, or a relative path. We set the INSTALL_NAME_DIR
# property here to force the former behavior so that anything linking against
# gpos will look for the shared library in its absolute install path.
set_target_properties(gpos PROPERTIES
                      INSTALL_NAME_DIR "${full_install_name_dir}")

# Uninstallation.
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake_uninstall.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
    IMMEDIATE @ONLY)

add_custom_target(uninstall
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)
