# Module to find GPOS.

find_path(GPOS_INCLUDE_DIR gpos/base.h)

find_library(GPOS_LIBRARY NAMES gpos libgpos)

set(GPOS_LIBRARIES ${GPOS_LIBRARY})
set(GPOS_INCLUDE_DIRS ${GPOS_INCLUDE_DIR})

if (GPOS_INCLUDE_DIR)
  # Try to compile and run a tiny program that includes gpos/version.h and
  # prints out the GPOS version number.
  try_run(VERSION_CHECK_STATUS_CODE
          VERSION_CHECK_COMPILE_OK
          "${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CheckGPOSVersion.bin"
          "${PROJECT_SOURCE_DIR}/cmake/GetGPOSVersion.cpp"
          CMAKE_FLAGS "-DINCLUDE_DIRECTORIES=${GPOS_INCLUDE_DIRS}"
          RUN_OUTPUT_VARIABLE GPOS_VERSION_STRING)
  if (NOT (VERSION_CHECK_COMPILE_OK AND (VERSION_CHECK_STATUS_CODE EQUAL 0)))
    message(WARNING "Unable to detect GPOS version.")
    set(GPOS_VERSION_STRING "0.0")
  endif()
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    GPOS
    REQUIRED_VARS GPOS_LIBRARY GPOS_INCLUDE_DIR
    VERSION_VAR GPOS_VERSION_STRING)

mark_as_advanced(GPOS_INCLUDE_DIR GPOS_LIBRARY)
