# Tries to find Zstd headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(Zstd)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  ZSTD_ROOT_DIR  Set this variable to the root installation of
#                    zstd if the module has problems finding
#                    the proper installation path.
#
# Variables defined by this module:
#
#  ZSTD_FOUND              System has Zstd libs/headers
#  ZSTD_LIBRARIES          The Zstd libraries
#  ZSTD_INCLUDE_DIRS       The location of Zstd headers

find_path(ZSTD_INCLUDE_DIRS
    NAMES zstd.h
    HINTS ${ZSTD_ROOT_DIR}/include)

find_library(ZSTD_LIBRARIES
    NAMES zstd
    HINTS ${ZSTD_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Zstd DEFAULT_MSG
    ZSTD_LIBRARIES
    ZSTD_INCLUDE_DIRS)

mark_as_advanced(
    ZSTD_ROOT_DIR
    ZSTD_LIBRARIES
    ZSTD_INCLUDE_DIRS)
