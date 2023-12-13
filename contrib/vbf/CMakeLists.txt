cmake_minimum_required(VERSION 3.1)

project(Vbf)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMakeModules")

enable_testing()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR})

# Eliminates warning about linker paths when linking both zlib and
# liblzma.
cmake_policy(SET CMP0003 NEW)

#-----------------------------------------------------------------------
# Retrieve the current version number
if (UNIX)
    execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/version.sh project
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        RESULT_VARIABLE VBF_VERSION_RESULT
        OUTPUT_VARIABLE VBF_VERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE)
	if(VBF_VERSION_RESULT)
        message(FATAL_ERROR "Cannot determine Vbf version number")
    endif(VBF_VERSION_RESULT)

    execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/version.sh libtool
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
		RESULT_VARIABLE LIBVBF_VERSION_RESULT
        OUTPUT_VARIABLE LIBVBF_VERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(LIBVBF_VERSION_RESULT)
        message(FATAL_ERROR "Cannot determine libvbf version number")
    endif(LIBVBF_VERSION_RESULT)

    execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/version.sh libcurrent
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        RESULT_VARIABLE LIBVBF_SOVERSION_RESULT
        OUTPUT_VARIABLE LIBVBF_SOVERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(LIBVBF_SOVERSION_RESULT)
        message(FATAL_ERROR "Cannot determine libvbf version number")
    endif(LIBVBF_SOVERSION_RESULT)

endif(UNIX)

#-----------------------------------------------------------------------
# Extract major.minor.patch from version number
if (UNIX)
    string(REGEX REPLACE "([0-9]+)\\..*"
        "\\1"
        VBF_MAJOR_VERSION
        ${VBF_VERSION}
    )
    string(REGEX REPLACE ".*\\.([0-9]+)\\..*"
        "\\1"
        VBF_MINOR_VERSION
        ${VBF_VERSION}
    )
    string(REGEX REPLACE ".*\\..*\\.([0-9]+).*"
        "\\1"
        VBF_PATCH_VERSION
        ${VBF_VERSION}
    )
    string(REGEX REPLACE ".*\\..*\\.[0-9]+(.*)"
        "\\1"
        VBF_VERSION_EXTENSION
        ${VBF_VERSION}
    )
endif(UNIX)

#-----------------------------------------------------------------------
# Source package support
include(CPackConfig.txt)
include(CheckLibraryExists)

if(CMAKE_COMPILER_IS_GNUCC)
    add_definitions(-W -Wall)
endif(CMAKE_COMPILER_IS_GNUCC)

# Thread support (only for *nix with pthreads)
set(THREADS_LIBRARIES)
if(UNIX AND CMAKE_COMPILER_IS_GNUCC)
    set(CMAKE_THREAD_PREFER_PTHREAD)
    find_package(Threads)

    if(NOT CMAKE_USE_PTHREADS_INIT)
        message(FATAL_ERROR "pthreads not found")
    endif(NOT CMAKE_USE_PTHREADS_INIT)

    add_definitions(-DTHREADSAFE -D_REENTRANT)
    set(THREADS_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
endif(UNIX AND CMAKE_COMPILER_IS_GNUCC)

include_directories(${Vbf_SOURCE_DIR}/src)

# Enable codecs
find_package(ZLIB)
if (ZLIB_FOUND)
    set(ZLIB_PKG zlib)
    add_definitions(-DDEFLATE_CODEC)
    include_directories(${ZLIB_INCLUDE_DIRS})
    message("Enabled deflate codec")
else (ZLIB_FOUND)
    set(ZLIB_PKG "")
    set(ZLIB_LIBRARIES "")
    message("Disabled deflate codec. zlib not found.")
endif (ZLIB_FOUND)

find_package(Snappy)
if (SNAPPY_FOUND AND ZLIB_FOUND)  # Snappy borrows crc32 from zlib
    set(SNAPPY_PKG libsnappy)
    add_definitions(-DSNAPPY_CODEC)
    include_directories(${SNAPPY_INCLUDE_DIRS})
    message("Enabled snappy codec")
else (SNAPPY_FOUND AND ZLIB_FOUND)
    set(SNAPPY_PKG "")
    set(SNAPPY_LIBRARIES "")
    message("Disabled snappy codec. libsnappy not found or zlib not found.")
endif (SNAPPY_FOUND AND ZLIB_FOUND)

find_package(Zstd)
if (ZSTD_FOUND)
    set(ZSTD_PKG libzstd)
    add_definitions(-DZSTD_CODEC)
    include_directories(${ZSTD_INCLUDE_DIRS})
    message("Enabled zstd codec")
else (ZSTD_FOUND)
    set(ZSTD_PKG "")
    set(ZSTD_LIBRARIES "")
    message("Disabled zstd codec. libzstd not found.")
endif (ZSTD_FOUND)

find_package(PkgConfig)
pkg_check_modules(LZMA liblzma)
if (LZMA_FOUND)
    set(LZMA_PKG liblzma)
    add_definitions(-DLZMA_CODEC)
    include_directories(${LZMA_INCLUDE_DIRS})
    link_directories(${LZMA_LIBRARY_DIRS})
    message("Enabled lzma codec")
else (LZMA_FOUND)
    set(LZMA_PKG "")
    set(LZMA_LIBRARIES "")
    message("Disabled lzma codec. liblzma not found.")
endif (LZMA_FOUND)

set(CODEC_LIBRARIES ${ZLIB_LIBRARIES} ${LZMA_LIBRARIES} ${SNAPPY_LIBRARIES} ${ZSTD_LIBRARIES})
set(CODEC_PKG "@ZLIB_PKG@ @LZMA_PKG@ @SNAPPY_PKG@ @ZSTD_PKG@")

add_subdirectory(src)
add_subdirectory(examples)
add_subdirectory(tests)

add_custom_target(pretty
    "${CMAKE_COMMAND}" -P "${CMAKE_CURRENT_SOURCE_DIR}/CMakeModules/cmake_pretty.cmake")
