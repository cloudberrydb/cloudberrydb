find_package(BISON REQUIRED)

## protobuf
include(FindProtobuf)
find_package(Protobuf 3.6.1 REQUIRED)

# ztsd
# in our image snapshot, zstd is managed using pkg-config, so so the pkg-config method is used first here
find_package(PkgConfig QUIET)
if(PKGCONFIG_FOUND)
    pkg_check_modules(ZSTD libzstd)
endif()
if(NOT ZSTD_FOUND)
    find_package(ZSTD QUIET)
    if(NOT ZSTD_FOUND)
        message(FATAL_ERROR "zstd not found")
    endif()
endif()

## for vectorazition
if (VEC_BUILD)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(GLIB REQUIRED glib-2.0)

    # resolve vectorization dependency, or the header files will miss
    message(STATUS "Resolve vectorization dependency ...")
    execute_process(
        COMMAND make download_arrow
        WORKING_DIRECTORY ${VEC_HOME}
        RESULT_VARIABLE CMD_RESULT
    )
    if(CMD_RESULT EQUAL 0)
        message(STATUS "Resolve vectorization dependency succeeded.")
    else()
        message(FATAL_ERROR "Resolve vectorization dependency failed with result: ${CMD_RESULT}")
    endif()
endif(VEC_BUILD)