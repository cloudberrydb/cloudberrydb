# Copyright (c) 2015, Pivotal Software, Inc. All Rights Reserved.

cmake_minimum_required(VERSION 3.0 FATAL_ERROR)
project(gpos LANGUAGES CXX C)

set(GPOS_VERSION_MAJOR 1)
set(GPOS_VERSION_MINOR 141)
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

# Need pthreads.
find_package(Threads REQUIRED)
if (NOT CMAKE_USE_PTHREADS_INIT)
  message(FATAL_ERROR "Found a threading library, but it is not pthreads.")
endif()

# Try to find atomic operations.
include(FindAtomics.cmake)

if ((NOT (COMPILER_HAS_FETCH_ADD_32 AND COMPILER_HAS_FETCH_ADD_64
          AND COMPILER_HAS_CAS_32 AND COMPILER_HAS_CAS_64))
    AND (NOT (${CMAKE_SYSTEM_NAME} MATCHES "SunOS")))
  message(FATAL_ERROR "Could not find GCC-style atomic built-ins or Solaris "
                      "atomic headers. GPOS will fail to build. Please try "
                      "using a recent g++ or clang++ compiler.")
endif()


include_directories(libgpos/include)

# Generate version-number header.
configure_file(version.h.in
               ${PROJECT_BINARY_DIR}/libgpos/include/gpos/version.h)

# Compile .cpp source files under libgpos/src into monolithic gpos library.
add_library(gpos
            libgpos/include/gpos/assert.h
            libgpos/include/gpos/base.h
            libgpos/include/gpos/common/banned_api.h
            libgpos/include/gpos/common/banned_gpdb.h
            libgpos/include/gpos/common/banned_posix_nonreentrant.h
            libgpos/include/gpos/common/banned_posix_reentrant.h
            libgpos/include/gpos/common/CAutoP.h
            libgpos/include/gpos/common/CAutoRef.h
            libgpos/include/gpos/common/CAutoRg.h
            libgpos/include/gpos/common/CDouble.h
            libgpos/include/gpos/common/CDouble.inl
            libgpos/include/gpos/common/CDynamicPtrArray.h
            libgpos/include/gpos/common/CDynamicPtrArray.inl
            libgpos/include/gpos/common/CDynamicPtrArrayUtils.h
            libgpos/include/gpos/common/CDynamicPtrArrayUtils.inl
            libgpos/include/gpos/common/CEnumSet.h
            libgpos/include/gpos/common/CEnumSetIter.h
            libgpos/include/gpos/common/CHashMap.h
            libgpos/include/gpos/common/CHashMap.inl
            libgpos/include/gpos/common/CHashMapIter.h
            libgpos/include/gpos/common/CHashMapIter.inl
            libgpos/include/gpos/common/CHashSet.h
            libgpos/include/gpos/common/CHashSet.inl
            libgpos/include/gpos/common/clibtypes.h
            libgpos/include/gpos/common/CList.h
            libgpos/include/gpos/common/CList.inl
            libgpos/include/gpos/common/CRefCount.h
            libgpos/include/gpos/common/CStack.h
            libgpos/include/gpos/common/CStack.inl
            libgpos/include/gpos/common/CSyncHashtable.h
            libgpos/include/gpos/common/CSyncHashtable.inl
            libgpos/include/gpos/common/CSyncHashtableAccessByIter.h
            libgpos/include/gpos/common/CSyncHashtableAccessByIter.inl
            libgpos/include/gpos/common/CSyncHashtableAccessByKey.h
            libgpos/include/gpos/common/CSyncHashtableAccessByKey.inl
            libgpos/include/gpos/common/CSyncHashtableAccessorBase.h
            libgpos/include/gpos/common/CSyncHashtableAccessorBase.inl
            libgpos/include/gpos/common/CSyncHashtableIter.h
            libgpos/include/gpos/common/CSyncHashtableIter.inl
            libgpos/include/gpos/common/CSyncList.h
            libgpos/include/gpos/common/CSyncList.inl
            libgpos/include/gpos/common/CSyncPool.h
            libgpos/include/gpos/common/CSyncPool.inl
            libgpos/include/gpos/common/ITimer.h
            libgpos/include/gpos/common/pthreadtypes.h
            libgpos/include/gpos/error/CAutoLogger.h
            libgpos/include/gpos/error/CErrorHandler.h
            libgpos/include/gpos/error/IErrorContext.h
            libgpos/include/gpos/io/CAutoFileDescriptor.h
            libgpos/include/gpos/io/IOstream.h
            libgpos/include/gpos/io/iotypes.h
            libgpos/include/gpos/memory/CCache.h
            libgpos/include/gpos/memory/CCacheAccessor.h
            libgpos/include/gpos/memory/CCacheEntry.h
            libgpos/include/gpos/memory/CMemoryPoolAlloc.h
            libgpos/include/gpos/memory/CMemoryPoolStatistics.h
            libgpos/include/gpos/memory/ICache.h
            libgpos/include/gpos/memory/IMemoryVisitor.h
            libgpos/include/gpos/memory/README.md
            libgpos/include/gpos/net/CAutoSocketProxy.h
            libgpos/include/gpos/net/CSocketConnector.h
            libgpos/include/gpos/net/nettypes.h
            libgpos/include/gpos/sync/CAtomicCounter.h
            libgpos/include/gpos/sync/CMutex.h
            libgpos/include/gpos/sync/CMutex.inl
            libgpos/include/gpos/sync/CSpinlock.h
            libgpos/include/gpos/sync/CSpinlock.inl
            libgpos/include/gpos/task/CTaskId.h
            libgpos/include/gpos/task/CTaskLocalStorageObject.h
            libgpos/include/gpos/task/CTraceFlagIter.h
            libgpos/include/gpos/task/ITaskScheduler.h
            libgpos/include/gpos/task/traceflags.h
            libgpos/include/gpos/types.h

            libgpos/include/gpos/_api.h
            libgpos/src/_api.cpp
            libgpos/include/gpos/utils.h
            libgpos/src/utils.cpp
            libgpos/include/gpos/common/CAutoTimer.h
            libgpos/src/common/CAutoTimer.cpp
            libgpos/include/gpos/common/CBitSet.h
            libgpos/src/common/CBitSet.cpp
            libgpos/include/gpos/common/CBitSetIter.h
            libgpos/src/common/CBitSetIter.cpp
            libgpos/include/gpos/common/CBitVector.h
            libgpos/src/common/CBitVector.cpp
            libgpos/include/gpos/common/CHeapObject.h
            libgpos/src/common/CHeapObject.cpp
            libgpos/include/gpos/common/clibwrapper.h
            libgpos/src/common/clibwrapper.cpp
            libgpos/include/gpos/common/CMainArgs.h
            libgpos/src/common/CMainArgs.cpp
            libgpos/include/gpos/common/CRandom.h
            libgpos/src/common/CRandom.cpp
            libgpos/include/gpos/common/CStackDescriptor.h
            libgpos/src/common/CStackDescriptor.cpp
            libgpos/include/gpos/common/CStackObject.h
            libgpos/src/common/CStackObject.cpp
            libgpos/include/gpos/common/CTimerUser.h
            libgpos/src/common/CTimerUser.cpp
            libgpos/include/gpos/common/CWallClock.h
            libgpos/src/common/CWallClock.cpp
            libgpos/include/gpos/common/pthreadwrapper.h
            libgpos/src/common/pthreadwrapper.cpp
            libgpos/include/gpos/common/syslibwrapper.h
            libgpos/src/common/syslibwrapper.cpp
            libgpos/include/gpos/error/CAutoExceptionStack.h
            libgpos/src/error/CAutoExceptionStack.cpp
            libgpos/include/gpos/error/CAutoTrace.h
            libgpos/src/error/CAutoTrace.cpp
            libgpos/include/gpos/error/CErrorContext.h
            libgpos/src/error/CErrorContext.cpp
            libgpos/include/gpos/error/CErrorHandlerStandard.h
            libgpos/src/error/CErrorHandlerStandard.cpp
            libgpos/include/gpos/error/CException.h
            libgpos/src/error/CException.cpp
            libgpos/include/gpos/error/CFSimulator.h
            libgpos/src/error/CFSimulator.cpp
            libgpos/include/gpos/error/CLogger.h
            libgpos/src/error/CLogger.cpp
            libgpos/include/gpos/error/CLoggerFile.h
            libgpos/src/error/CLoggerFile.cpp
            libgpos/include/gpos/error/CLoggerStream.h
            libgpos/src/error/CLoggerStream.cpp
            libgpos/include/gpos/error/CLoggerSyslog.h
            libgpos/src/error/CLoggerSyslog.cpp
            libgpos/include/gpos/error/CMessage.h
            libgpos/src/error/CMessage.cpp
            libgpos/include/gpos/error/CMessageRepository.h
            libgpos/src/error/CMessageRepository.cpp
            libgpos/include/gpos/error/CMessageTable.h
            libgpos/src/error/CMessageTable.cpp
            libgpos/include/gpos/error/CMiniDumper.h
            libgpos/src/error/CMiniDumper.cpp
            libgpos/include/gpos/error/CSerializable.h
            libgpos/src/error/CSerializable.cpp
            libgpos/include/gpos/error/ILogger.h
            libgpos/src/error/ILogger.cpp
            libgpos/include/gpos/io/CFileDescriptor.h
            libgpos/src/io/CFileDescriptor.cpp
            libgpos/include/gpos/io/CFileReader.h
            libgpos/src/io/CFileReader.cpp
            libgpos/include/gpos/io/CFileWriter.h
            libgpos/src/io/CFileWriter.cpp
            libgpos/include/gpos/io/COstream.h
            libgpos/src/io/COstream.cpp
            libgpos/include/gpos/io/COstreamBasic.h
            libgpos/src/io/COstreamBasic.cpp
            libgpos/include/gpos/io/COstreamFile.h
            libgpos/src/io/COstreamFile.cpp
            libgpos/include/gpos/io/COstreamString.h
            libgpos/src/io/COstreamString.cpp
            libgpos/include/gpos/io/ioutils.h
            libgpos/src/io/ioutils.cpp
            libgpos/include/gpos/memory/CAutoMemoryPool.h
            libgpos/src/memory/CAutoMemoryPool.cpp
            libgpos/include/gpos/memory/CCacheFactory.h
            libgpos/src/memory/CCacheFactory.cpp
            libgpos/include/gpos/memory/CMemoryPool.h
            libgpos/src/memory/CMemoryPool.cpp
            libgpos/include/gpos/memory/CMemoryPoolInjectFault.h
            libgpos/src/memory/CMemoryPoolInjectFault.cpp
            libgpos/include/gpos/memory/CMemoryPoolManager.h
            libgpos/src/memory/CMemoryPoolManager.cpp
            libgpos/include/gpos/memory/CMemoryPoolSlab.h
            libgpos/src/memory/CMemoryPoolSlab.cpp
            libgpos/include/gpos/memory/CMemoryPoolStack.h
            libgpos/src/memory/CMemoryPoolStack.cpp
            libgpos/include/gpos/memory/CMemoryPoolTracker.h
            libgpos/src/memory/CMemoryPoolTracker.cpp
            libgpos/include/gpos/memory/CMemoryVisitorPrint.h
            libgpos/src/memory/CMemoryVisitorPrint.cpp
            libgpos/include/gpos/memory/CSlab.h
            libgpos/src/memory/CSlab.cpp
            libgpos/include/gpos/memory/CSlabCache.h
            libgpos/src/memory/CSlabCache.cpp
            libgpos/include/gpos/memory/IMemoryPool.h
            libgpos/src/memory/IMemoryPool.cpp
            libgpos/include/gpos/net/CSocket.h
            libgpos/src/net/CSocket.cpp
            libgpos/include/gpos/net/CSocketConnectorUDS.h
            libgpos/src/net/CSocketConnectorUDS.cpp
            libgpos/include/gpos/net/CSocketListener.h
            libgpos/src/net/CSocketListener.cpp
            libgpos/include/gpos/net/CSocketListenerUDS.h
            libgpos/src/net/CSocketListenerUDS.cpp
            libgpos/include/gpos/net/netutils.h
            libgpos/src/net/netutils.cpp
            libgpos/include/gpos/string/CStringStatic.h
            libgpos/src/string/CStringStatic.cpp
            libgpos/include/gpos/string/CWString.h
            libgpos/src/string/CWString.cpp
            libgpos/include/gpos/string/CWStringBase.h
            libgpos/src/string/CWStringBase.cpp
            libgpos/include/gpos/string/CWStringConst.h
            libgpos/src/string/CWStringConst.cpp
            libgpos/include/gpos/string/CWStringDynamic.h
            libgpos/src/string/CWStringDynamic.cpp
            libgpos/include/gpos/string/CWStringStatic.h
            libgpos/src/string/CWStringStatic.cpp
            libgpos/include/gpos/sync/atomic.h
            libgpos/src/sync/atomic.cpp
            libgpos/include/gpos/sync/CAutoMutex.h
            libgpos/src/sync/CAutoMutex.cpp
            libgpos/include/gpos/sync/CAutoSpinlock.h
            libgpos/src/sync/CAutoSpinlock.cpp
            libgpos/include/gpos/sync/CEvent.h
            libgpos/src/sync/CEvent.cpp
            libgpos/include/gpos/task/CAutoSuspendAbort.h
            libgpos/src/task/CAutoSuspendAbort.cpp
            libgpos/include/gpos/task/CAutoTaskProxy.h
            libgpos/src/task/CAutoTaskProxy.cpp
            libgpos/include/gpos/task/CAutoTraceFlag.h
            libgpos/src/task/CAutoTraceFlag.cpp
            libgpos/include/gpos/task/CTask.h
            libgpos/src/task/CTask.cpp
            libgpos/include/gpos/task/CTaskContext.h
            libgpos/src/task/CTaskContext.cpp
            libgpos/include/gpos/task/CTaskLocalStorage.h
            libgpos/src/task/CTaskLocalStorage.cpp
            libgpos/include/gpos/task/CTaskSchedulerFifo.h
            libgpos/src/task/CTaskSchedulerFifo.cpp
            libgpos/include/gpos/task/CThreadManager.h
            libgpos/src/task/CThreadManager.cpp
            libgpos/include/gpos/task/CWorker.h
            libgpos/src/task/CWorker.cpp
            libgpos/include/gpos/task/CWorkerId.h
            libgpos/src/task/CWorkerId.cpp
            libgpos/include/gpos/task/CWorkerPoolManager.h
            libgpos/src/task/CWorkerPoolManager.cpp
            libgpos/include/gpos/task/ITask.h
            libgpos/src/task/ITask.cpp
            libgpos/include/gpos/task/IWorker.h
            libgpos/src/task/IWorker.cpp
            libgpos/include/gpos/test/CFSimulatorTestExt.h
            libgpos/src/test/CFSimulatorTestExt.cpp
            libgpos/include/gpos/test/CTimeSliceTest.h
            libgpos/src/test/CTimeSliceTest.cpp
            libgpos/include/gpos/test/CUnittest.h
            libgpos/src/test/CUnittest.cpp
            )
target_link_libraries(gpos ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_DL_LIBS})

# Extra system libs needed on Solaris.
if (${CMAKE_SYSTEM_NAME} MATCHES "SunOS")
  find_library(RT_LIBRARY NAMES rt librt)
  if (${RT_LIBRARY-NOTFOUND})
    message(WARNING "rt library not found. Linking may fail.")
  else()
    target_link_libraries(gpos ${RT_LIBRARY})
  endif()

  find_library(SOCKET_LIBRARY NAMES socket libsocket)
  if (${SOCKET_LIBRARY-NOTFOUND})
    message(WARNING "socket library not found. Linking may fail.")
  else()
    target_link_libraries(gpos ${SOCKET_LIBRARY})
  endif()
endif()

set_target_properties(gpos PROPERTIES
                      SOVERSION ${GPOS_ABI_VERSION}
                      VERSION ${GPOS_VERSION_STRING})

# Tests reside in the 'server' subdirectory.
enable_testing()
add_subdirectory(server)

# Installation.
option(VERBOSE_INSTALL_PATH "Install in a subdirectory path that includes GPOS version, CPU architecture, and bit width" OFF)
if (VERBOSE_INSTALL_PATH)
  set(installpath "libgpos/${GPOS_VERSION_STRING}/${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}/${CMAKE_BUILD_TYPE}")
  string(TOLOWER ${installpath} installpath)
  get_filename_component(full_install_name_dir "${installpath}/lib" ABSOLUTE)
  install(TARGETS gpos DESTINATION "${installpath}/lib")
  install(DIRECTORY libgpos/include/gpos DESTINATION "${installpath}/include")
  install(FILES "${PROJECT_BINARY_DIR}/libgpos/include/gpos/version.h"
          DESTINATION "${installpath}/include/gpos")
else()
  get_filename_component(full_install_name_dir "${CMAKE_INSTALL_PREFIX}/lib" ABSOLUTE)
  install(TARGETS gpos DESTINATION lib)
  install(DIRECTORY libgpos/include/gpos DESTINATION include)
  install(FILES "${PROJECT_BINARY_DIR}/libgpos/include/gpos/version.h"
          DESTINATION include/gpos)
endif()

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
