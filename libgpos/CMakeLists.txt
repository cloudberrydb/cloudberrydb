# Need pthreads.
find_package(Threads REQUIRED)
if (NOT CMAKE_USE_PTHREADS_INIT)
  message(FATAL_ERROR "Found a threading library, but it is not pthreads.")
endif()

# Try to find atomic operations.
include(FindAtomics)

if ((NOT (GPOS_GCC_FETCH_ADD_32 AND GPOS_GCC_FETCH_ADD_64
          AND GPOS_GCC_CAS_32 AND GPOS_GCC_CAS_64))
    AND (NOT (${CMAKE_SYSTEM_NAME} MATCHES "SunOS")))
  message(FATAL_ERROR "Could not find GCC-style atomic built-ins or Solaris "
                      "atomic headers. GPOS will fail to build. Please try "
                      "using a recent g++ or clang++ compiler.")
endif()

include_directories(include)

configure_file(config.h.in
        ${PROJECT_BINARY_DIR}/libgpos/include/gpos/config.h)

# for the generated config.h file.
include_directories(${PROJECT_BINARY_DIR}/libgpos/include/)

# Compile .cpp source files under libgpos/src into monolithic gpos library.
add_library(gpos
            include/gpos/assert.h
            include/gpos/base.h
            include/gpos/common/CAutoP.h
            include/gpos/common/CAutoRef.h
            include/gpos/common/CAutoRg.h
            include/gpos/common/CDouble.h
            include/gpos/common/CDynamicPtrArray.h
            include/gpos/common/CDynamicPtrArrayUtils.h
            include/gpos/common/CEnumSet.h
            include/gpos/common/CEnumSetIter.h
            include/gpos/common/CHashMap.h
            include/gpos/common/CHashMapIter.h
            include/gpos/common/CHashSet.h
            include/gpos/common/CHashSetIter.h
            include/gpos/common/clibtypes.h
            include/gpos/common/CList.h
            include/gpos/common/CRefCount.h
            include/gpos/common/CStack.h
            include/gpos/common/CSyncHashtable.h
            include/gpos/common/CSyncHashtableAccessByIter.h
            include/gpos/common/CSyncHashtableAccessByKey.h
            include/gpos/common/CSyncHashtableAccessorBase.h
            include/gpos/common/CSyncHashtableIter.h
            include/gpos/common/CSyncList.h
            include/gpos/common/CSyncPool.h
            include/gpos/common/ITimer.h
            include/gpos/common/pthreadtypes.h
            include/gpos/error/CAutoLogger.h
            include/gpos/error/CErrorHandler.h
            include/gpos/error/IErrorContext.h
            include/gpos/io/CAutoFileDescriptor.h
            include/gpos/io/IOstream.h
            include/gpos/io/iotypes.h
            include/gpos/memory/CCache.h
            include/gpos/memory/CCacheAccessor.h
            include/gpos/memory/CCacheEntry.h
            include/gpos/memory/CMemoryPoolAlloc.h
            include/gpos/memory/CMemoryPoolStatistics.h
            include/gpos/memory/ICache.h
            include/gpos/memory/IMemoryVisitor.h
            include/gpos/memory/README.md
            include/gpos/net/nettypes.h
            include/gpos/sync/CAtomicCounter.h
            include/gpos/sync/CMutex.h
            include/gpos/sync/CSpinlock.h
            include/gpos/task/CTaskId.h
            include/gpos/task/CTaskLocalStorageObject.h
            include/gpos/task/CTraceFlagIter.h
            include/gpos/task/ITaskScheduler.h
            include/gpos/task/traceflags.h
            include/gpos/types.h

            include/gpos/_api.h
            src/_api.cpp
            include/gpos/utils.h
            src/utils.cpp
            include/gpos/common/CAutoTimer.h
            src/common/CAutoTimer.cpp
            include/gpos/common/CBitSet.h
            src/common/CBitSet.cpp
            include/gpos/common/CBitSetIter.h
            src/common/CBitSetIter.cpp
            include/gpos/common/CBitVector.h
            src/common/CBitVector.cpp
            include/gpos/common/CHeapObject.h
            src/common/CHeapObject.cpp
            include/gpos/common/clibwrapper.h
            src/common/clibwrapper.cpp
            include/gpos/common/CMainArgs.h
            src/common/CMainArgs.cpp
            include/gpos/common/CRandom.h
            src/common/CRandom.cpp
            include/gpos/common/CStackDescriptor.h
            src/common/CStackDescriptor.cpp
            include/gpos/common/CStackObject.h
            src/common/CStackObject.cpp
            include/gpos/common/CTimerUser.h
            src/common/CTimerUser.cpp
            include/gpos/common/CWallClock.h
            src/common/CWallClock.cpp
            include/gpos/common/pthreadwrapper.h
            src/common/pthreadwrapper.cpp
            include/gpos/common/syslibwrapper.h
            src/common/syslibwrapper.cpp
            include/gpos/error/CAutoExceptionStack.h
            src/error/CAutoExceptionStack.cpp
            include/gpos/error/CAutoTrace.h
            src/error/CAutoTrace.cpp
            include/gpos/error/CErrorContext.h
            src/error/CErrorContext.cpp
            include/gpos/error/CErrorHandlerStandard.h
            src/error/CErrorHandlerStandard.cpp
            include/gpos/error/CException.h
            src/error/CException.cpp
            include/gpos/error/CFSimulator.h
            src/error/CFSimulator.cpp
            include/gpos/error/CLogger.h
            src/error/CLogger.cpp
            include/gpos/error/CLoggerFile.h
            src/error/CLoggerFile.cpp
            include/gpos/error/CLoggerStream.h
            src/error/CLoggerStream.cpp
            include/gpos/error/CLoggerSyslog.h
            src/error/CLoggerSyslog.cpp
            include/gpos/error/CMessage.h
            src/error/CMessage.cpp
            include/gpos/error/CMessageRepository.h
            src/error/CMessageRepository.cpp
            include/gpos/error/CMessageTable.h
            src/error/CMessageTable.cpp
            include/gpos/error/CMiniDumper.h
            src/error/CMiniDumper.cpp
            include/gpos/error/CSerializable.h
            src/error/CSerializable.cpp
            include/gpos/error/ILogger.h
            src/error/ILogger.cpp
            include/gpos/io/CFileDescriptor.h
            src/io/CFileDescriptor.cpp
            include/gpos/io/CFileReader.h
            src/io/CFileReader.cpp
            include/gpos/io/CFileWriter.h
            src/io/CFileWriter.cpp
            include/gpos/io/COstream.h
            src/io/COstream.cpp
            include/gpos/io/COstreamBasic.h
            src/io/COstreamBasic.cpp
            include/gpos/io/COstreamFile.h
            src/io/COstreamFile.cpp
            include/gpos/io/COstreamString.h
            src/io/COstreamString.cpp
            include/gpos/io/ioutils.h
            src/io/ioutils.cpp
            include/gpos/memory/CAutoMemoryPool.h
            src/memory/CAutoMemoryPool.cpp
            include/gpos/memory/CCacheFactory.h
            src/memory/CCacheFactory.cpp
            include/gpos/memory/CMemoryPool.h
            src/memory/CMemoryPool.cpp
            include/gpos/memory/CMemoryPoolInjectFault.h
            src/memory/CMemoryPoolInjectFault.cpp
            include/gpos/memory/CMemoryPoolManager.h
            src/memory/CMemoryPoolManager.cpp
            include/gpos/memory/CMemoryPoolSlab.h
            src/memory/CMemoryPoolSlab.cpp
            include/gpos/memory/CMemoryPoolStack.h
            src/memory/CMemoryPoolStack.cpp
            include/gpos/memory/CMemoryPoolTracker.h
            src/memory/CMemoryPoolTracker.cpp
            include/gpos/memory/CMemoryVisitorPrint.h
            src/memory/CMemoryVisitorPrint.cpp
            include/gpos/memory/CSlab.h
            src/memory/CSlab.cpp
            include/gpos/memory/CSlabCache.h
            src/memory/CSlabCache.cpp
            include/gpos/memory/IMemoryPool.h
            src/memory/IMemoryPool.cpp
            include/gpos/net/netutils.h
            src/net/netutils.cpp
            include/gpos/string/CStringStatic.h
            src/string/CStringStatic.cpp
            include/gpos/string/CWString.h
            src/string/CWString.cpp
            include/gpos/string/CWStringBase.h
            src/string/CWStringBase.cpp
            include/gpos/string/CWStringConst.h
            src/string/CWStringConst.cpp
            include/gpos/string/CWStringDynamic.h
            src/string/CWStringDynamic.cpp
            include/gpos/string/CWStringStatic.h
            src/string/CWStringStatic.cpp
            include/gpos/sync/atomic.h
            src/sync/atomic.cpp
            include/gpos/sync/CAutoMutex.h
            src/sync/CAutoMutex.cpp
            include/gpos/sync/CAutoSpinlock.h
            src/sync/CAutoSpinlock.cpp
            include/gpos/sync/CEvent.h
            src/sync/CEvent.cpp
            include/gpos/task/CAutoSuspendAbort.h
            src/task/CAutoSuspendAbort.cpp
            include/gpos/task/CAutoTaskProxy.h
            src/task/CAutoTaskProxy.cpp
            include/gpos/task/CAutoTraceFlag.h
            src/task/CAutoTraceFlag.cpp
            include/gpos/task/CTask.h
            src/task/CTask.cpp
            include/gpos/task/CTaskContext.h
            src/task/CTaskContext.cpp
            include/gpos/task/CTaskLocalStorage.h
            src/task/CTaskLocalStorage.cpp
            include/gpos/task/CTaskSchedulerFifo.h
            src/task/CTaskSchedulerFifo.cpp
            include/gpos/task/CThreadManager.h
            src/task/CThreadManager.cpp
            include/gpos/task/CWorker.h
            src/task/CWorker.cpp
            include/gpos/task/CWorkerId.h
            src/task/CWorkerId.cpp
            include/gpos/task/CWorkerPoolManager.h
            src/task/CWorkerPoolManager.cpp
            include/gpos/task/ITask.h
            src/task/ITask.cpp
            include/gpos/task/IWorker.h
            src/task/IWorker.cpp
            include/gpos/test/CFSimulatorTestExt.h
            src/test/CFSimulatorTestExt.cpp
            include/gpos/test/CTimeSliceTest.h
            src/test/CTimeSliceTest.cpp
            include/gpos/test/CUnittest.h
            src/test/CUnittest.cpp
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
                      SOVERSION ${GPORCA_ABI_VERSION}
                      VERSION ${GPORCA_VERSION_STRING})

# Tests reside in the 'server' subdirectory.
add_subdirectory(server)

# Installation.
option(VERBOSE_INSTALL_PATH "Install in a subdirectory path that includes GPOS version, CPU architecture, and bit width" OFF)
if (VERBOSE_INSTALL_PATH)
  set(installpath "libgpos/${GPORCA_VERSION_VERSION}/${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}/${CMAKE_BUILD_TYPE}")
  string(TOLOWER ${installpath} installpath)
  get_filename_component(full_install_name_dir "${installpath}/lib" ABSOLUTE)
  install(TARGETS gpos DESTINATION "${installpath}/lib")
  install(DIRECTORY include/gpos DESTINATION "${installpath}/include")
  install(FILES "${PROJECT_BINARY_DIR}/libgpos/include/gpos/config.h" DESTINATION "${installpath}/include/gpos")
else()
  get_filename_component(full_install_name_dir "${CMAKE_INSTALL_PREFIX}/lib" ABSOLUTE)
  install(TARGETS gpos DESTINATION lib)
  install(DIRECTORY include/gpos DESTINATION include)
  install(FILES "${PROJECT_BINARY_DIR}/libgpos/include/gpos/config.h" DESTINATION include/gpos)
endif()

