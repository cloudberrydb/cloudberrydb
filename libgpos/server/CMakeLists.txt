# Copyright (c) 2015, Pivotal Software, Inc. All Rights Reserved.

# Include path for unit test headers.
include_directories(include)

# CExceptionTest has a function that intentionally calls itself with infinite
# recursion to test exception handling when exhausting the stack. Some C++
# compilers (notably Clang) detect this and generate a warning. We suppress
# such warnings ONLY for CExceptionTest, which we compile as a static library
# separate from the rest of the tests.
check_cxx_compiler_flag("-Wno-infinite-recursion"
                        COMPILER_HAS_WNO_INFINITE_RECURSION)
if (COMPILER_HAS_WNO_INFINITE_RECURSION)
  set_source_files_properties(src/unittest/gpos/error/CExceptionTest.cpp
    PROPERTIES COMPILE_FLAGS "-Wno-infinite-recursion")
endif()

# CCacheTest sets some variables that are only read in DEBUG builds, causing
# warnings about unused variables for RELEASE builds. We suppress such warnings
# ONLY for CCacheTest, which we compile as a static library separate from the
# rest of the tests.
check_cxx_compiler_flag("-Wno-unused-variable"
                        COMPILER_HAS_WNO_UNUSED_VARIABLE)
if (COMPILER_HAS_WNO_UNUSED_VARIABLE)
  set_source_files_properties(src/unittest/gpos/memory/CCacheTest.cpp
    PROPERTIES COMPILE_FLAGS "-Wno-unused-variable")
endif()

# Monolithic gpos_test executable.
add_executable(gpos_test
               src/startup/main.cpp
               src/unittest/gpos/common/CAutoPTest.cpp
               src/unittest/gpos/common/CAutoRefTest.cpp
               src/unittest/gpos/common/CAutoRgTest.cpp
               src/unittest/gpos/common/CBitSetIterTest.cpp
               src/unittest/gpos/common/CBitSetTest.cpp
               src/unittest/gpos/common/CBitVectorTest.cpp
               src/unittest/gpos/common/CDoubleTest.cpp
               src/unittest/gpos/common/CDynamicPtrArrayTest.cpp
               src/unittest/gpos/common/CEnumSetTest.cpp
               src/unittest/gpos/common/CHashMapIterTest.cpp
               src/unittest/gpos/common/CHashMapTest.cpp
               src/unittest/gpos/common/CHashSetIterTest.cpp
               src/unittest/gpos/common/CHashSetTest.cpp
               src/unittest/gpos/common/CListTest.cpp
               src/unittest/gpos/common/CRefCountTest.cpp
               src/unittest/gpos/common/CStackTest.cpp
               src/unittest/gpos/common/CSyncHashtableTest.cpp
               src/unittest/gpos/common/CSyncListTest.cpp
               src/unittest/gpos/error/CErrorHandlerTest.cpp
               src/unittest/gpos/error/CExceptionTest.cpp
               src/unittest/gpos/error/CFSimulatorTest.cpp
               src/unittest/gpos/error/CLoggerTest.cpp
               src/unittest/gpos/error/CMessageRepositoryTest.cpp
               src/unittest/gpos/error/CMessageTableTest.cpp
               src/unittest/gpos/error/CMessageTest.cpp
               src/unittest/gpos/error/CMiniDumperTest.cpp
               src/unittest/gpos/io/CFileTest.cpp
               src/unittest/gpos/io/COstreamBasicTest.cpp
               src/unittest/gpos/io/COstreamFileTest.cpp
               src/unittest/gpos/io/COstreamStringTest.cpp
               src/unittest/gpos/memory/CCacheTest.cpp
               src/unittest/gpos/memory/CMemoryPoolBasicTest.cpp
               src/unittest/gpos/memory/IMemoryPoolTest.cpp
               src/unittest/gpos/memory/CMemoryPoolAllocTest.cpp
               src/unittest/gpos/string/CStringTest.cpp
               src/unittest/gpos/string/CWStringTest.cpp
               src/unittest/gpos/sync/CAutoMutexTest.cpp
               src/unittest/gpos/sync/CAutoSpinlockTest.cpp
               src/unittest/gpos/sync/CEventTest.cpp
               src/unittest/gpos/sync/CMutexTest.cpp
               src/unittest/gpos/sync/CSpinlockTest.cpp
               src/unittest/gpos/task/CAutoTaskProxyTest.cpp
               src/unittest/gpos/task/CTaskLocalStorageTest.cpp
               src/unittest/gpos/task/CWorkerPoolManagerTest.cpp
               src/unittest/gpos/test/CUnittestTest.cpp)
target_link_libraries(gpos_test
                      gpos)

# Convenience function to add the test specified by 'TEST_NAME' to the set of
# tests to be run by CTest.
function(add_gpos_test TEST_NAME)
  add_test(NAME gpos_test_${TEST_NAME}
           COMMAND gpos_test -U ${TEST_NAME})
endfunction()
# Adaptation of the add_gpos_test for setting up a custom allocator instead of
# using CMemoryPoolMalloc in gpos_init().
function(add_gpos_custom_alloc_test TEST_NAME)
  add_test(NAME gpos_custom_alloc_test_${TEST_NAME}
           COMMAND gpos_test -U ${TEST_NAME} "-c")
endfunction()

option(ENABLE_EXTENDED_TESTS
       "Enable extended tests for fault-injection and timing that may take a long time to run."
       OFF)

# Individual unit tests are added here in the same order they appear in
# src/startup/main.cpp

# common
add_gpos_test(CAutoPTest)
add_gpos_test(CAutoRefTest)
add_gpos_test(CAutoRgTest)
add_gpos_test(CBitSetIterTest)
add_gpos_test(CBitSetTest)
add_gpos_test(CBitVectorTest)
add_gpos_test(CDynamicPtrArrayTest)
add_gpos_test(CEnumSetTest)
add_gpos_test(CDoubleTest)
add_gpos_test(CHashMapTest)
add_gpos_test(CHashMapIterTest)
add_gpos_test(CHashSetTest)
add_gpos_test(CHashSetIterTest)
add_gpos_test(CRefCountTest)
add_gpos_test(CListTest)
add_gpos_test(CStackTest)
add_gpos_test(CSyncHashtableTest)
add_gpos_test(CSyncListTest)

# error
add_gpos_test(CErrorHandlerTest)
add_gpos_test(CExceptionTest)
add_gpos_test(CLoggerTest)
add_gpos_test(CMessageTest)
add_gpos_test(CMessageTableTest)
add_gpos_test(CMessageRepositoryTest)
add_gpos_test(CMiniDumperTest)

# io
add_gpos_test(COstreamBasicTest)
add_gpos_test(COstreamStringTest)
add_gpos_test(COstreamFileTest)
add_gpos_test(CFileTest)

# memory
add_gpos_test(CMemoryPoolTest)
add_gpos_test(CMemoryPoolBasicTest)
add_gpos_test(CCacheTest)

# custom allocator
add_gpos_custom_alloc_test(CMemoryPoolTest)
add_gpos_custom_alloc_test(CMemoryPoolBasicTest)
add_gpos_custom_alloc_test(CCacheTest)
add_gpos_custom_alloc_test(CMemoryPoolCustomAllocTest)

# string
add_gpos_test(CWStringTest)
add_gpos_test(CStringTest)

# sync
add_gpos_test(CAutoMutexTest)
add_gpos_test(CAutoSpinlockTest)
add_gpos_test(CEventTest)
add_gpos_test(CMutexTest)
add_gpos_test(CSpinlockTest)

# task
add_gpos_test(CAutoTaskProxyTest)
add_gpos_test(CWorkerPoolManagerTest)
add_gpos_test(CTaskLocalStorageTest)

# test
add_gpos_test(CUnittestTest_1)
add_gpos_test(CUnittestTest_2)
add_gpos_test(CUnittestTest_3)

if (${CMAKE_BUILD_TYPE} MATCHES "Debug")
  # fault-simulation
  add_gpos_test(CFSimulatorTest)
  if (ENABLE_EXTENDED_TESTS)
    # fault-simulation while running other tests
    add_gpos_test(CFSimulatorTestExt)

    # time-slicing
    add_gpos_test(CTimeSliceTest)
  endif()
endif()
