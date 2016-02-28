# Copyright (c) 2015, Pivotal Software, Inc. All Rights Reserved.

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/include"
                    "${CMAKE_SOURCE_DIR}/libgpdbcost/include"
                    "${CMAKE_SOURCE_DIR}/libgpopt/include"
                    "${CMAKE_SOURCE_DIR}/libnaucrates/include")

# Some tests use C99 long long integer constants, but the C++ dialect may not
# explicitly include C99. We supress such errors here.
add_library(gporca_test_longlong_tests
            src/unittest/dxl/statistics/CStatisticsTest.cpp
            src/unittest/gpopt/base/CConstraintTest.cpp
            src/unittest/gpopt/metadata/CPartConstraintTest.cpp)
check_cxx_compiler_flag("-Wno-long-long"
                        COMPILER_HAS_WNO_LONG_LONG)
if (COMPILER_HAS_WNO_LONG_LONG)
  set_target_properties(gporca_test_longlong_tests
                        PROPERTIES COMPILE_FLAGS "-Wno-long-long")
endif()

add_executable(gporca_test
               src/startup/main.cpp
               src/unittest/CTestUtils.cpp
               src/unittest/CConstExprEvaluatorForDates.cpp
               src/unittest/CSubqueryTestUtils.cpp
               src/unittest/dxl/CParseHandlerManagerTest.cpp
               src/unittest/dxl/CMDProviderTest.cpp
               src/unittest/dxl/CDXLMemoryManagerTest.cpp
               src/unittest/dxl/CXMLSerializerTest.cpp
               src/unittest/dxl/CDXLUtilsTest.cpp
               src/unittest/dxl/CMDProviderCommTest.cpp
               src/unittest/dxl/CParseHandlerTest.cpp
               src/unittest/dxl/comm/CCommunicatorTest.cpp
               src/unittest/dxl/base/CDatumTest.cpp
               src/unittest/gpopt/translate/CTranslatorExprToDXLTest.cpp
               src/unittest/gpopt/translate/CTpchTest.cpp
               src/unittest/gpopt/translate/CTranslatorDXLToExprTest.cpp
               src/unittest/gpopt/search/CTreeMapTest.cpp
               src/unittest/gpopt/search/COptimizationJobsTest.cpp
               src/unittest/gpopt/search/CSchedulerTest.cpp
               src/unittest/gpopt/search/CSearchStrategyTest.cpp
               src/unittest/gpopt/minidump/CAggTest.cpp
               src/unittest/gpopt/minidump/CPruneColumnsTest.cpp
               src/unittest/gpopt/minidump/CMissingStatsTest.cpp
               src/unittest/gpopt/minidump/CBitmapTest.cpp
               src/unittest/gpopt/minidump/CPartTblTest.cpp
               src/unittest/gpopt/minidump/CDMLTest.cpp
               src/unittest/gpopt/minidump/CMinidumpWithConstExprEvaluatorTest.cpp
               src/unittest/gpopt/minidump/CMiniDumperDXLTest.cpp
               src/unittest/gpopt/minidump/CMultilevelPartitionTest.cpp
               src/unittest/gpopt/minidump/CDirectDispatchTest.cpp
               src/unittest/gpopt/minidump/CCTETest.cpp
               src/unittest/gpopt/minidump/CICGTest.cpp
               src/unittest/gpopt/minidump/CSetopTest.cpp
               src/unittest/gpopt/minidump/CIndexTest.cpp
               src/unittest/gpopt/minidump/CTpcdsTest.cpp
               src/unittest/gpopt/minidump/CTVFTest.cpp
               src/unittest/gpopt/xforms/CDecorrelatorTest.cpp
               src/unittest/gpopt/xforms/CSubqueryHandlerTest.cpp
               src/unittest/gpopt/xforms/CXformTest.cpp
               src/unittest/gpopt/xforms/CXformFactoryTest.cpp
               src/unittest/gpopt/xforms/CJoinOrderTest.cpp
               src/unittest/gpopt/engine/CEngineTest.cpp
               src/unittest/gpopt/engine/CEnumeratorTest.cpp
               src/unittest/gpopt/operators/CExpressionTest.cpp
               src/unittest/gpopt/operators/CContradictionTest.cpp
               src/unittest/gpopt/operators/CExpressionPreprocessorTest.cpp
               src/unittest/gpopt/operators/CPredicateUtilsTest.cpp
               src/unittest/gpopt/operators/CCNFConverterTest.cpp
               src/unittest/gpopt/mdcache/CMDAccessorTest.cpp
               src/unittest/gpopt/eval/CConstExprEvaluatorDXLTest.cpp
               src/unittest/gpopt/eval/CConstExprEvaluatorDefaultTest.cpp
               src/unittest/gpopt/base/CKeyCollectionTest.cpp
               src/unittest/gpopt/base/CColumnFactoryTest.cpp
               src/unittest/gpopt/base/CStateMachineTest.cpp
               src/unittest/gpopt/base/CColRefSetIterTest.cpp
               src/unittest/gpopt/base/CStatsEquivClassTest.cpp
               src/unittest/gpopt/base/CColRefSetTest.cpp
               src/unittest/gpopt/base/CFunctionalDependencyTest.cpp
               src/unittest/gpopt/base/CMaxCardTest.cpp
               src/unittest/gpopt/base/CRangeTest.cpp
               src/unittest/gpopt/base/CDistributionSpecTest.cpp
               src/unittest/gpopt/base/COrderSpecTest.cpp
               src/unittest/gpopt/csq/CCorrelatedExecutionTest.cpp
               src/unittest/gpopt/cost/CCostTest.cpp
               src/unittest/gpopt/metadata/CTableDescriptorTest.cpp
               src/unittest/gpopt/metadata/CColumnDescriptorTest.cpp
               src/unittest/gpopt/metadata/CIndexDescriptorTest.cpp
               src/unittest/gpopt/metadata/CNameTest.cpp)
target_link_libraries(gporca_test
                      gporca_test_longlong_tests
                      gpdbcost
                      gpopt
                      naucrates
                      ${GPOS_LIBRARIES})

option(ENABLE_EXTENDED_TESTS
       "Enable extended tests for fault-injection and timing that may take a long time to run."
       OFF)

# Convenience function to add the test specified by 'TEST_NAME' to the set of
# tests to be run by CTest.
function(add_orca_test TEST_NAME)
  # Unit tests have many hard-coded paths to test data in optimizer/data. We
  # explicitly set the WORKING_DIRECTORY so that the files can be found
  # properly.
  add_test(NAME gporca_test_${TEST_NAME}
           COMMAND gporca_test -U ${TEST_NAME}
           WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endfunction()

# The ordering of tests and the conditions by which tests are enabled or
# disabled matches the static array of tests in "src/startup/main.cpp".

# Naucrates tests.
add_orca_test(CCostTest)
add_orca_test(CDatumTest)
add_orca_test(CDXLMemoryManagerTest)
add_orca_test(CDXLUtilsTest)
add_orca_test(CMDAccessorTest)
add_orca_test(CMDProviderTest)

if (NOT (${CMAKE_SYSTEM_NAME} MATCHES "SunOS"))
  add_orca_test(CMDProviderCommTest)
endif()

add_orca_test(CMiniDumperDXLTest)
add_orca_test(CExpressionPreprocessorTest)
add_orca_test(CICGTest)
add_orca_test(CMultilevelPartitionTest)
add_orca_test(CSetopTest)
add_orca_test(CDMLTest)
add_orca_test(CDirectDispatchTest)
add_orca_test(CTVFTest)
add_orca_test(CAggTest)
add_orca_test(CPruneColumnsTest)
add_orca_test(CMissingStatsTest)
add_orca_test(CIndexTest)
add_orca_test(CPartTblTest)
add_orca_test(CBitmapTest)
add_orca_test(CCTETest)

add_orca_test(CMinidumpWithConstExprEvaluatorTest)
add_orca_test(CParseHandlerManagerTest)
add_orca_test(CParseHandlerTest)
add_orca_test(CStatisticsTest)
add_orca_test(CTranslatorDXLToExprTest)
add_orca_test(CTranslatorExprToDXLTest)
add_orca_test(CXMLSerializerTest)

# Opt tests.
add_orca_test(CCNFConverterTest)
add_orca_test(CColumnDescriptorTest)
add_orca_test(CColumnFactoryTest)
add_orca_test(CColRefSetIterTest)
add_orca_test(CColRefSetTest)
add_orca_test(CConstraintTest)
add_orca_test(CStatsEquivClassTest)
add_orca_test(CContradictionTest)
add_orca_test(CCorrelatedExecutionTest)
add_orca_test(CDecorrelatorTest)
add_orca_test(CDistributionSpecTest)

if (GPOS_ARCH_BITS EQUAL 64)
  add_orca_test(CSubqueryHandlerTest)
endif()

add_orca_test(CEngineTest)
add_orca_test(CExpressionTest)
add_orca_test(CJoinOrderTest)
add_orca_test(CKeyCollectionTest)
add_orca_test(CMaxCardTest)
add_orca_test(CFunctionalDependencyTest)
add_orca_test(CNameTest)
add_orca_test(COrderSpecTest)
add_orca_test(CRangeTest)
add_orca_test(CPredicateUtilsTest)
add_orca_test(CPartConstraintTest)

if (NOT (${CMAKE_SYSTEM_NAME} MATCHES "SunOS"))
  add_orca_test(CSchedulerTest)
  add_orca_test(CSearchStrategyTest)
endif()

add_orca_test(COptimizationJobsTest)
add_orca_test(CStateMachineTest)
add_orca_test(CTableDescriptorTest)
add_orca_test(CIndexDescriptorTest)
add_orca_test(CTreeMapTest)
add_orca_test(CXformFactoryTest)
add_orca_test(CXformTest)

if (NOT (${CMAKE_SYSTEM_NAME} MATCHES "SunOS"))
  foreach(TPCH_QUERY_NUM RANGE 1 22)
    add_orca_test(CTpchTest_${TPCH_QUERY_NUM})
  endforeach()
endif()

if (NOT ((${CMAKE_BUILD_TYPE} MATCHES "Debug")
         OR (GPOS_ARCH_BITS EQUAL 32)
         OR (${CMAKE_SYSTEM_NAME} MATCHES "SunOS")))
  foreach(TPCDS_QUERY_NUM RANGE 1 99)
    add_orca_test(CTpcdsTest_${TPCDS_QUERY_NUM})
  endforeach()
endif()

if ((NOT (${CMAKE_SYSTEM_NAME} MATCHES "SunOS"))
    AND (${CMAKE_BUILD_TYPE} MATCHES "Debug"))
  add_orca_test(CCommunicatorTest)
endif()

add_orca_test(CConstExprEvaluatorDefaultTest)
add_orca_test(CConstExprEvaluatorDXLTest)

if (${CMAKE_BUILD_TYPE} MATCHES "Debug")
  if (ENABLE_EXTENDED_TESTS)
    # fault-simulation while running other tests
    add_orca_test(CFSimulatorTestExt)

    # time-slicing
    add_orca_test(CTimeSliceTest)
  endif()
endif()
