# Copyright (c) 2015, Pivotal Software, Inc. All Rights Reserved.

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/include"
                    "${CMAKE_SOURCE_DIR}/libgpdbcost/include"
                    "${CMAKE_SOURCE_DIR}/libgpopt/include"
                    "${CMAKE_SOURCE_DIR}/libnaucrates/include")
# for the generated config.h file.
include_directories(${PROJECT_BINARY_DIR}/libgpos/include/)

# Some tests use C99 long long integer constants, but the C++ dialect may not
# explicitly include C99. We supress such errors here.
check_cxx_compiler_flag("-Wno-long-long"
                        COMPILER_HAS_WNO_LONG_LONG)
if (COMPILER_HAS_WNO_LONG_LONG)
    set_source_files_properties(src/unittest/dxl/statistics/CStatisticsTest.cpp
                                src/unittest/dxl/statistics/CJoinCardinalityTest.cpp
                                src/unittest/gpopt/base/CConstraintTest.cpp
                                src/unittest/gpopt/metadata/CPartConstraintTest.cpp
                                PROPERTIES COMPILE_FLAGS "-Wno-long-long")
endif()

add_executable(gporca_test
               include/unittest/base.h
               include/unittest/dxl/statistics/CCardinalityTestUtils.h
               include/unittest/dxl/statistics/CStatisticsTest.h
               include/unittest/dxl/statistics/CJoinCardinalityTest.h
               include/unittest/gpopt/base/CConstraintTest.h
               include/unittest/gpopt/CTestUtils.inl
               include/unittest/gpopt/metadata/CPartConstraintTest.h

               src/startup/main.cpp
               include/unittest/gpopt/CTestUtils.h
               src/unittest/CTestUtils.cpp
               include/unittest/gpopt/CConstExprEvaluatorForDates.h
               src/unittest/CConstExprEvaluatorForDates.cpp
               include/unittest/gpopt/CSubqueryTestUtils.h
               src/unittest/CSubqueryTestUtils.cpp
               include/unittest/dxl/CParseHandlerManagerTest.h
               src/unittest/dxl/CParseHandlerManagerTest.cpp
               include/unittest/gpopt/mdcache/CMDProviderTest.h
               src/unittest/dxl/CMDProviderTest.cpp
               include/unittest/dxl/CDXLMemoryManagerTest.h
               src/unittest/dxl/CDXLMemoryManagerTest.cpp
               include/unittest/dxl/CXMLSerializerTest.h
               src/unittest/dxl/CXMLSerializerTest.cpp
               include/unittest/dxl/CDXLUtilsTest.h
               src/unittest/dxl/CDXLUtilsTest.cpp
               include/unittest/dxl/CParseHandlerTest.h
               src/unittest/dxl/CParseHandlerTest.cpp
               include/unittest/dxl/base/CDatumTest.h
               include/unittest/dxl/statistics/CCardinalityTestUtils.h
               src/unittest/dxl/statistics/CCardinalityTestUtils.cpp
               include/unittest/dxl/statistics/CStatisticsTest.h
               src/unittest/dxl/statistics/CStatisticsTest.cpp
               include/unittest/dxl/statistics/CJoinCardinalityTest.h
               src/unittest/dxl/statistics/CJoinCardinalityTest.cpp
               src/unittest/dxl/base/CDatumTest.cpp
               include/unittest/gpopt/translate/CTranslatorExprToDXLTest.h
               src/unittest/gpopt/translate/CTranslatorExprToDXLTest.cpp
               include/unittest/gpopt/translate/CTpchTest.h
               src/unittest/gpopt/translate/CTpchTest.cpp
               include/unittest/gpopt/translate/CTranslatorDXLToExprTest.h
               src/unittest/gpopt/translate/CTranslatorDXLToExprTest.cpp
               include/unittest/gpopt/search/CTreeMapTest.h
               src/unittest/gpopt/search/CTreeMapTest.cpp
               include/unittest/gpopt/search/COptimizationJobsTest.h
               src/unittest/gpopt/search/COptimizationJobsTest.cpp
               include/unittest/gpopt/search/CSchedulerTest.h
               src/unittest/gpopt/search/CSchedulerTest.cpp
               include/unittest/gpopt/search/CSearchStrategyTest.h
               src/unittest/gpopt/search/CSearchStrategyTest.cpp
               include/unittest/gpopt/minidump/CAggTest.h
               src/unittest/gpopt/minidump/CAggTest.cpp
               include/unittest/gpopt/minidump/CExistsSubqueryTest.h
               src/unittest/gpopt/minidump/CExistsSubqueryTest.cpp
               include/unittest/gpopt/minidump/CCollapseProjectTest.h
               src/unittest/gpopt/minidump/CCollapseProjectTest.cpp
               include/unittest/gpopt/minidump/CArrayExpansionTest.h
               src/unittest/gpopt/minidump/CArrayExpansionTest.cpp
               include/unittest/gpopt/minidump/CJoinOrderDPTest.h
               src/unittest/gpopt/minidump/CJoinOrderDPTest.cpp
               include/unittest/gpopt/minidump/CPruneColumnsTest.h
               src/unittest/gpopt/minidump/CPruneColumnsTest.cpp
               include/unittest/gpopt/minidump/CMissingStatsTest.h
               src/unittest/gpopt/minidump/CMissingStatsTest.cpp
               include/unittest/gpopt/minidump/CBitmapTest.h
               src/unittest/gpopt/minidump/CBitmapTest.cpp
               include/unittest/gpopt/minidump/CPartTblTest.h
               src/unittest/gpopt/minidump/CPartTblTest.cpp
               include/unittest/gpopt/minidump/CDMLTest.h
               src/unittest/gpopt/minidump/CDMLTest.cpp
               include/unittest/gpopt/minidump/CMinidumpWithConstExprEvaluatorTest.h
               src/unittest/gpopt/minidump/CMinidumpWithConstExprEvaluatorTest.cpp
               include/unittest/gpopt/minidump/CMiniDumperDXLTest.h
               src/unittest/gpopt/minidump/CMiniDumperDXLTest.cpp
               include/unittest/gpopt/minidump/CMultilevelPartitionTest.h
               src/unittest/gpopt/minidump/CMultilevelPartitionTest.cpp
               include/unittest/gpopt/minidump/CDirectDispatchTest.h
               src/unittest/gpopt/minidump/CDirectDispatchTest.cpp
               include/unittest/gpopt/minidump/CCTETest.h
               src/unittest/gpopt/minidump/CCTETest.cpp
               include/unittest/gpopt/minidump/CExternalTableTest.h
               src/unittest/gpopt/minidump/CExternalTableTest.cpp
               include/unittest/gpopt/minidump/CEscapeMechanismTest.h
               src/unittest/gpopt/minidump/CEscapeMechanismTest.cpp
               include/unittest/gpopt/minidump/CWindowTest.h
               src/unittest/gpopt/minidump/CWindowTest.cpp
               include/unittest/gpopt/minidump/CICGTest.h
               src/unittest/gpopt/minidump/CICGTest.cpp
               include/unittest/gpopt/minidump/CSetopTest.h
               src/unittest/gpopt/minidump/CSetopTest.cpp
               include/unittest/gpopt/minidump/CIndexTest.h
               src/unittest/gpopt/minidump/CIndexTest.cpp
               include/unittest/gpopt/minidump/CPullUpProjectElementTest.h
               src/unittest/gpopt/minidump/CPullUpProjectElementTest.cpp
               include/unittest/gpopt/minidump/CTpcdsTest.h
               src/unittest/gpopt/minidump/CTpcdsTest.cpp
               include/unittest/gpopt/minidump/CTVFTest.h
               src/unittest/gpopt/minidump/CTVFTest.cpp
               include/unittest/gpopt/xforms/CDecorrelatorTest.h
               src/unittest/gpopt/xforms/CDecorrelatorTest.cpp
               include/unittest/gpopt/xforms/CSubqueryHandlerTest.h
               src/unittest/gpopt/xforms/CSubqueryHandlerTest.cpp
               include/unittest/gpopt/xforms/CXformTest.h
               src/unittest/gpopt/xforms/CXformTest.cpp
               include/unittest/gpopt/xforms/CXformFactoryTest.h
               src/unittest/gpopt/xforms/CXformFactoryTest.cpp
               include/unittest/gpopt/xforms/CJoinOrderTest.h
               src/unittest/gpopt/xforms/CJoinOrderTest.cpp
               include/unittest/gpopt/engine/CEngineTest.h
               src/unittest/gpopt/engine/CEngineTest.cpp
               include/unittest/gpopt/engine/CEnumeratorTest.h
               src/unittest/gpopt/engine/CEnumeratorTest.cpp
               include/unittest/gpopt/operators/CExpressionTest.h
               src/unittest/gpopt/operators/CExpressionTest.cpp
               include/unittest/gpopt/operators/CContradictionTest.h
               src/unittest/gpopt/operators/CContradictionTest.cpp
               include/unittest/gpopt/operators/CExpressionPreprocessorTest.h
               src/unittest/gpopt/operators/CExpressionPreprocessorTest.cpp
               include/unittest/gpopt/operators/CPredicateUtilsTest.h
               src/unittest/gpopt/operators/CPredicateUtilsTest.cpp
               include/unittest/gpopt/mdcache/CMDAccessorTest.h
               src/unittest/gpopt/mdcache/CMDAccessorTest.cpp
               include/unittest/gpopt/eval/CConstExprEvaluatorDXLTest.h
               src/unittest/gpopt/eval/CConstExprEvaluatorDXLTest.cpp
               include/unittest/gpopt/eval/CConstExprEvaluatorDefaultTest.h
               src/unittest/gpopt/eval/CConstExprEvaluatorDefaultTest.cpp
               include/unittest/gpopt/base/CConstraintTest.h
               src/unittest/gpopt/base/CConstraintTest.cpp
               include/unittest/gpopt/base/CKeyCollectionTest.h
               src/unittest/gpopt/base/CKeyCollectionTest.cpp
               include/unittest/gpopt/base/CColumnFactoryTest.h
               src/unittest/gpopt/base/CColumnFactoryTest.cpp
               include/unittest/gpopt/base/CStateMachineTest.h
               src/unittest/gpopt/base/CStateMachineTest.cpp
               include/unittest/gpopt/base/CColRefSetIterTest.h
               src/unittest/gpopt/base/CColRefSetIterTest.cpp
               include/unittest/gpopt/base/CColRefSetTest.h
               src/unittest/gpopt/base/CColRefSetTest.cpp
               include/unittest/gpopt/base/CEquivalenceClassesTest.h
               src/unittest/gpopt/base/CEquivalenceClassesTest.cpp
               include/unittest/gpopt/base/CFunctionalDependencyTest.h
               src/unittest/gpopt/base/CFunctionalDependencyTest.cpp
               include/unittest/gpopt/base/CMaxCardTest.h
               src/unittest/gpopt/base/CMaxCardTest.cpp
               include/unittest/gpopt/base/CRangeTest.h
               src/unittest/gpopt/base/CRangeTest.cpp
               include/unittest/gpopt/base/CDistributionSpecTest.h
               src/unittest/gpopt/base/CDistributionSpecTest.cpp
               include/unittest/gpopt/base/COrderSpecTest.h
               src/unittest/gpopt/base/COrderSpecTest.cpp
               include/unittest/gpopt/csq/CCorrelatedExecutionTest.h
               src/unittest/gpopt/csq/CCorrelatedExecutionTest.cpp
               include/unittest/gpopt/cost/CCostTest.h
               src/unittest/gpopt/cost/CCostTest.cpp
               include/unittest/gpopt/metadata/CPartConstraintTest.h
               src/unittest/gpopt/metadata/CPartConstraintTest.cpp
               include/unittest/gpopt/metadata/CTableDescriptorTest.h
               src/unittest/gpopt/metadata/CTableDescriptorTest.cpp
               include/unittest/gpopt/metadata/CColumnDescriptorTest.h
               src/unittest/gpopt/metadata/CColumnDescriptorTest.cpp
               include/unittest/gpopt/metadata/CIndexDescriptorTest.h
               src/unittest/gpopt/metadata/CIndexDescriptorTest.cpp
               include/unittest/gpopt/metadata/CNameTest.h
               src/unittest/gpopt/metadata/CNameTest.cpp
               include/unittest/gpopt/minidump/CPhysicalParallelUnionAllTest.h
               src/unittest/gpopt/minidump/CPhysicalParallelUnionAllTest.cpp
        )
target_link_libraries(gporca_test
                      gpdbcost
                      gpopt
                      naucrates
                      gpos)

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
add_orca_test(CArrayExpansionTest)
add_orca_test(CJoinOrderDPTest)
add_orca_test(CMiniDumperDXLTest)
add_orca_test(CExpressionPreprocessorTest)
add_orca_test(CWindowTest)
add_orca_test(CICGTest)
add_orca_test(CMultilevelPartitionTest)
add_orca_test(CSetopTest)
add_orca_test(CDMLTest)
add_orca_test(CDirectDispatchTest)
add_orca_test(CTVFTest)
add_orca_test(CPullUpProjectElementTest)
add_orca_test(CAggTest)
add_orca_test(CExistsSubqueryTest)
add_orca_test(CCollapseProjectTest)
add_orca_test(CPruneColumnsTest)
add_orca_test(CMissingStatsTest)
add_orca_test(CIndexTest)
add_orca_test(CPartTblTest)
add_orca_test(CBitmapTest)
add_orca_test(CCTETest)
add_orca_test(CEscapeMechanismTest)
add_orca_test(CPhysicalParallelUnionAllTest)
add_orca_test(CMinidumpWithConstExprEvaluatorTest)
add_orca_test(CParseHandlerManagerTest)
add_orca_test(CParseHandlerTest)
add_orca_test(CStatisticsTest)
add_orca_test(CJoinCardinalityTest)
add_orca_test(CTranslatorDXLToExprTest)
add_orca_test(CTranslatorExprToDXLTest)
add_orca_test(CXMLSerializerTest)

# Opt tests.
add_orca_test(CColumnDescriptorTest)
add_orca_test(CColumnFactoryTest)
add_orca_test(CColRefSetIterTest)
add_orca_test(CColRefSetTest)
add_orca_test(CConstraintTest)
add_orca_test(CContradictionTest)
add_orca_test(CCorrelatedExecutionTest)
add_orca_test(CDecorrelatorTest)
add_orca_test(CDistributionSpecTest)

if (GPOS_ARCH_BITS EQUAL 64)
  add_orca_test(CSubqueryHandlerTest)
endif()

add_orca_test(CEngineTest)
add_orca_test(CEquivalenceClassesTest)
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
