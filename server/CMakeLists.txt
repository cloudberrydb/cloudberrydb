# Copyright (c) 2015, Pivotal Software, Inc. All Rights Reserved.

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/include"
                    "${PROJECT_SOURCE_DIR}/libgpdbcost/include"
                    "${PROJECT_SOURCE_DIR}/libgpopt/include"
                    "${PROJECT_SOURCE_DIR}/libnaucrates/include")
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

file(GLOB_RECURSE hdrs ${CMAKE_CURRENT_SOURCE_DIR}/include/*.h
                       ${CMAKE_CURRENT_SOURCE_DIR}/include/*.inl)
file(GLOB_RECURSE srcs ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)

# Add headers to make them visible in some IDEs (Clion, VS, Xcode)
list(APPEND srcs ${hdrs})

add_executable(gporca_test ${srcs})

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
add_orca_test(CSubqueryTest)
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
add_orca_test(CCastTest)
add_orca_test(CConstTblGetTest)
add_orca_test(COuterJoinTest)
add_orca_test(CPredicateTest)
add_orca_test(CSemiJoinTest)

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
