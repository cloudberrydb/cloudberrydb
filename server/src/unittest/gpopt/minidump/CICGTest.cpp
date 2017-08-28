//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CICGTest.cpp
//
//	@doc:
//		Test for installcheck-good bugs
//---------------------------------------------------------------------------

#include "gpos/task/CAutoTraceFlag.h"

#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/exception.h"
#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/engine/CCTEConfig.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/minidump/CMinidumperUtils.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLPhysicalNLJoin.h"
#include "naucrates/exception.h"

#include "unittest/base.h"
#include "unittest/gpopt/CConstExprEvaluatorForDates.h"
#include "unittest/gpopt/CTestUtils.h"
#include "unittest/gpopt/minidump/CICGTest.h"

using namespace gpdxl;

ULONG CICGTest::m_ulTestCounter = 0; // start from first test
ULONG CICGTest::m_ulUnsupportedTestCounter = 0; // start from first unsupported test
ULONG CICGTest::m_ulTestCounterPreferHashJoinToIndexJoin = 0;
ULONG CICGTest::m_ulTestCounterPreferIndexJoinToHashJoin = 0;
ULONG CICGTest::m_ulNegativeIndexApplyTestCounter = 0;

// minidump files
const CHAR *rgszFileNames[] =
	{
		"../data/dxl/minidump/ArrayCoerceExpr.mdp",
		"../data/dxl/minidump/DisableLargeTableBroadcast.mdp",
		"../data/dxl/minidump/InferPredicatesForLimit.mdp",
		"../data/dxl/minidump/OR-WithIsNullPred.mdp",
		"../data/dxl/minidump/ScSubqueryWithOuterRef.mdp",
		"../data/dxl/minidump/ExprOnScSubqueryWithOuterRef.mdp",
		"../data/dxl/minidump/InsertIntoNonNullAfterDroppingColumn.mdp",
		"../data/dxl/minidump/IN-Numeric.mdp",
		"../data/dxl/minidump/CollapseNot.mdp",
		"../data/dxl/minidump/OrderByNullsFirst.mdp",
		"../data/dxl/minidump/ConvertHashToRandomSelect.mdp",
		"../data/dxl/minidump/ConvertHashToRandomInsert.mdp",
		"../data/dxl/minidump/OptimizerConfigWithSegmentsForCosting.mdp",
		"../data/dxl/minidump/HJN-DeeperOuter.mdp",
		"../data/dxl/minidump/CTAS.mdp",
		"../data/dxl/minidump/CTAS-Random.mdp",
		"../data/dxl/minidump/Int2Predicate.mdp",
		"../data/dxl/minidump/CheckAsUser.mdp",
		"../data/dxl/minidump/ArrayConcat.mdp",
		"../data/dxl/minidump/ArrayRef.mdp",
		"../data/dxl/minidump/FoldedArrayCmp.mdp",
		"../data/dxl/minidump/ProjectRepeatedColumn1.mdp",
		"../data/dxl/minidump/ProjectRepeatedColumn2.mdp",
		"../data/dxl/minidump/Coalesce-With-Subquery.mdp",
		"../data/dxl/minidump/NullIf-With-Subquery.mdp",
		"../data/dxl/minidump/Switch-With-Subquery.mdp",
		"../data/dxl/minidump/Join-Disj-Subqs.mdp",
		"../data/dxl/minidump/Stat-Derivation-Leaf-Pattern.mdp",
		"../data/dxl/minidump/MultiLevelDecorrelationWithSemiJoins.mdp",
		"../data/dxl/minidump/QueryMismatchedDistribution.mdp",
		"../data/dxl/minidump/QueryMismatchedDistribution-DynamicIndexScan.mdp",
		"../data/dxl/minidump/UnnestSQJoins.mdp",
		"../data/dxl/minidump/IN-ArrayCmp.mdp",
		"../data/dxl/minidump/NOT-IN-ArrayCmp.mdp",
		"../data/dxl/minidump/NOT-IN-NotNullBoth.mdp",
		"../data/dxl/minidump/NOT-IN-NullInner.mdp",
		"../data/dxl/minidump/NOT-IN-NullOuter.mdp",
		"../data/dxl/minidump/Correlated-LASJ-With-Outer-Const.mdp",
		"../data/dxl/minidump/Correlated-LASJ-With-Outer-Col.mdp",
		"../data/dxl/minidump/Correlated-LASJ-With-Outer-Expr.mdp",
		"../data/dxl/minidump/Correlated-SemiJoin.mdp",
		"../data/dxl/minidump/CorrelatedSemiJoin-True.mdp",
		"../data/dxl/minidump/CorrelatedIN-LeftSemiJoin-True.mdp",
		"../data/dxl/minidump/CorrelatedIN-LeftSemiNotIn-True.mdp",
		"../data/dxl/minidump/Correlated-AntiSemiJoin.mdp",
		"../data/dxl/minidump/CorrelatedAntiSemiJoin-True.mdp",
		"../data/dxl/minidump/Correlation-With-Casting-1.mdp",
		"../data/dxl/minidump/Correlation-With-Casting-2.mdp",
		"../data/dxl/minidump/NLJ-BC-Outer-Spool-Inner.mdp",
		"../data/dxl/minidump/Self-Comparison.mdp",
		"../data/dxl/minidump/Self-Comparison-Nullable.mdp",
		"../data/dxl/minidump/SelectCheckConstraint.mdp",
		"../data/dxl/minidump/FullOuterJoin.mdp",
		"../data/dxl/minidump/FullOuterJoin2.mdp",
		"../data/dxl/minidump/AddEqualityPredicates.mdp",
		"../data/dxl/minidump/AddPredsInSubqueries.mdp",
		"../data/dxl/minidump/DeduplicatePredicates.mdp",
		"../data/dxl/minidump/LeftJoin-With-Pred-On-Inner.mdp",
		"../data/dxl/minidump/LeftJoin-With-Pred-On-Inner2.mdp",
		"../data/dxl/minidump/LeftJoin-With-Col-Const-Pred.mdp",
		"../data/dxl/minidump/LeftJoin-With-Coalesce.mdp",
		"../data/dxl/minidump/LOJWithFalsePred.mdp",
		"../data/dxl/minidump/LOJ-IsNullPred.mdp",
		"../data/dxl/minidump/Select-Proj-OuterJoin.mdp",
		"../data/dxl/minidump/ExpandJoinOrder.mdp",
		"../data/dxl/minidump/EquivClassesAndOr.mdp",
		"../data/dxl/minidump/EquivClassesLimit.mdp",
		"../data/dxl/minidump/Date-TimeStamp-HashJoin.mdp",
		"../data/dxl/minidump/TimeStamp-Date-HashJoin.mdp",
		"../data/dxl/minidump/MultiLevel-CorrelatedExec.mdp",
		"../data/dxl/minidump/OneLevel-CorrelatedExec.mdp",
		"../data/dxl/minidump/MultiLevel-IN-Subquery.mdp",
		"../data/dxl/minidump/Join-With-Subq-Preds-1.mdp",

#ifndef GPOS_32BIT
		// TODO:  - 05/14/2014: the plan generates a different plan only for
		// 32-bit rhel
		"../data/dxl/minidump/Join-With-Subq-Preds-2.mdp",
#endif // GPOS_32BIT

		"../data/dxl/minidump/Stats-For-Select-With-Outer-Refs.mdp",
		"../data/dxl/minidump/Factorized-Preds.mdp",
		"../data/dxl/minidump/Non-Hashjoinable-Pred.mdp",
		"../data/dxl/minidump/Non-Hashjoinable-Pred-2.mdp",
		"../data/dxl/minidump/Equiv-HashedDistr-1.mdp",
		"../data/dxl/minidump/Equiv-HashedDistr-2.mdp",
		"../data/dxl/minidump/SelectOnBpchar.mdp",
		"../data/dxl/minidump/SelectOnCastedCol.mdp",
		"../data/dxl/minidump/ArrayCmpAll.mdp",
		"../data/dxl/minidump/SemiJoin2InnerJoin.mdp",
		"../data/dxl/minidump/SemiJoin2Select-1.mdp",
		"../data/dxl/minidump/SemiJoin2Select-2.mdp",
		"../data/dxl/minidump/AntiSemiJoin2Select-1.mdp",
		"../data/dxl/minidump/AntiSemiJoin2Select-2.mdp",
		"../data/dxl/minidump/Subq-On-OuterRef.mdp",
		"../data/dxl/minidump/Subq-With-OuterRefCol.mdp",
		"../data/dxl/minidump/ConstTblGetUnderSubqWithOuterRef.mdp",
		"../data/dxl/minidump/ConstTblGetUnderSubqWithNoOuterRef.mdp",
		"../data/dxl/minidump/ConstTblGetUnderSubqUnderProjectNoOuterRef.mdp",
		"../data/dxl/minidump/ConstTblGetUnderSubqUnderProjectWithOuterRef.mdp",

		"../data/dxl/minidump/Subq-NoParams.mdp",
		"../data/dxl/minidump/Subq-JoinWithOuterRef.mdp",
		"../data/dxl/minidump/SubqAll-To-ScalarSubq.mdp",
		"../data/dxl/minidump/SubqAll-Limit1.mdp",
		"../data/dxl/minidump/ProjectUnderSubq.mdp",
		"../data/dxl/minidump/InSubqWithPrjListOuterRefNoInnerRefColumnIn.mdp",
		"../data/dxl/minidump/InSubqWithPrjListOuterRefNoInnerRefConstIn.mdp",
		"../data/dxl/minidump/InSubqWithPrjListOuterRefNoInnerRefColumnPlusConstIn.mdp",
		"../data/dxl/minidump/NestedInSubqWithPrjListOuterRefNoInnerRef.mdp",
#ifndef GPOS_DEBUG
		"../data/dxl/minidump/TPCDS-39-InnerJoin-JoinEstimate.mdp",
		"../data/dxl/minidump/TPCH-Partitioned-256GB.mdp",
		"../data/dxl/minidump/Tpcds-NonPart-Q70a.mdp",
#endif
		"../data/dxl/minidump/Sequence-With-Universal-Outer.mdp",

		"../data/dxl/minidump/IN.mdp",
		"../data/dxl/minidump/OR.mdp",
		"../data/dxl/minidump/UnsupportedStatsPredicate.mdp",
		"../data/dxl/minidump/LIKE-Pattern-green.mdp", // p_name like '%%green%%'
		"../data/dxl/minidump/LIKE-Pattern-green-2.mdp", //  '%%green%%' like p_name
		"../data/dxl/minidump/LIKE-Pattern-Empty.mdp", // like '%%' --> should not damp the cardinality of the input
		"../data/dxl/minidump/AssertMaxOneRow.mdp",
		"../data/dxl/minidump/AssertOneRowWithCorrelation.mdp",
		"../data/dxl/minidump/ProjectWithConstant.mdp",
		"../data/dxl/minidump/ProjectWithTextConstant.mdp",
		"../data/dxl/minidump/Join_OuterChild_DistUniversal.mdp",
		"../data/dxl/minidump/Nested-Or-Predicates.mdp",
		"../data/dxl/minidump/OrderByOuterRef.mdp",
		"../data/dxl/minidump/CTG-Filter.mdp",
		"../data/dxl/minidump/CTG-Join.mdp",
		"../data/dxl/minidump/UDA-AnyElement-1.mdp",
		"../data/dxl/minidump/UDA-AnyElement-2.mdp",
		"../data/dxl/minidump/UDA-AnyArray.mdp",
		"../data/dxl/minidump/JOIN-Pred-Cast-Int4.mdp",
		"../data/dxl/minidump/JOIN-Pred-Cast-Varchar.mdp",
		"../data/dxl/minidump/JOIN-int4-Eq-double.mdp",
		"../data/dxl/minidump/JOIN-cast2text-int4-Eq-cast2text-double.mdp",
		"../data/dxl/minidump/JOIN-int4-Eq-int2.mdp",

		"../data/dxl/minidump/CastOnSubquery.mdp",
		"../data/dxl/minidump/InnerJoin-With-OuterRefs.mdp",
		"../data/dxl/minidump/OuterJoin-With-OuterRefs.mdp",
#ifndef GPOS_DEBUG
		// TODO:  - Jul 14 2015; disabling it for debug build to reduce testing time
		"../data/dxl/minidump/TPCH-Q5.mdp",
#endif // GPOS_DEBUG
		"../data/dxl/minidump/JoinColWithOnlyNDV.mdp",
		"../data/dxl/minidump/InEqualityJoin.mdp",
		"../data/dxl/minidump/HashJoinOnRelabeledColumns.mdp",
		"../data/dxl/minidump/UseDistributionSatisfactionForUniversalInnerChild.mdp",
#ifndef GPOS_DEBUG
		// TODO:  - Jul 14 2015; disabling it for debug build to reduce testing time
		// "../data/dxl/minidump/HAWQ-TPCH09-NoTopBroadcast.mdp",
		"../data/dxl/minidump/HAWQ-TPCH-Stat-Derivation.mdp",
		"../data/dxl/minidump/HJN-Redistribute-One-Side.mdp",
#endif // GPOS_DEBUG
		"../data/dxl/minidump/LikePredStatsNotComparable.mdp",
		"../data/dxl/minidump/PredStatsNotComparable.mdp",
		"../data/dxl/minidump/ExtractPredicateFromDisj.mdp",
		"../data/dxl/minidump/ExtractPredicateFromDisjWithComputedColumns.mdp",
		"../data/dxl/minidump/IDF-NotNullConstant.mdp",
		"../data/dxl/minidump/IDF-NullConstant.mdp",
		"../data/dxl/minidump/INDF-NotNullConstant.mdp",
		"../data/dxl/minidump/INDF-NullConstant.mdp",
		"../data/dxl/minidump/NullConstant-INDF-Col.mdp",
		"../data/dxl/minidump/EqualityJoin.mdp",
		"../data/dxl/minidump/EffectsOfJoinFilter.mdp",
		"../data/dxl/minidump/Join-IDF.mdp",
		"../data/dxl/minidump/CoerceToDomain.mdp",
		"../data/dxl/minidump/CoerceViaIO.mdp",
		"../data/dxl/minidump/LeftOuter2InnerUnionAllAntiSemiJoin.mdp",
#ifndef GPOS_DEBUG
		// TODO:  - Jul 14 2015; disabling it for debug build to reduce testing time
		"../data/dxl/minidump/LeftOuter2InnerUnionAllAntiSemiJoin-Tpcds.mdp",
#endif
		"../data/dxl/minidump/EffectOfLocalPredOnJoin.mdp",
		"../data/dxl/minidump/EffectOfLocalPredOnJoin2.mdp",
		"../data/dxl/minidump/EffectOfLocalPredOnJoin3.mdp",
		"../data/dxl/minidump/ProjectSetFunction.mdp",
		"../data/dxl/minidump/MultipleSubqueriesInSelectClause.mdp",
		"../data/dxl/minidump/SemiJoin2Select-EnforceSubplan.mdp",
		"../data/dxl/minidump/EstimateJoinRowsForCastPredicates.mdp",
		"../data/dxl/minidump/MissingBoolColStats.mdp",
		"../data/dxl/minidump/ArrayCoerceCast.mdp",
		"../data/dxl/minidump/SimpleArrayCoerceCast.mdp",
#ifndef GPOS_DEBUG
		// TODO:  - 06/29/2015: the row estimate for 32-bit rhel is off in the 6th decimel place
		"../data/dxl/minidump/retail_28.mdp",
		"../data/dxl/minidump/JoinNDVRemain.mdp",
		"../data/dxl/minidump/Least-Greatest.mdp",
#endif
	};

struct UnSupportedTestCase
{
	// file name of minidump
	const CHAR *szFilename;

	// expected exception major
	ULONG ulMajor;

	// expected exception minor
	ULONG ulMinor;
};

// unsupported minidump files
const struct UnSupportedTestCase unSupportedTestCases[] =
	{
		{"../data/dxl/minidump/OneSegmentGather.mdp", gpdxl::ExmaDXL, gpdxl::ExmiExpr2DXLUnsupportedFeature},
		{"../data/dxl/minidump/CTEWithOuterReferences.mdp", gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedOp},
		{"../data/dxl/minidump/BitmapIndexUnsupportedOperator.mdp", gpopt::ExmaGPOPT, gpopt::ExmiNoPlanFound},
		{"../data/dxl/minidump/CTEMisAlignedProducerConsumer.mdp",gpopt::ExmaGPOPT, gpopt::ExmiCTEProducerConsumerMisAligned}
	};

// negative index apply tests
const CHAR *rgszNegativeIndexApplyFileNames[] =
	{
		"../data/dxl/minidump/Negative-IndexApply1.mdp",
		"../data/dxl/minidump/Negative-IndexApply2.mdp",
	};

// index join penalization tests
const CHAR *rgszPreferHashJoinVersusIndexJoin[] =
		{
				"../data/dxl/indexjoin/positive_04.mdp"
		};


//---------------------------------------------------------------------------
//	@function:
//		CICGTest::EresUnittest
//
//	@doc:
//		Unittest for expressions
//
//---------------------------------------------------------------------------
GPOS_RESULT
CICGTest::EresUnittest()
{

#ifdef GPOS_DEBUG
	// disable extended asserts before running test
	fEnableExtendedAsserts = false;
#endif // GPOS_DEBUG

	CUnittest rgut[] =
		{
		// keep test for testing partially supported operators/xforms
		GPOS_UNITTEST_FUNC(CICGTest::EresUnittest_RunUnsupportedMinidumpTests),
		GPOS_UNITTEST_FUNC(CICGTest::EresUnittest_NegativeIndexApplyTests),

		GPOS_UNITTEST_FUNC(CICGTest::EresUnittest_RunMinidumpTests),

#ifndef GPOS_DEBUG
		// This test is slow in debug build because it has to free a lot of memory structures
		GPOS_UNITTEST_FUNC(EresUnittest_PreferHashJoinVersusIndexJoinWhenRiskIsHigh)
#endif  // GPOS_DEBUG
		};

	GPOS_RESULT eres = CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));

#ifdef GPOS_DEBUG
	// enable extended asserts after running test
	fEnableExtendedAsserts = true;
#endif // GPOS_DEBUG

	// reset metadata cache
	CMDCache::Reset();

	return eres;
}


//---------------------------------------------------------------------------
//	@function:
//		CICGTest::EresUnittest_RunMinidumpTests
//
//	@doc:
//		Run all Minidump-based tests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CICGTest::EresUnittest_RunMinidumpTests()
{
	return CTestUtils::EresUnittest_RunTests(rgszFileNames, &m_ulTestCounter, GPOS_ARRAY_SIZE(rgszFileNames));
}


//---------------------------------------------------------------------------
//	@function:
//		CICGTest::EresUnittest_RunUnsupportedMinidumpTests
//
//	@doc:
//		Run all unsupported Minidump-based tests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CICGTest::EresUnittest_RunUnsupportedMinidumpTests()
{

	// enable (Redistribute, Broadcast) hash join plans
	CAutoTraceFlag atf1(EopttraceEnableRedistributeBroadcastHashJoin, true /*fVal*/);

	CAutoTraceFlag atf2(EopttraceDisableXformBase + CXform::ExfDynamicGet2DynamicTableScan, true);
	
	CAutoMemoryPool amp(CAutoMemoryPool::ElcNone);
	IMemoryPool *pmp = amp.Pmp();
	
	GPOS_RESULT eres = GPOS_OK;
	const ULONG ulTests = GPOS_ARRAY_SIZE(unSupportedTestCases);
	for (ULONG ul = m_ulUnsupportedTestCounter; ul < ulTests; ul++)
	{
		const CHAR *szFilename = unSupportedTestCases[ul].szFilename;
		CDXLMinidump *pdxlmd = CMinidumperUtils::PdxlmdLoad(pmp, szFilename);
		bool unmatchedException = false;
		ULONG unmatchedExceptionMajor = 0;
		ULONG unmatchedExceptionMinor = 0;

		GPOS_TRY
		{
			ICostModel *pcm = CTestUtils::Pcm(pmp);

			COptimizerConfig *poconf = pdxlmd->Poconf();
			CDXLNode *pdxlnPlan = CMinidumperUtils::PdxlnExecuteMinidump
									(
									pmp, 
									szFilename,
									poconf->Pcm()->UlHosts() /*ulSegments*/,
									1 /*ulSessionId*/, 
									1, /*ulCmdId*/
									poconf,
									NULL /*pceeval*/
									);


			GPOS_CHECK_ABORT;
			pdxlnPlan->Release();
			pcm->Release();

			// test should have thrown
			eres = GPOS_FAILED;
			break;
		}
		GPOS_CATCH_EX(ex)
		{
			unmatchedExceptionMajor = ex.UlMajor();
			unmatchedExceptionMinor = ex.UlMinor();

			// verify expected exception
			if (unSupportedTestCases[ul].ulMajor == unmatchedExceptionMajor
					&& unSupportedTestCases[ul].ulMinor == unmatchedExceptionMinor)
			{
				eres = GPOS_OK;
			}
			else
			{
				unmatchedException = true;
				eres = GPOS_FAILED;
			}
			GPOS_RESET_EX;
		}
		GPOS_CATCH_END;

		GPOS_DELETE(pdxlmd);
		m_ulUnsupportedTestCounter++;

		if (GPOS_FAILED == eres && unmatchedException)
		{
			CAutoTrace at(pmp);
			at.Os() << "Test failed due to unmatched exceptions." << std::endl;
			at.Os() << " Expected result: " << unSupportedTestCases[ul].ulMajor << "." << unSupportedTestCases[ul].ulMinor << std::endl;
			at.Os() << " Actual result: " << unmatchedExceptionMajor << "." << unmatchedExceptionMinor << std::endl;
		}
	}
	
	if (GPOS_OK == eres)
	{
		m_ulUnsupportedTestCounter = 0;
	}

	return eres;
}


//---------------------------------------------------------------------------
//	@function:
//		CICGTest::EresUnittest_NegativeIndexApplyTests
//
//	@doc:
//		Negative IndexApply tests;
//		optimizer should not be able to generate a plan
//
//---------------------------------------------------------------------------
GPOS_RESULT
CICGTest::EresUnittest_NegativeIndexApplyTests()
{
	// enable (Redistribute, Broadcast) hash join plans
	CAutoTraceFlag atf(EopttraceEnableRedistributeBroadcastHashJoin, true /*fVal*/);

	// disable physical scans and NLJ to force using index-apply
	CAutoTraceFlag atfDTS(EopttraceDisableXformBase + CXform::ExfDynamicGet2DynamicTableScan, true);
	CAutoTraceFlag atfTS(EopttraceDisableXformBase + CXform::ExfGet2TableScan, true);
	CAutoTraceFlag atfNLJ(EopttraceDisableXformBase + CXform::ExfInnerJoin2NLJoin, true);

	CAutoMemoryPool amp(CAutoMemoryPool::ElcNone);
	IMemoryPool *pmp = amp.Pmp();

	GPOS_RESULT eres = GPOS_OK;
	const ULONG ulTests = GPOS_ARRAY_SIZE(rgszNegativeIndexApplyFileNames);
	for (ULONG ul = m_ulNegativeIndexApplyTestCounter; ul < ulTests; ul++)
	{
		GPOS_TRY
		{
			ICostModel *pcm = CTestUtils::Pcm(pmp);

			COptimizerConfig *poconf = GPOS_NEW(pmp) COptimizerConfig
						(
						CEnumeratorConfig::Pec(pmp, 0 /*ullPlanId*/),
						CStatisticsConfig::PstatsconfDefault(pmp),
						CCTEConfig::PcteconfDefault(pmp),
						pcm,
						CHint::PhintDefault(pmp)
						);
			CDXLNode *pdxlnPlan = CMinidumperUtils::PdxlnExecuteMinidump
									(
									pmp,
									rgszNegativeIndexApplyFileNames[ul],
									GPOPT_TEST_SEGMENTS /*ulSegments*/,
									1 /*ulSessionId*/,
									1, /*ulCmdId*/
									poconf,
									NULL /*pceeval*/
									);
			GPOS_CHECK_ABORT;
			poconf->Release();
			pdxlnPlan->Release();
			pcm->Release();

			// test should have thrown
			eres = GPOS_FAILED;
			break;
		}
		GPOS_CATCH_EX(ex)
		{
			if (GPOS_MATCH_EX(ex, gpopt::ExmaGPOPT, gpopt::ExmiNoPlanFound))
			{
				GPOS_RESET_EX;
			}
			else
			{
				GPOS_RETHROW(ex);
			}
		}
		GPOS_CATCH_END;
		m_ulNegativeIndexApplyTestCounter++;
	}

	if (GPOS_OK == eres)
	{
		m_ulNegativeIndexApplyTestCounter = 0;
	}

	return eres;
}

//---------------------------------------------------------------------------
//	@function:
//		CICGTest::FDXLSatisfiesPredicate
//
//	@doc:
//		Check if all the operators in the given dxl fragment satisfy the given predicate
//
//---------------------------------------------------------------------------
BOOL
CICGTest::FDXLOpSatisfiesPredicate
	(
	CDXLNode *pdxl,
	FnDXLOpPredicate fdop
	)
{
	using namespace gpdxl;

	CDXLOperator *pdxlop = pdxl->Pdxlop();
	if (!fdop(pdxlop))
	{
		return false;
	}

	for (ULONG ul = 0; ul < pdxl->UlArity(); ul++)
	{
		if (!FDXLOpSatisfiesPredicate((*pdxl)[ul], fdop))
		{
			return false;
		}
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CICGTest::FIsNotIndexJoin
//
//	@doc:
//		Check if the given dxl operator is an Index Join
//
//---------------------------------------------------------------------------
BOOL
CICGTest::FIsNotIndexJoin
	(
	CDXLOperator *pdxlop
	)
{
	if (EdxlopPhysicalNLJoin == pdxlop->Edxlop())
	{
		if (CDXLPhysicalNLJoin::PdxlConvert(pdxlop)->FIndexNLJ())
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CICGTest::FHasNoIndexJoin
//
//	@doc:
//		Check if the given dxl fragment contains an Index Join
//
//---------------------------------------------------------------------------
BOOL
CICGTest::FHasNoIndexJoin
	(
	CDXLNode *pdxl
	)
{
	return FDXLOpSatisfiesPredicate(pdxl, FIsNotIndexJoin);
}

//---------------------------------------------------------------------------
//	@function:
//		CICGTest::EresUnittest_PenalizeIndexJoinVersusHashJoin
//
//	@doc:
//		Test that hash join is preferred versus index join when estimation
//		risk is high
//
//---------------------------------------------------------------------------
GPOS_RESULT
CICGTest::EresUnittest_PreferHashJoinVersusIndexJoinWhenRiskIsHigh()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// enable (Redistribute, Broadcast) hash join plans
	CAutoTraceFlag atf(EopttraceEnableRedistributeBroadcastHashJoin, true /*fVal*/);

	// When the risk threshold is infinite, we should pick index join
	DrgPcp *pdrgpcpUnlimited = GPOS_NEW(pmp) DrgPcp(pmp);
	ICostModelParams::SCostParam *pcpUnlimited = GPOS_NEW(pmp) ICostModelParams::SCostParam
								(
								CCostModelParamsGPDB::EcpIndexJoinAllowedRiskThreshold,
								ULONG_MAX,  // dVal
								0,  // dLowerBound
								ULONG_MAX  // dUpperBound
								);
	pdrgpcpUnlimited->Append(pcpUnlimited);
	GPOS_RESULT eres = CTestUtils::EresCheckOptimizedPlan
			(
			rgszPreferHashJoinVersusIndexJoin,
			GPOS_ARRAY_SIZE(rgszPreferHashJoinVersusIndexJoin),
			&m_ulTestCounterPreferIndexJoinToHashJoin,
			1, // ulSessionId
			1, // ulCmdId
			FHasIndexJoin,
			pdrgpcpUnlimited
			);
	pdrgpcpUnlimited->Release();

	if (GPOS_OK != eres)
	{
		return eres;
	}

	// When the risk threshold is zero, we should not pick index join
	DrgPcp *pdrgpcpNoIndexJoin = GPOS_NEW(pmp) DrgPcp(pmp);
	ICostModelParams::SCostParam *pcpNoIndexJoin = GPOS_NEW(pmp) ICostModelParams::SCostParam
								(
								CCostModelParamsGPDB::EcpIndexJoinAllowedRiskThreshold,
								0,  // dVal
								0,  // dLowerBound
								0  // dUpperBound
								);
	pdrgpcpNoIndexJoin->Append(pcpNoIndexJoin);
	eres = CTestUtils::EresCheckOptimizedPlan
			(
			rgszPreferHashJoinVersusIndexJoin,
			GPOS_ARRAY_SIZE(rgszPreferHashJoinVersusIndexJoin),
			&m_ulTestCounterPreferHashJoinToIndexJoin,
			1, // ulSessionId
			1, // ulCmdId
			FHasNoIndexJoin,
			pdrgpcpNoIndexJoin
			);
	pdrgpcpNoIndexJoin->Release();

	return eres;
}

// EOF
