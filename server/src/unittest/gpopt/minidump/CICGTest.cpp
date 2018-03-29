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
		"../data/dxl/minidump/MinCardinalityNaryJoin.mdp",
		"../data/dxl/minidump/DisableLargeTableBroadcast.mdp",
		"../data/dxl/minidump/InsertIntoNonNullAfterDroppingColumn.mdp",
		"../data/dxl/minidump/OptimizerConfigWithSegmentsForCosting.mdp",
		"../data/dxl/minidump/QueryMismatchedDistribution.mdp",
		"../data/dxl/minidump/QueryMismatchedDistribution-DynamicIndexScan.mdp",

#ifndef GPOS_DEBUG
		// TODO:  - Jul 14 2015; disabling it for debug build to reduce testing time
		// "../data/dxl/minidump/HAWQ-TPCH09-NoTopBroadcast.mdp",
		"../data/dxl/minidump/HAWQ-TPCH-Stat-Derivation.mdp",
		"../data/dxl/minidump/HJN-Redistribute-One-Side.mdp",
		"../data/dxl/minidump/TPCH-Q5.mdp",
		"../data/dxl/minidump/TPCDS-39-InnerJoin-JoinEstimate.mdp",
		"../data/dxl/minidump/TPCH-Partitioned-256GB.mdp",
		"../data/dxl/minidump/Tpcds-NonPart-Q70a.mdp",
		"../data/dxl/minidump/Tpcds-10TB-Q37-NoIndexJoin.mdp",

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
						CHint::PhintDefault(pmp),
						CWindowOids::Pwindowoids(pmp)
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
	ICostModelParams::SCostParam *pcpUnlimited =
		GPOS_NEW(pmp) ICostModelParams::SCostParam(
			CCostModelParamsGPDB::EcpIndexJoinAllowedRiskThreshold,
			gpos::ulong_max,  // dVal
			0,				  // dLowerBound
			gpos::ulong_max   // dUpperBound
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
