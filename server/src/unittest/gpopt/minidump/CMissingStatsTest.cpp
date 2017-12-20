//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal, Inc.
//
//	@filename:
//		CMissingStatsTest.cpp
//
//	@doc:
//		Test for optimizing queries with aggregates
//---------------------------------------------------------------------------

#include "unittest/gpopt/minidump/CMissingStatsTest.h"
#include "gpos/base.h"
#include "gpos/error/CAutoTrace.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/test/CUnittest.h"

#include "gpopt/exception.h"
#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/engine/CCTEConfig.h"
#include "gpopt/minidump/CMinidumperUtils.h"
#include "gpopt/optimizer/COptimizerConfig.h"

#include "unittest/gpopt/CTestUtils.h"

using namespace gpopt;

ULONG CMissingStatsTest::m_ulMissingStatsTestCounter = 0;  // start from first test

//---------------------------------------------------------------------------
//	@function:
//		CMissingStatsTest::EresUnittest
//
//	@doc:
//		Unittest for expressions
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMissingStatsTest::EresUnittest()
{

#ifdef GPOS_DEBUG
	// disable extended asserts before running test
	fEnableExtendedAsserts = false;
#endif // GPOS_DEBUG

	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(EresUnittest_RunTests),
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
//		CMissingStatsTest::EresUnittest_RunTests
//
//	@doc:
//		Run all Minidump-based tests with plan matching
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMissingStatsTest::EresUnittest_RunTests()
{
	SMissingStatsTestCase rgtc[] =
		{
			{"../data/dxl/minidump/MissingStats.mdp", 2},
			{"../data/dxl/minidump/NoMissingStatsAfterDroppedCol.mdp", 0},
			{"../data/dxl/minidump/NoMissingStats.mdp", 0},
			{"../data/dxl/minidump/NoMissingStatsForEmptyTable.mdp", 0},
			{"../data/dxl/minidump/NoMissingStatsAskingForSystemColFOJ.mdp", 0},
		};

	CAutoMemoryPool amp(CAutoMemoryPool::ElcNone);
	IMemoryPool *pmp = amp.Pmp();

	GPOS_RESULT eres = GPOS_OK;
	const ULONG ulTests = GPOS_ARRAY_SIZE(rgtc);
	for (ULONG ul = m_ulMissingStatsTestCounter; ((ul < ulTests) && (GPOS_OK == eres)); ul++)
	{
		ICostModel *pcm = CTestUtils::Pcm(pmp);
		CAutoTraceFlag atf1(EopttracePrintColsWithMissingStats, true /*fVal*/);

		COptimizerConfig *poconf = GPOS_NEW(pmp) COptimizerConfig
												(
												CEnumeratorConfig::Pec(pmp, 0 /*ullPlanId*/),
												CStatisticsConfig::PstatsconfDefault(pmp),
												CCTEConfig::PcteconfDefault(pmp),
												pcm,
												CHint::PhintDefault(pmp),
												CWindowOids::Pwindowoids(pmp)
												);
		SMissingStatsTestCase testCase = rgtc[ul];

		CDXLNode *pdxlnPlan = CMinidumperUtils::PdxlnExecuteMinidump
												(
												pmp,
												testCase.m_szInputFile,
												GPOPT_TEST_SEGMENTS /*ulSegments*/,
												1 /*ulSessionId*/,
												1, /*ulCmdId*/
												poconf,
												NULL /*pceeval*/
												);

		CStatisticsConfig *pstatsconf = poconf->Pstatsconf();

		DrgPmdid *pdrgmdidCol = GPOS_NEW(pmp) DrgPmdid(pmp);
		pstatsconf->CollectMissingStatsColumns(pdrgmdidCol);
		ULONG ulMissingStats = pdrgmdidCol->UlLength();

		if (ulMissingStats != testCase.m_ulExpectedMissingStats)
		{
			// for debug traces
			CWStringDynamic str(pmp);
			COstreamString oss(&str);

			// print objects
			oss << std::endl;
			oss << "Expected Number of Missing Columns: " << testCase.m_ulExpectedMissingStats;

			oss << std::endl;
			oss << "Number of Missing Columns: " << ulMissingStats;
			oss << std::endl;

			GPOS_TRACE(str.Wsz());
			eres = GPOS_FAILED;
		}

		GPOS_CHECK_ABORT;
		poconf->Release();
		pdxlnPlan->Release();

		m_ulMissingStatsTestCounter++;
	}

	if (GPOS_OK == eres)
	{
		m_ulMissingStatsTestCounter = 0;
	}

	return eres;
}

// EOF
