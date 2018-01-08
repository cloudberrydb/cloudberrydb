//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//---------------------------------------------------------------------------

#include "unittest/gpopt/minidump/CSemiJoinTest.h"
#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/test/CUnittest.h"

#include "gpopt/exception.h"
#include "gpopt/minidump/CMinidumperUtils.h"

#include "unittest/gpopt/CTestUtils.h"


using namespace gpopt;

ULONG CSemiJoinTest::m_ulTestCounter = 0;  // start from first test

// minidump files
const CHAR *rgszSemiMdpFiles[] =
{
	"../data/dxl/minidump/SemiJoin2InnerJoin.mdp",
	"../data/dxl/minidump/SemiJoin2Select-1.mdp",
	"../data/dxl/minidump/SemiJoin2Select-2.mdp",
	"../data/dxl/minidump/AntiSemiJoin2Select-1.mdp",
	"../data/dxl/minidump/AntiSemiJoin2Select-2.mdp",
	"../data/dxl/minidump/InEqualityJoin.mdp",
	"../data/dxl/minidump/SemiJoin2Select-EnforceSubplan.mdp",
	"../data/dxl/minidump/InSubqWithPrjListOuterRefNoInnerRefColumnIn.mdp",
	"../data/dxl/minidump/InSubqWithPrjListOuterRefNoInnerRefConstIn.mdp",
	"../data/dxl/minidump/InSubqWithPrjListOuterRefNoInnerRefColumnPlusConstIn.mdp",
	"../data/dxl/minidump/NestedInSubqWithPrjListOuterRefNoInnerRef.mdp",
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
};


GPOS_RESULT
CSemiJoinTest::EresUnittest()
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

// Run all Minidump-based tests with plan matching
GPOS_RESULT
CSemiJoinTest::EresUnittest_RunTests()
{
	return CTestUtils::EresUnittest_RunTests
						(
						rgszSemiMdpFiles,
						&m_ulTestCounter,
						GPOS_ARRAY_SIZE(rgszSemiMdpFiles)
						);
}

// EOF
