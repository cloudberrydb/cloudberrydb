//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//---------------------------------------------------------------------------

#include "unittest/gpopt/minidump/COuterJoinTest.h"
#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/test/CUnittest.h"

#include "gpopt/exception.h"
#include "gpopt/minidump/CMinidumperUtils.h"

#include "unittest/gpopt/CTestUtils.h"


using namespace gpopt;

ULONG COuterJoinTest::m_ulTestCounter = 0;  // start from first test

// minidump files
const CHAR *rgszOJMdpFiles[] =
{
	"../data/dxl/minidump/LOJ-Condition-False.mdp",
	"../data/dxl/minidump/FOJ-Condition-False.mdp",
	"../data/dxl/minidump/FullOuterJoin.mdp",
	"../data/dxl/minidump/FullOuterJoin2.mdp",
	"../data/dxl/minidump/LeftJoin-With-Pred-On-Inner.mdp",
	"../data/dxl/minidump/LeftJoin-With-Pred-On-Inner2.mdp",
	"../data/dxl/minidump/LeftJoin-With-Col-Const-Pred.mdp",
	"../data/dxl/minidump/LeftJoin-With-Coalesce.mdp",
	"../data/dxl/minidump/LOJWithFalsePred.mdp",
	"../data/dxl/minidump/LOJ-IsNullPred.mdp",
	"../data/dxl/minidump/Select-Proj-OuterJoin.mdp",
	"../data/dxl/minidump/OuterJoin-With-OuterRefs.mdp",
	"../data/dxl/minidump/Join-Disj-Subqs.mdp",
	"../data/dxl/minidump/Join-Dist-Key-No-Motion.mdp",
	"../data/dxl/minidump/EffectOfLocalPredOnJoin.mdp",
	"../data/dxl/minidump/EffectOfLocalPredOnJoin2.mdp",
	"../data/dxl/minidump/EffectOfLocalPredOnJoin3.mdp",
	"../data/dxl/minidump/LeftOuter2InnerUnionAllAntiSemiJoin.mdp",
#ifndef GPOS_DEBUG
	// TODO:  - Jul 14 2015; disabling it for debug build to reduce testing time
	"../data/dxl/minidump/LeftOuter2InnerUnionAllAntiSemiJoin-Tpcds.mdp",
#endif
};


GPOS_RESULT
COuterJoinTest::EresUnittest()
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
COuterJoinTest::EresUnittest_RunTests()
{
	return CTestUtils::EresUnittest_RunTests
						(
						rgszOJMdpFiles,
						&m_ulTestCounter,
						GPOS_ARRAY_SIZE(rgszOJMdpFiles)
						);
}

// EOF
