//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal, Inc.
//
//	@filename:
//		CExistsSubqueryTest.cpp
//
//	@doc:
//		Test for exists and not exists subquery optimization
//---------------------------------------------------------------------------

#include "unittest/gpopt/minidump/CExistsSubqueryTest.h"
#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/test/CUnittest.h"

#include "gpopt/exception.h"
#include "gpopt/minidump/CMinidumperUtils.h"

#include "unittest/gpopt/CTestUtils.h"


using namespace gpopt;

ULONG CExistsSubqueryTest::m_ulExistsSubQueryTestCounter = 0;  // start from first test

// minidump files
const CHAR *rgszExistsFileNames[] =
	{
		"../data/dxl/minidump/SubqExists-With-External-Corrs.mdp",
		"../data/dxl/minidump/SubqExists-Without-External-Corrs.mdp",
		"../data/dxl/minidump/Exists-SuperfluousEquality.mdp",
		"../data/dxl/minidump/NotExists-SuperfluousEquality.mdp",
		"../data/dxl/minidump/SimplifyExistsSubquery2Limit.mdp",
		"../data/dxl/minidump/NotExists-SuperflousOuterRefWithGbAgg.mdp",
	};


// unittest for expressions
GPOS_RESULT
CExistsSubqueryTest::EresUnittest()
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

// run all Minidump-based tests with plan matching
GPOS_RESULT
CExistsSubqueryTest::EresUnittest_RunTests()
{
	return CTestUtils::EresUnittest_RunTests
						(
						rgszExistsFileNames,
						&m_ulExistsSubQueryTestCounter,
						GPOS_ARRAY_SIZE(rgszExistsFileNames)
						);
}

// EOF
