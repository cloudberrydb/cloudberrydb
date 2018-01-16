//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//---------------------------------------------------------------------------

#include "unittest/gpopt/minidump/CScalarArrayCmpTest.h"
#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/test/CUnittest.h"

#include "gpopt/exception.h"
#include "gpopt/minidump/CMinidumperUtils.h"

#include "unittest/gpopt/CTestUtils.h"


using namespace gpopt;

ULONG CScalarArrayCmpTest::m_ulTestCounter = 0;  // start from first test

// minidump files
const CHAR *rgszScalarArrayCmpMdpFiles[] =
{
	"../data/dxl/minidump/InClauseWithMCV.mdp",
	"../data/dxl/minidump/CastedInClauseWithMCV.mdp",
	"../data/dxl/minidump/FilterScalarCast.mdp",
};


GPOS_RESULT
CScalarArrayCmpTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(EresUnittest_RunTests),
		};

	GPOS_RESULT eres = CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));

	// reset metadata cache
	CMDCache::Reset();

	return eres;
}

// Run all Minidump-based tests with plan matching
GPOS_RESULT
CScalarArrayCmpTest::EresUnittest_RunTests()
{
	return CTestUtils::EresUnittest_RunTests
						(
						rgszScalarArrayCmpMdpFiles,
						&m_ulTestCounter,
						GPOS_ARRAY_SIZE(rgszScalarArrayCmpMdpFiles)
						);
}

// EOF
