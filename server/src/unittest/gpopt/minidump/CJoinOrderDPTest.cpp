//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software
//
//	@filename:
//		CJoinOrderDPTest.cpp
//
//	@doc:
//		Test for n-ary join order
//---------------------------------------------------------------------------
#include "unittest/gpopt/minidump/CJoinOrderDPTest.h"
#include "unittest/gpopt/CTestUtils.h"



//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPTest::EresUnittest
//
//	@doc:
//		Unittest for array expansion in WHERE clause
//
//---------------------------------------------------------------------------
gpos::GPOS_RESULT
CJoinOrderDPTest::EresUnittest()
{

	ULONG ulTestCounter = 0;
	const CHAR *rgszFileNames[] =
	{
			"../data/dxl/minidump/CJoinOrderDPTest/DisableDPJoinOrderForLargeArity.mdp",
	};

	return CTestUtils::EresUnittest_RunTestsWithoutAdditionalTraceFlags
				(
					rgszFileNames,
					&ulTestCounter,
					GPOS_ARRAY_SIZE(rgszFileNames),
					true,
					true
				);
}
// EOF
