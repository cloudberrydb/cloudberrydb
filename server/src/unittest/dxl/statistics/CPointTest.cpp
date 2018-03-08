//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal Inc.
//
//	@filename:
//		CPointTest.cpp
//
//	@doc:
//		Tests for CPoint
//---------------------------------------------------------------------------

#ifndef __STDC_CONSTANT_MACROS
#define __STDC_CONSTANT_MACROS
#endif

#include <stdint.h>

#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/statistics/CPoint.h"

#include "unittest/base.h"
#include "unittest/dxl/statistics/CPointTest.h"
#include "unittest/gpopt/CTestUtils.h"

using namespace gpopt;

// unittest for statistics objects
GPOS_RESULT
CPointTest::EresUnittest()
{
	// tests that use shared optimization context
	CUnittest rgutSharedOptCtxt[] =
		{
		GPOS_UNITTEST_FUNC(CPointTest::EresUnittest_CPointInt4),
		GPOS_UNITTEST_FUNC(CPointTest::EresUnittest_CPointBool),
		};

	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// install opt context in TLS
	CAutoOptCtxt aoc(pmp, &mda, NULL /* pceeval */, CTestUtils::Pcm(pmp));

	return CUnittest::EresExecute(rgutSharedOptCtxt, GPOS_ARRAY_SIZE(rgutSharedOptCtxt));
}

// basic int4 point tests;
GPOS_RESULT
CPointTest::EresUnittest_CPointInt4()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// generate integer points
	CPoint *ppoint1 = CTestUtils::PpointInt4(pmp, 1);
	CPoint *ppoint2 = CTestUtils::PpointInt4(pmp, 2);

	GPOS_RTL_ASSERT_MSG(ppoint1->FEqual(ppoint1), "1 == 1");
	GPOS_RTL_ASSERT_MSG(ppoint1->FLessThan(ppoint2), "1 < 2");
	GPOS_RTL_ASSERT_MSG(ppoint2->FGreaterThan(ppoint1), "2 > 1");
	GPOS_RTL_ASSERT_MSG(ppoint1->FLessThanOrEqual(ppoint2), "1 <= 2");
	GPOS_RTL_ASSERT_MSG(ppoint2->FGreaterThanOrEqual(ppoint2), "2 >= 2");

	CDouble dDistance = ppoint2->DDistance(ppoint1);

	// should be 1.0
	GPOS_RTL_ASSERT_MSG(0.99 < dDistance
						&& dDistance < 1.01, "incorrect distance calculation");

	ppoint1->Release();
	ppoint2->Release();

	return GPOS_OK;
}

// basic bool point tests;
GPOS_RESULT
CPointTest::EresUnittest_CPointBool()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// generate boolean points
	CPoint *ppoint1 = CTestUtils::PpointBool(pmp, true);
	CPoint *ppoint2 = CTestUtils::PpointBool(pmp, false);

	// true == true
	GPOS_RTL_ASSERT_MSG(ppoint1->FEqual(ppoint1), "true must be equal to true");

	// true != false
	GPOS_RTL_ASSERT_MSG(ppoint1->FNotEqual(ppoint2), "true must not be equal to false");

	ppoint1->Release();
	ppoint2->Release();

	return GPOS_OK;
}

// EOF

