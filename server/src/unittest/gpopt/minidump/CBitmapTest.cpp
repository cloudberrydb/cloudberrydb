//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal, Inc.
//
//	@filename:
//		CBitmapTest.cpp
//
//	@doc:
//		Test for optimizing queries that can use a bitmap index
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/test/CUnittest.h"

#include "gpopt/exception.h"
#include "gpopt/minidump/CMinidumperUtils.h"

#include "unittest/gpopt/CTestUtils.h"

#include "unittest/gpopt/minidump/CBitmapTest.h"

using namespace gpopt;

ULONG CBitmapTest::m_ulBitmapTestCounter = 0;  // start from first test

// minidump files
const CHAR *rgszBitmapFileNames[] =
	{
	"../data/dxl/minidump/BitmapIndexScan.mdp",
	"../data/dxl/minidump/DynamicBitmapIndexScan.mdp",
	"../data/dxl/minidump/BitmapTableScan-AO.mdp",
	"../data/dxl/minidump/BitmapTableScan-Basic.mdp",
	"../data/dxl/minidump/BitmapTableScan-ColumnOnRightSide.mdp",
	"../data/dxl/minidump/BitmapTableScan-AndCondition.mdp",
	"../data/dxl/minidump/DynamicBitmapTableScan-Basic.mdp",
	"../data/dxl/minidump/DynamicBitmapTableScan-Heterogeneous.mdp",
	"../data/dxl/minidump/BitmapBoolOr.mdp",
	"../data/dxl/minidump/BitmapBoolOr-BoolColumn.mdp",
	"../data/dxl/minidump/BitmapBoolAnd.mdp",
	"../data/dxl/minidump/BitmapBoolOp-DeepTree.mdp",
	"../data/dxl/minidump/BitmapBoolOp-DeepTree2.mdp",
	"../data/dxl/minidump/BitmapBoolOp-DeepTree3.mdp",
	"../data/dxl/minidump/DynamicBitmapBoolOp.mdp",
	"../data/dxl/minidump/BitmapIndexScan-WithUnsupportedOperatorFilter.mdp",
	"../data/dxl/minidump/BitmapTableScan-AO-Btree.mdp",
	"../data/dxl/minidump/BitmapIndexApply-Basic-SelfJoin.mdp",
	"../data/dxl/minidump/BitmapIndexApply-Basic-TwoTables.mdp",
	"../data/dxl/minidump/BitmapIndexApply-Complex-Condition.mdp",
	"../data/dxl/minidump/BitmapIndexApply-PartTable.mdp",
	"../data/dxl/minidump/BitmapIndexApply-InnerSelect-Basic.mdp",
	"../data/dxl/minidump/BitmapIndexApply-InnerSelect-PartTable.mdp",

	};

//---------------------------------------------------------------------------
//	@function:
//		CBitmapTest::EresUnittest
//
//	@doc:
//		Unittest for expressions
//
//---------------------------------------------------------------------------
GPOS_RESULT
CBitmapTest::EresUnittest()
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
//		CBitmapTest::EresUnittest_RunTests
//
//	@doc:
//		Run all Minidump-based tests with plan matching
//
//---------------------------------------------------------------------------
GPOS_RESULT
CBitmapTest::EresUnittest_RunTests()
{
	return CTestUtils::EresUnittest_RunTests
						(
						rgszBitmapFileNames,
						&m_ulBitmapTestCounter,
						GPOS_ARRAY_SIZE(rgszBitmapFileNames)
						);
}

// EOF
