//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal, Inc.
//
//	@filename:
//		CSetopTest.cpp
//
//	@doc:
//		Test for optimizing queries with set operators
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

#include "unittest/gpopt/minidump/CSetopTest.h"

using namespace gpopt;

ULONG CSetopTest::m_ulSetopTestCounter = 0;  // start from first test

// minidump files
const CHAR *rgszSetOpFileNames[] =
	{
	"../data/dxl/minidump/ValueScanWithDuplicateAndSelfComparison.mdp",
	"../data/dxl/minidump/PushGbBelowNaryUnionAll.mdp",
	"../data/dxl/minidump/PushGbBelowNaryUnion-1.mdp",
	"../data/dxl/minidump/PushGbBelowNaryUnion-2.mdp",
#ifndef GPOS_DEBUG
	// TODO:  - Jul 14 2015; disabling it for debug build to reduce testing time
	"../data/dxl/minidump/MS-UnionAll-1.mdp",
	"../data/dxl/minidump/MS-UnionAll-2.mdp",
	"../data/dxl/minidump/MS-UnionAll-4.mdp",
#endif
	"../data/dxl/minidump/MS-UnionAll-5.mdp",
	"../data/dxl/minidump/MS-UnionAll-6.mdp",
	"../data/dxl/minidump/MS-UnionAll-7.mdp",
	"../data/dxl/minidump/Cascaded-UnionAll-Same-Cols-Order.mdp",
	"../data/dxl/minidump/Cascaded-UnionAll-Differing-Cols.mdp",
	"../data/dxl/minidump/Cascaded-UnionAll-Differing-Cols-Order.mdp",
	"../data/dxl/minidump/Union-Over-UnionAll.mdp",
	"../data/dxl/minidump/Nested-Setops.mdp",
	"../data/dxl/minidump/Nested-Setops-2.mdp",
	"../data/dxl/minidump/Except.mdp",
	"../data/dxl/minidump/IsNullUnionAllIsNotNull.mdp",
	"../data/dxl/minidump/UnionAllWithTruncatedOutput.mdp",
	"../data/dxl/minidump/Union-OuterRefs-Output.mdp",
	"../data/dxl/minidump/Union-OuterRefs-Casting-Output.mdp",
	"../data/dxl/minidump/Union-OuterRefs-InnerChild.mdp",
	"../data/dxl/minidump/UnionWithOuterRefs.mdp",
#ifndef GPOS_DEBUG
	// TODO:  - Jul 14 2015; disabling it for debug build to reduce testing time
	"../data/dxl/minidump/UnionAll.mdp",
	"../data/dxl/minidump/Union-On-HJNs.mdp",
#endif
	"../data/dxl/minidump/Union-Distributed-Table-With-Const-Table.mdp",
	"../data/dxl/minidump/ExceptAllCompatibleDataType.mdp",
	"../data/dxl/minidump/UnionAllCompatibleDataType.mdp",
	"../data/dxl/minidump/UnionOfDQAQueries.mdp",
	"../data/dxl/minidump/Union-Volatile-Func.mdp",
	"../data/dxl/minidump/Intersect-Volatile-Func.mdp",
	"../data/dxl/minidump/Except-Volatile-Func.mdp",
	"../data/dxl/minidump/PushSelectDownUnionAllOfCTG.mdp",
	"../data/dxl/minidump/Push-Subplan-Below-Union.mdp",
	"../data/dxl/minidump/PushSelectWithOuterRefBelowUnion.mdp",
	"../data/dxl/minidump/PushGbBelowUnion.mdp",
	"../data/dxl/minidump/UnionGbSubquery.mdp",
	"../data/dxl/minidump/AnyPredicate-Over-UnionOfConsts.mdp",
	"../data/dxl/minidump/Intersect-OuterRefs.mdp",
	"../data/dxl/minidump/EquivClassesUnion.mdp",
	"../data/dxl/minidump/EquivClassesIntersect.mdp",
	};

//---------------------------------------------------------------------------
//	@function:
//		CSetopTest::EresUnittest
//
//	@doc:
//		Unittest for expressions
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSetopTest::EresUnittest()
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
//		CSetopTest::EresUnittest_RunTests
//
//	@doc:
//		Run all Minidump-based tests with plan matching
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSetopTest::EresUnittest_RunTests()
{
	return CTestUtils::EresUnittest_RunTests
						(
						rgszSetOpFileNames,
						&m_ulSetopTestCounter,
						GPOS_ARRAY_SIZE(rgszSetOpFileNames)
						);
}

// EOF
