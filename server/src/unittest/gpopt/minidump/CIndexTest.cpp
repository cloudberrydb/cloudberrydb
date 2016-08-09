//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal, Inc.
//
//	@filename:
//		CIndexTest.cpp
//
//	@doc:
//		Test for optimizing queries on tables with indexes
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/test/CUnittest.h"

#include "gpopt/exception.h"
#include "gpopt/minidump/CMinidumperUtils.h"

#include "unittest/gpopt/CTestUtils.h"

#include "unittest/gpopt/minidump/CIndexTest.h"

using namespace gpopt;

ULONG CIndexTest::m_ulIndexTestCounter = 0;  // start from first test

// minidump files
const CHAR *rgszIndexFileNames[] =
	{
	"../data/dxl/minidump/BTreeIndex-Against-InList.mdp",
	"../data/dxl/minidump/BTreeIndex-Against-ScalarSubquery.mdp",
	"../data/dxl/minidump/BitmapIndex-Against-InList.mdp",
	"../data/dxl/minidump/DynamicIndexScan-Homogenous.mdp",
	"../data/dxl/minidump/DynamicIndexScan-BoolTrue.mdp",
	"../data/dxl/minidump/DynamicIndexScan-BoolFalse.mdp",
	"../data/dxl/minidump/DynamicIndexScan-Heterogenous-NoDTS.mdp",
	"../data/dxl/minidump/DynamicIndexScan-Heterogenous-Union.mdp",
	"../data/dxl/minidump/DynamicIndexScan-Heterogenous-Overlapping.mdp",
	"../data/dxl/minidump/DynamicIndexScan-Heterogenous.mdp",
	"../data/dxl/minidump/DynamicIndexScan-DefaultPartition.mdp",
	"../data/dxl/minidump/DynamicIndexScan-DefaultPartition-2.mdp",
	"../data/dxl/minidump/DynamicIndexScan-Homogenous-UnsupportedConstraint.mdp",
	"../data/dxl/minidump/DynamicIndexScan-Heterogenous-UnsupportedConstraint.mdp",
	"../data/dxl/minidump/DynamicIndexScan-Heterogenous-UnsupportedPredicate.mdp",
	"../data/dxl/minidump/DynamicIndexScan-Heterogenous-PartSelectEquality.mdp",
	"../data/dxl/minidump/DynamicIndexScan-Heterogenous-PartSelectRange.mdp",
	"../data/dxl/minidump/DynamicIndexScan-OpenEndedPartitions.mdp",
	"../data/dxl/minidump/IndexApply1.mdp",
	"../data/dxl/minidump/IndexApply1-CalibratedCostModel.mdp",
	"../data/dxl/minidump/IndexApply2.mdp",
	"../data/dxl/minidump/IndexApply3.mdp",
	"../data/dxl/minidump/IndexApply4.mdp",
	"../data/dxl/minidump/IndexApply-IndexCondDisjointWithHashedDistr.mdp",
	"../data/dxl/minidump/IndexApply-IndexCondIntersectWithHashedDistr.mdp",
	"../data/dxl/minidump/IndexApply-IndexCondMatchHashedDistr.mdp",
	"../data/dxl/minidump/IndexApply-IndexCondSubsetOfHashedDistr.mdp",
	"../data/dxl/minidump/IndexApply-IndexCondSupersetOfHashedDistr.mdp",
	"../data/dxl/minidump/IndexApply-IndexOnMasterOnlyTable.mdp",
	"../data/dxl/minidump/IndexApply-PartTable.mdp",
	"../data/dxl/minidump/IndexApply-Heterogeneous-NoDTS.mdp",
	"../data/dxl/minidump/IndexApply-Heterogeneous-DTS.mdp",
	"../data/dxl/minidump/IndexApply-No-Motion-Below-Join.mdp",
	"../data/dxl/minidump/IndexApply-Redistribute-Const-Table.mdp",
	"../data/dxl/minidump/IndexApply-PartResolverExpand.mdp",
	"../data/dxl/minidump/IndexApply-InnerSelect-Basic.mdp",
	"../data/dxl/minidump/IndexApply-InnerSelect-PartTable.mdp",
	"../data/dxl/minidump/IndexApply-InnerSelect-Heterogeneous-DTS.mdp",
	"../data/dxl/minidump/IndexApply-Heterogeneous-BothSidesPartitioned.mdp",
	"../data/dxl/minidump/IndexApply-PartKey-Is-IndexKey.mdp",
#ifndef GPOS_DEBUG
	// TODO:  - Jul 14 2015; disabling it for debug build to reduce testing time
	"../data/dxl/minidump/Tpcds-10TB-Q37-NoIndexJoin.mdp",
#endif // GPOS_DEBUG
	"../data/dxl/minidump/IndexScan-AOTable.mdp",
	"../data/dxl/minidump/IndexScan-DroppedColumns.mdp",
	"../data/dxl/minidump/IndexScan-BoolTrue.mdp",
	"../data/dxl/minidump/IndexScan-BoolFalse.mdp",
	"../data/dxl/minidump/IndexScan-Relabel.mdp",
	"../data/dxl/minidump/DynamicIndexScan-Relabel.mdp",
	"../data/dxl/minidump/DynamicIndexScan-DroppedColumns.mdp",
	"../data/dxl/minidump/DynamicIndexGet-OuterRefs.mdp",
	"../data/dxl/minidump/IndexGet-OuterRefs.mdp",
	};

//---------------------------------------------------------------------------
//	@function:
//		CIndexTest::EresUnittest
//
//	@doc:
//		Unittest for expressions
//
//---------------------------------------------------------------------------
GPOS_RESULT
CIndexTest::EresUnittest()
{

#ifdef GPOS_DEBUG
	// enable extended asserts after running test
	fEnableExtendedAsserts = true;
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
//		CIndexTest::EresUnittest_RunTests
//
//	@doc:
//		Run all Minidump-based tests with plan matching
//
//---------------------------------------------------------------------------
GPOS_RESULT
CIndexTest::EresUnittest_RunTests()
{
	return CTestUtils::EresUnittest_RunTests
						(
						rgszIndexFileNames,
						&m_ulIndexTestCounter,
						GPOS_ARRAY_SIZE(rgszIndexFileNames)
						);
}

// EOF
