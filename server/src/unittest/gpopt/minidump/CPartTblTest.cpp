//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal, Inc.
//
//	@filename:
//		CPartTblTest.cpp
//
//	@doc:
//		Test for optimizing queries on partitioned tables
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/test/CUnittest.h"

#include "gpopt/exception.h"
#include "gpopt/minidump/CMinidumperUtils.h"

#include "unittest/gpopt/CTestUtils.h"

#include "unittest/gpopt/minidump/CPartTblTest.h"

using namespace gpopt;

ULONG CPartTblTest::m_ulPartTblTestCounter = 0;  // start from first test

// minidump files
const CHAR *rgszPartTblFileNames[] =
	{
	"../data/dxl/minidump/NoPartConstraint-WhenNoDefaultPartsAndIndices.mdp",
	"../data/dxl/minidump/PartConstraint-WhenIndicesAndNoDefaultParts.mdp",
	"../data/dxl/minidump/PartConstraint-WithOnlyDefaultPartInfo.mdp",
	"../data/dxl/minidump/PartConstraint-WhenDefaultPartsAndIndices.mdp",
	"../data/dxl/minidump/CorrelatedNLJ-PartSelector-Subplan.mdp",
	"../data/dxl/minidump/DonotPushPartConstThruLimit.mdp",
	"../data/dxl/minidump/PartTbl-DPE-Correlated-NLOuter.mdp",
	"../data/dxl/minidump/Select-Over-PartTbl.mdp",
	"../data/dxl/minidump/PartTbl-DPE.mdp",
	"../data/dxl/minidump/PartTbl-DPE-WindowFunction.mdp",
	"../data/dxl/minidump/PartTbl-DPE-GroupBy.mdp",
	"../data/dxl/minidump/PartTbl-DPE-Limit.mdp",
	"../data/dxl/minidump/DPE-SemiJoin.mdp",
	"../data/dxl/minidump/DPE-IN.mdp",
	"../data/dxl/minidump/DPE-NOT-IN.mdp",
	"../data/dxl/minidump/HJN-DPE-Bitmap-Outer-Child.mdp",
	"../data/dxl/minidump/NLJ-Broadcast-DPE-Outer-Child.mdp",
	"../data/dxl/minidump/Part-Selection-IN.mdp",
	"../data/dxl/minidump/Part-Selection-NOT-IN.mdp",
	"../data/dxl/minidump/Part-Selection-ConstArray-1.mdp",
	"../data/dxl/minidump/Part-Selection-ConstArray-2.mdp",
	"../data/dxl/minidump/PartTbl-WindowFunction.mdp",
	"../data/dxl/minidump/PartTbl-MultiWayJoin.mdp",
	"../data/dxl/minidump/PartTbl-MultiWayJoinWithDPE.mdp",
	"../data/dxl/minidump/PartTbl-MultiWayJoinWithDPE-2.mdp",
	"../data/dxl/minidump/PartTbl-AsymmetricRangePredicate.mdp",
	"../data/dxl/minidump/PartTbl-NEqPredicate.mdp",
	"../data/dxl/minidump/PartTbl-SQExists.mdp",
	"../data/dxl/minidump/PartTbl-SQNotExists.mdp",
	"../data/dxl/minidump/PartTbl-SQAny.mdp",
	"../data/dxl/minidump/PartTbl-SQAll.mdp",
	"../data/dxl/minidump/PartTbl-SQScalar.mdp",
	"../data/dxl/minidump/PartTbl-HJ3.mdp",
	"../data/dxl/minidump/PartTbl-HJ4.mdp",
	"../data/dxl/minidump/PartTbl-HJ5.mdp",
	"../data/dxl/minidump/PartTbl-JoinOverGbAgg.mdp",
	"../data/dxl/minidump/PartTbl-JoinOverGbAgg-2.mdp",
	"../data/dxl/minidump/PartTbl-NonConstSelect.mdp",
	"../data/dxl/minidump/PartTbl-VolatileFunc.mdp",
	"../data/dxl/minidump/PartTbl-DateTime.mdp",
	"../data/dxl/minidump/PartTbl-ComplexRangePredicate-DefaultPart.mdp",
	"../data/dxl/minidump/PartTbl-ComplexRangePredicate-NoDefaultPart.mdp",
	"../data/dxl/minidump/PartTbl-LASJ.mdp",
	"../data/dxl/minidump/PartTbl-Relabel-Equality.mdp",
	"../data/dxl/minidump/PartTbl-Relabel-Range.mdp",
	"../data/dxl/minidump/PartTbl-DisablePartSelection.mdp",
	"../data/dxl/minidump/PartTbl-DisablePartSelectionJoin.mdp",
	"../data/dxl/minidump/PartTbl-JoinOverUnion-1.mdp",
	"../data/dxl/minidump/PartTbl-JoinOverUnion-2.mdp",
	"../data/dxl/minidump/PartTbl-JoinOverIntersect.mdp",
	"../data/dxl/minidump/PartTbl-JoinOverExcept.mdp",
	"../data/dxl/minidump/PartTbl-RangeJoinPred.mdp",
	"../data/dxl/minidump/PartTbl-ArrayIn.mdp",
	"../data/dxl/minidump/PartTbl-ArrayCoerce.mdp",
	"../data/dxl/minidump/PartTbl-Disjunction.mdp",
	"../data/dxl/minidump/PartTbl-ComplexPredicate1.mdp",
	"../data/dxl/minidump/PartTbl-ComplexPredicate2.mdp",
	"../data/dxl/minidump/PartTbl-ComplexPredicate3.mdp",
	"../data/dxl/minidump/PartTbl-ComplexPredicate4.mdp",
	"../data/dxl/minidump/PartTbl-ComplexPredicate5.mdp",
	"../data/dxl/minidump/PartTbl-IsNullPredicate.mdp",
	"../data/dxl/minidump/PartTbl-IsNotNullPredicate.mdp",
	"../data/dxl/minidump/PartTbl-IndexOnDefPartOnly.mdp",
	"../data/dxl/minidump/PartTbl-SubqueryOuterRef.mdp",
	"../data/dxl/minidump/PartTbl-CSQ-PartKey.mdp",
	"../data/dxl/minidump/PartTbl-CSQ-NonPartKey.mdp",
	"../data/dxl/minidump/PartTbl-LeftOuterHashJoin-DPE-IsNull.mdp",
	"../data/dxl/minidump/PartTbl-LeftOuterNLJoin-DPE-IsNull.mdp",
	"../data/dxl/minidump/PartTbl-List-DPE-Varchar-Predicates.mdp",
	"../data/dxl/minidump/PartTbl-List-DPE-Int-Predicates.mdp",
	"../data/dxl/minidump/PartTbl-PredicateWithCast.mdp",
	"../data/dxl/minidump/PartTbl-IDFList.mdp",
	"../data/dxl/minidump/PartTbl-IDFNull.mdp",
	"../data/dxl/minidump/PartTbl-PredicateWithCast.mdp",
	"../data/dxl/minidump/PartTbl-EqPredicateWithCastRange.mdp",
	"../data/dxl/minidump/PartTbl-PredicateWithCastList.mdp",
	"../data/dxl/minidump/PartTbl-IDFWithCast.mdp",
	"../data/dxl/minidump/PartTbl-PredicateWithCastMultiLevelList.mdp"
	};

//---------------------------------------------------------------------------
//	@function:
//		CPartTblTest::EresUnittest
//
//	@doc:
//		Unittest for expressions
//
//---------------------------------------------------------------------------
GPOS_RESULT
CPartTblTest::EresUnittest()
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
//		CPartTblTest::EresUnittest_RunTests
//
//	@doc:
//		Run all Minidump-based tests with plan matching
//
//---------------------------------------------------------------------------
GPOS_RESULT
CPartTblTest::EresUnittest_RunTests()
{
	return CTestUtils::EresUnittest_RunTests
						(
						rgszPartTblFileNames,
						&m_ulPartTblTestCounter,
						GPOS_ARRAY_SIZE(rgszPartTblFileNames)
						);
}

// EOF
