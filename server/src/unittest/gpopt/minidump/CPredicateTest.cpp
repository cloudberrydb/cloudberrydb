//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//---------------------------------------------------------------------------

#include "unittest/gpopt/minidump/CPredicateTest.h"
#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/test/CUnittest.h"

#include "gpopt/exception.h"
#include "gpopt/minidump/CMinidumperUtils.h"

#include "unittest/gpopt/CTestUtils.h"


using namespace gpopt;

ULONG CPredicateTest::m_ulTestCounter = 0;  // start from first test

// minidump files
const CHAR *rgszPredMdpFiles[] =
{
	"../data/dxl/minidump/InferPredicatesForLimit.mdp",
	"../data/dxl/minidump/OR-WithIsNullPred.mdp",
	"../data/dxl/minidump/Int2Predicate.mdp",
	"../data/dxl/minidump/AddEqualityPredicates.mdp",
	"../data/dxl/minidump/AddPredsInSubqueries.mdp",
	"../data/dxl/minidump/DeduplicatePredicates.mdp",
	"../data/dxl/minidump/EquivClassesAndOr.mdp",
	"../data/dxl/minidump/EquivClassesLimit.mdp",
	"../data/dxl/minidump/Join-With-Subq-Preds-1.mdp",
	"../data/dxl/minidump/Non-Hashjoinable-Pred.mdp",
	"../data/dxl/minidump/Non-Hashjoinable-Pred-2.mdp",
	"../data/dxl/minidump/Equiv-HashedDistr-1.mdp",
	"../data/dxl/minidump/Equiv-HashedDistr-2.mdp",
	"../data/dxl/minidump/UnsupportedStatsPredicate.mdp",
	"../data/dxl/minidump/LIKE-Pattern-green.mdp", // p_name like '%%green%%'
	"../data/dxl/minidump/LIKE-Pattern-green-2.mdp", //  '%%green%%' like p_name
	"../data/dxl/minidump/LIKE-Pattern-Empty.mdp", // like '%%' --> should not damp the cardinality of the input
	"../data/dxl/minidump/Nested-Or-Predicates.mdp",
	"../data/dxl/minidump/Factorized-Preds.mdp",
	"../data/dxl/minidump/IN.mdp",
	"../data/dxl/minidump/OR.mdp",
	"../data/dxl/minidump/LikePredStatsNotComparable.mdp",
	"../data/dxl/minidump/PredStatsNotComparable.mdp",
	"../data/dxl/minidump/ExtractPredicateFromDisj.mdp",
	"../data/dxl/minidump/ExtractPredicateFromDisjWithComputedColumns.mdp",
	"../data/dxl/minidump/IDF-NotNullConstant.mdp",
	"../data/dxl/minidump/IDF-NullConstant.mdp",
	"../data/dxl/minidump/INDF-NotNullConstant.mdp",
	"../data/dxl/minidump/INDF-NullConstant.mdp",
	"../data/dxl/minidump/NullConstant-INDF-Col.mdp",
	"../data/dxl/minidump/IN-Numeric.mdp",
	"../data/dxl/minidump/CollapseNot.mdp",
};


GPOS_RESULT
CPredicateTest::EresUnittest()
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
CPredicateTest::EresUnittest_RunTests()
{
	return CTestUtils::EresUnittest_RunTests
						(
						rgszPredMdpFiles,
						&m_ulTestCounter,
						GPOS_ARRAY_SIZE(rgszPredMdpFiles)
						);
}

// EOF
