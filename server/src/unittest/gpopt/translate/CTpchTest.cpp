//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CTpchTest.cpp
//
//	@doc:
//		Test for translating CExpressions into DXL for TPC-H benchmark
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CAutoP.h"
#include "gpos/io/COstreamString.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpos/test/CUnittest.h"
#include "gpos/task/CAutoTraceFlag.h"

#include "naucrates/md/CMDProviderMemory.h"

#include "gpopt/exception.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CPrintPrefix.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/operators/ops.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "gpopt/translate/CTranslatorExprToDXL.h"

#include "naucrates/dxl/CDXLUtils.h"

#include "unittest/gpopt/CTestUtils.h"
#include "unittest/gpopt/translate/CTpchTest.h"

using namespace gpopt;

ULONG CTpchTest::m_ulTestCounter = 0;
ULONG CTpchTest::m_ulTestCounterParts = 0;

// minidump files
const CHAR *rgszTpch[] =
	{
		"../data/dxl/tpch/q1.mdp",
		"../data/dxl/tpch/q2.mdp",
		"../data/dxl/tpch/q3.mdp",
		"../data/dxl/tpch/q4.mdp",
		"../data/dxl/tpch/q5.mdp",
		"../data/dxl/tpch/q6.mdp",
		"../data/dxl/tpch/q7.mdp",
		"../data/dxl/tpch/q8.mdp",
		"../data/dxl/tpch/q9.mdp",
		"../data/dxl/tpch/q10.mdp",
		"../data/dxl/tpch/q11.mdp",
		"../data/dxl/tpch/q12.mdp",
		"../data/dxl/tpch/q13.mdp",
		"../data/dxl/tpch/q14.mdp",
		"../data/dxl/tpch/q15.mdp",
		"../data/dxl/tpch/q16.mdp",
		"../data/dxl/tpch/q17.mdp",
		"../data/dxl/tpch/q18.mdp",
		"../data/dxl/tpch/q19.mdp",
		"../data/dxl/tpch/q20.mdp",
		"../data/dxl/tpch/q21.mdp",
		"../data/dxl/tpch/q22.mdp",

//		re-enable after regenerating minidump		
//		"../data/dxl/minidump/LargeJoins.mdp", // a test for joining all tables

};


// queries on partitioned tpch schema
const CHAR *szTpchPartsMD = "../data/dxl/tpch-partitioned/tpch_partitioned.xml";

const CHAR *rgszTpchPart[] =
{
		"../data/dxl/tpch-partitioned/query01.mdp",
		"../data/dxl/tpch-partitioned/query02.mdp",
		"../data/dxl/tpch-partitioned/query03.mdp",
		"../data/dxl/tpch-partitioned/query04.mdp",
		"../data/dxl/tpch-partitioned/query05.mdp",
		"../data/dxl/tpch-partitioned/query06.mdp",
		"../data/dxl/tpch-partitioned/query07.mdp",
		"../data/dxl/tpch-partitioned/query08.mdp",
		"../data/dxl/tpch-partitioned/query09.mdp",
		"../data/dxl/tpch-partitioned/query10.mdp",
		"../data/dxl/tpch-partitioned/query11.mdp",
		"../data/dxl/tpch-partitioned/query12.mdp",
		"../data/dxl/tpch-partitioned/query13.mdp",
		"../data/dxl/tpch-partitioned/query14.mdp",
		"../data/dxl/tpch-partitioned/query15.mdp",
		"../data/dxl/tpch-partitioned/query16.mdp",
		"../data/dxl/tpch-partitioned/query17.mdp",
		"../data/dxl/tpch-partitioned/query18.mdp",
		"../data/dxl/tpch-partitioned/query19.mdp",
		"../data/dxl/tpch-partitioned/query20.mdp",
		"../data/dxl/tpch-partitioned/query21.mdp",
		"../data/dxl/tpch-partitioned/query22.mdp",
};

//---------------------------------------------------------------------------
//	@function:
//		CTpchTest::EresSubtest
//
//	@doc:
//		Run translation tests for a single TPC-H query
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTpchTest::EresSubtest(ULONG ulQueryNum)
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

#ifdef GPOS_DEBUG
	const ULONG ulTests = GPOS_ARRAY_SIZE(rgszTpch);
	GPOS_ASSERT(ulQueryNum > 0);
	GPOS_ASSERT(ulQueryNum - 1 < ulTests);
#endif  // GPOS_DEBUG

	// TODO: 04/03/2013 - remove after generating minidumps
	BOOL fMatchPlans = false;

	GPOS_RESULT eres = GPOS_OK;

	// enable space pruning
	CAutoTraceFlag atf1(EopttraceEnableSpacePruning, true /*fVal*/);

	// run regular version of query
	eres = CTestUtils::EresRunMinidumps
		(
		pmp,
		rgszTpch + ulQueryNum - 1,
		1,
		&m_ulTestCounter,
		1, // ulSessionId
		1,  // ulCmdId
		false /* fMatchPlans */,
		true /* fTestSpacePruning */
		);

	// run version for partitioned tables
	if (eres == GPOS_OK) {
		// enable plan enumeration only if we will match plans
		CAutoTraceFlag atf2(EopttraceEnumeratePlans, fMatchPlans);

		// enable stats derivation for DPE
		CAutoTraceFlag atf3(EopttraceDeriveStatsForDPE, true /*fVal*/);

		eres = CTestUtils::EresRunMinidumpsUsingOneMDFile
			(
			pmp,
			szTpchPartsMD,
			rgszTpchPart + ulQueryNum - 1,
			1,
			&m_ulTestCounterParts,
			1,  // ulSessionId,
			1,  // ulCmdId
			false,  // fMatchPlans
			false  // fTestSpacePruning
			);
	}

	// reset metadata cache
	CMDCache::Reset();

	return eres;
}

// EOF
