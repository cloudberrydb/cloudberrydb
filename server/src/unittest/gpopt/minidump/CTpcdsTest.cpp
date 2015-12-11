//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CTpcdsTest.cpp
//
//	@doc:
//		Test for optimizing TPC-DS queries
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

#include "unittest/gpopt/minidump/CTpcdsTest.h"

using namespace gpopt;


ULONG CTpcdsTest::m_ulTestCounter = 0;  // start from first test
ULONG CTpcdsTest::m_ulTestCounterParts = 0;  // start from first test

// TODO:  - Jan 10, 2013; enable the following test queries
// after fixing the corresponding CTE bugs and generating new minidumps
// for them: q18, q22, q27, q77a

// TODO:  - Jan 29, 2013; enable the following test queries after
// we support the new implementation of full-outer join 51, 51a, 97

// const object that stores specifications for different flavors of a single
// TPC-DS query
struct SQuerySpec {
	// maximum number of entries in one of the arrays of minidump paths
	// below; increase if needed
	static const ULONG s_ulMaxPaths = 3;

	const CHAR *rgszMinidumps[s_ulMaxPaths];
	const CHAR *rgszMinidumpsPartitioned[s_ulMaxPaths];
	const CHAR *rgszMinidumpsPlanMatching[s_ulMaxPaths];
	const CHAR *rgszMinidumpsPartitionedPlanMatching[s_ulMaxPaths];
	const bool bComplex;  // if true, stats derivation is disabled to save on memory

	// count number of paths actually defined in 'rgszPathArray'
	static ULONG ulNumPaths(const CHAR** rgszPathArray) {
		for (ULONG ulPathIdx = 0u; ulPathIdx < s_ulMaxPaths; ++ulPathIdx) {
			if (NULL == rgszPathArray[ulPathIdx]) {
				return ulPathIdx;
			}
		}
		return s_ulMaxPaths;
	}
};

// macro that initializes an SQuerySpec for 'ulQueryNum' that has only a single
// variation and is not tested with plan-matching
#define QUERY_SPEC_SIMPLE(ulQueryNum)							\
	{										\
		{"../data/dxl/tpcds/tpcds_query" #ulQueryNum ".mdp"},			\
		{"../data/dxl/tpcds-partitioned/tpcds_query" #ulQueryNum ".mdp"},	\
		{},									\
		{},									\
		false									\
	}

// macro that initializes an SQuerySpec for 'ulQueryNum' that has a base version
// and a variation with suffix "a"
#define QUERY_SPEC_WITH_QUERY_A(ulQueryNum)							\
	{											\
		{										\
			"../data/dxl/tpcds/tpcds_query" #ulQueryNum ".mdp",			\
			"../data/dxl/tpcds/tpcds_query" #ulQueryNum "a.mdp",			\
		},										\
		{										\
			"../data/dxl/tpcds-partitioned/tpcds_query" #ulQueryNum ".mdp",		\
			"../data/dxl/tpcds-partitioned/tpcds_query" #ulQueryNum "a.mdp",	\
		},										\
		{},										\
		{},										\
		false										\
	}

SQuerySpec rgTpcdsByQuery[] =
{
	QUERY_SPEC_SIMPLE(1),
	QUERY_SPEC_SIMPLE(2),
	QUERY_SPEC_SIMPLE(3),
	QUERY_SPEC_SIMPLE(4),
	QUERY_SPEC_WITH_QUERY_A(5),
	QUERY_SPEC_SIMPLE(6),
	QUERY_SPEC_SIMPLE(7),
	QUERY_SPEC_SIMPLE(8),
	QUERY_SPEC_SIMPLE(9),
	QUERY_SPEC_SIMPLE(10),
	{
		{"../data/dxl/tpcds/tpcds_query11.mdp"},
		{"../data/dxl/tpcds-partitioned/tpcds_query11.mdp"},
		{"../data/dxl/tpcds/tpcds_query11.mdp"},
		{"../data/dxl/tpcds-partitioned/tpcds_query11.mdp"},
		false
	},
	QUERY_SPEC_SIMPLE(12),
	QUERY_SPEC_SIMPLE(13),
	{
		{
			"../data/dxl/tpcds/tpcds_query14a.mdp",
			"../data/dxl/tpcds/tpcds_query14b.mdp"
		},
		{
			"../data/dxl/tpcds-partitioned/tpcds_query14.mdp",
			"../data/dxl/tpcds-partitioned/tpcds_query14a.mdp",
			"../data/dxl/tpcds-partitioned/tpcds_query14b.mdp"
		},
		{},
		{},
		false
	},
	QUERY_SPEC_SIMPLE(15),
	QUERY_SPEC_SIMPLE(16),
	QUERY_SPEC_SIMPLE(17),
	QUERY_SPEC_WITH_QUERY_A(18),
	QUERY_SPEC_SIMPLE(19),
	QUERY_SPEC_SIMPLE(20),
	QUERY_SPEC_SIMPLE(21),
	{
		{"../data/dxl/tpcds/tpcds_query22a.mdp"},
		{},
		{"../data/dxl/tpcds/tpcds_query22.mdp"},
		{"../data/dxl/tpcds-partitioned/tpcds_query22.mdp"},
		false
	},
	QUERY_SPEC_SIMPLE(23),
	QUERY_SPEC_SIMPLE(24),
	QUERY_SPEC_SIMPLE(25),
	QUERY_SPEC_SIMPLE(26),
	QUERY_SPEC_WITH_QUERY_A(27),
	QUERY_SPEC_SIMPLE(28),
	QUERY_SPEC_SIMPLE(29),
	QUERY_SPEC_SIMPLE(30),
	QUERY_SPEC_SIMPLE(31),
	QUERY_SPEC_SIMPLE(32),
	QUERY_SPEC_SIMPLE(33),
	QUERY_SPEC_SIMPLE(34),
	QUERY_SPEC_SIMPLE(35),
	QUERY_SPEC_WITH_QUERY_A(36),
	QUERY_SPEC_SIMPLE(37),
	QUERY_SPEC_SIMPLE(38),
	QUERY_SPEC_SIMPLE(39),
	QUERY_SPEC_SIMPLE(40),
	QUERY_SPEC_SIMPLE(41),
	QUERY_SPEC_SIMPLE(42),
	QUERY_SPEC_SIMPLE(43),
	QUERY_SPEC_SIMPLE(44),
	QUERY_SPEC_SIMPLE(45),
	QUERY_SPEC_SIMPLE(46),
	QUERY_SPEC_SIMPLE(47),
	QUERY_SPEC_SIMPLE(48),
	QUERY_SPEC_SIMPLE(49),
	QUERY_SPEC_SIMPLE(50),
	QUERY_SPEC_WITH_QUERY_A(51),
	QUERY_SPEC_SIMPLE(52),
	QUERY_SPEC_SIMPLE(53),
	QUERY_SPEC_SIMPLE(54),
	QUERY_SPEC_SIMPLE(55),
	QUERY_SPEC_SIMPLE(56),
	QUERY_SPEC_SIMPLE(57),
	QUERY_SPEC_SIMPLE(58),
	QUERY_SPEC_SIMPLE(59),
	QUERY_SPEC_SIMPLE(60),
	QUERY_SPEC_SIMPLE(61),
	QUERY_SPEC_SIMPLE(62),
	QUERY_SPEC_SIMPLE(63),
	{
		{"../data/dxl/tpcds/tpcds_query64.mdp"},
		{"../data/dxl/tpcds-partitioned/tpcds_query64.mdp"},
		{},
		{},
		true
	},
	QUERY_SPEC_SIMPLE(65),
	QUERY_SPEC_SIMPLE(66),
	QUERY_SPEC_WITH_QUERY_A(67),
	QUERY_SPEC_SIMPLE(68),
	QUERY_SPEC_SIMPLE(69),
	QUERY_SPEC_WITH_QUERY_A(70),
	QUERY_SPEC_SIMPLE(71),
	QUERY_SPEC_SIMPLE(72),
	QUERY_SPEC_SIMPLE(73),
	QUERY_SPEC_SIMPLE(74),
	QUERY_SPEC_SIMPLE(75),
	QUERY_SPEC_SIMPLE(76),
	{
		{"../data/dxl/tpcds/tpcds_query77.mdp", "../data/dxl/tpcds/tpcds_query77a.mdp"},
		{"../data/dxl/tpcds-partitioned/tpcds_query77.mdp"},
		{},
		{},
		false
	},
	QUERY_SPEC_SIMPLE(78),
	QUERY_SPEC_SIMPLE(79),
	QUERY_SPEC_WITH_QUERY_A(80),
	QUERY_SPEC_SIMPLE(81),
	QUERY_SPEC_SIMPLE(82),
	QUERY_SPEC_SIMPLE(83),
	QUERY_SPEC_SIMPLE(84),
	QUERY_SPEC_SIMPLE(85),
	QUERY_SPEC_WITH_QUERY_A(86),
	QUERY_SPEC_SIMPLE(87),
	QUERY_SPEC_SIMPLE(88),
	QUERY_SPEC_SIMPLE(89),
	QUERY_SPEC_SIMPLE(90),
	QUERY_SPEC_SIMPLE(91),
	QUERY_SPEC_SIMPLE(92),
	QUERY_SPEC_SIMPLE(93),
	QUERY_SPEC_SIMPLE(94),
	QUERY_SPEC_SIMPLE(95),
	QUERY_SPEC_SIMPLE(96),
	QUERY_SPEC_SIMPLE(97),
	QUERY_SPEC_SIMPLE(98),
	QUERY_SPEC_SIMPLE(99)
};

#undef QUERY_SPEC_SIMPLE
#undef QUERY_SPEC_WITH_QUERY_A

// queries on partitioned tpcds schema
const CHAR *szMDTpcdsPartsMD = "../data/dxl/tpcds-partitioned/tpcds_partitioned.xml";

//---------------------------------------------------------------------------
//	@function:
//		CTpcdsTest::EresSubtest
//
//	@doc:
//		Run all tests for one particular TPC-DS query
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTpcdsTest::EresSubtest(ULONG ulQueryNum)
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

#ifdef GPOS_DEBUG
	const ULONG ulTests = GPOS_ARRAY_SIZE(rgTpcdsByQuery);
	GPOS_ASSERT(ulQueryNum > 0);
	GPOS_ASSERT(ulQueryNum - 1 < ulTests);
#endif  // GPOS_DEBUG

	// enable (Redistribute, Broadcast) hash join plans
	CAutoTraceFlag atf(EopttraceEnableRedistributeBroadcastHashJoin, true /*fVal*/);

	SQuerySpec& query_spec = rgTpcdsByQuery[ulQueryNum - 1];
	GPOS_RESULT eres = GPOS_OK;

	// basic test; no partitioning or plan-matching
	if (SQuerySpec::ulNumPaths(query_spec.rgszMinidumps)) {
		// enable space pruning
		CAutoTraceFlag atf(EopttraceEnableSpacePruning, true /*fVal*/);

		eres = CTestUtils::EresRunMinidumps(
			pmp,
			query_spec.rgszMinidumps,
			SQuerySpec::ulNumPaths(query_spec.rgszMinidumps),
			&m_ulTestCounter,
			1, // ulSessionId
			1,  // ulCmdId
			false, // fMatchPlans
			true // fTestSpacePruning
		);
	}
	if (eres != GPOS_OK) {
		return eres;
	}

	// test with partitioned tables
	if (SQuerySpec::ulNumPaths(query_spec.rgszMinidumps)) {
		// enable stats derivation for DPE
		CAutoTraceFlag atf1(EopttraceDeriveStatsForDPE, !query_spec.bComplex /*fVal*/);

		// enable space pruning
		CAutoTraceFlag atf2(EopttraceEnableSpacePruning, true /*fVal*/);

		eres = CTestUtils::EresRunMinidumpsUsingOneMDFile(
			pmp,
			szMDTpcdsPartsMD,
			query_spec.rgszMinidumpsPartitioned,
			SQuerySpec::ulNumPaths(query_spec.rgszMinidumpsPartitioned),
			&m_ulTestCounterParts,
			1, // ulSessionId
			1,  // ulCmdId
			false, // fMatchPlans
			false, // fTestSpacePruning
			NULL // pceeval
		);

		m_ulTestCounterParts = 0;
	}
	if (eres != GPOS_OK) {
		return eres;
	}

	// test with plan-matching
	if (SQuerySpec::ulNumPaths(query_spec.rgszMinidumpsPlanMatching)) {
		eres = CTestUtils::EresRunMinidumps(
			pmp,
			query_spec.rgszMinidumpsPlanMatching,
			SQuerySpec::ulNumPaths(query_spec.rgszMinidumpsPlanMatching),
			&m_ulTestCounter,
			1, // ulSessionId
			1,  // ulCmdId
			true, // fMatchPlans
			true // fTestSpacePruning
		);
	}
	if (eres != GPOS_OK) {
		return eres;
	}

	// test with partitioning and plan matching
	if (SQuerySpec::ulNumPaths(query_spec.rgszMinidumpsPartitionedPlanMatching)) {
		// enable stats derivation for DPE
		CAutoTraceFlag atf(EopttraceDeriveStatsForDPE, true /*fVal*/);

		eres = CTestUtils::EresRunMinidumpsUsingOneMDFile(
			pmp,
			szMDTpcdsPartsMD,
			query_spec.rgszMinidumpsPartitionedPlanMatching,
			SQuerySpec::ulNumPaths(query_spec.rgszMinidumpsPartitionedPlanMatching),
			&m_ulTestCounterParts,
			1, // ulSessionId
			1,  // ulCmdId
			true, // fMatchPlans
			true, // fTestSpacePruning
			NULL // pceeval
		);
	}

	return eres;
}

// EOF
