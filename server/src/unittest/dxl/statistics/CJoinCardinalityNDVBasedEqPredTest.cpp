//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.

#include <stdint.h>
#include <cmath>

#include "naucrates/statistics/CStatisticsUtils.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "unittest/gpopt/CTestUtils.h"
#include "naucrates/statistics/CJoinStatsProcessor.h"
#include "unittest/dxl/statistics/CJoinCardinalityNDVBasedEqPredTest.h"

#define GPDB_INT4_ADD_OP OID(551)

/* These tests test the accuracy of cardinality estimation
 * for equijoins with predicates of the form f(a) = b
 * For a query T1 JOIN T2 WHERE T1.a + 1 = T2.b, the function
 * CalcAllJoinStats takes an array of statistics objects
 * for the tables being joined (T1 and T2)
 * and the join predicate(s) (T1.a + 1 = T2.b) and
 * accumulates the statistics in the array according to the
 * predicate comparison type and join type
 * returning a statistics object for the whole join
 * The statistics object has an estimated number of rows
 * which is the calculated estimated cardinality for the join
 */


namespace
{
	class Fixture
	{
		private:

			const CAutoMemoryPool m_amp;
			CMDAccessor m_mda;
			const CAutoOptCtxt m_aoc;
			static IMDProvider *Pmdp()
			{
				CTestUtils::m_pmdpf->AddRef();
				return CTestUtils::m_pmdpf;
			}

			IStatisticsArray *m_pdrgpstat;

		public:

			Fixture
					(
						const CHAR *file_name
					) :
					m_amp(),
					m_mda(m_amp.Pmp(), CMDCache::Pcache(), CTestUtils::m_sysidDefault, Pmdp()),
					m_aoc(m_amp.Pmp(), &m_mda, NULL /* pceeval */, CTestUtils::GetCostModel(m_amp.Pmp())),
					m_pdrgpstat(GPOS_NEW(m_amp.Pmp()) IStatisticsArray(m_amp.Pmp()))
			{
				CHAR *szDXLInput = CDXLUtils::Read(Pmp(), file_name);
				GPOS_CHECK_ABORT;
				// read the stats from the input xml
				CDXLStatsDerivedRelationArray *dxl_derived_rel_stats_array = CDXLUtils::ParseDXLToStatsDerivedRelArray(Pmp(), szDXLInput,
																								 NULL);
				CStatisticsArray *pdrgpstats = CDXLUtils::ParseDXLToOptimizerStatisticObjArray(Pmp(), &m_mda, dxl_derived_rel_stats_array);
				GPOS_ASSERT(pdrgpstats != NULL);
				GPOS_ASSERT(2 == pdrgpstats->Size());
				// ParseDXLToOptimizerStatisticObjArray returns an array of CStatistics (CStatisticsArray)
				// and PStatsJoinArray takes an array of IStatistics (IStatisticsArray) as input
				// So, iterate through CStatisticsArray and append members to a IStatisticsArray
				ULONG arity = pdrgpstats->Size();
				for (ULONG ul = 0; ul < arity; ul++)
				{
					IStatistics *stats = (*pdrgpstats)[ul];
					stats->AddRef();
					m_pdrgpstat->Append(stats);
				}
				pdrgpstats->Release();
				dxl_derived_rel_stats_array->Release();
				GPOS_DELETE_ARRAY(szDXLInput);
			}

			~Fixture()
			{
				m_pdrgpstat->Release();
			}

			IMemoryPool *Pmp() const
			{
				return m_amp.Pmp();
			}

			IStatisticsArray *PdrgPstat()
			{
				return m_pdrgpstat;
			}
	};
}

namespace gpnaucrates
{
	// input xml file has stats for 3 columns from 2 relations, i.e. T1 (0, 1) and T2(2)
	const CHAR *file_name = "../data/dxl/statistics/Join-Statistics-NDVBasedCardEstimation-EqPred-Input.xml";

	// test cardinality for predicates of the form: a + 1 = b
	// for such predicates, NDV based cardinality estimation is applicable
	GPOS_RESULT
	CJoinCardinalityNDVBasedEqPredTest::EresUnittest_NDVEqCardEstimation()
	{
		CDouble dRowsExpected(10000); // the minimum cardinality is min(NDV a, NDV b)

		Fixture f(file_name);
		IMemoryPool *mp = f.Pmp();
		IStatisticsArray *statistics_array = f.PdrgPstat();

		CExpression *pexprLgGet = CTestUtils::PexprLogicalGet(mp);
		CLogicalGet *popGet = CLogicalGet::PopConvert(pexprLgGet->Pop());
		CColRefArray *colref_array = popGet->PdrgpcrOutput();

		// use the colid available in the input xml file
		CColRef *pcrLeft = (*colref_array)[2];
		CColRef *pcrRight = (*colref_array)[0];

		// create a scalar ident
		// CScalarIdent "column_0000" (0)
		CExpression *pexprScalarIdentRight = CUtils::PexprScalarIdent(mp, pcrRight);

		// create a scalar op expression column_0002 + 10
		//  CScalarOp (+)
		//	|--CScalarIdent "column_0002" (2)
		//	+--CScalarConst (10)
		CExpression *pexprScConst = CUtils::PexprScalarConstInt4(mp, 10 /* val */);
		CExpression *pexprScOp = CUtils::PexprScalarOp(mp, pcrLeft, pexprScConst, CWStringConst(GPOS_WSZ_LIT("+")),
													   GPOS_NEW(mp) CMDIdGPDB(GPDB_INT4_ADD_OP));

		// create a scalar comparision operator
		//	+--CScalarCmp (=)
		//	|--CScalarOp (+)
		//	|  |--CScalarIdent "column_0002" (2)
		//	|  +--CScalarConst (10)
		//	+--CScalarIdent "column_0000" (0)
		CExpression *pScalarCmp = CUtils::PexprScalarEqCmp(mp, pexprScOp, pexprScalarIdentRight);

		IStatistics *join_stats = CJoinStatsProcessor::CalcAllJoinStats(mp, statistics_array, pScalarCmp,
																	   IStatistics::EsjtInnerJoin);

		GPOS_ASSERT(NULL != join_stats);
		CDouble dRowsActual(join_stats->Rows());
		GPOS_RESULT eres = GPOS_OK;

		if (std::floor(dRowsActual.Get()) != dRowsExpected)
		{
			eres = GPOS_FAILED;
		}

		join_stats->Release();
		pexprLgGet->Release();
		pScalarCmp->Release();

		return eres;
	}

	// test cardinality for predicates of the form: a + c = b
	// for such predicates, NDV based cardinality estimation is not applicable
	GPOS_RESULT
	CJoinCardinalityNDVBasedEqPredTest::EresUnittest_NDVCardEstimationNotApplicableMultipleIdents()
	{
		// cartesian product / 2.5
		// 2.5 = 1/.4 -- where .4 is default selectivity
		CDouble dRowsExpected(76004000);

		Fixture f(file_name);
		IMemoryPool *mp = f.Pmp();
		IStatisticsArray *statistics_array = f.PdrgPstat();

		CExpression *pexprLgGet = CTestUtils::PexprLogicalGet(mp);
		CLogicalGet *popGet = CLogicalGet::PopConvert(pexprLgGet->Pop());
		CColRefArray *colref_array = popGet->PdrgpcrOutput();

		// use the colid available in the input xml file
		CColRef *pcrLeft1 = (*colref_array)[2];
		CColRef *pcrLeft2 = (*colref_array)[1];
		CColRef *pcrRight = (*colref_array)[0];

		// create a scalar ident
		// CScalarIdent "column_0000" (0)
		CExpression *pexprScalarIdentRight = CUtils::PexprScalarIdent(mp, pcrRight);
		CExpression *pexprScalarIdentLeft2 = CUtils::PexprScalarIdent(mp, pcrLeft2);

		// create a scalar op expression column_0002 + column_0001
		//  CScalarOp (+)
		//	|--CScalarIdent "column_0002" (2)
		//	+--CScalarIdent "column_0001" (1)
		CExpression *pexprScOp = CUtils::PexprScalarOp(mp, pcrLeft1, pexprScalarIdentLeft2,
													   CWStringConst(GPOS_WSZ_LIT("+")),
													   GPOS_NEW(mp) CMDIdGPDB(GPDB_INT4_ADD_OP));

		// create a scalar comparision operator
		//	+--CScalarCmp (=)
		//	|--CScalarOp (+)
		//	|  |--CScalarIdent "column_0002" (2)
		//	|  +--CScalarIdent "column_0001" (1)
		//	+--CScalarIdent "column_0000" (0)
		CExpression *pScalarCmp = CUtils::PexprScalarEqCmp(mp, pexprScOp, pexprScalarIdentRight);
		IStatistics *join_stats = CJoinStatsProcessor::CalcAllJoinStats(mp, statistics_array, pScalarCmp,
																	   IStatistics::EsjtInnerJoin);

		GPOS_ASSERT(NULL != join_stats);
		CDouble dRowsActual(join_stats->Rows());

		GPOS_RESULT eres = GPOS_OK;
		if (floor(dRowsActual.Get()) != dRowsExpected)
		{
			eres = GPOS_FAILED;
		}

		join_stats->Release();
		pexprLgGet->Release();
		pScalarCmp->Release();

		return eres;
	}

	// test cardinality for predicates of the form: a + 1 < b
	// for such predicates, NDV based cardinality estimation is not applicable
	// note: any symbol other than = is not supported for NDV based cardinality estimation
	GPOS_RESULT
	CJoinCardinalityNDVBasedEqPredTest::EresUnittest_NDVCardEstimationNotApplicableInequalityRange()
	{
		// cartesian product / 2.5
		// 2.5 = 1/.4 -- where .4 is default selectivity
		CDouble dRowsExpected(76004000);

		Fixture f(file_name);
		IMemoryPool *mp = f.Pmp();
		IStatisticsArray *statistics_array = f.PdrgPstat();

		CExpression *pexprLgGet = CTestUtils::PexprLogicalGet(mp);
		CLogicalGet *popGet = CLogicalGet::PopConvert(pexprLgGet->Pop());
		CColRefArray *colref_array = popGet->PdrgpcrOutput();

		// use the colid available in the input xml file
		CColRef *pcrLeft = (*colref_array)[2];
		CColRef *pcrRight = (*colref_array)[0];

		// create a scalar ident
		// CScalarIdent "column_0000" (0)
		CExpression *pexprScalarIdentRight = CUtils::PexprScalarIdent(mp, pcrRight);

		// create a scalar op expression column_0002 + 10
		//  CScalarOp (+)
		//	|--CScalarIdent "column_0002" (2)
		//	+--CScalarConst (10)
		CExpression *pexprScConst = CUtils::PexprScalarConstInt4(mp, 10 /* val */);
		CExpression *pexprScOp = CUtils::PexprScalarOp(mp, pcrLeft, pexprScConst, CWStringConst(GPOS_WSZ_LIT("+")),
													   GPOS_NEW(mp) CMDIdGPDB(GPDB_INT4_ADD_OP));

		// create a scalar comparision operator
		//	+--CScalarCmp (<=)
		//	|--CScalarOp (+)
		//	|  |--CScalarIdent "column_0002" (2)
		//	|  +--CScalarConst (10)
		//	+--CScalarIdent "column_0000" (0)
		CExpression *pScalarCmp = CUtils::PexprScalarCmp(mp,
														 pexprScOp,
														 pexprScalarIdentRight,
														 IMDType::EcmptLEq
		);

		IStatistics *join_stats = CJoinStatsProcessor::CalcAllJoinStats(mp, statistics_array, pScalarCmp,
																	   IStatistics::EsjtInnerJoin);

		GPOS_ASSERT(NULL != join_stats);
		CDouble dRowsActual(join_stats->Rows());

		GPOS_RESULT eres = GPOS_OK;
		if (floor(dRowsActual.Get()) != dRowsExpected)
		{
			eres = GPOS_FAILED;
		}

		join_stats->Release();
		pexprLgGet->Release();
		pScalarCmp->Release();

		return eres;
	}


	GPOS_RESULT
	CJoinCardinalityNDVBasedEqPredTest::EresUnittest()
	{
		CUnittest rgut[] =
				{
						GPOS_UNITTEST_FUNC(EresUnittest_NDVEqCardEstimation),
						GPOS_UNITTEST_FUNC(EresUnittest_NDVCardEstimationNotApplicableMultipleIdents),
						GPOS_UNITTEST_FUNC(EresUnittest_NDVCardEstimationNotApplicableInequalityRange)
				};

		GPOS_RESULT eres = CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));

		return eres;
	}
}

// EOF
