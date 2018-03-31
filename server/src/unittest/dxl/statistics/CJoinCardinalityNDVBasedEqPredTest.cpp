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
 * PstatsJoinArray takes an array of statistics objects
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

			DrgPstat *m_pdrgpstat;

		public:

			Fixture
					(
						const CHAR *szFileName
					) :
					m_amp(),
					m_mda(m_amp.Pmp(), CMDCache::Pcache(), CTestUtils::m_sysidDefault, Pmdp()),
					m_aoc(m_amp.Pmp(), &m_mda, NULL /* pceeval */, CTestUtils::Pcm(m_amp.Pmp())),
					m_pdrgpstat(GPOS_NEW(m_amp.Pmp()) DrgPstat(m_amp.Pmp()))
			{
				CHAR *szDXLInput = CDXLUtils::SzRead(Pmp(), szFileName);
				GPOS_CHECK_ABORT;
				// read the stats from the input xml
				DrgPdxlstatsderrel *pdrgpdxlstatsderrel = CDXLUtils::PdrgpdxlstatsderrelParseDXL(Pmp(), szDXLInput,
																								 NULL);
				DrgPstats *pdrgpstats = CDXLUtils::PdrgpstatsTranslateStats(Pmp(), &m_mda, pdrgpdxlstatsderrel);
				GPOS_ASSERT(pdrgpstats != NULL);
				GPOS_ASSERT(2 == pdrgpstats->UlLength());
				// PdrgpstatsTranslateStats returns an array of CStatistics (DrgPstats)
				// and PStatsJoinArray takes an array of IStatistics (DrgPstat) as input
				// So, iterate through DrgPstats and append members to a DrgPstat
				ULONG ulArity = pdrgpstats->UlLength();
				for (ULONG ul = 0; ul < ulArity; ul++)
				{
					IStatistics *pstats = (*pdrgpstats)[ul];
					pstats->AddRef();
					m_pdrgpstat->Append(pstats);
				}
				pdrgpstats->Release();
				pdrgpdxlstatsderrel->Release();
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

			DrgPstat *PdrgPstat()
			{
				return m_pdrgpstat;
			}
	};
}

namespace gpnaucrates
{
	// input xml file has stats for 3 columns from 2 relations, i.e. T1 (0, 1) and T2(2)
	const CHAR *szFileName = "../data/dxl/statistics/Join-Statistics-NDVBasedCardEstimation-EqPred-Input.xml";

	// test cardinality for predicates of the form: a + 1 = b
	// for such predicates, NDV based cardinality estimation is applicable
	GPOS_RESULT
	CJoinCardinalityNDVBasedEqPredTest::EresUnittest_NDVEqCardEstimation()
	{
		CDouble dRowsExpected(10000); // the minimum cardinality is min(NDV a, NDV b)

		Fixture f(szFileName);
		IMemoryPool *pmp = f.Pmp();
		DrgPstat *pdrgpstat = f.PdrgPstat();

		CExpression *pexprLgGet = CTestUtils::PexprLogicalGet(pmp);
		CLogicalGet *popGet = CLogicalGet::PopConvert(pexprLgGet->Pop());
		DrgPcr *pdrgpcr = popGet->PdrgpcrOutput();

		// use the colid available in the input xml file
		CColRef *pcrLeft = (*pdrgpcr)[2];
		CColRef *pcrRight = (*pdrgpcr)[0];

		// create a scalar ident
		// CScalarIdent "column_0000" (0)
		CExpression *pexprScalarIdentRight = CUtils::PexprScalarIdent(pmp, pcrRight);

		// create a scalar op expression column_0002 + 10
		//  CScalarOp (+)
		//	|--CScalarIdent "column_0002" (2)
		//	+--CScalarConst (10)
		CExpression *pexprScConst = CUtils::PexprScalarConstInt4(pmp, 10 /* iVal */);
		CExpression *pexprScOp = CUtils::PexprScalarOp(pmp, pcrLeft, pexprScConst, CWStringConst(GPOS_WSZ_LIT("+")),
													   GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_ADD_OP));

		// create a scalar comparision operator
		//	+--CScalarCmp (=)
		//	|--CScalarOp (+)
		//	|  |--CScalarIdent "column_0002" (2)
		//	|  +--CScalarConst (10)
		//	+--CScalarIdent "column_0000" (0)
		CExpression *pScalarCmp = CUtils::PexprScalarEqCmp(pmp, pexprScOp, pexprScalarIdentRight);

		IStatistics *pstatsJoin = CJoinStatsProcessor::PstatsJoinArray(pmp, pdrgpstat, pScalarCmp,
																	   IStatistics::EsjtInnerJoin);

		GPOS_ASSERT(NULL != pstatsJoin);
		CDouble dRowsActual(pstatsJoin->DRows());
		GPOS_RESULT eres = GPOS_OK;

		if (std::floor(dRowsActual.DVal()) != dRowsExpected)
		{
			eres = GPOS_FAILED;
		}

		pstatsJoin->Release();
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

		Fixture f(szFileName);
		IMemoryPool *pmp = f.Pmp();
		DrgPstat *pdrgpstat = f.PdrgPstat();

		CExpression *pexprLgGet = CTestUtils::PexprLogicalGet(pmp);
		CLogicalGet *popGet = CLogicalGet::PopConvert(pexprLgGet->Pop());
		DrgPcr *pdrgpcr = popGet->PdrgpcrOutput();

		// use the colid available in the input xml file
		CColRef *pcrLeft1 = (*pdrgpcr)[2];
		CColRef *pcrLeft2 = (*pdrgpcr)[1];
		CColRef *pcrRight = (*pdrgpcr)[0];

		// create a scalar ident
		// CScalarIdent "column_0000" (0)
		CExpression *pexprScalarIdentRight = CUtils::PexprScalarIdent(pmp, pcrRight);
		CExpression *pexprScalarIdentLeft2 = CUtils::PexprScalarIdent(pmp, pcrLeft2);

		// create a scalar op expression column_0002 + column_0001
		//  CScalarOp (+)
		//	|--CScalarIdent "column_0002" (2)
		//	+--CScalarIdent "column_0001" (1)
		CExpression *pexprScOp = CUtils::PexprScalarOp(pmp, pcrLeft1, pexprScalarIdentLeft2,
													   CWStringConst(GPOS_WSZ_LIT("+")),
													   GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_ADD_OP));

		// create a scalar comparision operator
		//	+--CScalarCmp (=)
		//	|--CScalarOp (+)
		//	|  |--CScalarIdent "column_0002" (2)
		//	|  +--CScalarIdent "column_0001" (1)
		//	+--CScalarIdent "column_0000" (0)
		CExpression *pScalarCmp = CUtils::PexprScalarEqCmp(pmp, pexprScOp, pexprScalarIdentRight);
		IStatistics *pstatsJoin = CJoinStatsProcessor::PstatsJoinArray(pmp, pdrgpstat, pScalarCmp,
																	   IStatistics::EsjtInnerJoin);

		GPOS_ASSERT(NULL != pstatsJoin);
		CDouble dRowsActual(pstatsJoin->DRows());

		GPOS_RESULT eres = GPOS_OK;
		if (floor(dRowsActual.DVal()) != dRowsExpected)
		{
			eres = GPOS_FAILED;
		}

		pstatsJoin->Release();
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

		Fixture f(szFileName);
		IMemoryPool *pmp = f.Pmp();
		DrgPstat *pdrgpstat = f.PdrgPstat();

		CExpression *pexprLgGet = CTestUtils::PexprLogicalGet(pmp);
		CLogicalGet *popGet = CLogicalGet::PopConvert(pexprLgGet->Pop());
		DrgPcr *pdrgpcr = popGet->PdrgpcrOutput();

		// use the colid available in the input xml file
		CColRef *pcrLeft = (*pdrgpcr)[2];
		CColRef *pcrRight = (*pdrgpcr)[0];

		// create a scalar ident
		// CScalarIdent "column_0000" (0)
		CExpression *pexprScalarIdentRight = CUtils::PexprScalarIdent(pmp, pcrRight);

		// create a scalar op expression column_0002 + 10
		//  CScalarOp (+)
		//	|--CScalarIdent "column_0002" (2)
		//	+--CScalarConst (10)
		CExpression *pexprScConst = CUtils::PexprScalarConstInt4(pmp, 10 /* iVal */);
		CExpression *pexprScOp = CUtils::PexprScalarOp(pmp, pcrLeft, pexprScConst, CWStringConst(GPOS_WSZ_LIT("+")),
													   GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_ADD_OP));

		// create a scalar comparision operator
		//	+--CScalarCmp (<=)
		//	|--CScalarOp (+)
		//	|  |--CScalarIdent "column_0002" (2)
		//	|  +--CScalarConst (10)
		//	+--CScalarIdent "column_0000" (0)
		CExpression *pScalarCmp = CUtils::PexprScalarCmp(pmp,
														 pexprScOp,
														 pexprScalarIdentRight,
														 IMDType::EcmptLEq
		);

		IStatistics *pstatsJoin = CJoinStatsProcessor::PstatsJoinArray(pmp, pdrgpstat, pScalarCmp,
																	   IStatistics::EsjtInnerJoin);

		GPOS_ASSERT(NULL != pstatsJoin);
		CDouble dRowsActual(pstatsJoin->DRows());

		GPOS_RESULT eres = GPOS_OK;
		if (floor(dRowsActual.DVal()) != dRowsExpected)
		{
			eres = GPOS_FAILED;
		}

		pstatsJoin->Release();
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
