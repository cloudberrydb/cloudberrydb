//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CStatisticsTest.cpp
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
#include "naucrates/statistics/CBucket.h"
#include "naucrates/statistics/CHistogram.h"
#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CStatisticsUtils.h"
#include "naucrates/statistics/CStatisticsUtils.h"
#include "naucrates/statistics/CLimitStatsProcessor.h"
#include "naucrates/statistics/CGroupByStatsProcessor.h"
#include "naucrates/statistics/CUnionAllStatsProcessor.h"
#include "naucrates/statistics/CFilterStatsProcessor.h"

#include "naucrates/base/CDatumGenericGPDB.h"
#include "naucrates/base/CDatumInt4GPDB.h"
#include "naucrates/base/CDatumBoolGPDB.h"

#include "gpopt/base/CQueryContext.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CScalarProjectElement.h"

#include "naucrates/dxl/CDXLUtils.h"

#include "unittest/base.h"
#include "unittest/dxl/statistics/CCardinalityTestUtils.h"
#include "unittest/dxl/statistics/CStatisticsTest.h"
#include "unittest/gpopt/CTestUtils.h"

#include "naucrates/md/IMDType.h"
#include "naucrates/md/CMDTypeGenericGPDB.h"

using namespace gpopt;

const CHAR *
szQuerySelect = "../data/dxl/statistics/SelectQuery.xml";
const CHAR *
szPlanSelect = "../data/dxl/statistics/SelectPlan.xml";

// unittest for statistics objects
GPOS_RESULT
CStatisticsTest::EresUnittest()
{
	// tests that use shared optimization context
	CUnittest rgutSharedOptCtxt[] =
		{
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CStatisticsBasic),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_UnionAll),
		// TODO,  Mar 18 2013 temporarily disabling the test
		// GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CStatisticsSelectDerivation),
		};

	// tests that use separate optimization contexts
	CUnittest rgutSeparateOptCtxt[] =
		{
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_GbAggWithRepeatedGbCols),
		};

	// run tests with shared optimization context first
	GPOS_RESULT eres = GPOS_FAILED;
	{
		CAutoMemoryPool amp;
		IMemoryPool *pmp = amp.Pmp();

		// setup a file-based provider
		CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
		pmdp->AddRef();
		CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

		// install opt context in TLS
		CAutoOptCtxt aoc
						(
						pmp,
						&mda,
						NULL /* pceeval */,
						CTestUtils::Pcm(pmp)
						);

		eres = CUnittest::EresExecute(rgutSharedOptCtxt, GPOS_ARRAY_SIZE(rgutSharedOptCtxt));

		if (GPOS_FAILED == eres)
		{
			return eres;
		}
	}

	// run tests with separate optimization contexts
	return CUnittest::EresExecute(rgutSeparateOptCtxt, GPOS_ARRAY_SIZE(rgutSeparateOptCtxt));
}

// testing statistical operations on Union All;
GPOS_RESULT
CStatisticsTest::EresUnittest_UnionAll()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	SStatsUnionAllSTestCase rgstatsunionalltc[] =
	{
		{"../data/dxl/statistics/UnionAll-Input-1.xml", "../data/dxl/statistics/UnionAll-Output-1.xml"},
	};

	const ULONG ulTestCases = GPOS_ARRAY_SIZE(rgstatsunionalltc);
	for (ULONG ul = 0; ul < ulTestCases; ul++)
	{
		SStatsUnionAllSTestCase elem = rgstatsunionalltc[ul];

		// read input/output DXL file
		CHAR *szDXLInput = CDXLUtils::SzRead(pmp, elem.m_szInputFile);
		CHAR *szDXLOutput = CDXLUtils::SzRead(pmp, elem.m_szOutputFile);

		GPOS_CHECK_ABORT;

		// parse the input statistics objects
		DrgPdxlstatsderrel *pdrgpdxlstatsderrel = CDXLUtils::PdrgpdxlstatsderrelParseDXL(pmp, szDXLInput, NULL);
		DrgPstats *pdrgpstatBefore = CDXLUtils::PdrgpstatsTranslateStats(pmp, pmda, pdrgpdxlstatsderrel);
		pdrgpdxlstatsderrel->Release();

		GPOS_ASSERT(NULL != pdrgpstatBefore);
		GPOS_ASSERT(2 == pdrgpstatBefore->UlLength());
		CStatistics *pstats1 = (*pdrgpstatBefore)[0];
		CStatistics *pstats2 = (*pdrgpstatBefore)[1];

		GPOS_CHECK_ABORT;

		DrgPul *pdrgpulColIdOutput = Pdrgpul(pmp, 1);
		DrgPul *pdrgpulColIdInput1 = Pdrgpul(pmp, 1);
		DrgPul *pdrgpulColIdInput2 = Pdrgpul(pmp, 2);

		CStatistics *pstatsOutput = CUnionAllStatsProcessor::PstatsUnionAll(pmp, pstats1, pstats2, pdrgpulColIdOutput, pdrgpulColIdInput1, pdrgpulColIdInput2);

		GPOS_ASSERT(NULL != pstatsOutput);

		DrgPstats *pdrgpstatOutput = GPOS_NEW(pmp) DrgPstats(pmp);
		pdrgpstatOutput->Append(pstatsOutput);

		// serialize and compare against expected stats
		CWStringDynamic *pstrOutput = CDXLUtils::PstrSerializeStatistics
													(
													pmp,
													pmda,
													pdrgpstatOutput,
													true /*fSerializeHeaderFooter*/,
													true /*fIndent*/
													);
		CWStringDynamic dstrExpected(pmp);
		dstrExpected.AppendFormat(GPOS_WSZ_LIT("%s"), szDXLOutput);

		GPOS_RESULT eres = GPOS_OK;
		CWStringDynamic str(pmp);
		COstreamString oss(&str);

		// compare the two dxls
		if (!pstrOutput->FEquals(&dstrExpected))
		{
			oss << "Output does not match expected DXL document" << std::endl;
			oss << "Actual: " << std::endl;
			oss << pstrOutput->Wsz() << std::endl;
			oss << "Expected: " << std::endl;
			oss << dstrExpected.Wsz() << std::endl;
			GPOS_TRACE(str.Wsz());
			
			eres = GPOS_FAILED;
		}


		// clean up
		pdrgpstatBefore->Release();
		pdrgpstatOutput->Release();

		GPOS_DELETE_ARRAY(szDXLInput);
		GPOS_DELETE_ARRAY(szDXLOutput);
		GPOS_DELETE(pstrOutput);

		if (GPOS_FAILED == eres)
		{
			return eres;
		}
	}

	return GPOS_OK;
}

// gbAgg test when grouping on repeated columns
GPOS_RESULT
CStatisticsTest::EresUnittest_GbAggWithRepeatedGbCols()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// install opt context in TLS
	CAutoOptCtxt aoc
					(
					pmp,
					&mda,
					NULL /* pceeval */,
					CTestUtils::Pcm(pmp)
					);

	CExpression *pexpr = CTestUtils::PexprLogicalJoin<CLogicalInnerJoin>(pmp);
	CDrvdPropRelational *pdprel = CDrvdPropRelational::Pdprel(pexpr->PdpDerive());
	CColRefSet *pcrs = pdprel->PcrsOutput();

	// create first GbAgg expression: GbAgg on top of given expression
	DrgPcr *pdrgpcr1 = GPOS_NEW(pmp) DrgPcr(pmp);
	pdrgpcr1->Append(pcrs->PcrFirst());
	CExpression *pexprGbAgg1 =
		CUtils::PexprLogicalGbAggGlobal(pmp, pdrgpcr1, pexpr, GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp)));

	// create second GbAgg expression: GbAgg with repeated base column on top of given expression
	DrgPcr *pdrgpcr2 = GPOS_NEW(pmp) DrgPcr(pmp);
	pdrgpcr2->Append(pcrs->PcrFirst());
	pdrgpcr2->Append(pcrs->PcrFirst());
	pexpr->AddRef();
	CExpression *pexprGbAgg2 =
			CUtils::PexprLogicalGbAggGlobal(pmp, pdrgpcr2, pexpr, GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp)));

	// create third GbAgg expression: GbAgg with a repeated projected base column on top of given expression
	pexpr->AddRef();
	CExpression *pexprPrj = CUtils::PexprAddProjection(pmp, pexpr, CUtils::PexprScalarIdent(pmp, pcrs->PcrFirst()));
	CColRef *pcrComputed = CScalarProjectElement::PopConvert((*(*pexprPrj)[1])[0]->Pop())->Pcr();
	DrgPcr *pdrgpcr3 = GPOS_NEW(pmp) DrgPcr(pmp);
	pdrgpcr3->Append(pcrs->PcrFirst());
	pdrgpcr3->Append(pcrComputed);
	CExpression *pexprGbAgg3 =
			CUtils::PexprLogicalGbAggGlobal(pmp, pdrgpcr3, pexprPrj, GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp)));

	// derive stats on different GbAgg expressions
	CReqdPropRelational *prprel = GPOS_NEW(pmp) CReqdPropRelational(GPOS_NEW(pmp) CColRefSet(pmp));
	(void) pexprGbAgg1->PstatsDerive(prprel, NULL /* pdrgpstatCtxt */);
	(void) pexprGbAgg2->PstatsDerive(prprel, NULL /* pdrgpstatCtxt */);
	(void) pexprGbAgg3->PstatsDerive(prprel, NULL /* pdrgpstatCtxt */);

	BOOL fRows1EqualRows2 = (pexprGbAgg1->Pstats()->DRows() == pexprGbAgg2->Pstats()->DRows());
	BOOL fRows2EqualRows3 = (pexprGbAgg2->Pstats()->DRows() == pexprGbAgg3->Pstats()->DRows());

	{
		CAutoTrace at(pmp);
		at.Os() << std::endl << "pexprGbAgg1:" <<  std::endl << *pexprGbAgg1 << std::endl;
		at.Os() << std::endl << "pexprGbAgg2:" <<  std::endl << *pexprGbAgg2 << std::endl;
		at.Os() << std::endl << "pexprGbAgg3:" <<  std::endl << *pexprGbAgg3 << std::endl;
	}

	// cleanup
	pexprGbAgg1->Release();
	pexprGbAgg2->Release();
	pexprGbAgg3->Release();
	prprel->Release();

	if (fRows1EqualRows2 && fRows2EqualRows3)
	{
		return GPOS_OK;
	}

	return GPOS_FAILED;
}

// generates example int histogram corresponding to dimension table
CHistogram*
CStatisticsTest::PhistExampleInt4Dim
	(
	IMemoryPool *pmp
	)
{
	// generate histogram of the form [0, 10), [10, 20), [20, 30) ... [80, 90)
	DrgPbucket *pdrgppbucket = GPOS_NEW(pmp) DrgPbucket(pmp);
	for (ULONG ulIdx = 0; ulIdx < 9; ulIdx++)
	{
		INT iLower = INT(ulIdx * 10);
		INT iUpper = iLower + INT(10);
		CDouble dFrequency(0.1);
		CDouble dDistinct(10.0);
		CBucket *pbucket = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(pmp, iLower, iUpper, dFrequency, dDistinct);
		pdrgppbucket->Append(pbucket);
	}

	return  GPOS_NEW(pmp) CHistogram(pdrgppbucket);
}

// create a table descriptor with two columns having the given names.
CTableDescriptor *
CStatisticsTest::PtabdescTwoColumnSource
	(
	IMemoryPool *pmp,
	const CName &nameTable,
	const IMDTypeInt4 *pmdtype,
	const CWStringConst &strColA,
	const CWStringConst &strColB
	)
{
	CTableDescriptor *ptabdesc = GPOS_NEW(pmp) CTableDescriptor
									(
									pmp,
									GPOS_NEW(pmp) CMDIdGPDB(GPOPT_TEST_REL_OID1, 1, 1),
									nameTable,
									false, // fConvertHashToRandom
									IMDRelation::EreldistrRandom,
									IMDRelation::ErelstorageHeap,
									0  // ulExecuteAsUser
									);

	for (ULONG ul = 0; ul < 2; ul++)
	{
		// create a shallow constant string to embed in a name
		const CWStringConst *pstrName = &strColA;
		if (0 < ul)
		{
			pstrName = &strColB;
		}
		CName nameColumn(pstrName);

		CColumnDescriptor *pcoldesc = GPOS_NEW(pmp) CColumnDescriptor
											(
											pmp,
											pmdtype,
											IDefaultTypeModifier,
											nameColumn,
											ul + 1,
											false /*fNullable*/
											);
		ptabdesc->AddColumn(pcoldesc);
	}

	return ptabdesc;
}

// basic statistics test
GPOS_RESULT
CStatisticsTest::EresUnittest_CStatisticsBasic()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();

	const IMDTypeInt4 *pmdtypeint4 = COptCtxt::PoctxtFromTLS()->Pmda()->PtMDType<IMDTypeInt4>();

	CWStringConst strRelAlias(GPOS_WSZ_LIT("Rel1"));
	CWStringConst strColA(GPOS_WSZ_LIT("a"));
	CWStringConst strColB(GPOS_WSZ_LIT("b"));
	CTableDescriptor *ptabdesc =
			PtabdescTwoColumnSource(pmp, CName(&strRelAlias), pmdtypeint4, strColA, strColB);
	CExpression *pexprGet = CTestUtils::PexprLogicalGet(pmp, ptabdesc, &strRelAlias);

	if (NULL == pcf->PcrLookup(1 /*ulId*/))
	{
		// create column references for grouping columns
		(void) pcf->PcrCreate
				(
				pmdtypeint4,
				IDefaultTypeModifier,
				0 /* iAttno */,
				false /*FNullable*/,
				1 /* ulId */,
				CName(&strColA),
				pexprGet->Pop()->UlOpId()
				);
	}

	if (NULL == pcf->PcrLookup(2 /*ulId*/))
	{
		(void) pcf->PcrCreate
				(
				pmdtypeint4,
				IDefaultTypeModifier,
				1 /* iAttno */,
				false /*FNullable*/,
				2 /* ulId */,
				CName(&strColB),
				pexprGet->Pop()->UlOpId()
				);
	}

	// create hash map from colid -> histogram
	HMUlHist *phmulhist = GPOS_NEW(pmp) HMUlHist(pmp);

	// generate bool histogram for column 1
	phmulhist->FInsert(GPOS_NEW(pmp) ULONG(1), CCardinalityTestUtils::PhistExampleBool(pmp));

	// generate int histogram for column 2
	phmulhist->FInsert(GPOS_NEW(pmp) ULONG(2), CCardinalityTestUtils::PhistExampleInt4(pmp));

	// array capturing columns for which width information is available
	HMUlDouble *phmuldoubleWidth = GPOS_NEW(pmp) HMUlDouble(pmp);

	// width for boolean
	phmuldoubleWidth->FInsert(GPOS_NEW(pmp) ULONG(1), GPOS_NEW(pmp) CDouble(1.0));

	// width for int
	phmuldoubleWidth->FInsert(GPOS_NEW(pmp) ULONG(2), GPOS_NEW(pmp) CDouble(4.0));

	CStatistics *pstats = GPOS_NEW(pmp) CStatistics(pmp, phmulhist, phmuldoubleWidth, 1000.0 /* dRows */, false /* fEmpty */);
	pstats->DRows();

	GPOS_TRACE(GPOS_WSZ_LIT("pstats"));

	// before stats
	CCardinalityTestUtils::PrintStats(pmp, pstats);

	// create a filter: column 1: [25,45), column 2: [true, true)
	DrgPstatspred *pdrgpstatspred = Pdrgpstatspred1(pmp);

	CStatsPredConj *pstatspred = GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspred);
	CStatistics *pstats1 = CFilterStatsProcessor::PstatsFilter(pmp, pstats, pstatspred, true /* fCapNdvs */);
	pstats1->DRows();

	GPOS_TRACE(GPOS_WSZ_LIT("pstats1 after filter"));

	// after stats
	CCardinalityTestUtils::PrintStats(pmp, pstats1);

	// create another statistics structure with a single int4 column with id 10
	HMUlHist *phmulhist2 = GPOS_NEW(pmp) HMUlHist(pmp);
	phmulhist2->FInsert(GPOS_NEW(pmp) ULONG(10), PhistExampleInt4Dim(pmp));

	HMUlDouble *phmuldoubleWidth2 = GPOS_NEW(pmp) HMUlDouble(pmp);
	phmuldoubleWidth2->FInsert(GPOS_NEW(pmp) ULONG(10), GPOS_NEW(pmp) CDouble(4.0));

	CStatistics *pstats2 = GPOS_NEW(pmp) CStatistics(pmp, phmulhist2, phmuldoubleWidth2, 100.0 /* dRows */, false /* fEmpty */);

	GPOS_TRACE(GPOS_WSZ_LIT("pstats2"));
	CCardinalityTestUtils::PrintStats(pmp, pstats2);

	// join pstats with pstats2
	CStatsPredJoin *pstatspredjoin = GPOS_NEW(pmp) CStatsPredJoin(2, CStatsPred::EstatscmptEq, 10);
	DrgPstatspredjoin *pdrgpstatspredjoin = GPOS_NEW(pmp) DrgPstatspredjoin(pmp);
	pdrgpstatspredjoin->Append(pstatspredjoin);
	CStatistics *pstats3 = pstats->PstatsInnerJoin(pmp, pstats2, pdrgpstatspredjoin);

	GPOS_TRACE(GPOS_WSZ_LIT("pstats3 = pstats JOIN pstats2 on (col2 = col10)"));
	// after stats
	CCardinalityTestUtils::PrintStats(pmp, pstats3);

	// group by pstats on columns 1 and 2
	DrgPul *pdrgpulGC = GPOS_NEW(pmp) DrgPul(pmp);
	pdrgpulGC->Append(GPOS_NEW(pmp) ULONG(1));
	pdrgpulGC->Append(GPOS_NEW(pmp) ULONG(2));

	DrgPul *pdrgpulAgg = GPOS_NEW(pmp) DrgPul(pmp);
	CStatistics *pstats4 = CGroupByStatsProcessor::PstatsGroupBy(pmp, pstats, pdrgpulGC, pdrgpulAgg, NULL /*pbsKeys*/);

	GPOS_TRACE(GPOS_WSZ_LIT("pstats4 = pstats group by"));
	CCardinalityTestUtils::PrintStats(pmp, pstats4);

	// LASJ stats
	CStatistics *pstats5 = pstats->PstatsLASJoin(pmp, pstats2, pdrgpstatspredjoin, true /* fIgnoreLasjHistComputation */);

	GPOS_TRACE(GPOS_WSZ_LIT("pstats5 = pstats LASJ pstats2 on (col2 = col10)"));
	CCardinalityTestUtils::PrintStats(pmp, pstats5);

	// union all
	DrgPul *pdrgpulColIds = GPOS_NEW(pmp) DrgPul(pmp);
	pdrgpulColIds->Append(GPOS_NEW(pmp) ULONG(1));
	pdrgpulColIds->Append(GPOS_NEW(pmp) ULONG(2));
	pdrgpulColIds->AddRef();
	pdrgpulColIds->AddRef();
	pdrgpulColIds->AddRef();

	CStatistics *pstats6 = CUnionAllStatsProcessor::PstatsUnionAll(pmp, pstats, pstats, pdrgpulColIds, pdrgpulColIds, pdrgpulColIds);

	GPOS_TRACE(GPOS_WSZ_LIT("pstats6 = pstats1 union all pstats1"));
	CCardinalityTestUtils::PrintStats(pmp, pstats6);

	CStatistics *pstats7 = CLimitStatsProcessor::PstatsLimit(pmp, pstats, CDouble(4.0));

	GPOS_TRACE(GPOS_WSZ_LIT("pstats7 = pstats limit 4"));
	CCardinalityTestUtils::PrintStats(pmp, pstats7);

	pstats->Release();
	pstats1->Release();
	pstats2->Release();
	pstats3->Release();
	pstats4->Release();
	pstats5->Release();
	pstats6->Release();
	pstats7->Release();
	pstatspred->Release();
	pdrgpstatspredjoin->Release();
	pdrgpulGC->Release();
	pdrgpulAgg->Release();
	pdrgpulColIds->Release();
	pexprGet->Release();

	return GPOS_OK;
}

// create a filter clause
DrgPstatspred *
CStatisticsTest::Pdrgpstatspred1
	(
	IMemoryPool *pmp
	)
{
	DrgPstatspred *pdrgpstatspred = GPOS_NEW(pmp) DrgPstatspred(pmp);

	// col1 = true
	StatsFilterBool(pmp, 1, true, pdrgpstatspred);

	// col2 >= 25 and col2 < 35
	StatsFilterInt4(pmp, 2, 25, 35, pdrgpstatspred);

	return pdrgpstatspred;
}

// create a filter clause
DrgPstatspred *
CStatisticsTest::Pdrgpstatspred2
	(
	IMemoryPool *pmp
	)
{
	// contain for filters
	DrgPstatspred *pdrgpstatspred = GPOS_NEW(pmp) DrgPstatspred(pmp);

	// create int4 filter column 2: [5,15)::int4
	StatsFilterInt4(pmp, 2, 5, 15, pdrgpstatspred);

	// create numeric filter column 3: [1.0, 2.0)::numeric
	CWStringDynamic *pstrLowerNumeric = GPOS_NEW(pmp) CWStringDynamic(pmp, GPOS_WSZ_LIT("AAAACgAAAQABAA=="));
	CWStringDynamic *pstrUpperNumeric = GPOS_NEW(pmp) CWStringDynamic(pmp, GPOS_WSZ_LIT("AAAACgAAAQACAA=="));

	StatsFilterNumeric(pmp, 3, pstrLowerNumeric, pstrUpperNumeric, CDouble(1.0), CDouble(2.0), pdrgpstatspred);

	GPOS_DELETE(pstrLowerNumeric);
	GPOS_DELETE(pstrUpperNumeric);

	// create a date filter column 4: ['01-01-2012', '01-21-2012')::date
	CWStringDynamic *pstrLowerDate = GPOS_NEW(pmp) CWStringDynamic(pmp, GPOS_WSZ_LIT("HxEAAA=="));
	CWStringDynamic *pstrUpperDate = GPOS_NEW(pmp) CWStringDynamic(pmp, GPOS_WSZ_LIT("LREAAA=="));
	LINT lLowerDate = LINT(4383) * LINT(INT64_C(86400000000)); // microseconds per day
	LINT lUpperDate = LINT(4397) * LINT(INT64_C(86400000000)); // microseconds per day
	StatsFilterGeneric(pmp, 4, GPDB_DATE, pstrLowerDate, pstrUpperDate, lLowerDate, lUpperDate, pdrgpstatspred);

	GPOS_DELETE(pstrLowerDate);
	GPOS_DELETE(pstrUpperDate);

	// create timestamp filter column 5: ['01-01-2012 00:01:00', '01-01-2012 10:00:00')::timestamp
	CWStringDynamic *pstrLowerTS = GPOS_NEW(pmp) CWStringDynamic(pmp, GPOS_WSZ_LIT("ACcI7mpYAQA="));
	CWStringDynamic *pstrUpperTS = GPOS_NEW(pmp) CWStringDynamic(pmp, GPOS_WSZ_LIT("AAg5THNYAQA="));
	LINT lLowerTS = LINT(INT64_C(378691260000000)); // microseconds
	LINT lUpperTS = LINT(INT64_C(378727200000000)); // microseconds

	StatsFilterGeneric(pmp, 5, GPDB_TIMESTAMP, pstrLowerTS, pstrUpperTS, lLowerTS, lUpperTS, pdrgpstatspred);

	GPOS_DELETE(pstrLowerTS);
	GPOS_DELETE(pstrUpperTS);

	return pdrgpstatspred;
}

// create a stats filter on integer range
void
CStatisticsTest::StatsFilterInt4
	(
	IMemoryPool *pmp,
	ULONG ulColId,
	INT iLower,
	INT iUpper,
	DrgPstatspred *pdrgpstatspred
	)
{
	CStatsPredPoint *pstatspred1 = GPOS_NEW(pmp) CStatsPredPoint
												(
												ulColId,
												CStatsPred::EstatscmptGEq,
												CTestUtils::PpointInt4(pmp, iLower)
												);

	CStatsPredPoint *pstatspred2 = GPOS_NEW(pmp) CStatsPredPoint
												(
												ulColId,
												CStatsPred::EstatscmptL,
												CTestUtils::PpointInt4(pmp, iUpper)
												);

	pdrgpstatspred->Append(pstatspred1);
	pdrgpstatspred->Append(pstatspred2);
}

// create a stats filter on boolean
void
CStatisticsTest::StatsFilterBool
	(
	IMemoryPool *pmp,
	ULONG ulColId,
	BOOL fValue,
	DrgPstatspred *pdrgpstatspred
	)
{
	CStatsPredPoint *pstatspred1 = GPOS_NEW(pmp) CStatsPredPoint
												(
												ulColId,
												CStatsPred::EstatscmptEq,
												CTestUtils::PpointBool(pmp, fValue)
												);

	pdrgpstatspred->Append(pstatspred1);
}

// create a stats filter on numeric types
void
CStatisticsTest::StatsFilterNumeric
	(
	IMemoryPool *pmp,
	ULONG ulColId,
	CWStringDynamic *pstrLowerEncoded,
	CWStringDynamic *pstrUpperEncoded,
	CDouble dValLower,
	CDouble dValUpper,
	DrgPstatspred *pdrgpstatspred
	)
{
	CStatsPredPoint *pstatspred1 = GPOS_NEW(pmp) CStatsPredPoint
												(
												ulColId,
												CStatsPred::EstatscmptGEq,
												CCardinalityTestUtils::PpointNumeric(pmp, pstrLowerEncoded, dValLower)
												);

	CStatsPredPoint *pstatspred2 = GPOS_NEW(pmp) CStatsPredPoint
												(
												ulColId,
												CStatsPred::EstatscmptL,
												CCardinalityTestUtils::PpointNumeric(pmp, pstrUpperEncoded, dValUpper)
												);

	pdrgpstatspred->Append(pstatspred1);
	pdrgpstatspred->Append(pstatspred2);
}

// create a stats filter on other types
void
CStatisticsTest::StatsFilterGeneric
	(
	IMemoryPool *pmp,
	ULONG ulColId,
	OID oid,
	CWStringDynamic *pstrLowerEncoded,
	CWStringDynamic *pstrUpperEncoded,
	LINT lValueLower,
	LINT lValueUpper,
	DrgPstatspred *pdrgpstatspred
	)
{
	CStatsPredPoint *pstatspred1 = GPOS_NEW(pmp) CStatsPredPoint
												(
												ulColId,
												CStatsPred::EstatscmptGEq,
												CCardinalityTestUtils::PpointGeneric(pmp, oid, pstrLowerEncoded, lValueLower)
												);

	CStatsPredPoint *pstatspred2 = GPOS_NEW(pmp) CStatsPredPoint
												(
												ulColId,
												CStatsPred::EstatscmptL,
												CCardinalityTestUtils::PpointGeneric(pmp, oid, pstrUpperEncoded, lValueUpper)
												);

	pdrgpstatspred->Append(pstatspred1);
	pdrgpstatspred->Append(pstatspred2);
}

// derivation over select query
GPOS_RESULT
CStatisticsTest::EresUnittest_CStatisticsSelectDerivation()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	return
		CTestUtils::EresTranslate
			(
			pmp,
			szQuerySelect,
			szPlanSelect,
			true // ignore mismatch in output dxl due to column id differences
			);
}

// EOF
