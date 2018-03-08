//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal Inc.
//
//	@filename:
//		CHistogramTest.cpp
//
//	@doc:
//		Testing merging most common values (MCV) and histograms
//---------------------------------------------------------------------------

#ifndef __STDC_CONSTANT_MACROS
#define __STDC_CONSTANT_MACROS
#endif

#include <stdint.h>

#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/statistics/CPoint.h"
#include "naucrates/statistics/CHistogram.h"
#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CStatisticsUtils.h"

#include "naucrates/dxl/CDXLUtils.h"

#include "unittest/base.h"
#include "unittest/dxl/statistics/CCardinalityTestUtils.h"
#include "unittest/dxl/statistics/CMCVTest.h"
#include "unittest/gpopt/CTestUtils.h"

using namespace gpopt;

// DXL files
const CHAR *
szMCVSortExpectedFileName = "../data/dxl/statistics/MCV-Sort-Output.xml";


// unittest for statistics objects
GPOS_RESULT
CMCVTest::EresUnittest()
{
	// tests that use shared optimization context
	CUnittest rgutSharedOptCtxt[] =
		{
		GPOS_UNITTEST_FUNC(CMCVTest::EresUnittest_SortInt4MCVs),
		GPOS_UNITTEST_FUNC(CMCVTest::EresUnittest_MergeHistMCV),
		};

	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// install opt context in TLS
	CAutoOptCtxt aoc(pmp, &mda, NULL /* pceeval */, CTestUtils::Pcm(pmp));

	return CUnittest::EresExecute(rgutSharedOptCtxt, GPOS_ARRAY_SIZE(rgutSharedOptCtxt));
}

// test sorting of Int4 MCVs and their associated frequencies
GPOS_RESULT
CMCVTest::EresUnittest_SortInt4MCVs()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(CMDIdGPDB::m_mdidInt4);
	const IMDType *pmdtype = pmda->Pmdtype(pmdid);

	// create three integer MCVs
	CPoint *ppoint1 = CTestUtils::PpointInt4(pmp, 5);
	CPoint *ppoint2 = CTestUtils::PpointInt4(pmp, 1);
	CPoint *ppoint3 = CTestUtils::PpointInt4(pmp, 10);
	DrgPdatum *pdrgpdatumMCV = GPOS_NEW(pmp) DrgPdatum(pmp);
	IDatum *pdatum1 = ppoint1->Pdatum();
	IDatum *pdatum2 = ppoint2->Pdatum();
	IDatum *pdatum3 = ppoint3->Pdatum();
	pdatum1->AddRef();
	pdatum2->AddRef();
	pdatum3->AddRef();
	pdrgpdatumMCV->Append(pdatum1);
	pdrgpdatumMCV->Append(pdatum2);
	pdrgpdatumMCV->Append(pdatum3);

	// create corresponding frequencies
	DrgPdouble *pdrgpdFreq = GPOS_NEW(pmp) DrgPdouble(pmp);
	// in GPDB, MCVs are stored in descending frequencies
	pdrgpdFreq->Append(GPOS_NEW(pmp) CDouble(0.4));
	pdrgpdFreq->Append(GPOS_NEW(pmp) CDouble(0.2));
	pdrgpdFreq->Append(GPOS_NEW(pmp) CDouble(0.1));

	// exercise MCV sorting function
	CHistogram *phistMCV = CStatisticsUtils::PhistTransformMCV
								(
								pmp,
								pmdtype,
								pdrgpdatumMCV,
								pdrgpdFreq,
								pdrgpdatumMCV->UlLength()
								);

	// create hash map from colid -> histogram
	HMUlHist *phmulhist = GPOS_NEW(pmp) HMUlHist(pmp);

	// generate int histogram for column 1
	phmulhist->FInsert(GPOS_NEW(pmp) ULONG(1), phistMCV);

	// column width for int4
	HMUlDouble *phmuldoubleWidth = GPOS_NEW(pmp) HMUlDouble(pmp);
	phmuldoubleWidth->FInsert(GPOS_NEW(pmp) ULONG(1), GPOS_NEW(pmp) CDouble(4.0));

	CStatistics *pstats = GPOS_NEW(pmp) CStatistics
									(
									pmp,
									phmulhist,
									phmuldoubleWidth,
									1000.0 /* dRows */,
									false /* fEmpty */
									);

	// put stats object in an array in order to serialize
	DrgPstats *pdrgpstats = GPOS_NEW(pmp) DrgPstats(pmp);
	pdrgpstats->Append(pstats);

	// serialize stats object
	CWStringDynamic *pstrOutput = CDXLUtils::PstrSerializeStatistics(pmp, pmda, pdrgpstats, true, true);
	GPOS_TRACE(pstrOutput->Wsz());

	// get expected output
	CWStringDynamic str(pmp);
	COstreamString oss(&str);
	CHAR *szDXLExpected = CDXLUtils::SzRead(pmp, szMCVSortExpectedFileName);
	CWStringDynamic dstrExpected(pmp);
	dstrExpected.AppendFormat(GPOS_WSZ_LIT("%s"), szDXLExpected);

	GPOS_RESULT eres = CTestUtils::EresCompare
								(
								oss,
								pstrOutput,
								&dstrExpected,
								false // mismatch will not be ignored
								);

	// cleanup
	GPOS_DELETE(pstrOutput);
	GPOS_DELETE_ARRAY(szDXLExpected);
	pdrgpdatumMCV->Release();
	pdrgpdFreq->Release();
	pdrgpstats->Release();
	ppoint1->Release();
	ppoint2->Release();
	ppoint3->Release();
	pmdid->Release();

	return eres;
}

// test merging MCVs and histogram
GPOS_RESULT
CMCVTest::EresUnittest_MergeHistMCV()
{
	SMergeTestElem rgMergeTestElem[] =
	{
		{
		"../data/dxl/statistics/Merge-Input-MCV-Int.xml",
		"../data/dxl/statistics/Merge-Input-Histogram-Int.xml",
		"../data/dxl/statistics/Merge-Output-Int.xml"
		},

		{
		"../data/dxl/statistics/Merge-Input-MCV-Numeric.xml",
		"../data/dxl/statistics/Merge-Input-Histogram-Numeric.xml",
		"../data/dxl/statistics/Merge-Output-Numeric.xml"
		},

		{
		"../data/dxl/statistics/Merge-Input-MCV-BPChar.xml",
		"../data/dxl/statistics/Merge-Input-Histogram-BPChar.xml",
		"../data/dxl/statistics/Merge-Output-BPChar.xml"
		}
	};

	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	ULONG ulLen = GPOS_ARRAY_SIZE(rgMergeTestElem);
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		// read input MCVs DXL file
		CHAR *szDXLInputMCV = CDXLUtils::SzRead(pmp, rgMergeTestElem[ul].szInputMCVFile);
		// read input histogram DXL file
		CHAR *szDXLInputHist = CDXLUtils::SzRead(pmp, rgMergeTestElem[ul].szInputHistFile);

		GPOS_CHECK_ABORT;

		CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

		// parse the stats objects
		DrgPdxlstatsderrel *pdrgpdxlstatsderrelMCV = CDXLUtils::PdrgpdxlstatsderrelParseDXL(pmp, szDXLInputMCV, NULL);
		DrgPdxlstatsderrel *pdrgpdxlstatsderrelHist = CDXLUtils::PdrgpdxlstatsderrelParseDXL(pmp, szDXLInputHist, NULL);

		GPOS_CHECK_ABORT;

		CDXLStatsDerivedRelation *pdxlstatsderrelMCV = (*pdrgpdxlstatsderrelMCV)[0];
		const DrgPdxlstatsdercol *pdrgpdxlstatsdercolMCV = pdxlstatsderrelMCV->Pdrgpdxlstatsdercol();
		CDXLStatsDerivedColumn *pdxlstatsdercolMCV = (*pdrgpdxlstatsdercolMCV)[0];
		DrgPbucket *pdrgppbucketMCV = CDXLUtils::Pdrgpbucket(pmp, pmda, pdxlstatsdercolMCV);
		CHistogram *phistMCV =  GPOS_NEW(pmp) CHistogram(pdrgppbucketMCV);

		CDXLStatsDerivedRelation *pdxlstatsderrelHist = (*pdrgpdxlstatsderrelHist)[0];
		const DrgPdxlstatsdercol *pdrgpdxlstatsdercolHist = pdxlstatsderrelHist->Pdrgpdxlstatsdercol();
		CDXLStatsDerivedColumn *pdxlstatsdercolHist = (*pdrgpdxlstatsdercolHist)[0];
		DrgPbucket *pdrgppbucketHist = CDXLUtils::Pdrgpbucket(pmp, pmda, pdxlstatsdercolHist);
		CHistogram *phistHist =  GPOS_NEW(pmp) CHistogram(pdrgppbucketHist);

		GPOS_CHECK_ABORT;

		// exercise the merge
		CHistogram *phistMerged = CStatisticsUtils::PhistMergeMcvHist(pmp, phistMCV, phistHist);

		// create hash map from colid -> histogram
		HMUlHist *phmulhist = GPOS_NEW(pmp) HMUlHist(pmp);

		// generate int histogram for column 1
		ULONG ulColId = pdxlstatsdercolMCV->UlColId();
		phmulhist->FInsert(GPOS_NEW(pmp) ULONG(ulColId), phistMerged);

		// column width for int4
		HMUlDouble *phmuldoubleWidth = GPOS_NEW(pmp) HMUlDouble(pmp);
		CDouble dWidth = pdxlstatsdercolMCV->DWidth();
		phmuldoubleWidth->FInsert(GPOS_NEW(pmp) ULONG(ulColId), GPOS_NEW(pmp) CDouble(dWidth));

		CStatistics *pstats = GPOS_NEW(pmp) CStatistics
										(
										pmp,
										phmulhist,
										phmuldoubleWidth,
										pdxlstatsderrelMCV->DRows(),
										pdxlstatsderrelMCV->FEmpty()
										);

		// put stats object in an array in order to serialize
		DrgPstats *pdrgpstats = GPOS_NEW(pmp) DrgPstats(pmp);
		pdrgpstats->Append(pstats);

		// serialize stats object
		CWStringDynamic *pstrOutput = CDXLUtils::PstrSerializeStatistics(pmp, pmda, pdrgpstats, true, true);
		GPOS_TRACE(pstrOutput->Wsz());

		// get expected output
		CWStringDynamic str(pmp);
		COstreamString oss(&str);
		CHAR *szDXLExpected = CDXLUtils::SzRead(pmp, rgMergeTestElem[ul].szMergedFile);
		CWStringDynamic dstrExpected(pmp);
		dstrExpected.AppendFormat(GPOS_WSZ_LIT("%s"), szDXLExpected);

		GPOS_RESULT eres = CTestUtils::EresCompare
									(
									oss,
									pstrOutput,
									&dstrExpected,
									false // mismatch will not be ignored
									);

		// cleanup
		GPOS_DELETE_ARRAY(szDXLInputMCV);
		GPOS_DELETE_ARRAY(szDXLInputHist);
		GPOS_DELETE_ARRAY(szDXLExpected);
		GPOS_DELETE(pstrOutput);
		pdrgpdxlstatsderrelMCV->Release();
		pdrgpdxlstatsderrelHist->Release();
		GPOS_DELETE(phistMCV);
		GPOS_DELETE(phistHist);
		pdrgpstats->Release();

		if (GPOS_OK != eres)
		{
			return eres;
		}
	}

	return GPOS_OK;
}

// EOF

