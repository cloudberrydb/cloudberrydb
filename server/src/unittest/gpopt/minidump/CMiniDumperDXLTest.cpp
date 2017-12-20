//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMiniDumperDXLTest.cpp
//
//	@doc:
//		Test for DXL-based minidumps
//---------------------------------------------------------------------------
#include "gpos/io/COstreamString.h"
#include "gpos/io/COstreamFile.h"
#include "gpos/task/CAutoTraceFlag.h"

#include "naucrates/base/CQueryToDXLResult.h"
#include "naucrates/dxl/CDXLUtils.h"

#include "gpopt/base/CQueryContext.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/minidump/CDXLMinidump.h"
#include "gpopt/minidump/CMiniDumperDXL.h"
#include "gpopt/minidump/CMinidumperUtils.h"
#include "gpopt/minidump/CSerializableQuery.h"
#include "gpopt/minidump/CSerializableMDAccessor.h"
#include "gpopt/minidump/CSerializablePlan.h"
#include "gpopt/minidump/CSerializableStackTrace.h"
#include "gpopt/minidump/CSerializableOptimizerConfig.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "gpopt/translate/CTranslatorExprToDXL.h"

#include "unittest/base.h"
#include "unittest/gpopt/minidump/CMiniDumperDXLTest.h"
#include "unittest/gpopt/translate/CTranslatorExprToDXLTest.h"
#include "unittest/gpopt/CTestUtils.h"

#include <fstream>

static
const CHAR *szQueryFile= "../data/dxl/minidump/Query.xml";

//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperDXLTest::EresUnittest
//
//	@doc:
//		Unittest for DXL minidumps
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMiniDumperDXLTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CMiniDumperDXLTest::EresUnittest_Basic),
		GPOS_UNITTEST_FUNC(CMiniDumperDXLTest::EresUnittest_Load),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}



//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperDXLTest::EresUnittest_Basic
//
//	@doc:
//		Test minidumps in case of an exception
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMiniDumperDXLTest::EresUnittest_Basic()
{
	CAutoMemoryPool amp(CAutoMemoryPool::ElcNone);
	IMemoryPool *pmp = amp.Pmp();

	CWStringDynamic minidumpstr(pmp);
	COstreamString oss(&minidumpstr);
	CMiniDumperDXL mdrs(pmp);
	mdrs.Init(&oss);
	
	CHAR szFileName[GPOS_FILE_NAME_BUF_SIZE];

	GPOS_TRY
	{
		CSerializableStackTrace serStackTrace;
		
		// read the dxl document
		CHAR *szQueryDXL = CDXLUtils::SzRead(pmp, szQueryFile);

		// parse the DXL query tree from the given DXL document
		CQueryToDXLResult *ptroutput = 
				CDXLUtils::PdxlnParseDXLQuery(pmp, szQueryDXL, NULL);
		GPOS_CHECK_ABORT;

		CSerializableQuery serQuery(pmp, ptroutput->Pdxln(), ptroutput->PdrgpdxlnOutputCols(), ptroutput->PdrgpdxlnCTE());
		
		// setup a file-based provider
		CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
		pmdp->AddRef();

		// we need to use an auto pointer for the cache here to ensure
		// deleting memory of cached objects when we throw
		CAutoP<CMDAccessor::MDCache> apcache;
		apcache = CCacheFactory::PCacheCreate<gpopt::IMDCacheObject*, gpopt::CMDKey*>
					(
					true, // fUnique
					0 /* unlimited cache quota */,
					CMDKey::UlHashMDKey,
					CMDKey::FEqualMDKey
					);

		CMDAccessor::MDCache *pcache = apcache.Pt();

		CMDAccessor mda(pmp, pcache, CTestUtils::m_sysidDefault, pmdp);

		CSerializableMDAccessor serMDA(&mda);
		
		CAutoTraceFlag atfPrintQuery(EopttracePrintQuery, true);
		CAutoTraceFlag atfPrintPlan(EopttracePrintPlan, true);
		CAutoTraceFlag atfTest(EtraceTest, true);

		COptimizerConfig *poconf = GPOS_NEW(pmp) COptimizerConfig
												(
												CEnumeratorConfig::Pec(pmp, 0 /*ullPlanId*/),
												CStatisticsConfig::PstatsconfDefault(pmp),
												CCTEConfig::PcteconfDefault(pmp),
												ICostModel::PcmDefault(pmp),
												CHint::PhintDefault(pmp),
												CWindowOids::Pwindowoids(pmp)
												);

		// setup opt ctx
		CAutoOptCtxt aoc
						(
						pmp,
						&mda,
						NULL,  /* pceeval */
						CTestUtils::Pcm(pmp)
						);

		// translate DXL Tree -> Expr Tree
		CTranslatorDXLToExpr *pdxltr = GPOS_NEW(pmp) CTranslatorDXLToExpr(pmp, &mda);
		CExpression *pexprTranslated =	pdxltr->PexprTranslateQuery
													(
													ptroutput->Pdxln(),
													ptroutput->PdrgpdxlnOutputCols(),
													ptroutput->PdrgpdxlnCTE()
													);
		
		gpdxl::DrgPul *pdrgul = pdxltr->PdrgpulOutputColRefs();
		gpmd::DrgPmdname *pdrgpmdname = pdxltr->Pdrgpmdname();

		ULONG ulSegments = GPOPT_TEST_SEGMENTS;
		CQueryContext *pqc = CQueryContext::PqcGenerate(pmp, pexprTranslated, pdrgul, pdrgpmdname, true /*fDeriveStats*/);

		// optimize logical expression tree into physical expression tree.

		CEngine eng(pmp);

		CSerializableOptimizerConfig serOptConfig(pmp, poconf);
		
		eng.Init(pqc, NULL /*pdrgpss*/);
		eng.Optimize();
		
		CExpression *pexprPlan = eng.PexprExtractPlan();
		(void) pexprPlan->PrppCompute(pmp, pqc->Prpp());

		// translate plan into DXL
		DrgPi *pdrgpiSegments = GPOS_NEW(pmp) DrgPi(pmp);


		GPOS_ASSERT(0 < ulSegments);

		for (ULONG ul = 0; ul < ulSegments; ul++)
		{
			pdrgpiSegments->Append(GPOS_NEW(pmp) INT(ul));
		}

		CTranslatorExprToDXL ptrexprtodxl(pmp, &mda, pdrgpiSegments);
		CDXLNode *pdxlnPlan = ptrexprtodxl.PdxlnTranslate(pexprPlan, pqc->PdrgPcr(), pqc->Pdrgpmdname());
		GPOS_ASSERT(NULL != pdxlnPlan);
		
		CSerializablePlan serPlan(pmp, pdxlnPlan, poconf->Pec()->UllPlanId(), poconf->Pec()->UllPlanSpaceSize());
		GPOS_CHECK_ABORT;

		// simulate an exception 
		GPOS_OOM_CHECK(NULL);
	}	
	GPOS_CATCH_EX(ex)
	{
		// unless we're simulating faults, the exception must be OOM
		GPOS_ASSERT_IMP
			(
			!GPOS_FTRACE(EtraceSimulateAbort) && !GPOS_FTRACE(EtraceSimulateIOError) && !IWorker::m_fEnforceTimeSlices,
			CException::ExmaSystem == ex.UlMajor() && CException::ExmiOOM == ex.UlMinor()
			);
		
		mdrs.Finalize();

		GPOS_RESET_EX;

		CWStringDynamic str(pmp);
		COstreamString oss(&str);
		oss << std::endl << "Minidump" << std::endl;
		oss << minidumpstr.Wsz();
		oss << std::endl;
		
		// dump the same to a temp file
		ULONG ulSessionId = 1;
		ULONG ulCommandId = 1;

		CMinidumperUtils::GenerateMinidumpFileName(szFileName, GPOS_FILE_NAME_BUF_SIZE, ulSessionId, ulCommandId, NULL /*szMinidumpFileName*/);

		std::wofstream osMinidump(szFileName);
		osMinidump << minidumpstr.Wsz();

		oss << "Minidump file: " << szFileName << std::endl;

		GPOS_TRACE(str.Wsz());
	}
	GPOS_CATCH_END;
	
	// TODO:  - Feb 11, 2013; enable after fixing problems with serializing
	// XML special characters (OPT-2996)
//	// try to load minidump file
//	CDXLMinidump *pdxlmd = CMinidumperUtils::PdxlmdLoad(pmp, szFileName);
//	GPOS_ASSERT(NULL != pdxlmd);
//	delete pdxlmd;

		
	// delete temp file
	ioutils::Unlink(szFileName);
	
	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperDXLTest::EresUnittest_Load
//
//	@doc:
//		Load a minidump file
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMiniDumperDXLTest::EresUnittest_Load()
{
	CAutoMemoryPool amp(CAutoMemoryPool::ElcExc);
	IMemoryPool *pmp = amp.Pmp();
	
	const CHAR *rgszMinidumps[] =
	{
		 "../data/dxl/minidump/Minidump.xml",
	};
	ULONG ulTestCounter = 0;

	GPOS_RESULT eres =
			CTestUtils::EresRunMinidumps
						(
						pmp,
						rgszMinidumps,
						1, // ulTests
						&ulTestCounter,
						1, // ulSessionId
						1,  // ulCmdId
						false, // fMatchPlans
						false // fTestSpacePruning
						);
	return eres;

}
// EOF
