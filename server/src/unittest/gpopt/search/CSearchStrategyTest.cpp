//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSearchStrategyTest.cpp
//
//	@doc:
//		Test for search strategy
//---------------------------------------------------------------------------
#include "gpopt/exception.h"

#include "gpopt/engine/CEngine.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/search/CSearchStage.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpopt/xforms/CXformFactory.h"

#include "unittest/base.h"
#include "unittest/gpopt/CTestUtils.h"
#include "unittest/gpopt/engine/CEngineTest.h"
#include "unittest/gpopt/search/CSchedulerTest.h"
#include "unittest/gpopt/search/CSearchStrategyTest.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/parser/CParseHandlerDXL.h"

//---------------------------------------------------------------------------
//	@function:
//		CSearchStrategyTest::EresUnittest
//
//	@doc:
//		Unittest for scheduler
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSearchStrategyTest::EresUnittest()
{
	CUnittest rgut[] =
		{
#ifdef GPOS_DEBUG
		GPOS_UNITTEST_FUNC(CSearchStrategyTest::EresUnittest_RecursiveOptimize),
#endif // GPOS_DEBUG
		GPOS_UNITTEST_FUNC(CSearchStrategyTest::EresUnittest_MultiThreadedOptimize),
		GPOS_UNITTEST_FUNC(CSearchStrategyTest::EresUnittest_Parsing),
		GPOS_UNITTEST_FUNC_THROW
			(
			CSearchStrategyTest::EresUnittest_Timeout,
			gpopt::ExmaGPOPT,
			gpopt::ExmiNoPlanFound
			),
		GPOS_UNITTEST_FUNC_THROW
			(
			CSearchStrategyTest::EresUnittest_ParsingWithException,
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLXercesParseError
			),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CSearchStrategyTest::Optimize
//
//	@doc:
//		Run optimize function on given expression
//
//---------------------------------------------------------------------------
void
CSearchStrategyTest::Optimize
	(
	IMemoryPool *pmp,
	Pfpexpr pfnGenerator,
	DrgPss *pdrgpss,
	PfnOptimize pfnOptimize
	)
{
	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache());
	mda.RegisterProvider(CTestUtils::m_sysidDefault, pmdp);

	// install opt context in TLS
	{
		CAutoOptCtxt aoc
						(
						pmp,
						&mda,
						NULL,  /* pceeval */
						CTestUtils::Pcm(pmp)
						);
		CExpression *pexpr = pfnGenerator(pmp);
		pfnOptimize(pmp, pexpr, pdrgpss);
		pexpr->Release();
	}
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CSearchStrategyTest::EresUnittest_RecursiveOptimize
//
//	@doc:
//		Test search strategy with recursive optimization
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSearchStrategyTest::EresUnittest_RecursiveOptimize()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	Optimize(pmp, CTestUtils::PexprLogicalSelectOnOuterJoin, PdrgpssRandom(pmp), CEngineTest::BuildMemoRecursive);

	return GPOS_OK;
}
#endif // GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CSearchStrategyTest::EresUnittest_MultiThreadedOptimize
//
//	@doc:
//		Test search strategy with multi-threaded optimization
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSearchStrategyTest::EresUnittest_MultiThreadedOptimize()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	Optimize(pmp, CTestUtils::PexprLogicalSelectOnOuterJoin, PdrgpssRandom(pmp), CSchedulerTest::BuildMemoMultiThreaded);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CSearchStrategyTest::EresUnittest_Parsing
//
//	@doc:
//		Test search strategy with multi-threaded optimization
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSearchStrategyTest::EresUnittest_Parsing()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	CParseHandlerDXL *pphDXL = CDXLUtils::PphdxlParseDXLFile(pmp,"../data/dxl/search/strategy0.xml", NULL);
	DrgPss *pdrgpss = pphDXL->Pdrgpss();
	const ULONG ulSize = pdrgpss->UlLength();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CAutoTrace at(pmp);
		(*pdrgpss)[ul]->OsPrint(at.Os());
	}
	pdrgpss->AddRef();
	Optimize(pmp, CTestUtils::PexprLogicalSelectOnOuterJoin, pdrgpss, CSchedulerTest::BuildMemoMultiThreaded);

	GPOS_DELETE(pphDXL);

	return GPOS_OK;
}



//---------------------------------------------------------------------------
//	@function:
//		CSearchStrategyTest::EresUnittest_Timeout
//
//	@doc:
//		Test search strategy that times out
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSearchStrategyTest::EresUnittest_Timeout()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	CAutoTraceFlag atf(EopttracePrintOptimizationStatistics, true);
	CParseHandlerDXL *pphDXL = CDXLUtils::PphdxlParseDXLFile(pmp,"../data/dxl/search/timeout-strategy.xml", NULL);
	DrgPss *pdrgpss = pphDXL->Pdrgpss();
	pdrgpss->AddRef();
	Optimize(pmp, CTestUtils::PexprLogicalNAryJoin, pdrgpss, CSchedulerTest::BuildMemoMultiThreaded);

	GPOS_DELETE(pphDXL);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CSearchStrategyTest::EresUnittest_ParsingWithException
//
//	@doc:
//		Test exception handling when search strategy
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSearchStrategyTest::EresUnittest_ParsingWithException()
{

	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	CParseHandlerDXL *pphDXL = CDXLUtils::PphdxlParseDXLFile(pmp,"../data/dxl/search/wrong-strategy.xml", NULL);
	GPOS_DELETE(pphDXL);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CSearchStrategyTest::PdrgpssRandom
//
//	@doc:
//		Generate a search strategy with random xform allocation
//
//---------------------------------------------------------------------------
DrgPss *
CSearchStrategyTest::PdrgpssRandom
	(
	IMemoryPool *pmp
	)
{
	DrgPss *pdrgpss = GPOS_NEW(pmp) DrgPss(pmp);
	CXformSet *pxfsFst = GPOS_NEW(pmp) CXformSet(pmp);
	CXformSet *pxfsSnd = GPOS_NEW(pmp) CXformSet(pmp);

	// first xforms set contains essential rules to produce simple equality join plan
	(void) pxfsFst->FExchangeSet(CXform::ExfGet2TableScan);
	(void) pxfsFst->FExchangeSet(CXform::ExfSelect2Filter);
	(void) pxfsFst->FExchangeSet(CXform::ExfInnerJoin2HashJoin);

	// second xforms set contains all other rules
	pxfsSnd->Union(CXformFactory::Pxff()->PxfsExploration());
	pxfsSnd->Union(CXformFactory::Pxff()->PxfsImplementation());
	pxfsSnd->Difference(pxfsFst);

	pdrgpss->Append(GPOS_NEW(pmp) CSearchStage(pxfsFst, 1000 /*ulTimeThreshold*/, CCost(10E4) /*costThreshold*/));
	pdrgpss->Append(GPOS_NEW(pmp) CSearchStage(pxfsSnd, 10000 /*ulTimeThreshold*/, CCost(10E8) /*costThreshold*/));

	return pdrgpss;
}


// EOF
