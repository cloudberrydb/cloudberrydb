//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CCostTest.cpp
//
//	@doc:
//		Tests for basic operations on cost
//---------------------------------------------------------------------------
#include "gpos/error/CAutoTrace.h"
#include "gpos/task/CAutoTraceFlag.h"

#include "gpopt/cost/CCost.h"
#include "gpopt/cost/ICostModelParams.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/minidump/CMinidumperUtils.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/operators/CLogicalInnerJoin.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/parser/CParseHandlerDXL.h"

#include "unittest/base.h"
#include "unittest/gpopt/cost/CCostTest.h"
#include "unittest/gpopt/CTestUtils.h"

#include "gpdbcost/CCostModelGPDB.h"
#include "gpdbcost/CCostModelGPDBLegacy.h"

//---------------------------------------------------------------------------
//	@function:
//		CCostTest::EresUnittest
//
//	@doc:
//		Driver for unittests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCostTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CCostTest::EresUnittest_Bool),
		GPOS_UNITTEST_FUNC(CCostTest::EresUnittest_Arithmetic),
		GPOS_UNITTEST_FUNC(CCostTest::EresUnittest_Params),
		GPOS_UNITTEST_FUNC(CCostTest::EresUnittest_Parsing),
		GPOS_UNITTEST_FUNC(EresUnittest_SetParams),

		// TODO: : re-enable test after resolving exception throwing problem on OSX
		// GPOS_UNITTEST_FUNC_THROW(CCostTest::EresUnittest_ParsingWithException, gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CCostTest::EresUnittest_Arithmetic
//
//	@doc:
//		Test arithmetic operations
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCostTest::EresUnittest_Arithmetic()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CCost cost1(2.5);
	CCost cost2(3.0);
	CCost cost3(5.49);
	CCost cost4(5.51);
	CCost cost5(7.49);
	CCost cost6(7.51);

	CCost costAdd(cost1 + cost2);
	CCost costMultiply(cost1 * cost2);

	GPOS_ASSERT(costAdd > cost3);
	GPOS_ASSERT(costAdd < cost4);

	GPOS_ASSERT(costMultiply > cost5);
	GPOS_ASSERT(costMultiply < cost6);

	CAutoTrace at(pmp);
	IOstream &os(at.Os());

	os << "Arithmetic operations: " << std::endl
	   << cost1 << " + " << cost2 << " = " << costAdd << std::endl
	   << cost1 << " * " << cost2 << " = " << costMultiply << std::endl;

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostTest::EresUnittest_Bool
//
//	@doc:
//		Test comparison operations
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCostTest::EresUnittest_Bool()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CCost cost1(2.5);
	CCost cost2(3.5);

	GPOS_ASSERT(cost1 < cost2);
	GPOS_ASSERT(cost2 > cost1);

	CAutoTrace at(pmp);
	IOstream &os(at.Os());

	os << "Boolean operations: " << std::endl
	   << cost1 << " < " << cost2 << " = " << (cost1 < cost2) << std::endl
	   << cost2 << " > " << cost1 << " = " << (cost2 > cost1) << std::endl;

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CCostTest::TestParams
//
//	@doc:
//		Test cost model parameters
//
//---------------------------------------------------------------------------
void
CCostTest::TestParams
	(
	IMemoryPool *pmp,
	BOOL fCalibrated
	)
{
	CAutoTrace at(pmp);
	IOstream &os(at.Os());

	ICostModelParams *pcp =  NULL;
	CDouble dSeqIOBandwidth(0.0);
	CDouble dRandomIOBandwidth(0.0);
	CDouble dTupProcBandwidth(0.0);
	CDouble dNetBandwidth(0.0);
	CDouble dSegments(0.0);
	CDouble dNLJFactor(0.0);
	CDouble dHashFactor(0.0);
	CDouble dDefaultCost(0.0);

	if (fCalibrated)
	{
		pcp = ((CCostModelGPDB *) COptCtxt::PoctxtFromTLS()->Pcm())->Pcp();

		dSeqIOBandwidth = pcp->PcpLookup(CCostModelParamsGPDB::EcpSeqIOBandwidth)->DVal();
		dRandomIOBandwidth = pcp->PcpLookup(CCostModelParamsGPDB::EcpRandomIOBandwidth)->DVal();
		dTupProcBandwidth = pcp->PcpLookup(CCostModelParamsGPDB::EcpTupProcBandwidth)->DVal();
		dNetBandwidth = pcp->PcpLookup(CCostModelParamsGPDB::EcpNetBandwidth)->DVal();
		dSegments = pcp->PcpLookup(CCostModelParamsGPDB::EcpSegments)->DVal();
		dNLJFactor = pcp->PcpLookup(CCostModelParamsGPDB::EcpNLJFactor)->DVal();
		dHashFactor = pcp->PcpLookup(CCostModelParamsGPDB::EcpHashFactor)->DVal();
		dDefaultCost = pcp->PcpLookup(CCostModelParamsGPDB::EcpDefaultCost)->DVal();
	}
	else
	{
		pcp = ((CCostModelGPDBLegacy *) COptCtxt::PoctxtFromTLS()->Pcm())->Pcp();

		dSeqIOBandwidth = pcp->PcpLookup(CCostModelParamsGPDBLegacy::EcpSeqIOBandwidth)->DVal();
		dRandomIOBandwidth = pcp->PcpLookup(CCostModelParamsGPDBLegacy::EcpRandomIOBandwidth)->DVal();
		dTupProcBandwidth = pcp->PcpLookup(CCostModelParamsGPDBLegacy::EcpTupProcBandwidth)->DVal();
		dNetBandwidth = pcp->PcpLookup(CCostModelParamsGPDBLegacy::EcpNetBandwidth)->DVal();
		dSegments = pcp->PcpLookup(CCostModelParamsGPDBLegacy::EcpSegments)->DVal();
		dNLJFactor = pcp->PcpLookup(CCostModelParamsGPDBLegacy::EcpNLJFactor)->DVal();
		dHashFactor = pcp->PcpLookup(CCostModelParamsGPDBLegacy::EcpHashFactor)->DVal();
		dDefaultCost = pcp->PcpLookup(CCostModelParamsGPDBLegacy::EcpDefaultCost)->DVal();
	}

	os << std::endl << "Lookup cost model params by id: " << std::endl;
	os << "Seq I/O bandwidth: " << dSeqIOBandwidth << std::endl;
	os << "Random I/O bandwidth: " << dRandomIOBandwidth << std::endl;
	os << "Tuple proc bandwidth: " << dTupProcBandwidth << std::endl;
	os << "Network bandwidth: " << dNetBandwidth << std::endl;
	os << "Segments: " << dSegments << std::endl;
	os << "NLJ Factor: " << dNLJFactor << std::endl;
	os << "Hash Factor: " << dHashFactor << std::endl;
	os << "Default Cost: " << dDefaultCost << std::endl;

	CDouble dSeqIOBandwidth1 = pcp->PcpLookup("SeqIOBandwidth")->DVal();
	CDouble dRandomIOBandwidth1 = pcp->PcpLookup("RandomIOBandwidth")->DVal();
	CDouble dTupProcBandwidth1 = pcp->PcpLookup("TupProcBandwidth")->DVal();
	CDouble dNetBandwidth1 = pcp->PcpLookup("NetworkBandwidth")->DVal();
	CDouble dSegments1 = pcp->PcpLookup("Segments")->DVal();
	CDouble dNLJFactor1 = pcp->PcpLookup("NLJFactor")->DVal();
	CDouble dHashFactor1 = pcp->PcpLookup("HashFactor")->DVal();
	CDouble dDefaultCost1 = pcp->PcpLookup("DefaultCost")->DVal();

	os << std::endl << "Lookup cost model params by name: " << std::endl;
	os << "Seq I/O bandwidth: " << dSeqIOBandwidth1 << std::endl;
	os << "Random I/O bandwidth: " << dRandomIOBandwidth1 << std::endl;
	os << "Tuple proc bandwidth: " << dTupProcBandwidth1 << std::endl;
	os << "Network bandwidth: " << dNetBandwidth1 << std::endl;
	os << "Segments: " << dSegments1 << std::endl;
	os << "NLJ Factor: " << dNLJFactor1 << std::endl;
	os << "Hash Factor: " << dHashFactor1 << std::endl;
	os << "Default Cost: " << dDefaultCost1 << std::endl;

	GPOS_ASSERT(dSeqIOBandwidth == dSeqIOBandwidth1);
	GPOS_ASSERT(dRandomIOBandwidth == dRandomIOBandwidth1);
	GPOS_ASSERT(dTupProcBandwidth == dTupProcBandwidth1);
	GPOS_ASSERT(dNetBandwidth == dNetBandwidth1);
	GPOS_ASSERT(dSegments == dSegments1);
	GPOS_ASSERT(dNLJFactor == dNLJFactor1);
	GPOS_ASSERT(dHashFactor == dHashFactor1);
	GPOS_ASSERT(dDefaultCost == dDefaultCost1);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostTest::EresUnittest_Params
//
//	@doc:
//		Cost model parameters
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCostTest::EresUnittest_Params()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	{
		// install opt context in TLS
		CAutoOptCtxt aoc
						(
						pmp,
						&mda,
						NULL, /* pceeval */
						CTestUtils::Pcm(pmp)
						);

		TestParams(pmp, false /*fCalibrated*/);
	}

	{
		// install opt context in TLS
		CAutoOptCtxt aoc
						(
						pmp,
						&mda,
						NULL, /* pceeval */
						GPOS_NEW(pmp) CCostModelGPDB(pmp, GPOPT_TEST_SEGMENTS)
						);

		TestParams(pmp, true /*fCalibrated*/);
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostTest::EresUnittest_Parsing
//
//	@doc:
//		Test parsing cost params from external file
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCostTest::EresUnittest_Parsing()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	CParseHandlerDXL *pphDXL = CDXLUtils::PphdxlParseDXLFile(pmp,"../data/dxl/cost/cost0.xml", NULL);
	ICostModelParams *pcp = pphDXL->Pcp();

	{
		CAutoTrace at(pmp);
		at.Os() << " Parsed cost params: " << std::endl;
		pcp->OsPrint(at.Os());
	}
	GPOS_DELETE(pphDXL);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CSearchStrategyTest::EresUnittest_ParsingWithException
//
//	@doc:
//		Test exception handling when parsing cost params
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCostTest::EresUnittest_ParsingWithException()
{

	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	CParseHandlerDXL *pphDXL = CDXLUtils::PphdxlParseDXLFile(pmp,"../data/dxl/cost/wrong-cost.xml", NULL);
	GPOS_DELETE(pphDXL);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostTest::EresUnittest_SetParams
//
//	@doc:
//		Test of setting cost model params
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCostTest::EresUnittest_SetParams()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	ICostModel *pcm = GPOS_NEW(pmp) CCostModelGPDB(pmp, GPOPT_TEST_SEGMENTS);

	// install opt context in TLS
	CAutoOptCtxt aoc(pmp, &mda, NULL, /* pceeval */ pcm);

	// generate in-equality join expression
	CExpression *pexprOuter = CTestUtils::PexprLogicalGet(pmp);
	const CColRef *pcrOuter = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput()->PcrAny();
	CExpression *pexprInner = CTestUtils::PexprLogicalGet(pmp);
	const CColRef *pcrInner = CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->PcrsOutput()->PcrAny();
	CExpression *pexprPred = CUtils::PexprScalarCmp(pmp, pcrOuter, pcrInner, IMDType::EcmptNEq);
	CExpression *pexpr = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(pmp, pexprOuter, pexprInner, pexprPred);

	// optimize in-equality join based on default cost model params
	CExpression *pexprPlan1 = NULL;
	{
		CEngine eng(pmp);

		// generate query context
		CQueryContext *pqc = CTestUtils::PqcGenerate(pmp, pexpr);

		// Initialize engine
		eng.Init(pqc, NULL /*pdrgpss*/);

		// optimize query
		eng.Optimize();

		// extract plan
		pexprPlan1 = eng.PexprExtractPlan();
		GPOS_ASSERT(NULL != pexprPlan1);

		GPOS_DELETE(pqc);
	}

	// change NLJ cost factor
	ICostModelParams::SCostParam *pcp = pcm->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpNLJFactor);
	CDouble dNLJFactor = CDouble(2.0);
	CDouble dVal = pcp->DVal() * dNLJFactor;
	pcm->Pcp()->SetParam(pcp->UlId(), dVal, dVal - 0.5, dVal + 0.5);

	// optimize again after updating NLJ cost factor
	CExpression *pexprPlan2 = NULL;
	{
		CEngine eng(pmp);

		// generate query context
		CQueryContext *pqc = CTestUtils::PqcGenerate(pmp, pexpr);

		// Initialize engine
		eng.Init(pqc, NULL /*pdrgpss*/);

		// optimize query
		eng.Optimize();

		// extract plan
		pexprPlan2 = eng.PexprExtractPlan();
		GPOS_ASSERT(NULL != pexprPlan2);

		GPOS_DELETE(pqc);
	}

	{
		CAutoTrace at(pmp);
		at.Os() << "\nPLAN1: \n" << *pexprPlan1;
		at.Os() << "\nNLJ Cost1: " << (*pexprPlan1)[0]->Cost();
		at.Os() << "\n\nPLAN2: \n" << *pexprPlan2;
		at.Os() << "\nNLJ Cost2: " << (*pexprPlan2)[0]->Cost();
	}
	GPOS_ASSERT((*pexprPlan2)[0]->Cost() >= (*pexprPlan1)[0]->Cost() * dNLJFactor &&
			"expected NLJ cost in PLAN2 to be larger than NLJ cost in PLAN1");

	// clean up
	pexpr->Release();
	pexprPlan1->Release();
	pexprPlan2->Release();

	return GPOS_OK;
}

// EOF
