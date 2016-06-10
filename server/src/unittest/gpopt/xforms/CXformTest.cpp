//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CXformTest.cpp
//
//	@doc:
//		Test for CXForm
//---------------------------------------------------------------------------
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpopt/base/CQueryContext.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXform.h"
#include "gpopt/xforms/xforms.h"

#include "unittest/base.h"
#include "unittest/gpopt/xforms/CXformTest.h"
#include "unittest/gpopt/xforms/CDecorrelatorTest.h"
#include "unittest/gpopt/translate/CTranslatorExprToDXLTest.h"
#include "unittest/gpopt/CTestUtils.h"


//---------------------------------------------------------------------------
//	@function:
//		CXformTest::EresUnittest
//
//	@doc:
//		Unittest driver
//
//---------------------------------------------------------------------------
GPOS_RESULT
CXformTest::EresUnittest()
{
	CUnittest rgut[] =
	{
		GPOS_UNITTEST_FUNC(CXformTest::EresUnittest_ApplyXforms),
		GPOS_UNITTEST_FUNC(CXformTest::EresUnittest_ApplyXforms_CTE),
#ifdef GPOS_DEBUG
		GPOS_UNITTEST_FUNC(CXformTest::EresUnittest_Mapping),
#endif // GPOS_DEBUG
	};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CXformTest::EresUnittest_ApplyXforms
//
//	@doc:
//		Test application of different xforms
//
//---------------------------------------------------------------------------
GPOS_RESULT
CXformTest::EresUnittest_ApplyXforms()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	typedef CExpression *(*Pfpexpr)(IMemoryPool*);
	Pfpexpr rgpf[] = 
					{
					CTestUtils::PexprLogicalApplyWithOuterRef<CLogicalInnerApply>,
					CTestUtils::PexprLogicalApply<CLogicalLeftSemiApply>,
					CTestUtils::PexprLogicalApply<CLogicalLeftAntiSemiApply>,
					CTestUtils::PexprLogicalApply<CLogicalLeftAntiSemiApplyNotIn>,
					CTestUtils::PexprLogicalApplyWithOuterRef<CLogicalLeftOuterApply>,
					CTestUtils::PexprLogicalGet,
					CTestUtils::PexprLogicalExternalGet,
					CTestUtils::PexprLogicalSelect,
					CTestUtils::PexprLogicalLimit,
					CTestUtils::PexprLogicalJoin<CLogicalInnerJoin>,
					CTestUtils::PexprLogicalJoin<CLogicalLeftOuterJoin>,
					CTestUtils::PexprLogicalJoin<CLogicalLeftSemiJoin>,
					CTestUtils::PexprLogicalJoin<CLogicalLeftAntiSemiJoin>,
					CTestUtils::PexprLogicalJoin<CLogicalLeftAntiSemiJoinNotIn>,
					CTestUtils::PexprLogicalGbAgg,
					CTestUtils::PexprLogicalGbAggOverJoin,
					CTestUtils::PexprLogicalGbAggWithSum,
					CTestUtils::PexprLogicalGbAggDedupOverInnerJoin,
					CTestUtils::PexprLogicalNAryJoin,
					CTestUtils::PexprLeftOuterJoinOnNAryJoin,
					CTestUtils::PexprLogicalProject,
					CTestUtils::PexprLogicalSequence,
					CTestUtils::PexprLogicalGetPartitioned,
					CTestUtils::PexprLogicalSelectPartitioned,
					CTestUtils::PexprLogicalDynamicGet,
					CTestUtils::PexprJoinPartitionedInner<CLogicalInnerJoin>,
					CTestUtils::PexprLogicalSelectCmpToConst,
					CTestUtils::PexprLogicalTVFTwoArgs,
					CTestUtils::PexprLogicalTVFNoArgs,
					CTestUtils::PexprLogicalInsert,
					CTestUtils::PexprLogicalDelete,
					CTestUtils::PexprLogicalUpdate,
					CTestUtils::PexprLogicalAssert,
					CTestUtils::PexprLogicalJoin<CLogicalFullOuterJoin>,
					PexprJoinTree,
					CTestUtils::PexprLogicalJoinWithPartitionedAndIndexedInnerChild,
					};

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// install opt context in TLS
	CAutoOptCtxt aoc
					(
					pmp,
					&mda,
					NULL,  /* pceeval */
					CTestUtils::Pcm(pmp)
					);

	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(rgpf); ul++)
	{
		CWStringDynamic str(pmp);
		COstreamString oss(&str);

		// generate simple expression
		CExpression *pexpr = rgpf[ul](pmp);
		ApplyExprXforms(pmp, oss, pexpr);

		GPOS_TRACE(str.Wsz());
		pexpr->Release();
	}

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformTest::EresUnittest_ApplyXforms_CTE
//
//	@doc:
//		Test application of CTE-related xforms
//
//---------------------------------------------------------------------------
GPOS_RESULT
CXformTest::EresUnittest_ApplyXforms_CTE()
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
					NULL,  /* pceeval */
					CTestUtils::Pcm(pmp)
					);

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);

	// create producer
	ULONG ulCTEId = 0;
	CExpression *pexprProducer = CTestUtils::PexprLogicalCTEProducerOverSelect(pmp, ulCTEId);
	COptCtxt::PoctxtFromTLS()->Pcteinfo()->AddCTEProducer(pexprProducer);

	pdrgpexpr->Append(pexprProducer);

	DrgPcr *pdrgpcrProducer = CLogicalCTEProducer::PopConvert(pexprProducer->Pop())->Pdrgpcr();
	DrgPcr *pdrgpcrConsumer = CUtils::PdrgpcrCopy(pmp, pdrgpcrProducer);

	CExpression *pexprConsumer =
			GPOS_NEW(pmp) CExpression
						(
						pmp,
						GPOS_NEW(pmp) CLogicalCTEConsumer(pmp, ulCTEId, pdrgpcrConsumer)
						);

	pdrgpexpr->Append(pexprConsumer);
	COptCtxt::PoctxtFromTLS()->Pcteinfo()->IncrementConsumers(ulCTEId);

	pexprConsumer->AddRef();
	CExpression *pexprSelect = CTestUtils::PexprLogicalSelect(pmp, pexprConsumer);
	pdrgpexpr->Append(pexprSelect);

	pexprSelect->AddRef();
	CExpression *pexprAnchor =
			GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalCTEAnchor(pmp, ulCTEId),
					pexprSelect
					);

	pdrgpexpr->Append(pexprAnchor);

	const ULONG ulLen = pdrgpexpr->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CWStringDynamic str(pmp);
		COstreamString oss(&str);

		ApplyExprXforms(pmp, oss, (*pdrgpexpr)[ul]);

		GPOS_TRACE(str.Wsz());
	}
	pdrgpexpr->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformTest::ApplyExprXforms
//
//	@doc:
//		Apply different xforms for the given expression
//
//---------------------------------------------------------------------------
void
CXformTest::ApplyExprXforms
	(
	IMemoryPool *pmp,
	IOstream &os,
	CExpression *pexpr
	)
{
	os << std::endl << "EXPR:" << std::endl;
	(void) pexpr->OsPrint(os);

	for (ULONG ulXformId = 0; ulXformId < CXform::ExfSentinel; ulXformId++)
	{
		CXform *pxform = CXformFactory::Pxff()->Pxf((CXform::EXformId) ulXformId);
		os << std::endl <<"XFORM " << pxform->SzId() << ":" << std::endl;

		CXformContext *pxfctxt = GPOS_NEW(pmp) CXformContext(pmp);
		CXformResult *pxfres = GPOS_NEW(pmp) CXformResult(pmp);

#ifdef GPOS_DEBUG
		if (pxform->FCheckPattern(pexpr) && CXform::FPromising(pmp, pxform, pexpr))
		{
			if (CXform::ExfExpandNAryJoinMinCard == pxform->Exfid())
			{
				GPOS_ASSERT(COperator::EopLogicalNAryJoin == pexpr->Pop()->Eopid());

				// derive stats on NAry join expression
				CExpressionHandle exprhdl(pmp);
				exprhdl.Attach(pexpr);
				exprhdl.DeriveStats(pmp, pmp, NULL /*prprel*/, NULL /*pdrgpstatCtxt*/);
			}

			pxform->Transform(pxfctxt, pxfres, pexpr);

			CExpression *pexprResult = pxfres->PexprNext();
			while (NULL != pexprResult)
			{
				GPOS_ASSERT(pexprResult->FMatchDebug(pexprResult));

				pexprResult = pxfres->PexprNext();
			}
			(void) pxfres->OsPrint(os);
		}
#endif // GPOS_DEBUG

		pxfres->Release();
		pxfctxt->Release();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CXformTest::PexprStarJoinTree
//
//	@doc:
//		Generate a randomized star join tree
//
//---------------------------------------------------------------------------
CExpression *
CXformTest::PexprStarJoinTree
	(
	IMemoryPool *pmp,
	ULONG ulTabs
	)
{
	
	CExpression *pexprLeft = CTestUtils::PexprLogicalGet(pmp);
	
	for (ULONG ul = 1; ul < ulTabs; ul++)
	{
		CDrvdPropRelational *pdprelLeft = CDrvdPropRelational::Pdprel(pexprLeft->PdpDerive());
		CColRef *pcrLeft = pdprelLeft->PcrsOutput()->PcrAny();
	
		CExpression *pexprRight = CTestUtils::PexprLogicalGet(pmp);

		CDrvdPropRelational *pdprelRight = CDrvdPropRelational::Pdprel(pexprRight->PdpDerive());
		CColRef *pcrRight = pdprelRight->PcrsOutput()->PcrAny();
		
		CExpression *pexprPred = CUtils::PexprScalarEqCmp(pmp, pcrLeft, pcrRight);
		
		pexprLeft = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(pmp, pexprLeft, pexprRight, pexprPred);
	}
	
	return pexprLeft;	
}
	

//---------------------------------------------------------------------------
//	@function:
//		CXformTest::PexprJoinTree
//
//	@doc:
//		Generate a randomized star join tree
//
//---------------------------------------------------------------------------
CExpression *
CXformTest::PexprJoinTree
	(
	IMemoryPool *pmp
	)
{
	return PexprStarJoinTree(pmp, 3);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CXformTest::EresUnittest_Mapping
//
//	@doc:
//		Test name -> xform mapping
//
//---------------------------------------------------------------------------
GPOS_RESULT
CXformTest::EresUnittest_Mapping()
{
	for (ULONG ul = 0; ul < CXform::ExfSentinel; ul++)
	{
		CXform::EXformId exfid = (CXform::EXformId) ul;
		CXform *pxform = CXformFactory::Pxff()->Pxf(exfid);
		CXform *pxformMapped = CXformFactory::Pxff()->Pxf(pxform->SzId());
		GPOS_ASSERT(pxform == pxformMapped);
	}

	return GPOS_OK;
}
#endif // GPOS_DEBUG


// EOF
