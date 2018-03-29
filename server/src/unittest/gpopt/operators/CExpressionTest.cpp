//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CExpressionTest.cpp
//
//	@doc:
//		Test for CExpression
//---------------------------------------------------------------------------
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/base/CDatumInt8GPDB.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/IMDScalarOp.h"

#include "gpopt/base/CCTEReq.h"
#include "gpopt/base/CDrvdPropCtxtPlan.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/mdcache/CAutoMDAccessor.h"
#include "gpopt/operators/ops.h"

#include "unittest/base.h"
#include "unittest/gpopt/operators/CExpressionTest.h"
#include "unittest/gpopt/CTestUtils.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CExpressionTest::EresUnittest
//
//	@doc:
//		Create required properties which are empty, except for required column set, given by 'pcrs'.
// 		Caller takes ownership of returned pointer.
//
//---------------------------------------------------------------------------
CReqdPropPlan *
CExpressionTest::PrppCreateRequiredProperties(IMemoryPool *pmp, CColRefSet *pcrs)
{
	COrderSpec *pos = GPOS_NEW(pmp) COrderSpec(pmp);
	CDistributionSpec *pds = GPOS_NEW(pmp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
	CRewindabilitySpec *prs = GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtNone /*ert*/);
	CEnfdOrder *peo = GPOS_NEW(pmp) CEnfdOrder(pos, CEnfdOrder::EomSatisfy);
	CEnfdDistribution *ped = GPOS_NEW(pmp) CEnfdDistribution(pds, CEnfdDistribution::EdmSatisfy);
	CEnfdRewindability *per = GPOS_NEW(pmp) CEnfdRewindability(prs, CEnfdRewindability::ErmSatisfy);
	CCTEReq *pcter = GPOS_NEW(pmp) CCTEReq(pmp);
	return GPOS_NEW(pmp) CReqdPropPlan(pcrs, peo, ped, per, pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionTest::EresUnittest
//
//	@doc:
//		Creates a logical GroupBy and a Get as its child. The columns in the
//		Get follow the format wszColNameFormat.
//		Caller takes ownership of returned pointer.
//
//---------------------------------------------------------------------------
CExpression *
CExpressionTest::PexprCreateGbyWithColumnFormat(IMemoryPool *pmp, const WCHAR *wszColNameFormat)
{
	CWStringConst strRelName(GPOS_WSZ_LIT("Rel1"));
	CMDIdGPDB *pmdidRel = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_TEST_REL_OID1, 1, 1);
	CTableDescriptor *ptabdesc = CTestUtils::PtabdescPlainWithColNameFormat(
			pmp, 3, pmdidRel, wszColNameFormat, CName(&strRelName), false);
	CWStringConst strRelAlias(GPOS_WSZ_LIT("Rel1"));
	CExpression *pexprGet = CTestUtils::PexprLogicalGet(pmp, ptabdesc, &strRelAlias);
	return CTestUtils::PexprLogicalGbAggWithInput(pmp, pexprGet);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionTest::EresUnittest
//
//	@doc:
//		Unittest for expressions
//
//---------------------------------------------------------------------------
GPOS_RESULT
CExpressionTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CExpressionTest::EresUnittest_SimpleOps),
		GPOS_UNITTEST_FUNC(CExpressionTest::EresUnittest_Union),
		GPOS_UNITTEST_FUNC(CExpressionTest::EresUnittest_BitmapGet),
		GPOS_UNITTEST_FUNC(CExpressionTest::EresUnittest_Const),
#ifdef GPOS_DEBUG
		GPOS_UNITTEST_FUNC(CExpressionTest::EresUnittest_ComparisonTypes),
#endif // GPOS_DEBUG
		GPOS_UNITTEST_FUNC(CExpressionTest::EresUnittest_FValidPlan),
		GPOS_UNITTEST_FUNC(CExpressionTest::EresUnittest_FValidPlan_InvalidOrder),
		GPOS_UNITTEST_FUNC(CExpressionTest::EresUnittest_FValidPlan_InvalidDistribution),
		GPOS_UNITTEST_FUNC(CExpressionTest::EresUnittest_FValidPlan_InvalidRewindability),
		GPOS_UNITTEST_FUNC(CExpressionTest::EresUnittest_FValidPlan_InvalidCTEs),

#ifdef GPOS_DEBUG
		GPOS_UNITTEST_FUNC(CExpressionTest::EresUnittest_FValidPlanError),
#endif // GPOS_DEBUG
		GPOS_UNITTEST_FUNC(EresUnittest_ReqdCols),
#ifdef GPOS_DEBUG
		GPOS_UNITTEST_FUNC_ASSERT(CExpressionTest::EresUnittest_InvalidSetOp),
#endif // GPOS_DEBUG
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionTest::EresUnittest_SimpleOps
//
//	@doc:
//		Basic tree builder test
//
//---------------------------------------------------------------------------
GPOS_RESULT
CExpressionTest::EresUnittest_SimpleOps()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	typedef CExpression *(*Pfpexpr)(IMemoryPool*);

	Pfpexpr rgpf[] =
		{
		CTestUtils::PexprLogicalGet,
		CTestUtils::PexprLogicalExternalGet,
		CTestUtils::PexprLogicalGetPartitioned,
		CTestUtils::PexprLogicalSelect,
		CTestUtils::PexprLogicalSelectCmpToConst,
		CTestUtils::PexprLogicalSelectArrayCmp,
		CTestUtils::PexprLogicalSelectPartitioned,
		CTestUtils::PexprLogicalJoin<CLogicalInnerJoin>,
		CTestUtils::PexprLogicalJoin<CLogicalLeftOuterJoin>,
		CTestUtils::PexprLogicalJoin<CLogicalLeftSemiJoin>,
		CTestUtils::PexprLogicalJoin<CLogicalLeftAntiSemiJoin>,
		CTestUtils::PexprLogicalJoin<CLogicalLeftAntiSemiJoinNotIn>,
		CTestUtils::PexprLogicalGbAgg,
		CTestUtils::PexprLogicalGbAggOverJoin,
		CTestUtils::PexprLogicalGbAggWithSum,
		CTestUtils::PexprLogicalLimit,
		CTestUtils::PexprLogicalNAryJoin,
		CTestUtils::PexprLogicalProject,
		CTestUtils::PexprConstTableGet5,
		CTestUtils::PexprLogicalDynamicGet,
		CTestUtils::PexprLogicalSequence,
		CTestUtils::PexprLogicalTVFTwoArgs,
		CTestUtils::PexprLogicalAssert,
		PexprComplexJoinTree
		};

#ifdef GPOS_DEBUG
	// misestimation risk for the roots of the plans in rgpf
	ULONG rgulRisk[] =
		{ 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 2, 1, 2, 1, 1, 1, 1, 1, 1, 6 };
#endif // GPOS_DEBUG

	for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgpf); i++)
	{
		// install opt context in TLS
		CAutoOptCtxt aoc
						(
						pmp,
						&mda,
						NULL,  /* pceeval */
						CTestUtils::Pcm(pmp)
						);

		// generate simple expression
		CExpression *pexpr = rgpf[i](pmp);

		// self-match
		GPOS_ASSERT(pexpr->FMatchDebug(pexpr));

		// debug print
		CWStringDynamic str(pmp, GPOS_WSZ_LIT("\n"));
		COstreamString oss(&str);

		oss << "EXPR:" << std::endl << *pexpr << std::endl;
		GPOS_TRACE(str.Wsz());
		str.Reset();

#ifdef GPOS_DEBUG
		// derive properties on expression
		CDrvdPropRelational *pdprel = CDrvdPropRelational::Pdprel(pexpr->PdpDerive());

		oss << std::endl << "DERIVED PROPS:" << std::endl;
		GPOS_TRACE(str.Wsz());
		str.Reset();
		pexpr->DbgPrint();

		// copy expression
		CColRef *pcrOld = pdprel->PcrsOutput()->PcrAny();
		CColRef *pcrNew = COptCtxt::PoctxtFromTLS()->Pcf()->PcrCreate(pcrOld);
		HMUlCr *phmulcr = GPOS_NEW(pmp) HMUlCr(pmp);

		BOOL fResult = phmulcr->FInsert(GPOS_NEW(pmp) ULONG(pcrOld->UlId()), pcrNew);
		GPOS_ASSERT(fResult);
		CExpression *pexprCopy = pexpr->PexprCopyWithRemappedColumns(pmp, phmulcr, true /*fMustExist*/);
		phmulcr->Release();
		oss << std::endl << "COPIED EXPRESSION (AFTER MAPPING " << *pcrOld << " TO " << *pcrNew << "):" << std::endl;
		GPOS_TRACE(str.Wsz());
		str.Reset();
		pexprCopy->DbgPrint();
		pexprCopy->Release();

		// derive stats on expression
		CReqdPropRelational *prprel = GPOS_NEW(pmp) CReqdPropRelational(GPOS_NEW(pmp) CColRefSet(pmp));
		DrgPstat *pdrgpstatCtxt = GPOS_NEW(pmp) DrgPstat(pmp);
		IStatistics *pstats = pexpr->PstatsDerive(prprel, pdrgpstatCtxt);
		GPOS_ASSERT(NULL != pstats);

		oss << "Expected risk: " << rgulRisk[i] << std::endl;
		oss << std::endl << "STATS:" << *pstats << std::endl;
		GPOS_TRACE(str.Wsz());

		GPOS_ASSERT(rgulRisk[i] == pstats->UlStatsEstimationRisk());

		prprel->Release();
		pdrgpstatCtxt->Release();
#endif // GPOS_DEBUG

		// cleanup
		pexpr->Release();
	}

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//		CExpressionTest::EresUnittest_Union
//
//	@doc:
//		Basic tree builder test w/ Unions
//
//---------------------------------------------------------------------------
GPOS_RESULT
CExpressionTest::EresUnittest_Union()
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

	// build union tree of depth 2
	CExpression *pexpr = CTestUtils::PexprLogicalUnion(pmp, 2);

	// debug print
	CWStringDynamic str(pmp);
	COstreamString oss(&str);
	pexpr->OsPrint(oss);

	GPOS_TRACE(str.Wsz());

	// derive properties on expression
	(void) pexpr->PdpDerive();

#ifdef GPOS_DEBUG
	CReqdPropRelational *prprel = GPOS_NEW(pmp) CReqdPropRelational(GPOS_NEW(pmp) CColRefSet(pmp));
	DrgPstat *pdrgpstatCtxt = GPOS_NEW(pmp) DrgPstat(pmp);
	IStatistics *pstats = pexpr->PstatsDerive(prprel, pdrgpstatCtxt);
	GPOS_ASSERT(NULL != pstats);

	// We expect a risk of 3 because every Union increments the risk.
	GPOS_ASSERT(3 == pstats->UlStatsEstimationRisk());
	pdrgpstatCtxt->Release();
	prprel->Release();
#endif // GPOS_DEBUG

	// cleanup
	pexpr->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//		CExpressionTest::EresUnittest_BitmapScan
//
//	@doc:
//		Basic tree builder test with bitmap index and table get.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CExpressionTest::EresUnittest_BitmapGet()
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

	CWStringConst strRelName(GPOS_WSZ_LIT("MyTable"));
	CWStringConst strRelAlias(GPOS_WSZ_LIT("T"));
	CMDIdGPDB *pmdidRel = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID, 1, 1);
	const WCHAR *wszColNameFormat = GPOS_WSZ_LIT("column_%04d");
	CTableDescriptor *ptabdesc = CTestUtils::PtabdescPlainWithColNameFormat(
			pmp, 3, pmdidRel, wszColNameFormat, CName(&strRelName), false);

	// get the index associated with the table
	const IMDRelation *pmdrel = mda.Pmdrel(ptabdesc->Pmdid());
	GPOS_ASSERT(0 < pmdrel->UlIndices());

	// create an index descriptor
	IMDId *pmdidIndex = pmdrel->PmdidIndex(0 /*ulPos*/);
	const IMDIndex *pmdindex = mda.Pmdindex(pmdidIndex);

	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();

	const ULONG ulCols = pmdrel->UlColumns();
	GPOS_ASSERT(2 < ulCols);

	// create an index on the first column
	const IMDColumn *pmdcol = pmdrel->Pmdcol(0);
	const IMDType *pmdtype = mda.Pmdtype(pmdcol->PmdidType());
	CColRef *pcrFirst = pcf->PcrCreate(pmdtype, pmdcol->ITypeModifier());

	CExpression *pexprIndexCond = CUtils::PexprScalarEqCmp
								(
								pmp,
								pcrFirst,
								CUtils::PexprScalarConstInt4(pmp, 20 /*iVal*/)
								);

	CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(CMDIdGPDB::m_mdidUnknown);
	CIndexDescriptor *pindexdesc = CIndexDescriptor::Pindexdesc(pmp, ptabdesc, pmdindex);
	CExpression *pexprBitmapIndex =
			GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CScalarBitmapIndexProbe(pmp, pindexdesc, pmdid),
					pexprIndexCond
					);

	DrgPcr *pdrgpcrTable = GPOS_NEW(pmp) DrgPcr(pmp);
	for (ULONG ul = 0; ul < ulCols; ++ul)
	{
		const IMDColumn *pmdcol = pmdrel->Pmdcol(ul);
		const IMDType *pmdtype = mda.Pmdtype(pmdcol->PmdidType());
		CColRef *pcr = pcf->PcrCreate(pmdtype, pmdcol->ITypeModifier());
		pdrgpcrTable->Append(pcr);
	}

	CExpression *pexprTableCond = CUtils::PexprScalarEqCmp
								(
								pmp,
								pcrFirst,
								CUtils::PexprScalarConstInt4(pmp, 20 /*iVal*/)
								);

	CExpression *pexprBitmapTableGet =
			GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalBitmapTableGet
							(
							pmp,
							ptabdesc,
							gpos::ulong_max, // pgexprOrigin
							GPOS_NEW(pmp) CName(pmp, CName(&strRelAlias)),
							pdrgpcrTable
							),
					pexprTableCond,
					pexprBitmapIndex
					);

	// debug print
	CWStringDynamic str(pmp);
	COstreamString oss(&str);
	pexprBitmapTableGet->OsPrint(oss);

	CWStringConst strExpectedDebugPrint(GPOS_WSZ_LIT(
		"+--CLogicalBitmapTableGet , Table Name: (\"MyTable\"), Columns: [\"ColRef_0001\" (1), \"ColRef_0002\" (2), \"ColRef_0003\" (3)]\n"
		"   |--CScalarCmp (=)\n"
		"   |  |--CScalarIdent \"ColRef_0000\" (0)\n"
		"   |  +--CScalarConst (20)\n"
		"   +--CScalarBitmapIndexProbe   Bitmap Index Name: (\"T_a\")\n"
		"      +--CScalarCmp (=)\n"
		"         |--CScalarIdent \"ColRef_0000\" (0)\n"
		"         +--CScalarConst (20)\n"));

	GPOS_ASSERT(str.FEquals(&strExpectedDebugPrint));

	// derive properties on expression
	(void) pexprBitmapTableGet->PdpDerive();

	// test matching of bitmap index probe expressions
	CMDIdGPDB *pmdid2 = GPOS_NEW(pmp) CMDIdGPDB(CMDIdGPDB::m_mdidUnknown);
	CIndexDescriptor *pindexdesc2 = CIndexDescriptor::Pindexdesc(pmp, ptabdesc, pmdindex);
	CExpression *pexprIndexCond2 = CUtils::PexprScalarEqCmp
								(
								pmp,
								pcrFirst,
								CUtils::PexprScalarConstInt4(pmp, 20 /*iVal*/)
								);
	CExpression *pexprBitmapIndex2 =
			GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CScalarBitmapIndexProbe(pmp, pindexdesc2, pmdid2),
					pexprIndexCond2
					);
	CWStringDynamic strIndex2(pmp);
	COstreamString ossIndex2(&strIndex2);
	pexprBitmapIndex2->OsPrint(ossIndex2);
	CWStringConst strExpectedDebugPrintIndex2(GPOS_WSZ_LIT(
		"+--CScalarBitmapIndexProbe   Bitmap Index Name: (\"T_a\")\n"
		"   +--CScalarCmp (=)\n"
		"      |--CScalarIdent \"ColRef_0000\" (0)\n"
		"      +--CScalarConst (20)\n"));

	GPOS_ASSERT(strIndex2.FEquals(&strExpectedDebugPrintIndex2));
	GPOS_ASSERT(pexprBitmapIndex2->FMatch(pexprBitmapIndex));

	pmdid->AddRef();
	pexprBitmapIndex->AddRef();
	pexprBitmapIndex2->AddRef();
	CExpression *pexprBitmapBoolOp1 =
			GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CScalarBitmapBoolOp(pmp, CScalarBitmapBoolOp::EbitmapboolAnd, pmdid),
					pexprBitmapIndex,
					pexprBitmapIndex2
					);

	pmdid->AddRef();
	pexprBitmapIndex->AddRef();
	CExpression *pexprBitmapBoolOp2 =
			GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CScalarBitmapBoolOp(pmp, CScalarBitmapBoolOp::EbitmapboolAnd, pmdid),
					pexprBitmapIndex,
					pexprBitmapIndex2
					);
	GPOS_ASSERT(pexprBitmapBoolOp2->FMatch(pexprBitmapBoolOp1));

	// cleanup
	pexprBitmapBoolOp2->Release();
	pexprBitmapBoolOp1->Release();
	pexprBitmapTableGet->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionTest::EresUnittest_Const
//
//	@doc:
//		Test of scalar constant expressions
//
//---------------------------------------------------------------------------
GPOS_RESULT
CExpressionTest::EresUnittest_Const()
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

	BOOL fVal = true;
	CExpression *pexprTrue = CUtils::PexprScalarConstBool(pmp, fVal);

	fVal = false;
	CExpression *pexprFalse = CUtils::PexprScalarConstBool(pmp, fVal);

	ULONG ulVal = 123456;
	CExpression *pexprUl = CUtils::PexprScalarConstInt4(pmp, ulVal);
	CExpression *pexprUl2nd = CUtils::PexprScalarConstInt4(pmp, ulVal);

	ulVal = 1;
	CExpression *pexprUlOne = CUtils::PexprScalarConstInt4(pmp, ulVal);

	CWStringDynamic str(pmp);
	COstreamString oss(&str);
	oss << std::endl;
	pexprTrue->OsPrint(oss);
	pexprFalse->OsPrint(oss);
	pexprUl->OsPrint(oss);
	pexprUl2nd->OsPrint(oss);
	pexprUlOne->OsPrint(oss);

#ifdef GPOS_DEBUG
	CScalarConst *pscalarconstTrue = CScalarConst::PopConvert(pexprTrue->Pop());
	CScalarConst *pscalarconstFalse = CScalarConst::PopConvert(pexprFalse->Pop());
	CScalarConst *pscalarconstUl = CScalarConst::PopConvert(pexprUl->Pop());
	CScalarConst *pscalarconstUl2nd = CScalarConst::PopConvert(pexprUl2nd->Pop());
	CScalarConst *pscalarconstUlOne = CScalarConst::PopConvert(pexprUlOne->Pop());
#endif // GPOS_DEBUG

	GPOS_ASSERT(pscalarconstUl->UlHash() == pscalarconstUl2nd->UlHash());
	GPOS_ASSERT(!pscalarconstTrue->FMatch(pscalarconstFalse));
	GPOS_ASSERT(!pscalarconstTrue->FMatch(pscalarconstUlOne));

	pexprTrue->Release();
	pexprFalse->Release();
	pexprUl->Release();
	pexprUl2nd->Release();
	pexprUlOne->Release();

	GPOS_TRACE(str.Wsz());

	return GPOS_OK;
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CExpressionTest::EresUnittest_ComparisonTypes
//
//	@doc:
//		Test of scalar comparison types
//
//---------------------------------------------------------------------------
GPOS_RESULT
CExpressionTest::EresUnittest_ComparisonTypes()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache());
	mda.RegisterProvider(CTestUtils::m_sysidDefault, pmdp);

	// install opt context in TLS
	CAutoOptCtxt aoc
					(
					pmp,
					&mda,
					NULL,  /* pceeval */
					CTestUtils::Pcm(pmp)
					);

	const IMDType *pmdtype = mda.PtMDType<IMDTypeInt4>();

	GPOS_ASSERT(IMDType::EcmptEq == CUtils::Ecmpt(pmdtype->PmdidCmp(IMDType::EcmptEq)));
	GPOS_ASSERT(IMDType::EcmptL == CUtils::Ecmpt(pmdtype->PmdidCmp(IMDType::EcmptL)));
	GPOS_ASSERT(IMDType::EcmptG == CUtils::Ecmpt(pmdtype->PmdidCmp(IMDType::EcmptG)));

	const IMDScalarOp *pmdscopEq = mda.Pmdscop(pmdtype->PmdidCmp(IMDType::EcmptEq));
	const IMDScalarOp *pmdscopLT = mda.Pmdscop(pmdtype->PmdidCmp(IMDType::EcmptL));
	const IMDScalarOp *pmdscopGT = mda.Pmdscop(pmdtype->PmdidCmp(IMDType::EcmptG));

	GPOS_ASSERT(IMDType::EcmptNEq == CUtils::Ecmpt(pmdscopEq->PmdidOpInverse()));
	GPOS_ASSERT(IMDType::EcmptLEq == CUtils::Ecmpt(pmdscopGT->PmdidOpInverse()));
	GPOS_ASSERT(IMDType::EcmptGEq == CUtils::Ecmpt(pmdscopLT->PmdidOpInverse()));

	return GPOS_OK;
}
#endif // GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CExpressionTest::SetupPlanForFValidPlanTest
//
//	@doc:
//		Helper function for the FValidPlan tests
//
//---------------------------------------------------------------------------
void CExpressionTest::SetupPlanForFValidPlanTest
	(
	IMemoryPool *pmp,
	CExpression **ppexprGby,
	CColRefSet **ppcrs,
	CExpression **ppexprPlan,
	CReqdPropPlan **pprpp
	)
{
	*ppexprGby = PexprCreateGbyWithColumnFormat(pmp, GPOS_WSZ_LIT("Test Column%d"));

	// Create a column requirement using the first output column of the group by.
	CColRefSet *pcrsGby = CDrvdPropRelational::Pdprel((*ppexprGby)->PdpDerive())->PcrsOutput();
	*ppcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	(*ppcrs)->Include(pcrsGby->PcrFirst());

	*pprpp = PrppCreateRequiredProperties(pmp, *ppcrs);
	CExpressionHandle exprhdl(pmp);
	exprhdl.Attach(*ppexprGby);
	exprhdl.InitReqdProps(*pprpp);

	// Optimize the logical plan under default required properties, which are always satisfied.
	CEngine eng(pmp);
	CAutoP<CQueryContext> pqc;
	pqc = CTestUtils::PqcGenerate(pmp, *ppexprGby);
	eng.Init(pqc.Pt(), NULL /*pdrgpss*/);
	eng.Optimize();
	*ppexprPlan = eng.PexprExtractPlan();
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionTest::EresUnittest_FValidPlan
//
//	@doc:
//		Test for CExpression::FValidPlan
// 		Test now just very basic cases. More complex cases are covered by minidump tests
// 		in CEnumeratorTest.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CExpressionTest::EresUnittest_FValidPlan()
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
	const IMDType *pmdtype = mda.PtMDType<IMDTypeInt4>();

	// Create a group-by with a get child. Properties required contain one of the columns in the group by.
	// Test that the plan is valid.
	{
		CExpression *pexprGby = NULL;
		CColRefSet *pcrs = NULL;
		CExpression *pexprPlan = NULL;
		CReqdPropPlan *prpp = NULL;

		SetupPlanForFValidPlanTest(pmp, &pexprGby, &pcrs, &pexprPlan, &prpp);
		CDrvdPropCtxtPlan *pdpctxtplan = GPOS_NEW(pmp) CDrvdPropCtxtPlan(pmp);

		// Test that prpp is actually satisfied.
		GPOS_ASSERT(pexprPlan->FValidPlan(prpp, pdpctxtplan));
		pdpctxtplan->Release();
		pexprPlan->Release();
		prpp->Release();
		pexprGby->Release();
	}
	// Create a group-by with a get child. Properties required contain one column that doesn't exist.
	// Test that the plan is NOT valid.
	{
		CExpression *pexprGby = PexprCreateGbyWithColumnFormat(pmp, GPOS_WSZ_LIT("Test Column%d"));

		(void) pexprGby->PdpDerive();
		CColRefSet *pcrsInvalid = GPOS_NEW(pmp) CColRefSet(pmp);
		CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();

		// Creating the column reference with the column factory ensures that it's a brand new column.
		CColRef *pcrComputed = pcf->PcrCreate(pmdtype, IDefaultTypeModifier);
		pcrsInvalid->Include(pcrComputed);

		CReqdPropPlan *prpp = PrppCreateRequiredProperties(pmp, pcrsInvalid);
		CExpressionHandle exprhdl(pmp);
		exprhdl.Attach(pexprGby);
		exprhdl.InitReqdProps(prpp);

		// Optimize the logical plan, but under default required properties, which are always satisfied.
		CEngine eng(pmp);
		CAutoP<CQueryContext> pqc;
		pqc = CTestUtils::PqcGenerate(pmp, pexprGby);
		eng.Init(pqc.Pt(), NULL /*pdrgpss*/);
		eng.Optimize();
		CExpression *pexprPlan = eng.PexprExtractPlan();

		CDrvdPropCtxtPlan *pdpctxtplan = GPOS_NEW(pmp) CDrvdPropCtxtPlan(pmp);

		// Test that prpp is actually unsatisfied.
		GPOS_ASSERT(!pexprPlan->FValidPlan(prpp, pdpctxtplan));
		pdpctxtplan->Release();
		pexprPlan->Release();
		prpp->Release();
		pexprGby->Release();
	}

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionTest::EresUnittest_FValidPlan_InvalidOrder
//
//	@doc:
//		Test for CExpression::FValidPlan with incompatible order properties
//
//---------------------------------------------------------------------------
GPOS_RESULT
CExpressionTest::EresUnittest_FValidPlan_InvalidOrder()
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

	CExpression *pexprGby = NULL;
	CColRefSet *pcrs = NULL;
	CExpression *pexprPlan = NULL;
	CReqdPropPlan *prpp = NULL;

	SetupPlanForFValidPlanTest(pmp, &pexprGby, &pcrs, &pexprPlan, &prpp);
	CDrvdPropCtxtPlan *pdpctxtplan = GPOS_NEW(pmp) CDrvdPropCtxtPlan(pmp);

	// Create similar requirements, but
	// add an order requirement using a couple of output columns of a Get
	CExpression *pexprGet = CTestUtils::PexprLogicalGet(pmp);
	CColRefSet *pcrsGet = CDrvdPropRelational::Pdprel(pexprGet->PdpDerive())->PcrsOutput();
	CColRefSet *pcrsGetCopy = GPOS_NEW(pmp) CColRefSet(pmp, *pcrsGet);

	DrgPcr *pdrgpcrGet = pcrsGetCopy->Pdrgpcr(pmp);
	GPOS_ASSERT(2 <= pdrgpcrGet->UlLength());

	COrderSpec *pos = GPOS_NEW(pmp) COrderSpec(pmp);
	const IMDType *pmdtypeint4 = mda.PtMDType<IMDTypeInt4>();
	IMDId *pmdidInt4LT = pmdtypeint4->PmdidCmp(IMDType::EcmptL);
	pmdidInt4LT->AddRef();
	pos->Append(pmdidInt4LT, (*pdrgpcrGet)[1], COrderSpec::EntFirst);
	CEnfdOrder *peo = GPOS_NEW(pmp) CEnfdOrder(pos, CEnfdOrder::EomSatisfy);

	CDistributionSpec *pds = GPOS_NEW(pmp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
	CRewindabilitySpec *prs = GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtNone /*ert*/);
	CEnfdDistribution *ped = GPOS_NEW(pmp) CEnfdDistribution(pds, CEnfdDistribution::EdmExact);
	CEnfdRewindability *per = GPOS_NEW(pmp) CEnfdRewindability(prs, CEnfdRewindability::ErmSatisfy);
	CCTEReq *pcter = GPOS_NEW(pmp) CCTEReq(pmp);
	CReqdPropPlan *prppIncompatibleOrder = GPOS_NEW(pmp) CReqdPropPlan(pcrsGetCopy, peo, ped, per, pcter);

	CPartInfo *ppartinfo = GPOS_NEW(pmp) CPartInfo(pmp);
	prppIncompatibleOrder->InitReqdPartitionPropagation(pmp, ppartinfo);

	// Test that the plan is not valid.
	GPOS_ASSERT(!pexprPlan->FValidPlan(prppIncompatibleOrder, pdpctxtplan));
	pdpctxtplan->Release();
	prppIncompatibleOrder->Release();
	pdrgpcrGet->Release();
	ppartinfo->Release();
	prpp->Release();
	pexprGet->Release();
	pexprPlan->Release();
	pexprGby->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionTest::EresUnittest_FValidPlan_InvalidDistribution
//
//	@doc:
//		Test for CExpression::FValidPlan with incompatible distribution properties
//
//---------------------------------------------------------------------------
GPOS_RESULT
CExpressionTest::EresUnittest_FValidPlan_InvalidDistribution()
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

	CExpression *pexprGby = NULL;
	CColRefSet *pcrs = NULL;
	CExpression *pexprPlan = NULL;
	CReqdPropPlan *prpp = NULL;

	SetupPlanForFValidPlanTest(pmp, &pexprGby, &pcrs, &pexprPlan, &prpp);
	CDrvdPropCtxtPlan *pdpctxtplan = GPOS_NEW(pmp) CDrvdPropCtxtPlan(pmp);
	COrderSpec *pos = GPOS_NEW(pmp) COrderSpec(pmp);
	CDistributionSpec *pds = GPOS_NEW(pmp) CDistributionSpecRandom();
	CRewindabilitySpec *prs = GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtNone /*ert*/);
	CEnfdOrder *peo = GPOS_NEW(pmp) CEnfdOrder(pos, CEnfdOrder::EomSatisfy);
	CEnfdDistribution *ped = GPOS_NEW(pmp) CEnfdDistribution(pds, CEnfdDistribution::EdmExact);
	CEnfdRewindability *per = GPOS_NEW(pmp) CEnfdRewindability(prs, CEnfdRewindability::ErmSatisfy);
	CCTEReq *pcter = GPOS_NEW(pmp) CCTEReq(pmp);
	CColRefSet *pcrsCopy = GPOS_NEW(pmp) CColRefSet(pmp, *pcrs);
	CReqdPropPlan *prppIncompatibleDistribution = GPOS_NEW(pmp) CReqdPropPlan(pcrsCopy, peo, ped, per, pcter);
	CPartInfo *ppartinfo = GPOS_NEW(pmp) CPartInfo(pmp);
	prppIncompatibleDistribution->InitReqdPartitionPropagation(pmp, ppartinfo);

	// Test that the plan is not valid.
	GPOS_ASSERT(!pexprPlan->FValidPlan(prppIncompatibleDistribution, pdpctxtplan));
	pdpctxtplan->Release();
	pexprPlan->Release();
	prppIncompatibleDistribution->Release();
	prpp->Release();
	pexprGby->Release();
	ppartinfo->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionTest::EresUnittest_FValidPlan_InvalidRewindability
//
//	@doc:
//		Test for CExpression::FValidPlan with incompatible rewindability properties
//
//---------------------------------------------------------------------------
GPOS_RESULT
CExpressionTest::EresUnittest_FValidPlan_InvalidRewindability()
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

	CExpression *pexprGby = NULL;
	CColRefSet *pcrs = NULL;
	CExpression *pexprPlan = NULL;
	CReqdPropPlan *prpp = NULL;

	SetupPlanForFValidPlanTest(pmp, &pexprGby, &pcrs, &pexprPlan, &prpp);
	CDrvdPropCtxtPlan *pdpctxtplan = GPOS_NEW(pmp) CDrvdPropCtxtPlan(pmp);

	COrderSpec *pos = GPOS_NEW(pmp) COrderSpec(pmp);
	CDistributionSpec *pds = GPOS_NEW(pmp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
	CRewindabilitySpec *prs = GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtGeneral /*ert*/);
	CEnfdOrder *peo = GPOS_NEW(pmp) CEnfdOrder(pos, CEnfdOrder::EomSatisfy);
	CEnfdDistribution *ped = GPOS_NEW(pmp) CEnfdDistribution(pds, CEnfdDistribution::EdmExact);
	CEnfdRewindability *per = GPOS_NEW(pmp) CEnfdRewindability(prs, CEnfdRewindability::ErmSatisfy);
	CCTEReq *pcter = GPOS_NEW(pmp) CCTEReq(pmp);
	CColRefSet *pcrsCopy = GPOS_NEW(pmp) CColRefSet(pmp, *pcrs);
	CReqdPropPlan *prppIncompatibleRewindability = GPOS_NEW(pmp) CReqdPropPlan(pcrsCopy, peo, ped, per, pcter);
	CPartInfo *ppartinfo = GPOS_NEW(pmp) CPartInfo(pmp);
	prppIncompatibleRewindability->InitReqdPartitionPropagation(pmp, ppartinfo);

	// Test that the plan is not valid.
	GPOS_ASSERT(!pexprPlan->FValidPlan(prppIncompatibleRewindability, pdpctxtplan));
	pdpctxtplan->Release();
	pexprPlan->Release();
	prppIncompatibleRewindability->Release();
	prpp->Release();
	pexprGby->Release();
	ppartinfo->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionTest::EresUnittest_FValidPlan_InvalidCTEs
//
//	@doc:
//		Test for CExpression::FValidPlan with incompatible CTE properties
//
//---------------------------------------------------------------------------
GPOS_RESULT
CExpressionTest::EresUnittest_FValidPlan_InvalidCTEs()
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

	CExpression *pexprGby = NULL;
	CColRefSet *pcrs = NULL;
	CExpression *pexprPlan = NULL;
	CReqdPropPlan *prpp = NULL;

	SetupPlanForFValidPlanTest(pmp, &pexprGby, &pcrs, &pexprPlan, &prpp);
	CDrvdPropCtxtPlan *pdpctxtplan = GPOS_NEW(pmp) CDrvdPropCtxtPlan(pmp);

	COrderSpec *pos = GPOS_NEW(pmp) COrderSpec(pmp);
	CDistributionSpec *pds = GPOS_NEW(pmp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
	CRewindabilitySpec *prs = GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtNone /*ert*/);
	CEnfdOrder *peo = GPOS_NEW(pmp) CEnfdOrder(pos, CEnfdOrder::EomSatisfy);
	CEnfdDistribution *ped = GPOS_NEW(pmp) CEnfdDistribution(pds, CEnfdDistribution::EdmExact);
	CEnfdRewindability *per = GPOS_NEW(pmp) CEnfdRewindability(prs, CEnfdRewindability::ErmSatisfy);
	CCTEReq *pcter = GPOS_NEW(pmp) CCTEReq(pmp);
	ULONG ulCTEId = 0;

	CExpression *pexprProducer = CTestUtils::PexprLogicalCTEProducerOverSelect(pmp, ulCTEId);
	CDrvdPropPlan *pdpplan = CDrvdPropPlan::Pdpplan(pexprPlan->PdpDerive());
	pdpplan->AddRef();
	pcter->Insert(ulCTEId, CCTEMap::EctProducer /*ect*/, true /*fRequired*/, pdpplan);
	CColRefSet *pcrsCopy = GPOS_NEW(pmp) CColRefSet(pmp, *pcrs);
	CReqdPropPlan *prppIncompatibleCTE = GPOS_NEW(pmp) CReqdPropPlan(pcrsCopy, peo, ped, per, pcter);
	CPartInfo *ppartinfo = GPOS_NEW(pmp) CPartInfo(pmp);
	prppIncompatibleCTE->InitReqdPartitionPropagation(pmp, ppartinfo);

	// Test that the plan is not valid.
	GPOS_ASSERT(!pexprPlan->FValidPlan(prppIncompatibleCTE, pdpctxtplan));
	pdpctxtplan->Release();
	pexprPlan->Release();
	prppIncompatibleCTE->Release();
	prpp->Release();
	pexprGby->Release();
	ppartinfo->Release();
	pexprProducer->Release();

	return GPOS_OK;
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CExpressionTest::EresUnittest_FValidPlanError
//
//	@doc:
//		Tests that CExpression::FValidPlan fails with an assert exception in debug mode
//		for bad input.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CExpressionTest::EresUnittest_FValidPlanError()
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
	const IMDType *pmdtype = mda.PtMDType<IMDTypeInt4>();

	GPOS_RESULT eres = GPOS_OK;
	// Test that in debug mode GPOS_ASSERT fails for non-physical expressions.
	{
		CColRefSet *pcrsInvalid = GPOS_NEW(pmp) CColRefSet(pmp);
		CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();
		CColRef *pcrComputed = pcf->PcrCreate(pmdtype, IDefaultTypeModifier);
		pcrsInvalid->Include(pcrComputed);

		CReqdPropPlan *prpp = PrppCreateRequiredProperties(pmp, pcrsInvalid);
		IDatum *pdatum = GPOS_NEW(pmp) gpnaucrates::CDatumInt8GPDB(CTestUtils::m_sysidDefault,
				1 /*lVal*/, false /*fNull*/);
		CExpression *pexpr = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarConst(pmp, pdatum));

		CDrvdPropCtxtPlan *pdpctxtplan = GPOS_NEW(pmp) CDrvdPropCtxtPlan(pmp);
		GPOS_TRY
		{
			// FValidPlan should fail for expressions which are not physical.
			if (!pexpr->FValidPlan(prpp, pdpctxtplan))
			{
				eres = GPOS_FAILED;
			}
			pdpctxtplan->Release();
		}
		GPOS_CATCH_EX(ex)
		{
			pdpctxtplan->Release();
			if (!GPOS_MATCH_EX(ex, CException::ExmaSystem, CException::ExmiAssert))
			{
				GPOS_RETHROW(ex);
			}
			else
			{
				GPOS_RESET_EX;
			}
		}
		GPOS_CATCH_END;

		pexpr->Release();
		prpp->Release();
	}

	return eres;
}
#endif // GPOS_DEBUG



//---------------------------------------------------------------------------
//	@function:
//		CExpressionTest::EresCheckCachedReqdCols
//
//	@doc:
//		Helper for checking cached required columns
//
//---------------------------------------------------------------------------
GPOS_RESULT
CExpressionTest::EresCheckCachedReqdCols
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CReqdPropPlan *prppInput
	)
{
	if (pexpr->Pop()->FScalar())
	{
		// scalar operators have no required columns
		return GPOS_OK;
	}

	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != prppInput);

	CExpressionHandle exprhdl(pmp);
	exprhdl.Attach(pexpr);

	// init required properties of expression
	exprhdl.InitReqdProps(prppInput);

	// create array of child derived properties
	DrgPdp *pdrgpdp = GPOS_NEW(pmp) DrgPdp(pmp);

	GPOS_RESULT eres = GPOS_OK;
	const ULONG ulArity =  pexpr->UlArity();
	for (ULONG ul = 0; GPOS_OK == eres && ul < ulArity; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		if (pexprChild->Pop()->FScalar())
		{
			continue;
		}
		GPOS_ASSERT(NULL != pexprChild->Prpp());

		// extract cached required columns of the n-th child
		CColRefSet *pcrsChildReqd = pexprChild->Prpp()->PcrsRequired();

		// compute required columns of the n-th child
		exprhdl.ComputeChildReqdCols(ul, pdrgpdp);

		// check if cached columns pointer is the same as computed columns pointer,
		// if this is not the case, then we have re-computed the same set of columns and test should fail
		if (pcrsChildReqd != exprhdl.Prpp(ul)->PcrsRequired())
		{
			CAutoTrace at(pmp);
			at.Os() << "\nExpression: \n" << *pexprChild;
			at.Os() << "\nCached cols: " << pcrsChildReqd << " : " << *pcrsChildReqd;
			at.Os() << "\nComputed cols: " <<  exprhdl.Prpp(ul)->PcrsRequired() << " : " << *exprhdl.Prpp(ul)->PcrsRequired();

			eres = GPOS_FAILED;
			continue;
		}

		// call the function recursively for child expression
		GPOS_RESULT eres = EresCheckCachedReqdCols(pmp, pexprChild, exprhdl.Prpp(ul));
		if (GPOS_FAILED == eres)
		{
			eres = GPOS_FAILED;
			continue;
		}

		// add plan props of current child to derived props array
		CDrvdProp *pdp = pexprChild->PdpDerive();
		pdp->AddRef();
		pdrgpdp->Append(pdp);
	}

	pdrgpdp->Release();

	return eres;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionTest::EresComputeReqdCols
//
//	@doc:
//		Helper for testing required column computation
//
//---------------------------------------------------------------------------
GPOS_RESULT
CExpressionTest::EresComputeReqdCols
	(
	const CHAR *szFilePath
	)
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// reset metadata cache
	CMDCache::Reset();

	// set up MD providers
	CMDProviderMemory *pmdp = GPOS_NEW(pmp) CMDProviderMemory(pmp, szFilePath);
	GPOS_CHECK_ABORT;

	GPOS_RESULT eres = GPOS_FAILED;
	{
		CAutoMDAccessor amda(pmp, pmdp,  CTestUtils::m_sysidDefault);
		CAutoOptCtxt aoc(pmp, amda.Pmda(), NULL,  /* pceeval */ CTestUtils::Pcm(pmp));

		// read query expression
		CExpression *pexpr = CTestUtils::PexprReadQuery(pmp, szFilePath);

		// optimize query
		CEngine eng(pmp);
		CQueryContext *pqc = CTestUtils::PqcGenerate(pmp, pexpr);
		eng.Init(pqc, NULL /*pdrgpss*/);
		eng.Optimize();

		// extract plan and decorate it with required columns
		CExpression *pexprPlan = eng.PexprExtractPlan();
		(void) pexprPlan->PrppCompute(pmp, pqc->Prpp());

		// attempt computing required columns again --
		// we make sure that we reuse cached required columns at each operator
		eres = EresCheckCachedReqdCols(pmp, pexprPlan, pqc->Prpp());

		pexpr->Release();
		pexprPlan->Release();
		GPOS_DELETE(pqc);
	}

	return eres;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionTest::EresUnittest_ReqdCols
//
//	@doc:
//		Test for required columns computation
//
//---------------------------------------------------------------------------
GPOS_RESULT
CExpressionTest::EresUnittest_ReqdCols()
{
	const CHAR *rgszTests[] =
	{
		"../data/dxl/tpch/q1.mdp",
		"../data/dxl/tpch/q3.mdp",
		"../data/dxl/expressiontests/WinFunc-OuterRef-Partition-Query.xml",
		"../data/dxl/expressiontests/WinFunc-OuterRef-Partition-Order-Query.xml",
	};

	GPOS_RESULT eres = GPOS_OK;
	for (ULONG ul = 0; GPOS_OK == eres && ul < GPOS_ARRAY_SIZE(rgszTests); ul++)
	{
		const CHAR *szFilePath = rgszTests[ul];
		eres = EresComputeReqdCols(szFilePath);
	}

	return eres;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionTest::EresUnittest_InvalidSetOp
//
//	@doc:
//		Test for invalid SetOp expression,
//		SetOp is expected to have no outer references in input columns,
//		when an outer reference needs to be fed to SetOp as input, we must
//		project it first and feed the projected column into SetOp
//
//		For example, this is an invalid SetOp expression since it consumes
//		an outer reference from inner child
//
//			+--CLogicalUnion [Output: "col0" (0)], [Input: ("col0" (0)), ("col6" (6))]
//			   |--CLogicalGet ("T1"), Columns: ["col0" (0)]
//			   +--CLogicalGet ("T2"), Columns: ["col3" (3)]
//
//
//		the valid expression should looks like this:
//
//			+--CLogicalUnion [Output: "col0" (0)], [Input: ("col0" (0)), ("col7" (7))]
//			   |--CLogicalGet ("T1"), Columns: ["col0" (0)]
//			   +--CLogicalProject
//			       |--CLogicalGet ("T2"), Columns: ["col3" (3)]
//			       +--CScalarProjectList
//			       		+--CScalarProjectElement  "col7" (7)
//			       		      +--CScalarIdent "col6" (6)
//
//
//---------------------------------------------------------------------------
GPOS_RESULT
CExpressionTest::EresUnittest_InvalidSetOp()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// Setup an MD cache with a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	{
		CAutoOptCtxt aoc(pmp, &mda, NULL /* pceeval */, CTestUtils::Pcm(pmp));

		// create two different Get expressions
		CWStringConst strName1(GPOS_WSZ_LIT("T1"));
		CMDIdGPDB *pmdid1 = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_TEST_REL_OID1, 1, 1);
		CTableDescriptor *ptabdesc1 = CTestUtils::PtabdescCreate(pmp, 3, pmdid1, CName(&strName1));
		CWStringConst strAlias1(GPOS_WSZ_LIT("T1Alias"));
		CExpression *pexprGet1 = CTestUtils::PexprLogicalGet(pmp, ptabdesc1, &strAlias1);
		CColRefSet *pcrsOutput1 = CDrvdPropRelational::Pdprel(pexprGet1->PdpDerive())->PcrsOutput();

		CWStringConst strName2(GPOS_WSZ_LIT("T2"));
		CMDIdGPDB *pmdid2 = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_TEST_REL_OID2, 1, 1);
		CTableDescriptor *ptabdesc2 = CTestUtils::PtabdescCreate(pmp, 3, pmdid2, CName(&strName2));
		CWStringConst strAlias2(GPOS_WSZ_LIT("T2Alias"));
		CExpression *pexprGet2 = CTestUtils::PexprLogicalGet(pmp, ptabdesc2, &strAlias2);

		// create output columns of SetOp from output columns of first Get expression
		DrgPcr *pdrgpcrOutput = GPOS_NEW(pmp) DrgPcr(pmp);
		pdrgpcrOutput->Append(pcrsOutput1->PcrFirst());

		// create input columns of SetOp while including an outer reference in inner child
		DrgDrgPcr *pdrgpdrgpcrInput = GPOS_NEW(pmp) DrgDrgPcr(pmp);

		DrgPcr *pdrgpcr1 = GPOS_NEW(pmp) DrgPcr(pmp);
		pdrgpcr1->Append(pcrsOutput1->PcrFirst());
		pdrgpdrgpcrInput->Append(pdrgpcr1);

		DrgPcr *pdrgpcr2 = GPOS_NEW(pmp) DrgPcr(pmp);
		CColRef *pcrOuterRef = COptCtxt::PoctxtFromTLS()->Pcf()->PcrCreate(pcrsOutput1->PcrFirst());
		pdrgpcr2->Append(pcrOuterRef);
		pdrgpdrgpcrInput->Append(pdrgpcr2);

		// create invalid SetOp expression
		CLogicalUnion *pop = GPOS_NEW(pmp) CLogicalUnion(pmp, pdrgpcrOutput, pdrgpdrgpcrInput);
		CExpression *pexprSetOp = GPOS_NEW(pmp) CExpression(pmp, pop, pexprGet1, pexprGet2);

		{
			CAutoTrace at(pmp);
			at.Os() << "\nInvalid SetOp Expression: \n" << *pexprSetOp;
		}

		// deriving relational properties must fail
		(void) pexprSetOp->PdpDerive();

		pexprSetOp->Release();
	}

	return GPOS_FAILED;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionTest::PexprComplexJoinTree
//
//	@doc:
// 		Return an expression with several joins
//
//---------------------------------------------------------------------------
CExpression *CExpressionTest::PexprComplexJoinTree
	(
	IMemoryPool *pmp
	)
{
	// The plan will have this shape
	//
	//	+--CLogicalInnerJoin
	//	   |--CLogicalUnion
	//	   |  |--CLogicalLeftOuterJoin
	//	   |  |  |--CLogicalInnerJoin
	//	   |  |  |  |--CLogicalGet
	//	   |  |  |  +--CLogicalSelect
	//	   |  |  +--CLogicalInnerJoin
	//	   |  |     |--CLogicalGet
	//	   |  |     +--CLogicalDynamicGet
	//	   |  +--CLogicalLeftOuterJoin
	//	   |     |--CLogicalGet
	//	   |     +--CLogicalGet
	//	   +--CLogicalInnerJoin
	//	      |--CLogicalGet
	//	      +--CLogicalSelect

	CExpression *pexprInnerJoin1 = CTestUtils::PexprLogicalJoin<CLogicalInnerJoin>
									(
									pmp,
									CTestUtils::PexprLogicalGet(pmp),
									CTestUtils::PexprLogicalSelect(pmp)
									);
	CExpression *pexprJoinIndex = CTestUtils::PexprLogicalJoinWithPartitionedAndIndexedInnerChild(pmp);
	CExpression *pexprLeftJoin =
			CTestUtils::PexprLogicalJoin<CLogicalLeftOuterJoin>(pmp, pexprInnerJoin1, pexprJoinIndex);

	CExpression *pexprOuterJoin = CTestUtils::PexprLogicalJoin<CLogicalLeftOuterJoin>(pmp);
	CExpression *pexprInnerJoin4 =
			CTestUtils::PexprLogicalJoin<CLogicalInnerJoin>(pmp, pexprLeftJoin, pexprOuterJoin);

	CExpression *pexprInnerJoin5 = CTestUtils::PexprLogicalJoin<CLogicalInnerJoin>
									(
									pmp,
									CTestUtils::PexprLogicalGet(pmp),
									CTestUtils::PexprLogicalSelect(pmp)
									);
	CExpression *pexprTopJoin =
			CTestUtils::PexprLogicalJoin<CLogicalInnerJoin>(pmp, pexprInnerJoin4, pexprInnerJoin5);

	return pexprTopJoin;
}

// EOF
