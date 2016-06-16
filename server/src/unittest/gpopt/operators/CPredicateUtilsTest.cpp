//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPredicateUtilsTest.cpp
//
//	@doc:
//		Test for predicate utilities
//---------------------------------------------------------------------------
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/operators/CExpressionPreprocessor.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CPredicateUtils.h"

#include "unittest/base.h"
#include "unittest/gpopt/operators/CPredicateUtilsTest.h"
#include "unittest/gpopt/CTestUtils.h"

#include "naucrates/md/CMDIdGPDB.h"


//---------------------------------------------------------------------------
//	@function:
//		CPredicateUtilsTest::EresUnittest
//
//	@doc:
//		Unittest for predicate utilities
//
//---------------------------------------------------------------------------
GPOS_RESULT
CPredicateUtilsTest::EresUnittest()
{

	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CPredicateUtilsTest::EresUnittest_Conjunctions),
		GPOS_UNITTEST_FUNC(CPredicateUtilsTest::EresUnittest_Disjunctions),
		GPOS_UNITTEST_FUNC(CPredicateUtilsTest::EresUnittest_PlainEqualities),
		GPOS_UNITTEST_FUNC(CPredicateUtilsTest::EresUnittest_Implication),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CPredicateUtilsTest::EresUnittest_Conjunctions
//
//	@doc:
//		Test extraction and construction of conjuncts
//
//---------------------------------------------------------------------------
GPOS_RESULT
CPredicateUtilsTest::EresUnittest_Conjunctions()
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

	// build conjunction
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	const ULONG ulConjs = 3;
	for (ULONG ul = 0; ul < ulConjs; ul++)
	{
		pdrgpexpr->Append(CUtils::PexprScalarConstBool(pmp, true /*fValue*/));
	}
	CExpression *pexprConjunction = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopAnd, pdrgpexpr);
	
	// break into conjuncts
	DrgPexpr *pdrgpexprExtract = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprConjunction);
	GPOS_ASSERT(pdrgpexprExtract->UlLength() == ulConjs);
	
	// collapse into single conjunct
	CExpression *pexpr = CPredicateUtils::PexprConjunction(pmp, pdrgpexprExtract);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CUtils::FScalarConstTrue(pexpr));
	pexpr->Release();
	
	// collapse empty input array to conjunct
	CExpression *pexprSingleton = CPredicateUtils::PexprConjunction(pmp, NULL /*pdrgpexpr*/);
	GPOS_ASSERT(NULL != pexprSingleton);
	pexprSingleton->Release();

	pexprConjunction->Release();

	// conjunction on scalar comparisons
	CExpression *pexprGet = CTestUtils::PexprLogicalGet(pmp);
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprGet->PdpDerive())->PcrsOutput();
	CColRef *pcr1 = pcrs->PcrAny();
	CColRef *pcr2 = pcrs->PcrFirst();
	CExpression *pexprCmp1 = CUtils::PexprScalarCmp(pmp, pcr1, pcr2, IMDType::EcmptEq);
	CExpression *pexprCmp2 = CUtils::PexprScalarCmp(pmp, pcr1, CUtils::PexprScalarConstInt4(pmp, 1 /*iVal*/), IMDType::EcmptEq);

	CExpression *pexprConj = CPredicateUtils::PexprConjunction(pmp, pexprCmp1, pexprCmp2);
	pdrgpexprExtract = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprConj);
	GPOS_ASSERT(2 == pdrgpexprExtract->UlLength());
	pdrgpexprExtract->Release();

	pexprCmp1->Release();
	pexprCmp2->Release();
	pexprConj->Release();
	pexprGet->Release();

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CPredicateUtilsTest::EresUnittest_Disjunctions
//
//	@doc:
//		Test extraction and construction of disjuncts
//
//---------------------------------------------------------------------------
GPOS_RESULT
CPredicateUtilsTest::EresUnittest_Disjunctions()
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

	// build disjunction
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	const ULONG ulDisjs = 3;
	for (ULONG ul = 0; ul < ulDisjs; ul++)
	{
		pdrgpexpr->Append(CUtils::PexprScalarConstBool(pmp, false /*fValue*/));
	}
	CExpression *pexprDisjunction = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopOr, pdrgpexpr);

	// break into disjuncts
	DrgPexpr *pdrgpexprExtract = CPredicateUtils::PdrgpexprDisjuncts(pmp, pexprDisjunction);
	GPOS_ASSERT(pdrgpexprExtract->UlLength() == ulDisjs);

	// collapse into single disjunct
	CExpression *pexpr = CPredicateUtils::PexprDisjunction(pmp, pdrgpexprExtract);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CUtils::FScalarConstFalse(pexpr));
	pexpr->Release();

	// collapse empty input array to disjunct
	CExpression *pexprSingleton = CPredicateUtils::PexprDisjunction(pmp, NULL /*pdrgpexpr*/);
	GPOS_ASSERT(NULL != pexprSingleton);
	pexprSingleton->Release();

	pexprDisjunction->Release();

	// disjunction on scalar comparisons
	CExpression *pexprGet = CTestUtils::PexprLogicalGet(pmp);
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprGet->PdpDerive())->PcrsOutput();
	CColRefSetIter crsi(*pcrs);

#ifdef GPOS_DEBUG
	BOOL fAdvance =
#endif
	crsi.FAdvance();
	GPOS_ASSERT(fAdvance);
	CColRef *pcr1 = crsi.Pcr();

#ifdef GPOS_DEBUG
	fAdvance =
#endif
	crsi.FAdvance();
	GPOS_ASSERT(fAdvance);
	CColRef *pcr2 = crsi.Pcr();

#ifdef GPOS_DEBUG
	fAdvance =
#endif
	crsi.FAdvance();
	GPOS_ASSERT(fAdvance);
	CColRef *pcr3 = crsi.Pcr();

	CExpression *pexprCmp1 = CUtils::PexprScalarCmp(pmp, pcr1, pcr2, IMDType::EcmptEq);
	CExpression *pexprCmp2 = CUtils::PexprScalarCmp(pmp, pcr1, CUtils::PexprScalarConstInt4(pmp, 1 /*iVal*/), IMDType::EcmptEq);

	{
		CExpression *pexprDisj = CPredicateUtils::PexprDisjunction(pmp, pexprCmp1, pexprCmp2);
		pdrgpexprExtract = CPredicateUtils::PdrgpexprDisjuncts(pmp, pexprDisj);
		GPOS_ASSERT(2 == pdrgpexprExtract->UlLength());
		pdrgpexprExtract->Release();
		pexprDisj->Release();
	}


	{
		DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
		CExpression *pexprCmp3 = CUtils::PexprScalarCmp(pmp, pcr2, pcr1, IMDType::EcmptG);
		CExpression *pexprCmp4 = CUtils::PexprScalarCmp(pmp, CUtils::PexprScalarConstInt4(pmp, 200 /*iVal*/), pcr3, IMDType::EcmptL);
		pexprCmp1->AddRef();
		pexprCmp2->AddRef();

		pdrgpexpr->Append(pexprCmp3);
		pdrgpexpr->Append(pexprCmp4);
		pdrgpexpr->Append(pexprCmp1);
		pdrgpexpr->Append(pexprCmp2);

		CExpression *pexprDisj = CPredicateUtils::PexprDisjunction(pmp, pdrgpexpr);
		pdrgpexprExtract = CPredicateUtils::PdrgpexprDisjuncts(pmp, pexprDisj);
		GPOS_ASSERT(4 == pdrgpexprExtract->UlLength());
		pdrgpexprExtract->Release();
		pexprDisj->Release();
	}

	pexprCmp1->Release();
	pexprCmp2->Release();
	pexprGet->Release();

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CPredicateUtilsTest::EresUnittest_PlainEqualities
//
//	@doc:
//		Test the extraction of equality predicates between scalar identifiers
//
//---------------------------------------------------------------------------
GPOS_RESULT
CPredicateUtilsTest::EresUnittest_PlainEqualities()
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

	CExpression *pexprLeft = CTestUtils::PexprLogicalGet(pmp);
	CExpression *pexprRight = CTestUtils::PexprLogicalGet(pmp);

	DrgPexpr *pdrgpexprOriginal = GPOS_NEW(pmp) DrgPexpr(pmp);

	CColRefSet *pcrsLeft = CDrvdPropRelational::Pdprel(pexprLeft->PdpDerive())->PcrsOutput();
	CColRefSet *pcrsRight = CDrvdPropRelational::Pdprel(pexprRight->PdpDerive())->PcrsOutput();

	CColRef *pcrLeft = pcrsLeft->PcrAny();
	CColRef *pcrRight = pcrsRight->PcrAny();

	// generate an equality predicate between two column reference
	CExpression *pexprScIdentEquality =
		CUtils::PexprScalarEqCmp(pmp, pcrLeft, pcrRight);

	pexprScIdentEquality->AddRef();
	pdrgpexprOriginal->Append(pexprScIdentEquality);

	// generate a non-equality predicate between two column reference
	CExpression *pexprScIdentInequality =
		CUtils::PexprScalarCmp(pmp, pcrLeft, pcrRight, CWStringConst(GPOS_WSZ_LIT("<")), GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_LT_OP));

	pexprScIdentInequality->AddRef();
	pdrgpexprOriginal->Append(pexprScIdentInequality);

	// generate an equality predicate between a column reference and a constant value
	CExpression *pexprScalarConstInt4 = CUtils::PexprScalarConstInt4(pmp, 10 /*fValue*/);
	CExpression *pexprScIdentConstEquality = CUtils::PexprScalarEqCmp(pmp, pexprScalarConstInt4, pcrRight);

	pdrgpexprOriginal->Append(pexprScIdentConstEquality);

	GPOS_ASSERT(3 == pdrgpexprOriginal->UlLength());

	DrgPexpr *pdrgpexprResult = CPredicateUtils::PdrgpexprPlainEqualities(pmp, pdrgpexprOriginal);

	GPOS_ASSERT(1 == pdrgpexprResult->UlLength());

	// clean up
	pdrgpexprOriginal->Release();
	pdrgpexprResult->Release();
	pexprLeft->Release();
	pexprRight->Release();
	pexprScIdentEquality->Release();
	pexprScIdentInequality->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CPredicateUtilsTest::EresUnittest_Implication
//
//	@doc:
//		Test removal of implied predicates
//
//---------------------------------------------------------------------------
GPOS_RESULT
CPredicateUtilsTest::EresUnittest_Implication()
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

	// generate a two cascaded joins
	CWStringConst strName1(GPOS_WSZ_LIT("Rel1"));
	CMDIdGPDB *pmdid1 = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_TEST_REL_OID1, 1, 1);
	CTableDescriptor *ptabdesc1 = CTestUtils::PtabdescCreate(pmp, 3, pmdid1, CName(&strName1));
	CWStringConst strAlias1(GPOS_WSZ_LIT("Rel1"));
	CExpression *pexprRel1 = CTestUtils::PexprLogicalGet(pmp, ptabdesc1, &strAlias1);

	CWStringConst strName2(GPOS_WSZ_LIT("Rel2"));
	CMDIdGPDB *pmdid2 = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_TEST_REL_OID2, 1, 1);
	CTableDescriptor *ptabdesc2 = CTestUtils::PtabdescCreate(pmp, 3, pmdid2, CName(&strName2));
	CWStringConst strAlias2(GPOS_WSZ_LIT("Rel2"));
	CExpression *pexprRel2 = CTestUtils::PexprLogicalGet(pmp, ptabdesc2, &strAlias2);

	CWStringConst strName3(GPOS_WSZ_LIT("Rel3"));
	CMDIdGPDB *pmdid3 = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_TEST_REL_OID3, 1, 1);
	CTableDescriptor *ptabdesc3 = CTestUtils::PtabdescCreate(pmp, 3, pmdid3, CName(&strName3));
	CWStringConst strAlias3(GPOS_WSZ_LIT("Rel3"));
	CExpression *pexprRel3 = CTestUtils::PexprLogicalGet(pmp, ptabdesc3, &strAlias3);

	CExpression *pexprJoin1 = CTestUtils::PexprLogicalJoin<CLogicalInnerJoin>(pmp, pexprRel1, pexprRel2);
	CExpression *pexprJoin2 = CTestUtils::PexprLogicalJoin<CLogicalInnerJoin>(pmp, pexprJoin1, pexprRel3);

	{
		CAutoTrace at(pmp);
		at.Os() << "Original expression:" << std::endl << *pexprJoin2 <<std::endl;
	}

	// imply new predicates by deriving constraints
	CExpression *pexprConstraints = CExpressionPreprocessor::PexprAddPredicatesFromConstraints(pmp, pexprJoin2);

	{
		CAutoTrace at(pmp);
		at.Os() << "Expression with implied predicates:" << std::endl << *pexprConstraints <<std::endl;;
	}

	// minimize join predicates by removing implied conjuncts
	CExpressionHandle exprhdl(pmp);
	exprhdl.Attach(pexprConstraints);
	CExpression *pexprMinimizedPred = CPredicateUtils::PexprRemoveImpliedConjuncts(pmp, (*pexprConstraints)[2], exprhdl);

	{
		CAutoTrace at(pmp);
		at.Os() << "Minimized join predicate:" << std::endl << *pexprMinimizedPred <<std::endl;
	}

	DrgPexpr *pdrgpexprOriginalConjuncts = CPredicateUtils::PdrgpexprConjuncts(pmp,  (*pexprConstraints)[2]);
	DrgPexpr *pdrgpexprNewConjuncts = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprMinimizedPred);

	GPOS_ASSERT(pdrgpexprNewConjuncts->UlLength() < pdrgpexprOriginalConjuncts->UlLength());

	// clean up
	pdrgpexprOriginalConjuncts->Release();
	pdrgpexprNewConjuncts->Release();
	pexprJoin2->Release();
	pexprConstraints->Release();
	pexprMinimizedPred->Release();

	return GPOS_OK;
}
// EOF
