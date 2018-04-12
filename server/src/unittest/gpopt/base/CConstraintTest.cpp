//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CConstraintTest.cpp
//
//	@doc:
//		Test for constraint
//---------------------------------------------------------------------------

#ifndef __STDC_CONSTANT_MACROS
#define __STDC_CONSTANT_MACROS
#endif

#include <stdint.h>

#include "gpos/task/CAutoTraceFlag.h"

#include "unittest/base.h"
#include "unittest/gpopt/base/CConstraintTest.h"
#include "unittest/gpopt/CConstExprEvaluatorForDates.h"

#include "naucrates/base/CDatumInt8GPDB.h"

#include "gpopt/base/CDefaultComparator.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/operators/CPredicateUtils.h"

#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/IMDScalarOp.h"

// number of microseconds in one day
const LINT CConstraintTest::lMicrosecondsPerDay = 24 * 60 * 60 * INT64_C(1000000);

// date for '01-01-2012'
const LINT CConstraintTest::lInternalRepresentationFor2012_01_01 =
		LINT(4383) * CConstraintTest::lMicrosecondsPerDay;

// date for '01-21-2012'
const LINT CConstraintTest::lInternalRepresentationFor2012_01_21 =
		LINT(5003) * CConstraintTest::lMicrosecondsPerDay;

// date for '01-02-2012'
const LINT CConstraintTest::lInternalRepresentationFor2012_01_02 =
		LINT(4384) * CConstraintTest::lMicrosecondsPerDay;

// date for '01-22-2012'
const LINT CConstraintTest::lInternalRepresentationFor2012_01_22 =
		LINT(5004) * CConstraintTest::lMicrosecondsPerDay;

// byte representation for '01-01-2012'
const WCHAR *CConstraintTest::wszInternalRepresentationFor2012_01_01 =
		GPOS_WSZ_LIT("HxEAAA==");

// byte representation for '01-21-2012'
const WCHAR *CConstraintTest::wszInternalRepresentationFor2012_01_21 =
		GPOS_WSZ_LIT("MxEAAA==");

// byte representation for '01-02-2012'
const WCHAR *CConstraintTest::wszInternalRepresentationFor2012_01_02 =
		GPOS_WSZ_LIT("IBEAAA==");

// byte representation for '01-22-2012'
const WCHAR *CConstraintTest::wszInternalRepresentationFor2012_01_22 =
		GPOS_WSZ_LIT("MhEAAA==");

static GPOS_RESULT EresUnittest_CConstraintIntervalFromArrayExprIncludesNull();

//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::EresUnittest
//
//	@doc:
//		Unittest for ranges
//
//---------------------------------------------------------------------------
GPOS_RESULT
CConstraintTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(EresUnittest_CConstraintIntervalFromArrayExprIncludesNull),
		GPOS_UNITTEST_FUNC(CConstraintTest::EresUnittest_CInterval),
		GPOS_UNITTEST_FUNC(CConstraintTest::EresUnittest_CIntervalFromScalarExpr),
		GPOS_UNITTEST_FUNC(CConstraintTest::EresUnittest_CConjunction),
		GPOS_UNITTEST_FUNC(CConstraintTest::EresUnittest_CDisjunction),
		GPOS_UNITTEST_FUNC(CConstraintTest::EresUnittest_CNegation),
		GPOS_UNITTEST_FUNC(CConstraintTest::EresUnittest_CConstraintFromScalarExpr),
		GPOS_UNITTEST_FUNC(CConstraintTest::EresUnittest_CConstraintIntervalConvertsTo),
		GPOS_UNITTEST_FUNC(CConstraintTest::EresUnittest_CConstraintIntervalPexpr),
		GPOS_UNITTEST_FUNC(CConstraintTest::EresUnittest_CConstraintIntervalFromArrayExpr),
#ifdef GPOS_DEBUG
		GPOS_UNITTEST_FUNC_THROW
			(
			CConstraintTest::EresUnittest_NegativeTests,
			gpos::CException::ExmaSystem,
			gpos::CException::ExmiAssert
			),
#endif // GPOS_DEBUG
		GPOS_UNITTEST_FUNC(CConstraintTest::EresUnittest_ConstraintsOnDates),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::EresUnittest_CInterval
//
//	@doc:
//		Interval tests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CConstraintTest::EresUnittest_CInterval()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	CConstExprEvaluatorForDates *pceeval = GPOS_NEW(pmp) CConstExprEvaluatorForDates(pmp);

	// install opt context in TLS
	CAutoOptCtxt aoc(pmp, &mda, pceeval, CTestUtils::Pcm(pmp));
	GPOS_ASSERT(NULL != COptCtxt::PoctxtFromTLS()->Pcomp());

	IMDTypeInt8 *pmdtypeint8 = (IMDTypeInt8 *) mda.PtMDType<IMDTypeInt8>(CTestUtils::m_sysidDefault);
	IMDId *pmdid = pmdtypeint8->Pmdid();

	CExpression *pexprGet = CTestUtils::PexprLogicalGet(pmp);
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprGet->PdpDerive())->PcrsOutput();
	CColRef *pcr =  pcrs->PcrAny();

	// first interval
	CConstraintInterval *pciFirst = PciFirstInterval(pmp, pmdid, pcr);
	PrintConstraint(pmp, pciFirst);

	// second interval
	CConstraintInterval *pciSecond = PciSecondInterval(pmp, pmdid, pcr);
	PrintConstraint(pmp, pciSecond);

	// intersection
	CConstraintInterval *pciIntersect = pciFirst->PciIntersect(pmp, pciSecond);
	PrintConstraint(pmp, pciIntersect);

	// union
	CConstraintInterval *pciUnion = pciFirst->PciUnion(pmp, pciSecond);
	PrintConstraint(pmp, pciUnion);

	// diff 1
	CConstraintInterval *pciDiff1 = pciFirst->PciDifference(pmp, pciSecond);
	PrintConstraint(pmp, pciDiff1);

	// diff 2
	CConstraintInterval *pciDiff2 = pciSecond->PciDifference(pmp, pciFirst);
	PrintConstraint(pmp, pciDiff2);

	// complement
	CConstraintInterval *pciComp = pciFirst->PciComplement(pmp);
	PrintConstraint(pmp, pciComp);

	// containment
	GPOS_ASSERT(!pciFirst->FContains(pciSecond));
	GPOS_ASSERT(pciFirst->FContains(pciDiff1));
	GPOS_ASSERT(!pciSecond->FContains(pciFirst));
	GPOS_ASSERT(pciSecond->FContains(pciDiff2));

	// equality
	CConstraintInterval *pciThird = PciFirstInterval(pmp, pmdid, pcr);
	pciThird->AddRef();
	CConstraintInterval *pciFourth = pciThird;
	GPOS_ASSERT(!pciFirst->FEquals(pciSecond));
	GPOS_ASSERT(!pciFirst->FEquals(pciDiff1));
	GPOS_ASSERT(!pciSecond->FEquals(pciDiff2));
	GPOS_ASSERT(pciFirst->FEquals(pciThird));
	GPOS_ASSERT(pciFourth->FEquals(pciThird));

	pciFirst->Release();
	pciSecond->Release();
	pciThird->Release();
	pciFourth->Release();
	pciIntersect->Release();
	pciUnion->Release();
	pciDiff1->Release();
	pciDiff2->Release();
	pciComp->Release();

	pexprGet->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::EresUnittest_CConjunction
//
//	@doc:
//		Conjunction test
//
//---------------------------------------------------------------------------
GPOS_RESULT
CConstraintTest::EresUnittest_CConjunction()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	CConstExprEvaluatorForDates *pceeval = GPOS_NEW(pmp) CConstExprEvaluatorForDates(pmp);

	// install opt context in TLS
	CAutoOptCtxt aoc(pmp, &mda, pceeval, CTestUtils::Pcm(pmp));
	GPOS_ASSERT(NULL != COptCtxt::PoctxtFromTLS()->Pcomp());

	IMDTypeInt8 *pmdtypeint8 = (IMDTypeInt8 *) mda.PtMDType<IMDTypeInt8>(CTestUtils::m_sysidDefault);
	IMDId *pmdid = pmdtypeint8->Pmdid();

	CExpression *pexprGet1 = CTestUtils::PexprLogicalGet(pmp);
	CColRefSet *pcrs1 = CDrvdPropRelational::Pdprel(pexprGet1->PdpDerive())->PcrsOutput();
	CColRef *pcr1 =  pcrs1->PcrAny();

	CExpression *pexprGet2 = CTestUtils::PexprLogicalGet(pmp);
	CColRefSet *pcrs2 = CDrvdPropRelational::Pdprel(pexprGet2->PdpDerive())->PcrsOutput();
	CColRef *pcr2 =  pcrs2->PcrAny();

	CConstraintConjunction *pcconj1 = Pcstconjunction(pmp, pmdid, pcr1);
	PrintConstraint(pmp, pcconj1);
	GPOS_ASSERT(!pcconj1->FContradiction());

	CConstraintConjunction *pcconj2 = Pcstconjunction(pmp, pmdid, pcr2);
	PrintConstraint(pmp, pcconj2);

	DrgPcnstr *pdrgpcst = GPOS_NEW(pmp) DrgPcnstr(pmp);
	pcconj1->AddRef();
	pcconj2->AddRef();
	pdrgpcst->Append(pcconj1);
	pdrgpcst->Append(pcconj2);

	CConstraintConjunction *pcconjTop = GPOS_NEW(pmp) CConstraintConjunction(pmp, pdrgpcst);
	PrintConstraint(pmp, pcconjTop);

	// containment
	GPOS_ASSERT(!pcconj1->FContains(pcconj2));
	GPOS_ASSERT(!pcconj2->FContains(pcconj1));
	GPOS_ASSERT(pcconj1->FContains(pcconjTop));
	GPOS_ASSERT(!pcconjTop->FContains(pcconj1));
	GPOS_ASSERT(pcconj2->FContains(pcconjTop));
	GPOS_ASSERT(!pcconjTop->FContains(pcconj2));

	// equality
	GPOS_ASSERT(!pcconj1->FEquals(pcconjTop));
	GPOS_ASSERT(!pcconjTop->FEquals(pcconj2));

	pcconjTop->Release();
	pcconj1->Release();
	pcconj2->Release();

	pexprGet1->Release();
	pexprGet2->Release();

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::Pcstconjunction
//
//	@doc:
//		Build a conjunction
//
//---------------------------------------------------------------------------
CConstraintConjunction *
CConstraintTest::Pcstconjunction
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	CColRef *pcr
	)
{
	// first interval
	CConstraintInterval *pciFirst = PciFirstInterval(pmp, pmdid, pcr);

	// second interval
	CConstraintInterval *pciSecond = PciSecondInterval(pmp, pmdid, pcr);

	DrgPcnstr *pdrgpcst = GPOS_NEW(pmp) DrgPcnstr(pmp);
	pdrgpcst->Append(pciFirst);
	pdrgpcst->Append(pciSecond);

	return GPOS_NEW(pmp) CConstraintConjunction(pmp, pdrgpcst);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::Pcstdisjunction
//
//	@doc:
//		Build a disjunction
//
//---------------------------------------------------------------------------
CConstraintDisjunction *
CConstraintTest::Pcstdisjunction
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	CColRef *pcr
	)
{
	// first interval
	CConstraintInterval *pciFirst = PciFirstInterval(pmp, pmdid, pcr);

	// second interval
	CConstraintInterval *pciSecond = PciSecondInterval(pmp, pmdid, pcr);

	DrgPcnstr *pdrgpcst = GPOS_NEW(pmp) DrgPcnstr(pmp);
	pdrgpcst->Append(pciFirst);
	pdrgpcst->Append(pciSecond);

	return GPOS_NEW(pmp) CConstraintDisjunction(pmp, pdrgpcst);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::EresUnittest_CDisjunction
//
//	@doc:
//		Conjunction test
//
//---------------------------------------------------------------------------
GPOS_RESULT
CConstraintTest::EresUnittest_CDisjunction()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	CConstExprEvaluatorForDates *pceeval = GPOS_NEW(pmp) CConstExprEvaluatorForDates(pmp);

	// install opt context in TLS
	CAutoOptCtxt aoc(pmp, &mda, pceeval, CTestUtils::Pcm(pmp));
	GPOS_ASSERT(NULL != COptCtxt::PoctxtFromTLS()->Pcomp());

	IMDTypeInt8 *pmdtypeint8 = (IMDTypeInt8 *) mda.PtMDType<IMDTypeInt8>(CTestUtils::m_sysidDefault);
	IMDId *pmdid = pmdtypeint8->Pmdid();

	CExpression *pexprGet1 = CTestUtils::PexprLogicalGet(pmp);
	CColRefSet *pcrs1 = CDrvdPropRelational::Pdprel(pexprGet1->PdpDerive())->PcrsOutput();
	CColRef *pcr1 =  pcrs1->PcrAny();

	CExpression *pexprGet2 = CTestUtils::PexprLogicalGet(pmp);
	CColRefSet *pcrs2 = CDrvdPropRelational::Pdprel(pexprGet2->PdpDerive())->PcrsOutput();
	CColRef *pcr2 =  pcrs2->PcrAny();

	CConstraintDisjunction *pcdisj1 = Pcstdisjunction(pmp, pmdid, pcr1);
	PrintConstraint(pmp, pcdisj1);
	GPOS_ASSERT(!pcdisj1->FContradiction());

	CConstraintDisjunction *pcdisj2 = Pcstdisjunction(pmp, pmdid, pcr2);
	PrintConstraint(pmp, pcdisj2);

	DrgPcnstr *pdrgpcst = GPOS_NEW(pmp) DrgPcnstr(pmp);
	pcdisj1->AddRef();
	pcdisj2->AddRef();
	pdrgpcst->Append(pcdisj1);
	pdrgpcst->Append(pcdisj2);

	CConstraintDisjunction *pcdisjTop = GPOS_NEW(pmp) CConstraintDisjunction(pmp, pdrgpcst);
	PrintConstraint(pmp, pcdisjTop);

	// containment
	GPOS_ASSERT(!pcdisj1->FContains(pcdisj2));
	GPOS_ASSERT(!pcdisj2->FContains(pcdisj1));
	GPOS_ASSERT(!pcdisj1->FContains(pcdisjTop));
	GPOS_ASSERT(pcdisjTop->FContains(pcdisj1));
	GPOS_ASSERT(!pcdisj2->FContains(pcdisjTop));
	GPOS_ASSERT(pcdisjTop->FContains(pcdisj2));

	// equality
	GPOS_ASSERT(!pcdisj1->FEquals(pcdisjTop));
	GPOS_ASSERT(!pcdisjTop->FEquals(pcdisj2));

	pcdisjTop->Release();
	pcdisj1->Release();
	pcdisj2->Release();

	pexprGet1->Release();
	pexprGet2->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::EresUnittest_CNegation
//
//	@doc:
//		Conjunction test
//
//---------------------------------------------------------------------------
GPOS_RESULT
CConstraintTest::EresUnittest_CNegation()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	CConstExprEvaluatorForDates *pceeval = GPOS_NEW(pmp) CConstExprEvaluatorForDates(pmp);

	// install opt context in TLS
	CAutoOptCtxt aoc(pmp, &mda, pceeval, CTestUtils::Pcm(pmp));
	GPOS_ASSERT(NULL != COptCtxt::PoctxtFromTLS()->Pcomp());

	IMDTypeInt8 *pmdtypeint8 = (IMDTypeInt8 *) mda.PtMDType<IMDTypeInt8>(CTestUtils::m_sysidDefault);
	IMDId *pmdid = pmdtypeint8->Pmdid();

	CExpression *pexprGet = CTestUtils::PexprLogicalGet(pmp);
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprGet->PdpDerive())->PcrsOutput();
	CColRef *pcr =  pcrs->PcrAny();

	CConstraintInterval *pci = PciFirstInterval(pmp, pmdid, pcr);
	PrintConstraint(pmp, pci);

	pci->AddRef();
	CConstraintNegation *pcn1 = GPOS_NEW(pmp) CConstraintNegation(pmp, pci);
	PrintConstraint(pmp, pcn1);

	pcn1->AddRef();
	CConstraintNegation *pcn2 = GPOS_NEW(pmp) CConstraintNegation(pmp, pcn1);
	PrintConstraint(pmp, pcn2);

	// containment
	GPOS_ASSERT(!pcn1->FContains(pci));
	GPOS_ASSERT(!pci->FContains(pcn1));
	GPOS_ASSERT(!pcn2->FContains(pcn1));
	GPOS_ASSERT(!pcn1->FContains(pcn2));
	GPOS_ASSERT(pci->FContains(pcn2));
	GPOS_ASSERT(pcn2->FContains(pci));

	// equality
	GPOS_ASSERT(!pcn1->FEquals(pci));
	GPOS_ASSERT(!pcn1->FEquals(pcn2));
	GPOS_ASSERT(pci->FEquals(pcn2));

	pcn2->Release();
	pcn1->Release();
	pci->Release();

	pexprGet->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::EresUnittest_CIntervalFromScalarExpr
//
//	@doc:
//		Interval from scalar tests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CConstraintTest::EresUnittest_CIntervalFromScalarExpr()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	CConstExprEvaluatorForDates *pceeval = GPOS_NEW(pmp) CConstExprEvaluatorForDates(pmp);

	// install opt context in TLS
	CAutoOptCtxt aoc(pmp, &mda, pceeval, CTestUtils::Pcm(pmp));
	GPOS_ASSERT(NULL != COptCtxt::PoctxtFromTLS()->Pcomp());

	CExpression *pexprGet = CTestUtils::PexprLogicalGet(pmp);
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprGet->PdpDerive())->PcrsOutput();
	CColRef *pcr =  pcrs->PcrAny();

	// from ScalarCmp
	GPOS_RESULT eres1 = EresUnittest_CIntervalFromScalarCmp(pmp, &mda, pcr);

	// from ScalarBool
	GPOS_RESULT eres2 = EresUnittest_CIntervalFromScalarBoolOp(pmp, &mda, pcr);

	pexprGet->Release();
	if (GPOS_OK == eres1 && GPOS_OK == eres2)
	{
		return GPOS_OK;
	}

	return GPOS_FAILED;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::EresUnittest_CConstraintFromScalarExpr
//
//	@doc:
//		Constraint from scalar tests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CConstraintTest::EresUnittest_CConstraintFromScalarExpr()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	CConstExprEvaluatorForDates *pceeval = GPOS_NEW(pmp) CConstExprEvaluatorForDates(pmp);

	// install opt context in TLS
	CAutoOptCtxt aoc(pmp, &mda, pceeval, CTestUtils::Pcm(pmp));
	GPOS_ASSERT(NULL != COptCtxt::PoctxtFromTLS()->Pcomp());

	CExpression *pexprGet1 = CTestUtils::PexprLogicalGet(pmp);
	CColRefSet *pcrs1 = CDrvdPropRelational::Pdprel(pexprGet1->PdpDerive())->PcrsOutput();
	CColRef *pcr1 =  pcrs1->PcrAny();

	CExpression *pexprGet2 = CTestUtils::PexprLogicalGet(pmp);
	CColRefSet *pcrs2 = CDrvdPropRelational::Pdprel(pexprGet2->PdpDerive())->PcrsOutput();
	CColRef *pcr2 =  pcrs2->PcrAny();

	DrgPcrs *pdrgpcrs = NULL;

	// expression with 1 column
	CExpression *pexpr = PexprScalarCmp(pmp, &mda, pcr1, IMDType::EcmptG, 15);
	CConstraint *pcnstr = CConstraint::PcnstrFromScalarExpr(pmp, pexpr, &pdrgpcrs);
	PrintConstraint(pmp, pcnstr);
	PrintEquivClasses(pmp, pdrgpcrs);
	pdrgpcrs->Release();
	pdrgpcrs = NULL;
	pcnstr->Release();
	pexpr->Release();

	// expression with 2 columns
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexpr->Append(PexprScalarCmp(pmp, &mda, pcr1, IMDType::EcmptG, 15));
	pdrgpexpr->Append(PexprScalarCmp(pmp, &mda, pcr2, IMDType::EcmptL, 20));

	CExpression *pexprAnd = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopAnd, pdrgpexpr);
	(void) pexprAnd->PdpDerive();

	CConstraint *pcnstrAnd = CConstraint::PcnstrFromScalarExpr(pmp, pexprAnd, &pdrgpcrs);
	PrintConstraint(pmp, pcnstrAnd);
	PrintEquivClasses(pmp, pdrgpcrs);
	pdrgpcrs->Release();
	pdrgpcrs = NULL;

	pcnstrAnd->Release();
	pexprAnd->Release();

	// equality predicate with 2 columns
	CExpression *pexprEq = CUtils::PexprScalarEqCmp(pmp, pcr1, pcr2);
	(void) pexprEq->PdpDerive();
	CConstraint *pcnstrEq = CConstraint::PcnstrFromScalarExpr(pmp, pexprEq, &pdrgpcrs);
	PrintConstraint(pmp, pcnstrEq);
	PrintEquivClasses(pmp, pdrgpcrs);

	pcnstrEq->Release();
	pexprEq->Release();

	pdrgpcrs->Release();

	pexprGet1->Release();
	pexprGet2->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::EresUnittest_CConstraintIntervalConvertsTo
//
//	@doc:
//		Tests CConstraintInterval::ConvertsToIn and
//		CConstraintInterval::ConvertsToNotIn
//
//---------------------------------------------------------------------------
GPOS_RESULT
CConstraintTest::EresUnittest_CConstraintIntervalConvertsTo()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	CConstExprEvaluatorForDates *pceeval = GPOS_NEW(pmp) CConstExprEvaluatorForDates(pmp);

	// install opt context in TLS
	CAutoOptCtxt aoc(pmp, &mda, pceeval, CTestUtils::Pcm(pmp));
	GPOS_ASSERT(NULL != COptCtxt::PoctxtFromTLS()->Pcomp());

	// create a range which should convert to an IN array expression
	const SRangeInfo rgRangeInfoIn[] =
			{
				{CRange::EriIncluded, -1000, CRange::EriIncluded, -1000},
				{CRange::EriIncluded, -5, CRange::EriIncluded, -5},
				{CRange::EriIncluded, 0, CRange::EriIncluded, 0}
			};

	// metadata id
	IMDTypeInt8 *pmdtypeint8 = (IMDTypeInt8 *) mda.PtMDType<IMDTypeInt8>(CTestUtils::m_sysidDefault);
	IMDId *pmdid = pmdtypeint8->Pmdid();

	// get a column ref
	CExpression *pexprGet = CTestUtils::PexprLogicalGet(pmp);
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprGet->PdpDerive())->PcrsOutput();
	CColRef *pcr =  pcrs->PcrAny();

	// create constraint
	DrgPrng *pdrgprng = Pdrgprng(pmp, pmdid, rgRangeInfoIn, GPOS_ARRAY_SIZE(rgRangeInfoIn));
	CConstraintInterval *pcnstin = GPOS_NEW(pmp) CConstraintInterval(pmp, pcr, pdrgprng, true);

	PrintConstraint(pmp, pcnstin);

	// should convert to in
	GPOS_ASSERT(pcnstin->FConvertsToIn());
	GPOS_ASSERT(!pcnstin->FConvertsToNotIn());

	CConstraintInterval *pcnstNotIn = pcnstin->PciComplement(pmp);

	// should convert to a not in statement after taking the complement
	GPOS_ASSERT(pcnstNotIn->FConvertsToNotIn());
	GPOS_ASSERT(!pcnstNotIn->FConvertsToIn());

	pcnstin->Release();
	pcnstNotIn->Release();
	pexprGet->Release();

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::EresUnittest_CConstraintIntervalPexpr
//
//	@doc:
//		Tests CConstraintInterval::PexprConstructArrayScalar
//
//---------------------------------------------------------------------------
GPOS_RESULT
CConstraintTest::EresUnittest_CConstraintIntervalPexpr()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	CConstExprEvaluatorForDates *pceeval = GPOS_NEW(pmp) CConstExprEvaluatorForDates(pmp);

	// install opt context in TLS
	CAutoOptCtxt aoc(pmp, &mda, pceeval, CTestUtils::Pcm(pmp));
	GPOS_RTL_ASSERT(NULL != COptCtxt::PoctxtFromTLS()->Pcomp());

	CAutoTraceFlag atf(EopttraceArrayConstraints, true);

	// create a range which should convert to an IN array expression
	const SRangeInfo rgRangeInfoIn[] =
			{
				{CRange::EriIncluded, -1000, CRange::EriIncluded, -1000},
				{CRange::EriIncluded, -5, CRange::EriIncluded, -5},
				{CRange::EriIncluded, 0, CRange::EriIncluded, 0}
			};

	// metadata id
	IMDTypeInt8 *pmdtypeint8 = (IMDTypeInt8 *) mda.PtMDType<IMDTypeInt8>(CTestUtils::m_sysidDefault);
	IMDId *pmdid = pmdtypeint8->Pmdid();

	// get a column ref
	CExpression *pexprGet = CTestUtils::PexprLogicalGet(pmp);
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprGet->PdpDerive())->PcrsOutput();
	CColRef *pcr =  pcrs->PcrAny();

	DrgPrng *pdrgprng = NULL;
	CConstraintInterval *pcnstin = NULL;
	CExpression *pexpr = NULL;
	CConstraintInterval *pcnstNotIn = NULL;

	// IN CONSTRAINT FOR SIMPLE INTERVAL (WITHOUT NULL)

	// create constraint
	pdrgprng = Pdrgprng(pmp, pmdid, rgRangeInfoIn, GPOS_ARRAY_SIZE(rgRangeInfoIn));
	pcnstin = GPOS_NEW(pmp) CConstraintInterval(pmp, pcr, pdrgprng, false);

	pexpr = pcnstin->PexprScalar(pmp); // pexpr is owned by the constraint
	PrintConstraint(pmp, pcnstin);

	GPOS_RTL_ASSERT(!pcnstin->FConvertsToNotIn());
	GPOS_RTL_ASSERT(pcnstin->FConvertsToIn());
	GPOS_RTL_ASSERT(CUtils::FScalarArrayCmp(pexpr));
	GPOS_RTL_ASSERT(3 == CUtils::UlCountOperator(pexpr, COperator::EopScalarConst));

	pcnstin->Release();


	// IN CONSTRAINT FOR SIMPLE INTERVAL WITH NULL

	// create constraint
	pdrgprng = Pdrgprng(pmp, pmdid, rgRangeInfoIn, GPOS_ARRAY_SIZE(rgRangeInfoIn));
	pcnstin = GPOS_NEW(pmp) CConstraintInterval(pmp, pcr, pdrgprng, true);

	pexpr = pcnstin->PexprScalar(pmp); // pexpr is owned by the constraint
	PrintConstraint(pmp, pcnstin);

	GPOS_RTL_ASSERT(!pcnstin->FConvertsToNotIn());
	GPOS_RTL_ASSERT(pcnstin->FConvertsToIn());
	GPOS_RTL_ASSERT(CUtils::FScalarArrayCmp(pexpr));
	GPOS_RTL_ASSERT(4 == CUtils::UlCountOperator(pexpr, COperator::EopScalarConst));

	pcnstin->Release();


	// NOT IN CONSTRAINT FOR SIMPLE INTERVAL WITHOUT NULL

	// create constraint
	pdrgprng = Pdrgprng(pmp, pmdid, rgRangeInfoIn, GPOS_ARRAY_SIZE(rgRangeInfoIn));
	pcnstin = GPOS_NEW(pmp) CConstraintInterval(pmp, pcr, pdrgprng, true);

	pcnstNotIn = pcnstin->PciComplement(pmp);
	pcnstin->Release();

	pexpr = pcnstNotIn->PexprScalar(pmp); // pexpr is owned by the constraint
	PrintConstraint(pmp, pcnstNotIn);

	GPOS_RTL_ASSERT(pcnstNotIn->FConvertsToNotIn());
	GPOS_RTL_ASSERT(!pcnstNotIn->FConvertsToIn());
	GPOS_RTL_ASSERT(CUtils::FScalarArrayCmp(pexpr));
	GPOS_RTL_ASSERT(3 == CUtils::UlCountOperator(pexpr, COperator::EopScalarConst));

	pcnstNotIn->Release();


	// NOT IN CONSTRAINT FOR SIMPLE INTERVAL WITH NULL

	// create constraint
	pdrgprng = Pdrgprng(pmp, pmdid, rgRangeInfoIn, GPOS_ARRAY_SIZE(rgRangeInfoIn));
	pcnstin = GPOS_NEW(pmp) CConstraintInterval(pmp, pcr, pdrgprng, false);

	pcnstNotIn = pcnstin->PciComplement(pmp);
	pcnstin->Release();

	pexpr = pcnstNotIn->PexprScalar(pmp); // pexpr is owned by the constraint
	PrintConstraint(pmp, pcnstNotIn);

	GPOS_RTL_ASSERT(pcnstNotIn->FConvertsToNotIn());
	GPOS_RTL_ASSERT(!pcnstNotIn->FConvertsToIn());
	GPOS_RTL_ASSERT(CUtils::FScalarArrayCmp(pexpr));
	GPOS_RTL_ASSERT(4 == CUtils::UlCountOperator(pexpr, COperator::EopScalarConst));

	pcnstNotIn->Release();


	pexprGet->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::EresUnittest_CConstraintIntervalFromArrayExpr
//
//	@doc:
//		Tests CConstraintInterval::PcnstrIntervalFromScalarArrayCmp
//
//---------------------------------------------------------------------------
GPOS_RESULT
CConstraintTest::EresUnittest_CConstraintIntervalFromArrayExpr()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	CConstExprEvaluatorForDates *pceeval = GPOS_NEW(pmp) CConstExprEvaluatorForDates(pmp);

	// install opt context in TLS
	CAutoOptCtxt aoc(pmp, &mda, pceeval, CTestUtils::Pcm(pmp));
	GPOS_ASSERT(NULL != COptCtxt::PoctxtFromTLS()->Pcomp());

	CAutoTraceFlag atf(EopttraceArrayConstraints, true);

	// Create an IN array expression
	CExpression *pexpr = CTestUtils::PexprLogicalSelectArrayCmp(pmp);
	// get a ref to the comparison column
	CColRef *pcr = CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->PcrsOutput()->PcrAny();

	// remove the array child
	CExpression *pexprArrayComp = (*pexpr->PdrgPexpr())[1];
	GPOS_ASSERT(CUtils::FScalarArrayCmp(pexprArrayComp));

	CConstraintInterval *pcnstIn = CConstraintInterval::PciIntervalFromScalarExpr(pmp, pexprArrayComp, pcr);
	GPOS_ASSERT(CConstraint::EctInterval == pcnstIn->Ect());
	GPOS_ASSERT(pcnstIn->Pdrgprng()->UlLength() == CUtils::UlCountOperator(pexprArrayComp, COperator::EopScalarConst));

	pcnstIn->Release();
	pexpr->Release();

	// test a NOT IN expression

	CExpression *pexprNotIn = CTestUtils::PexprLogicalSelectArrayCmp(pmp, CScalarArrayCmp::EarrcmpAll, IMDType::EcmptNEq);
	CExpression *pexprArrayNotInComp = (*pexprNotIn->PdrgPexpr())[1];
	CColRef *pcrNot = CDrvdPropRelational::Pdprel(pexprNotIn->PdpDerive())->PcrsOutput()->PcrAny();
	CConstraintInterval *pcnstNotIn = CConstraintInterval::PciIntervalFromScalarExpr(pmp, pexprArrayNotInComp, pcrNot);
	GPOS_ASSERT(CConstraint::EctInterval == pcnstNotIn->Ect());
	// a NOT IN range array should have one more element than the expression array consts
	GPOS_ASSERT(pcnstNotIn->Pdrgprng()->UlLength() == 1 + CUtils::UlCountOperator(pexprArrayNotInComp, COperator::EopScalarConst));

	pexprNotIn->Release();
	pcnstNotIn->Release();

	// create an IN expression with repeated values
	DrgPi *pdrgpi = GPOS_NEW(pmp) DrgPi(pmp);
	INT aiValsRepeat[] = {5,1,2,5,3,4,5};
	ULONG aiValsLength = sizeof(aiValsRepeat)/sizeof(INT);
	for (ULONG ul = 0; ul < aiValsLength; ul++)
	{
		pdrgpi->Append(GPOS_NEW(pmp) INT(aiValsRepeat[ul]));
	}
	CExpression *pexprInRepeatsSelect =
			CTestUtils::PexprLogicalSelectArrayCmp(pmp, CScalarArrayCmp::EarrcmpAny, IMDType::EcmptEq, pdrgpi);
	CColRef *pcrInRepeats = CDrvdPropRelational::Pdprel(pexprInRepeatsSelect->PdpDerive())->PcrsOutput()->PcrAny();
	CExpression *pexprArrayCmpRepeats = (*pexprInRepeatsSelect->PdrgPexpr())[1];
	// add 2 repeated values and one unique
	CConstraintInterval *pcnstInRepeats = CConstraintInterval::PciIntervalFromScalarExpr(pmp, pexprArrayCmpRepeats, pcrInRepeats);
	GPOS_ASSERT(5 == pcnstInRepeats->Pdrgprng()->UlLength());
	pexprInRepeatsSelect->Release();
	pcnstInRepeats->Release();

	// create a NOT IN expression with repeated values
	CExpression *pexprNotInRepeatsSelect = CTestUtils::PexprLogicalSelectArrayCmp(pmp, CScalarArrayCmp::EarrcmpAll, IMDType::EcmptNEq, pdrgpi);
	CColRef *pcrNotInRepeats = CDrvdPropRelational::Pdprel(pexprNotInRepeatsSelect->PdpDerive())->PcrsOutput()->PcrAny();
	CExpression *pexprNotInArrayCmpRepeats = (*pexprNotInRepeatsSelect->PdrgPexpr())[1];
	CConstraintInterval *pcnstNotInRepeats = CConstraintInterval::PciIntervalFromScalarExpr(pmp, pexprNotInArrayCmpRepeats, pcrNotInRepeats);
	// a total of 5 unique ScalarConsts in the expression will result in 6 ranges
	GPOS_ASSERT(6 == pcnstNotInRepeats->Pdrgprng()->UlLength());
	pexprNotInRepeatsSelect->Release();
	pcnstNotInRepeats->Release();
	pdrgpi->Release();

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::EresUnittest_CConstraintIntervalFromArrayExprIncludesNull
//
//	@doc:
//		Tests CConstraintInterval::PcnstrIntervalFromScalarArrayCmp in cases
//		where NULL is in the scalar array expression
//
//---------------------------------------------------------------------------
GPOS_RESULT
EresUnittest_CConstraintIntervalFromArrayExprIncludesNull()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	CConstExprEvaluatorForDates *pceeval = GPOS_NEW(pmp) CConstExprEvaluatorForDates(pmp);

	// install opt context in TLS
	CAutoOptCtxt aoc(pmp, &mda, pceeval, CTestUtils::Pcm(pmp));
	GPOS_ASSERT(NULL != COptCtxt::PoctxtFromTLS()->Pcomp());

	CAutoTraceFlag atf(EopttraceArrayConstraints, true);

	// test for includes NULL
	// create an IN expression with repeated values
	DrgPi *pdrgpi = GPOS_NEW(pmp) DrgPi(pmp);
	INT rngiValues[] = {1,2};
	ULONG ulValsLength = GPOS_ARRAY_SIZE(rngiValues);
	for (ULONG ul = 0; ul < ulValsLength; ul++)
	{
		pdrgpi->Append(GPOS_NEW(pmp) INT(rngiValues[ul]));
	}
	CExpression *pexprIn =
		CTestUtils::PexprLogicalSelectArrayCmp(pmp, CScalarArrayCmp::EarrcmpAny, IMDType::EcmptEq, pdrgpi);

	CExpression *pexprArrayChild = (*(*pexprIn)[1])[1];
	// create a int4 datum
	const IMDTypeInt4 *pmdtypeint4 = mda.PtMDType<IMDTypeInt4>();
	IDatumInt4 *pdatumNull =  pmdtypeint4->PdatumInt4(pmp, 0, true);

	CExpression *pexprConstNull =
		GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarConst(pmp, (IDatum*) pdatumNull));
	pexprArrayChild->PdrgPexpr()->Append(pexprConstNull);

	CColRef *pcr = CDrvdPropRelational::Pdprel(pexprIn->PdpDerive())->PcrsOutput()->PcrAny();
	CConstraintInterval *pci = CConstraintInterval::PciIntervalFromScalarExpr(pmp, (*pexprIn)[1], pcr);
	GPOS_RTL_ASSERT(pci->FIncludesNull());
	GPOS_RTL_ASSERT(2 == pci->Pdrgprng()->UlLength());
	pexprIn->Release();
	pci->Release();
	pdrgpi->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::EresUnittest_CIntervalFromScalarCmp
//
//	@doc:
//		Interval from scalar comparison
//
//---------------------------------------------------------------------------
GPOS_RESULT
CConstraintTest::EresUnittest_CIntervalFromScalarCmp
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	CColRef *pcr
	)
{
	IMDType::ECmpType rgecmpt[] =
			{
			IMDType::EcmptEq,
			IMDType::EcmptNEq,
			IMDType::EcmptL,
			IMDType::EcmptLEq,
			IMDType::EcmptG,
			IMDType::EcmptGEq,
			};

	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(rgecmpt); ul++)
	{
		CExpression *pexprScCmp = PexprScalarCmp(pmp, pmda, pcr, rgecmpt[ul], 4);
		CConstraintInterval *pci = CConstraintInterval::PciIntervalFromScalarExpr(pmp, pexprScCmp, pcr);
		PrintConstraint(pmp, pci);

		pci->Release();
		pexprScCmp->Release();
	}

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::EresUnittest_CIntervalFromScalarBoolOp
//
//	@doc:
//		Interval from scalar bool op
//
//---------------------------------------------------------------------------
GPOS_RESULT
CConstraintTest::EresUnittest_CIntervalFromScalarBoolOp
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	CColRef *pcr
	)
{
	// AND
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexpr->Append(PexprScalarCmp(pmp, pmda, pcr, IMDType::EcmptL, 5));
	pdrgpexpr->Append(PexprScalarCmp(pmp, pmda, pcr, IMDType::EcmptGEq, 0));

	CExpression *pexpr = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopAnd, pdrgpexpr);
	(void) pexpr->PdpDerive();

	CConstraintInterval *pciAnd = CConstraintInterval::PciIntervalFromScalarExpr(pmp, pexpr, pcr);
	PrintConstraint(pmp, pciAnd);

	pciAnd->Release();
	(void) pexpr->Release();

	// OR
	pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexpr->Append(PexprScalarCmp(pmp, pmda, pcr, IMDType::EcmptL, 5));
	pdrgpexpr->Append(PexprScalarCmp(pmp, pmda, pcr, IMDType::EcmptGEq, 10));

	pexpr = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopOr, pdrgpexpr);
	(void) pexpr->PdpDerive();

	CConstraintInterval *pciOr = CConstraintInterval::PciIntervalFromScalarExpr(pmp, pexpr, pcr);
	PrintConstraint(pmp, pciOr);

	pciOr->Release();
	pexpr->Release();

	// NOT
	pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexpr->Append(CUtils::PexprIsNull(pmp, CUtils::PexprScalarIdent(pmp, pcr)));

	pexpr = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopNot, pdrgpexpr);
	(void) pexpr->PdpDerive();

	CConstraintInterval *pciNot = CConstraintInterval::PciIntervalFromScalarExpr(pmp, pexpr, pcr);
	PrintConstraint(pmp, pciNot);

	pciNot->Release();
	pexpr->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::PexprScalarCmp
//
//	@doc:
//		Generate comparison expression
//
//---------------------------------------------------------------------------
CExpression *
CConstraintTest::PexprScalarCmp
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	CColRef *pcr,
	IMDType::ECmpType ecmpt,
	LINT lVal
	)
{
	CExpression *pexprConst = CUtils::PexprScalarConstInt8(pmp, lVal);

	const IMDTypeInt8 *pmdtypeint8 = pmda->PtMDType<IMDTypeInt8>();
	IMDId *pmdidOp = pmdtypeint8->PmdidCmp(ecmpt);
	pmdidOp->AddRef();

	const CMDName mdname = pmda->Pmdscop(pmdidOp)->Mdname();

	CWStringConst strOpName(mdname.Pstr()->Wsz());

	CExpression *pexpr = CUtils::PexprScalarCmp(pmp, pcr, pexprConst, strOpName, pmdidOp);
	(void) pexpr->PdpDerive();
	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::PciFirstInterval
//
//	@doc:
//		Create an interval
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintTest::PciFirstInterval
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	CColRef *pcr
	)
{
	const SRangeInfo rgRangeInfo[] =
			{
				{CRange::EriExcluded, -1000, CRange::EriIncluded, -20},
				{CRange::EriExcluded, -15, CRange::EriExcluded, -5},
				{CRange::EriIncluded, 0, CRange::EriExcluded, 5},
				{CRange::EriIncluded, 10, CRange::EriIncluded, 10},
				{CRange::EriExcluded, 20, CRange::EriExcluded, 1000},
			};

	DrgPrng *pdrgprng = Pdrgprng(pmp, pmdid, rgRangeInfo, GPOS_ARRAY_SIZE(rgRangeInfo));

	return GPOS_NEW(pmp) CConstraintInterval(pmp, pcr, pdrgprng, true /*fIsNull*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::PciSecondInterval
//
//	@doc:
//		Create an interval
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintTest::PciSecondInterval
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	CColRef *pcr
	)
{
	const SRangeInfo rgRangeInfo[] =
			{
				{CRange::EriExcluded, -30, CRange::EriExcluded, 1},
				{CRange::EriIncluded, 3, CRange::EriIncluded, 3},
				{CRange::EriExcluded, 10, CRange::EriExcluded, 25},
			};

	DrgPrng *pdrgprng = Pdrgprng(pmp, pmdid, rgRangeInfo, GPOS_ARRAY_SIZE(rgRangeInfo));

	return GPOS_NEW(pmp) CConstraintInterval(pmp, pcr, pdrgprng, false /*fIsNull*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::Pdrgprng
//
//	@doc:
//		Construct an array of ranges to be used to create an interval
//
//---------------------------------------------------------------------------
DrgPrng *
CConstraintTest::Pdrgprng
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	const SRangeInfo rgRangeInfo[],
	ULONG ulRanges
	)
{
	DrgPrng *pdrgprng = GPOS_NEW(pmp) DrgPrng(pmp);

	for (ULONG ul = 0; ul < ulRanges; ul++)
	{
		SRangeInfo rnginfo = rgRangeInfo[ul];
		pmdid->AddRef();
		CRange *prange = GPOS_NEW(pmp) CRange
									(
									pmdid,
									COptCtxt::PoctxtFromTLS()->Pcomp(),
									GPOS_NEW(pmp) CDatumInt8GPDB(CTestUtils::m_sysidDefault, (LINT) rnginfo.iLeft),
									rnginfo.eriLeft,
									GPOS_NEW(pmp) CDatumInt8GPDB(CTestUtils::m_sysidDefault, (LINT) rnginfo.iRight),
									rnginfo.eriRight
									);
		pdrgprng->Append(prange);
	}

	return pdrgprng;
}


//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::PrintConstraint
//
//	@doc:
//		Print a constraint and its expression representation
//
//---------------------------------------------------------------------------
void
CConstraintTest::PrintConstraint
	(
	IMemoryPool *pmp,
	CConstraint *pcnstr
	)
{
	CExpression *pexpr = pcnstr->PexprScalar(pmp);

	// debug print
	CAutoTrace at(pmp);
	at.Os() << std::endl;
	at.Os() << "CONSTRAINT:" << std::endl << *pcnstr << std::endl << "EXPR:" << std::endl << *pexpr << std::endl;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::PrintEquivClasses
//
//	@doc:
//		Print equivalent classes
//
//---------------------------------------------------------------------------
void
CConstraintTest::PrintEquivClasses
	(
	IMemoryPool *pmp,
	DrgPcrs *pdrgpcrs,
	BOOL fExpected
	)
{
	// debug print
	CAutoTrace at(pmp);
	at.Os() << std::endl;
	if (fExpected)
	{
		at.Os() << "EXPECTED ";
	}
	else
	{
		at.Os() << "ACTUAL ";
	}
	at.Os() << "EQUIVALENCE CLASSES: [ ";

	if (NULL == pdrgpcrs)
	{
		at.Os() << "]" << std::endl;

		return;
	}

	for (ULONG ul = 0; ul < pdrgpcrs->UlLength(); ul++)
	{
		at.Os() << "{" << *(*pdrgpcrs)[ul] << "} ";
	}

	at.Os() << "]" << std::endl;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::EresUnittest_NegativeTests
//
//	@doc:
//		Tests for unconstrainable types.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CConstraintTest::EresUnittest_NegativeTests()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

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

	CConstExprEvaluatorForDates *pceeval = GPOS_NEW(pmp) CConstExprEvaluatorForDates(pmp);

	// install opt context in TLS
	CAutoOptCtxt aoc(pmp, &mda, pceeval, CTestUtils::Pcm(pmp));
	GPOS_ASSERT(NULL != COptCtxt::PoctxtFromTLS()->Pcomp());

	const IMDType *pmdtype = mda.Pmdtype(&CMDIdGPDB::m_mdidText);
	CWStringConst str(GPOS_WSZ_LIT("text_col"));
	CName name(pmp, &str);
	CAutoP<CColRef> pcr(COptCtxt::PoctxtFromTLS()->Pcf()->PcrCreate(pmdtype, IDefaultTypeModifier, OidInvalidCollation, name));

	// create a text interval: ['baz', 'foobar')
	CAutoP<CWStringDynamic> pstrLower1(GPOS_NEW(pmp) CWStringDynamic(pmp, GPOS_WSZ_LIT("AAAAB2Jheg==")));
	CAutoP<CWStringDynamic> pstrUpper1(GPOS_NEW(pmp) CWStringDynamic(pmp, GPOS_WSZ_LIT("AAAACmZvb2Jhcg==")));
	const LINT lLower1 = 571163436;
	const LINT lUpper1 = 322061118;

	// 'text' is not a constrainable type, so the interval construction should assert-fail
	CConstraintInterval *pciFirst =
					CTestUtils::PciGenericInterval
						(
						pmp,
						&mda,
						CMDIdGPDB::m_mdidText,
						pcr.Pt(),
						pstrLower1.Pt(),
						lLower1,
						CRange::EriIncluded,
						pstrUpper1.Pt(),
						lUpper1,
						CRange::EriExcluded
						);
	PrintConstraint(pmp, pciFirst);
	pciFirst->Release();

	return GPOS_OK;
}
#endif // GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CConstraintTest::EresUnittest_ConstraintsOnDates
//	@doc:
//		Test constraints on date intervals.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CConstraintTest::EresUnittest_ConstraintsOnDates()
{
	CAutoTraceFlag atf(EopttraceEnableConstantExpressionEvaluation, true /*fVal*/);

	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	CConstExprEvaluatorForDates *pceeval = GPOS_NEW(pmp) CConstExprEvaluatorForDates(pmp);

	// install opt context in TLS
	CAutoOptCtxt aoc(pmp, &mda, pceeval, CTestUtils::Pcm(pmp));
	GPOS_ASSERT(NULL != COptCtxt::PoctxtFromTLS()->Pcomp());

	const IMDType *pmdtype = mda.Pmdtype(&CMDIdGPDB::m_mdidDate);
	CWStringConst str(GPOS_WSZ_LIT("date_col"));
	CName name(pmp, &str);
	CAutoP<CColRef> pcr(COptCtxt::PoctxtFromTLS()->Pcf()->PcrCreate(pmdtype, IDefaultTypeModifier, OidInvalidCollation, name));

	// create a date interval: ['01-01-2012', '01-21-2012')
	CWStringDynamic pstrLowerDate1(pmp, wszInternalRepresentationFor2012_01_01);
	CWStringDynamic pstrUpperDate1(pmp, wszInternalRepresentationFor2012_01_21);
	CConstraintInterval *pciFirst =
			CTestUtils::PciGenericInterval
				(
				pmp,
				&mda,
				CMDIdGPDB::m_mdidDate,
				pcr.Pt(),
				&pstrLowerDate1,
				lInternalRepresentationFor2012_01_01,
				CRange::EriIncluded,
				&pstrUpperDate1,
				lInternalRepresentationFor2012_01_21,
				CRange::EriExcluded
				);
	PrintConstraint(pmp, pciFirst);

	// create a date interval: ['01-02-2012', '01-22-2012')
	CWStringDynamic pstrLowerDate2(pmp, wszInternalRepresentationFor2012_01_02);
	CWStringDynamic pstrUpperDate2(pmp, wszInternalRepresentationFor2012_01_22);
	CConstraintInterval *pciSecond =
			CTestUtils::PciGenericInterval
				(
				pmp,
				&mda,
				CMDIdGPDB::m_mdidDate,
				pcr.Pt(),
				&pstrLowerDate2,
				lInternalRepresentationFor2012_01_02,
				CRange::EriIncluded,
				&pstrUpperDate2,
				lInternalRepresentationFor2012_01_22,
				CRange::EriExcluded
				);
	PrintConstraint(pmp, pciSecond);

	// intersection
	CConstraintInterval *pciIntersect = pciFirst->PciIntersect(pmp, pciSecond);
	PrintConstraint(pmp, pciIntersect);
	CConstraintInterval *pciIntersectExpected =
			CTestUtils::PciGenericInterval
				(
				pmp,
				&mda,
				CMDIdGPDB::m_mdidDate,
				pcr.Pt(),
				&pstrLowerDate2,
				lInternalRepresentationFor2012_01_02,
				CRange::EriIncluded,
				&pstrUpperDate1,
				lInternalRepresentationFor2012_01_21,
				CRange::EriExcluded
				);
	GPOS_ASSERT(pciIntersectExpected->FEquals(pciIntersect));

	// union
	CConstraintInterval *pciUnion = pciFirst->PciUnion(pmp, pciSecond);
	PrintConstraint(pmp, pciUnion);
	CConstraintInterval *pciUnionExpected =
			CTestUtils::PciGenericInterval
				(
				pmp,
				&mda,
				CMDIdGPDB::m_mdidDate,
				pcr.Pt(),
				&pstrLowerDate1,
				lInternalRepresentationFor2012_01_01,
				CRange::EriIncluded,
				&pstrUpperDate2,
				lInternalRepresentationFor2012_01_22,
				CRange::EriExcluded
				);
	GPOS_ASSERT(pciUnionExpected->FEquals(pciUnion));

	// difference between pciFirst and pciSecond
	CConstraintInterval *pciDiff1 = pciFirst->PciDifference(pmp, pciSecond);
	PrintConstraint(pmp, pciDiff1);
	CConstraintInterval *pciDiff1Expected =
			CTestUtils::PciGenericInterval
				(
				pmp,
				&mda,
				CMDIdGPDB::m_mdidDate,
				pcr.Pt(),
				&pstrLowerDate1,
				lInternalRepresentationFor2012_01_01,
				CRange::EriIncluded,
				&pstrLowerDate2,
				lInternalRepresentationFor2012_01_02,
				CRange::EriExcluded
				);
	GPOS_ASSERT(pciDiff1Expected->FEquals(pciDiff1));

	// difference between pciSecond and pciFirst
	CConstraintInterval *pciDiff2 = pciSecond->PciDifference(pmp, pciFirst);
	PrintConstraint(pmp, pciDiff2);
	CConstraintInterval *pciDiff2Expected =
			CTestUtils::PciGenericInterval
				(
				pmp,
				&mda,
				CMDIdGPDB::m_mdidDate,
				pcr.Pt(),
				&pstrUpperDate1,
				lInternalRepresentationFor2012_01_21,
				CRange::EriIncluded,
				&pstrUpperDate2,
				lInternalRepresentationFor2012_01_22,
				CRange::EriExcluded
				);
	GPOS_ASSERT(pciDiff2Expected->FEquals(pciDiff2));

	// containment
	GPOS_ASSERT(!pciFirst->FContains(pciSecond));
	GPOS_ASSERT(pciFirst->FContains(pciDiff1));
	GPOS_ASSERT(!pciSecond->FContains(pciFirst));
	GPOS_ASSERT(pciSecond->FContains(pciDiff2));
	GPOS_ASSERT(pciFirst->FContains(pciFirst));
	GPOS_ASSERT(pciSecond->FContains(pciSecond));

	// equality
	// create a third interval identical to the first
	CConstraintInterval *pciThird =
			CTestUtils::PciGenericInterval
				(
				pmp,
				&mda,
				CMDIdGPDB::m_mdidDate,
				pcr.Pt(),
				&pstrLowerDate1,
				lInternalRepresentationFor2012_01_01,
				CRange::EriIncluded,
				&pstrUpperDate1,
				lInternalRepresentationFor2012_01_21,
				CRange::EriExcluded
				);
	GPOS_ASSERT(!pciFirst->FEquals(pciSecond));
	GPOS_ASSERT(!pciFirst->FEquals(pciDiff1));
	GPOS_ASSERT(!pciSecond->FEquals(pciDiff2));
	GPOS_ASSERT(pciFirst->FEquals(pciThird));

	pciThird->Release();
	pciDiff2Expected->Release();
	pciDiff1Expected->Release();
	pciUnionExpected->Release();
	pciIntersectExpected->Release();
	pciDiff2->Release();
	pciDiff1->Release();
	pciUnion->Release();
	pciIntersect->Release();
	pciSecond->Release();
	pciFirst->Release();

	return GPOS_OK;
}

// EOF
