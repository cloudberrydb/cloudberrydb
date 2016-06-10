//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CStatsEquivClassTest.cpp
//
//	@doc:
//		Test for constraint
//---------------------------------------------------------------------------

#include "unittest/base.h"
#include "unittest/gpopt/base/CStatsEquivClassTest.h"
#include "unittest/gpopt/base/CConstraintTest.h"

#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/base/CStatsEquivClass.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"

//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClassTest::EresUnittest
//
//	@doc:
//		Unittest for ranges
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatsEquivClassTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CStatsEquivClassTest::EresUnittest_EquivClassFromScalarExpr),
		};

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
					NULL, /* pceeval */
					CTestUtils::Pcm(pmp)
					);

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClassTest::EresUnittest_EquivClassFromScalarExpr
//
//	@doc:
//		Equivalence class from scalar expressions tests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatsEquivClassTest::EresUnittest_EquivClassFromScalarExpr()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();

	SEquivClassSTestCase rgequivclasstc[] =
	{
		{PequivclasstcScIdCmpConst},
		{PequivclasstcScIdCmpScId},
		{PequivclasstcSameCol},
		{PequivclasstcConj1},
		{PequivclasstcConj2},
		{PequivclasstcConj3},
		{PequivclasstcConj4},
		{PequivclasstcConj5},
		{PequivclasstcDisj1},
		{PequivclasstcDisj2},
		{PequivclasstcDisjConj1},
		{PequivclasstcDisjConj2},
		{PequivclasstcDisjConj3},
		{PequivclasstcCast}
	};

	const ULONG ulTestCases = GPOS_ARRAY_SIZE(rgequivclasstc);

	for (ULONG ul = 0; ul < ulTestCases; ul++)
	{
		SEquivClassSTestCase elem = rgequivclasstc[ul];

		GPOS_CHECK_ABORT;

		// generate the test case
		FnPequivclass *pf = elem.m_pf;
		GPOS_ASSERT(NULL != pf);
		SEquivClass *pequivclass = pf(pmp, pcf, &mda);

		CExpression *pexpr = pequivclass->Pexpr();
		DrgPcrs *pdrgpcrsExpected = pequivclass->PdrgpcrsEquivClass();
		DrgPcrs *pdrgpcrsActual = CStatsEquivClass::PdrgpcrsEquivClassFromScalarExpr(pmp, pexpr);

		{
			// debug print
			CAutoTrace at(pmp);
			at.Os() << std::endl;
			at.Os() << "EXPR:" << std::endl << *pexpr << std::endl;
		}

		CConstraintTest::PrintEquivClasses(pmp, pdrgpcrsActual, false /* fExpected */);
		CConstraintTest::PrintEquivClasses(pmp, pdrgpcrsExpected, true  /* fExpected */);

		if (!FMatchingEquivClasses(pdrgpcrsActual, pdrgpcrsExpected))
		{
			return GPOS_FAILED;
		}

		// clean up
		CRefCount::SafeRelease(pdrgpcrsActual);
		GPOS_DELETE(pequivclass);
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClassTest::PequivclasstcScIdCmpConst
//
//	@doc:
//		Expression and equivalence classes of a  scalar comparison between
//		column reference and a constant
//
//---------------------------------------------------------------------------
CStatsEquivClassTest::SEquivClass *
CStatsEquivClassTest::PequivclasstcScIdCmpConst
	(
	IMemoryPool *pmp,
	CColumnFactory *pcf,
	CMDAccessor *pmda
	)
{
	const IMDTypeInt4 *pmdtypeint4 = pmda->PtMDType<IMDTypeInt4>();

	CColRef *pcr = pcf->PcrCreate(pmdtypeint4);
	IDatum *pdatum = (IDatum *)  pmda->PtMDType<IMDTypeInt4>(CTestUtils::m_sysidDefault)->PdatumInt4(pmp, 1, false /* fNull */);
	CExpression *pexprConst = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarConst(pmp, pdatum));

	// col1 == const
	CExpression *pexpr = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr), pexprConst);

	// Equivalence class {}
	DrgPcrs *pdrpcrs = NULL;

	SEquivClass *pequivclass = GPOS_NEW(pmp) SEquivClass(pexpr, pdrpcrs);

	return pequivclass;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClassTest::PequivclasstcScIdCmpScId
//
//	@doc:
//		Expression and equivalence classes of a scalar comparison between
//		column references
//
//---------------------------------------------------------------------------
CStatsEquivClassTest::SEquivClass *
CStatsEquivClassTest::PequivclasstcScIdCmpScId
	(
	IMemoryPool *pmp,
	CColumnFactory *pcf,
	CMDAccessor *pmda
	)
{
	const IMDTypeInt4 *pmdtypeint4 = pmda->PtMDType<IMDTypeInt4>();

	CColRef *pcr1 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr2 = pcf->PcrCreate(pmdtypeint4);

	// col1 == col2
	CExpression *pexpr = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr2));

	// Equivalence class {1,2}
	DrgPcrs *pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);
	pdrgpcrs->Append(CStatsEquivClass::PcrsEquivClass(pmp, pcr1, pcr2));

	SEquivClass *pequivclass = GPOS_NEW(pmp) SEquivClass(pexpr, pdrgpcrs);

	return pequivclass;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClassTest::PequivclasstcSameCol
//
//	@doc:
//		Expression and equivalence classes of a predicate on same column
//
//---------------------------------------------------------------------------
CStatsEquivClassTest::SEquivClass *
CStatsEquivClassTest::PequivclasstcSameCol
	(
	IMemoryPool *pmp,
	CColumnFactory *pcf,
	CMDAccessor *pmda
	)
{
	const IMDTypeInt4 *pmdtypeint4 = pmda->PtMDType<IMDTypeInt4>();

	CColRef *pcr1 = pcf->PcrCreate(pmdtypeint4);

	// col1 == col1
	CExpression *pexpr = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr1));

	// Equivalence class {}
	DrgPcrs *pdrgpcrs = NULL;

	SEquivClass *pequivclass = GPOS_NEW(pmp) SEquivClass(pexpr, pdrgpcrs);

	return pequivclass;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClassTest::PequivclasstcConj1
//
//	@doc:
//		Expression and its equivalence class for a conjunctive predicate
//
//---------------------------------------------------------------------------
CStatsEquivClassTest::SEquivClass *
CStatsEquivClassTest::PequivclasstcConj1
	(
	IMemoryPool *pmp,
	CColumnFactory *pcf,
	CMDAccessor *pmda
	)
{
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);

	const IMDTypeInt4 *pmdtypeint4 = pmda->PtMDType<IMDTypeInt4>();

	CColRef *pcr1 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr2 = pcf->PcrCreate(pmdtypeint4);

	// (col1 == col2) AND (col1 == const) AND (col1 == col1)
	//  (col1 == col2)
	CExpression *pexpr1 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr2));
	pdrgpexpr->Append(pexpr1);

	//  (col1 == const)
	IDatum *pdatum = (IDatum *)  pmda->PtMDType<IMDTypeInt4>(CTestUtils::m_sysidDefault)->PdatumInt4(pmp, 1, false /* fNull */);
	CExpression *pexprConst = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarConst(pmp, pdatum));
	CExpression *pexpr2 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), pexprConst);
	pdrgpexpr->Append(pexpr2);

	//  (col1 == col1)
	CExpression *pexpr3 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr1));
	pdrgpexpr->Append(pexpr3);

	CExpression *pexpr = CPredicateUtils::PexprConjunction(pmp, pdrgpexpr);

	// Equivalence class {1,2}
	DrgPcrs *pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);
	pdrgpcrs->Append(CStatsEquivClass::PcrsEquivClass(pmp, pcr1, pcr2));

	SEquivClass *pequivclass = GPOS_NEW(pmp) SEquivClass(pexpr, pdrgpcrs);

	return pequivclass;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClassTest::PequivclasstcConj2
//
//	@doc:
//		Expression and equivalence classes for a conjunctive predicate
//
//---------------------------------------------------------------------------
CStatsEquivClassTest::SEquivClass *
CStatsEquivClassTest::PequivclasstcConj2
	(
	IMemoryPool *pmp,
	CColumnFactory *pcf,
	CMDAccessor *pmda
	)
{
	const IMDTypeInt4 *pmdtypeint4 = pmda->PtMDType<IMDTypeInt4>();

	CColRef *pcr1 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr2 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr3 = pcf->PcrCreate(pmdtypeint4);

	//  (col1 = col2) AND (col1 = col3)
	DrgPexpr *pdrgpexprA = GPOS_NEW(pmp) DrgPexpr(pmp);

	//  (col1 = col2)
	CExpression *pexpr1 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr2));
	pdrgpexprA->Append(pexpr1);

	//  (col1 = col3)
	CExpression *pexpr2 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr3));
	pdrgpexprA->Append(pexpr2);

	CExpression *pexpr = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopAnd, pdrgpexprA);

	// Equivalence class {1,2,3}
	DrgPcrs *pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);
	CColRefSet *pcrs = CStatsEquivClass::PcrsEquivClass(pmp, pcr1, pcr2);
	pcrs->Include(pcr3);
	pdrgpcrs->Append(pcrs);

	return GPOS_NEW(pmp) SEquivClass(pexpr, pdrgpcrs);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClassTest::PequivclasstcConj3
//
//	@doc:
//		Expression and equivalence classes for a conjunctive predicate
//
//---------------------------------------------------------------------------
CStatsEquivClassTest::SEquivClass *
CStatsEquivClassTest::PequivclasstcConj3
	(
	IMemoryPool *pmp,
	CColumnFactory *pcf,
	CMDAccessor *pmda
	)
{
	const IMDTypeInt4 *pmdtypeint4 = pmda->PtMDType<IMDTypeInt4>();

	CColRef *pcr1 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr2 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr3 = pcf->PcrCreate(pmdtypeint4);

	//  (col1 = col2) AND (col2 = col3)
	DrgPexpr *pdrgpexprA = GPOS_NEW(pmp) DrgPexpr(pmp);

	//  (col1 = col2)
	CExpression *pexpr1 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr2));
	pdrgpexprA->Append(pexpr1);

	//  (col2 = col3)
	CExpression *pexpr2 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr2), CUtils::PexprScalarIdent(pmp, pcr3));
	pdrgpexprA->Append(pexpr2);

	CExpression *pexpr = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopAnd, pdrgpexprA);

	// Equivalence class {1,2,3}
	DrgPcrs *pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);
	CColRefSet *pcrs = CStatsEquivClass::PcrsEquivClass(pmp, pcr1, pcr2);
	pcrs->Include(pcr3);
	pdrgpcrs->Append(pcrs);

	return GPOS_NEW(pmp) SEquivClass(pexpr, pdrgpcrs);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClassTest::PequivclasstcConj4
//
//	@doc:
//		Expression and equivalence classes for a conjunctive predicate
//
//---------------------------------------------------------------------------
CStatsEquivClassTest::SEquivClass *
CStatsEquivClassTest::PequivclasstcConj4
	(
	IMemoryPool *pmp,
	CColumnFactory *pcf,
	CMDAccessor *pmda
	)
{
	const IMDTypeInt4 *pmdtypeint4 = pmda->PtMDType<IMDTypeInt4>();

	CColRef *pcr1 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr2 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr3 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr4 = pcf->PcrCreate(pmdtypeint4);

	//  (col1 = col2) AND (col3 = col4)
	DrgPexpr *pdrgpexprA = GPOS_NEW(pmp) DrgPexpr(pmp);

	//  (col1 = col2)
	CExpression *pexpr1 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr2));
	pdrgpexprA->Append(pexpr1);

	//  (col3 = col4)
	CExpression *pexpr2 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr3), CUtils::PexprScalarIdent(pmp, pcr4));
	pdrgpexprA->Append(pexpr2);

	CExpression *pexpr = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopAnd, pdrgpexprA);

	// Equivalence class {1,2} {3,4}
	DrgPcrs *pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);
	pdrgpcrs->Append(CStatsEquivClass::PcrsEquivClass(pmp, pcr1, pcr2));
	pdrgpcrs->Append(CStatsEquivClass::PcrsEquivClass(pmp, pcr3, pcr4));

	return GPOS_NEW(pmp) SEquivClass(pexpr, pdrgpcrs);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClassTest::PequivclasstcConj5
//
//	@doc:
//		Expression and equivalence classes for a conjunctive predicate
//
//---------------------------------------------------------------------------
CStatsEquivClassTest::SEquivClass *
CStatsEquivClassTest::PequivclasstcConj5
	(
	IMemoryPool *pmp,
	CColumnFactory *pcf,
	CMDAccessor *pmda
	)
{
	const IMDTypeInt4 *pmdtypeint4 = pmda->PtMDType<IMDTypeInt4>();

	CColRef *pcr1 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr2 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr3 = pcf->PcrCreate(pmdtypeint4);

	//  (col1 = col2) AND (col3 = col3)
	DrgPexpr *pdrgpexprA = GPOS_NEW(pmp) DrgPexpr(pmp);

	//  (col1 = col2)
	CExpression *pexpr1 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr2));
	pdrgpexprA->Append(pexpr1);

	//  (col3 = col3)
	CExpression *pexpr2 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr3), CUtils::PexprScalarIdent(pmp, pcr3));
	pdrgpexprA->Append(pexpr2);

	CExpression *pexpr = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopAnd, pdrgpexprA);

	// Equivalence class {1,2}
	DrgPcrs *pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);
	pdrgpcrs->Append(CStatsEquivClass::PcrsEquivClass(pmp, pcr1, pcr2));

	return GPOS_NEW(pmp) SEquivClass(pexpr, pdrgpcrs);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClassTest::PequivclasstcDisj1
//
//	@doc:
//		Expression and its equivalence class for a disjunctive predicate
//
//---------------------------------------------------------------------------
CStatsEquivClassTest::SEquivClass *
CStatsEquivClassTest::PequivclasstcDisj1
	(
	IMemoryPool *pmp,
	CColumnFactory *pcf,
	CMDAccessor *pmda
	)
{
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);

	const IMDTypeInt4 *pmdtypeint4 = pmda->PtMDType<IMDTypeInt4>();

	CColRef *pcr1 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr2 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr3 = pcf->PcrCreate(pmdtypeint4);

	//  (col1 = col2)
	CExpression *pexpr1 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr2));
	pdrgpexpr->Append(pexpr1);

	//  (col1 = const)
	IDatum *pdatum = (IDatum *)  pmda->PtMDType<IMDTypeInt4>(CTestUtils::m_sysidDefault)->PdatumInt4(pmp, 1, false /* fNull */);
	CExpression *pexprConst = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarConst(pmp, pdatum));
	CExpression *pexpr2 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), pexprConst);
	pdrgpexpr->Append(pexpr2);

	//  (col1 = col1)
	CExpression *pexpr3 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr1));
	pdrgpexpr->Append(pexpr3);

	//  (col1 = col3)
	CExpression *pexpr4 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr3));
	pdrgpexpr->Append(pexpr4);

	CExpression *pexpr = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopOr, pdrgpexpr);

	// Equivalence class {}
	DrgPcrs *pdrgpcrs = NULL;

	SEquivClass *pequivclass = GPOS_NEW(pmp) SEquivClass(pexpr, pdrgpcrs);

	return pequivclass;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClassTest::PequivclasstcDisj2
//
//	@doc:
//		Expression and its equivalence class for a disjunctive predicate
//
//---------------------------------------------------------------------------
CStatsEquivClassTest::SEquivClass *
CStatsEquivClassTest::PequivclasstcDisj2
	(
	IMemoryPool *pmp,
	CColumnFactory *pcf,
	CMDAccessor *pmda
	)
{
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);

	const IMDTypeInt4 *pmdtypeint4 = pmda->PtMDType<IMDTypeInt4>();

	CColRef *pcr1 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr2 = pcf->PcrCreate(pmdtypeint4);

	//  (col1 = col2)
	CExpression *pexpr1 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr2));
	pdrgpexpr->Append(pexpr1);

	//  (col1 = col2)
	CExpression *pexpr2 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr2));
	pdrgpexpr->Append(pexpr2);

	CExpression *pexpr = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopOr, pdrgpexpr);

	// Equivalence class {1,2}
	DrgPcrs *pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);
	pdrgpcrs->Append(CStatsEquivClass::PcrsEquivClass(pmp, pcr1, pcr2));

	return GPOS_NEW(pmp) SEquivClass(pexpr, pdrgpcrs);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClassTest::PequivclasstcDisjConj1
//
//	@doc:
//		Expression and equivalence classes of a predicate with disjunction and conjunction
//
//---------------------------------------------------------------------------
CStatsEquivClassTest::SEquivClass *
CStatsEquivClassTest::PequivclasstcDisjConj1
	(
	IMemoryPool *pmp,
	CColumnFactory *pcf,
	CMDAccessor *pmda
	)
{
	const IMDTypeInt4 *pmdtypeint4 = pmda->PtMDType<IMDTypeInt4>();

	CColRef *pcr1 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr2 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr3 = pcf->PcrCreate(pmdtypeint4);

	//  (col1 = col2) AND (col1 = col3)
	DrgPexpr *pdrgpexprA = GPOS_NEW(pmp) DrgPexpr(pmp);

	//  (col1 = col2)
	CExpression *pexpr1 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr2));
	pdrgpexprA->Append(pexpr1);

	//  (col1 = col3)
	CExpression *pexpr2 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr3));
	pdrgpexprA->Append(pexpr2);

	CExpression *pexprA = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopAnd, pdrgpexprA);

	//  (col3 = col2) AND (col1 = col2)
	DrgPexpr *pdrgpexprB = GPOS_NEW(pmp) DrgPexpr(pmp);

	//  (col3 = col2)
	CExpression *pexpr3 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr3), CUtils::PexprScalarIdent(pmp, pcr2));
	pdrgpexprB->Append(pexpr3);

	//  (col1 = col2)
	CExpression *pexpr4 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr2));
	pdrgpexprB->Append(pexpr4);

	CExpression *pexprB = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopAnd, pdrgpexprB);

	// exprA OR exprB
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexpr->Append(pexprA);
	pdrgpexpr->Append(pexprB);

	CExpression *pexpr = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopOr, pdrgpexpr);

	// Equivalence class {1,2,3}
	DrgPcrs *pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);
	CColRefSet *pcrs = CStatsEquivClass::PcrsEquivClass(pmp, pcr1, pcr2);
	pcrs->Include(pcr3);
	pdrgpcrs->Append(pcrs);

	return GPOS_NEW(pmp) SEquivClass(pexpr, pdrgpcrs);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClassTest::PequivclasstcDisjConj2
//
//	@doc:
//		Expression and equivalence classes of a predicate with disjunction and conjunction
//
//---------------------------------------------------------------------------
CStatsEquivClassTest::SEquivClass *
CStatsEquivClassTest::PequivclasstcDisjConj2
	(
	IMemoryPool *pmp,
	CColumnFactory *pcf,
	CMDAccessor *pmda
	)
{
	const IMDTypeInt4 *pmdtypeint4 = pmda->PtMDType<IMDTypeInt4>();

	CColRef *pcr1 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr2 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr3 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr4 = pcf->PcrCreate(pmdtypeint4);

	//  (col1 = col2) AND (col1 = col3)
	DrgPexpr *pdrgpexprA = GPOS_NEW(pmp) DrgPexpr(pmp);

	//  (col1 = col2)
	CExpression *pexpr1 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr2));
	pdrgpexprA->Append(pexpr1);

	//  (col3 = col4)
	CExpression *pexpr2 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr3), CUtils::PexprScalarIdent(pmp, pcr4));
	pdrgpexprA->Append(pexpr2);

	CExpression *pexprA = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopAnd, pdrgpexprA);

	//  (col1 = col2) AND (col2 = col4)
	DrgPexpr *pdrgpexprB = GPOS_NEW(pmp) DrgPexpr(pmp);

	//  (col1 = col2)
	CExpression *pexpr3 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr2));
	pdrgpexprB->Append(pexpr3);

	//  (col2 = col4)
	CExpression *pexpr4 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr2), CUtils::PexprScalarIdent(pmp, pcr4));
	pdrgpexprB->Append(pexpr4);

	CExpression *pexprB = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopAnd, pdrgpexprB);

	// exprA OR exprB
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexpr->Append(pexprA);
	pdrgpexpr->Append(pexprB);

	CExpression *pexpr = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopOr, pdrgpexpr);

	// Equivalence class {1,2}
	DrgPcrs *pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);
	CColRefSet *pcrs = CStatsEquivClass::PcrsEquivClass(pmp, pcr1, pcr2);
	pdrgpcrs->Append(pcrs);

	return GPOS_NEW(pmp) SEquivClass(pexpr, pdrgpcrs);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClassTest::PequivclasstcDisjConj3
//
//	@doc:
//		Expression and equivalence classes of a predicate with disjunction and conjunction
//
//---------------------------------------------------------------------------
CStatsEquivClassTest::SEquivClass *
CStatsEquivClassTest::PequivclasstcDisjConj3
	(
	IMemoryPool *pmp,
	CColumnFactory *pcf,
	CMDAccessor *pmda
	)
{
	const IMDTypeInt4 *pmdtypeint4 = pmda->PtMDType<IMDTypeInt4>();

	CColRef *pcr1 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr2 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr3 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr4 = pcf->PcrCreate(pmdtypeint4);

	// exprA: (col1 = col2) AND (col3 = col4)
	DrgPexpr *pdrgpexprA = GPOS_NEW(pmp) DrgPexpr(pmp);

	//  (col1 = col2)
	CExpression *pexpr1 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr2));
	pdrgpexprA->Append(pexpr1);

	//  (col3 = col4)
	CExpression *pexpr2 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr3), CUtils::PexprScalarIdent(pmp, pcr4));
	pdrgpexprA->Append(pexpr2);

	CExpression *pexprA = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopAnd, pdrgpexprA);

	// exprB: (col1 = col3) AND (col2 = col4)
	DrgPexpr *pdrgpexprB = GPOS_NEW(pmp) DrgPexpr(pmp);

	//  (col1 = col3)
	CExpression *pexpr3 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr1), CUtils::PexprScalarIdent(pmp, pcr3));
	pdrgpexprB->Append(pexpr3);

	//  (col2 = col4)
	CExpression *pexpr4 = CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarIdent(pmp, pcr2), CUtils::PexprScalarIdent(pmp, pcr4));
	pdrgpexprB->Append(pexpr4);

	CExpression *pexprB = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopAnd, pdrgpexprB);

	// exprA OR exprB
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexpr->Append(pexprA);
	pdrgpexpr->Append(pexprB);

	CExpression *pexpr = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopOr, pdrgpexpr);

	// Equivalence class {}
	DrgPcrs *pdrgpcrs = NULL;

	return GPOS_NEW(pmp) SEquivClass(pexpr, pdrgpcrs);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClassTest::PequivclasstcCast
//
//	@doc:
//		Expression and equivalence classes of a scalar comparison between
//		column references
//
//---------------------------------------------------------------------------
CStatsEquivClassTest::SEquivClass *
CStatsEquivClassTest::PequivclasstcCast
	(
	IMemoryPool *pmp,
	CColumnFactory *pcf,
	CMDAccessor *pmda
	)
{
	const IMDTypeInt4 *pmdtypeint4 = pmda->PtMDType<IMDTypeInt4>();
	const IMDTypeInt4 *pmdtypeint8 = pmda->PtMDType<IMDTypeInt4>();

	// col1::int8 = col2
	CColRef *pcr1 = pcf->PcrCreate(pmdtypeint4);
	CColRef *pcr2 = pcf->PcrCreate(pmdtypeint4);

	CExpression *pexprScId1 = CUtils::PexprScalarIdent(pmp, pcr1);
	CExpression *pexprCast = CPredicateUtils::PexprCast(pmp, pmda, pexprScId1, pmdtypeint8->Pmdid());

	CExpression *pexpr = CUtils::PexprScalarEqCmp(pmp, pexprCast, CUtils::PexprScalarIdent(pmp, pcr2));
	DrgPcrs *pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);
	pdrgpcrs->Append(CStatsEquivClass::PcrsEquivClass(pmp, pcr1, pcr2));

	// Equivalence class {}
	SEquivClass *pequivclass = GPOS_NEW(pmp) SEquivClass(pexpr, pdrgpcrs);

	return pequivclass;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClassTest::FMatchingEquivClasses
//
//	@doc:
//		Check if the two equivalence classes match
//
//---------------------------------------------------------------------------
BOOL
CStatsEquivClassTest::FMatchingEquivClasses
	(
	const DrgPcrs *pdrgpcrsActual,
	const DrgPcrs *pdrgpcrsExpected
	)
{
	if (NULL != pdrgpcrsExpected && NULL != pdrgpcrsActual)
	{
		const ULONG ulActual = pdrgpcrsActual->UlLength();
		const ULONG ulExpected = pdrgpcrsExpected->UlLength();

		if (ulActual < ulExpected)
		{
			// there can be equivalence classes that can be singleton in pdrgpcrsActual
			return false;
		}

		for (ULONG ulA = 0; ulA < ulActual; ulA++)
		{
			CColRefSet *pcrsActual = (*pdrgpcrsActual)[ulA];

			BOOL fMatch = (1 == pcrsActual->CElements());
			for (ULONG ulE = 0; ulE < ulExpected && !fMatch; ulE++)
			{
				CColRefSet *pcrsExpected = (*pdrgpcrsExpected)[ulE];

				if (pcrsActual->FEqual(pcrsExpected))
				{
					fMatch = true;
				}
			}

			if (!fMatch)
			{
				return false;
			}
		}

		return true;
	}

	return (UlEquivClasses(pdrgpcrsExpected) == UlEquivClasses(pdrgpcrsActual));
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClassTest::UlEquivClasses
//
//	@doc:
//		Return the number of non-singleton equivalence classes
//
//---------------------------------------------------------------------------
ULONG
CStatsEquivClassTest::UlEquivClasses
	(
	const DrgPcrs *pdrgpcrs
	)
{
	ULONG ulSize = 0;

	if (NULL == pdrgpcrs)
	{
		return ulSize;
	}

	const ULONG ulActual = pdrgpcrs->UlLength();
	for (ULONG ul = 0; ul < ulActual; ul++)
	{
		CColRefSet *pcrs = (*pdrgpcrs)[ul];

		if (1 != pcrs->CElements())
		{
			ulSize++;
		}
	}

	return ulSize;
}

// EOF
