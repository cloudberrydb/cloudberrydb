//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSubqueryTestUtils.cpp
//
//	@doc:
//		Implementation of test utility functions
//---------------------------------------------------------------------------

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/base/CQueryContext.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/ops.h"

#include "unittest/gpopt/CSubqueryTestUtils.h"

#include "naucrates/md/CMDIdGPDB.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::GenerateGetExpressions
//
//	@doc:
//		Helper for generating a pair of randomized Get expressions
//
//---------------------------------------------------------------------------
void
CSubqueryTestUtils::GenerateGetExpressions
	(
	IMemoryPool *pmp,
	CExpression **ppexprOuter,
	CExpression **ppexprInner
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != ppexprOuter);
	GPOS_ASSERT(NULL != ppexprInner);

	// outer expression
	CWStringConst strNameR(GPOS_WSZ_LIT("Rel1"));
	CMDIdGPDB *pmdidR = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_TEST_REL_OID1, 1, 1);
	CTableDescriptor *ptabdescR = CTestUtils::PtabdescCreate(pmp, 3 /*ulCols*/, pmdidR, CName(&strNameR));
	*ppexprOuter = CTestUtils::PexprLogicalGet(pmp, ptabdescR, &strNameR);

	// inner expression
	CWStringConst strNameS(GPOS_WSZ_LIT("Rel2"));
	CMDIdGPDB *pmdidS = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_TEST_REL_OID2, 1, 1);
	CTableDescriptor *ptabdescS = CTestUtils::PtabdescCreate(pmp, 3 /*ulCols*/, pmdidS, CName(&strNameS));
	*ppexprInner = CTestUtils::PexprLogicalGet(pmp, ptabdescS, &strNameS);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprJoinWithAggSubquery
//
//	@doc:
//		Generate randomized join expression with a subquery predicate
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprJoinWithAggSubquery
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	GPOS_ASSERT(NULL != pmp);

	CExpression *pexprLeft = NULL;
	CExpression *pexprRight = NULL;
	GenerateGetExpressions(pmp, &pexprLeft, &pexprRight);

	CExpression *pexprInner = CTestUtils::PexprLogicalGet(pmp);
	CExpression *pexprSelect = PexprSelectWithAggSubquery(pmp, pexprLeft, pexprInner, fCorrelated);

	(*pexprSelect)[0]->AddRef();
	(*pexprSelect)[1]->AddRef();

	CExpression *pexpr = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalInnerJoin(pmp),
											  (*pexprSelect)[0],
											  pexprRight,
											  (*pexprSelect)[1]);

	pexprSelect->Release();

	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithAggSubquery
//
//	@doc:
//		Generate a Select expression with a subquery equality predicate
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithAggSubquery
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	BOOL fCorrelated
	)
{
	GPOS_ASSERT(NULL != pexprOuter);
	GPOS_ASSERT(NULL != pexprInner);

	// get any column
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput();
	CColRef *pcrLeft =  pcrs->PcrAny();

	// generate agg subquery
	CExpression *pexprSubq = PexprSubqueryAgg(pmp, pexprOuter, pexprInner, fCorrelated);

	// generate equality predicate
	CExpression *pexprPredicate = CUtils::PexprScalarEqCmp(pmp, pcrLeft, pexprSubq);

	return CUtils::PexprLogicalSelect(pmp, pexprOuter, pexprPredicate);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithAggSubqueryConstComparison
//
//	@doc:
//		Generate a Select expression with a subquery equality predicate
//		involving constant
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithAggSubqueryConstComparison
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	BOOL fCorrelated
	)
{
	GPOS_ASSERT(NULL != pexprOuter);
	GPOS_ASSERT(NULL != pexprInner);

	// generate agg subquery
	CExpression *pexprSubq = PexprSubqueryAgg(pmp, pexprOuter, pexprInner, fCorrelated);

	CExpression *pexprConst = CUtils::PexprScalarConstInt8(pmp, 0 /*iVal*/);

	// generate equality predicate
	CExpression *pexprPredicate = CUtils::PexprScalarEqCmp(pmp, pexprConst, pexprSubq);

	return CUtils::PexprLogicalSelect(pmp, pexprOuter, pexprPredicate);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprProjectWithAggSubquery
//
//	@doc:
//		Generate a Project expression with a subquery equality predicate
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprProjectWithAggSubquery
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	BOOL fCorrelated
	)
{
	GPOS_ASSERT(NULL != pexprOuter);
	GPOS_ASSERT(NULL != pexprInner);

	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	// generate agg subquery
	CExpression *pexprSubq = PexprSubqueryAgg(pmp, pexprOuter, pexprInner, fCorrelated);

	// generate a computed column
	CScalarSubquery *popSubquery = CScalarSubquery::PopConvert(pexprSubq->Pop());
	const IMDType *pmdtype = pmda->Pmdtype(popSubquery->PmdidType());
	CColRef *pcrComputed = pcf->PcrCreate(pmdtype, popSubquery->ITypeModifier(), OidInvalidCollation);

	// generate a scalar project list
	CExpression *pexprPrjElem = CUtils::PexprScalarProjectElement(pmp, pcrComputed, pexprSubq);
	CExpression *pexprPrjList =
		GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp), pexprPrjElem);

	return CUtils::PexprLogicalProject(pmp, pexprOuter, pexprPrjList, false /*fNewComputedCol*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithAggSubquery
//
//	@doc:
//		Generate randomized Select expression with a subquery predicate
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithAggSubquery
	(
	IMemoryPool *pmp,
	 BOOL fCorrelated
	)
{
	GPOS_ASSERT(NULL != pmp);

	CExpression *pexprOuter = NULL;
	CExpression *pexprInner = NULL;
	GenerateGetExpressions(pmp, &pexprOuter, &pexprInner);

	return PexprSelectWithAggSubquery(pmp, pexprOuter, pexprInner, fCorrelated);
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithAggSubqueryConstComparison
//
//	@doc:
//		Generate randomized Select expression with a subquery predicate
//		involving constant
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithAggSubqueryConstComparison
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	GPOS_ASSERT(NULL != pmp);

	CExpression *pexprOuter = NULL;
	CExpression *pexprInner = NULL;
	GenerateGetExpressions(pmp, &pexprOuter, &pexprInner);

	return PexprSelectWithAggSubqueryConstComparison(pmp, pexprOuter, pexprInner, fCorrelated);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprProjectWithAggSubquery
//
//	@doc:
//		Generate randomized Project expression with a subquery in project
//		element
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprProjectWithAggSubquery
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	GPOS_ASSERT(NULL != pmp);

	CExpression *pexprOuter = NULL;
	CExpression *pexprInner = NULL;
	GenerateGetExpressions(pmp, &pexprOuter, &pexprInner);

	return PexprProjectWithAggSubquery(pmp, pexprOuter, pexprInner, fCorrelated);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithAggSubqueryOverJoin
//
//	@doc:
//		Generate a random select expression with a subquery over join predicate
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithAggSubqueryOverJoin
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	GPOS_ASSERT(NULL != pmp);

	// generate a pair of get expressions
	CExpression *pexprR = NULL;
	CExpression *pexprS = NULL;
	GenerateGetExpressions(pmp, &pexprR, &pexprS);

	// generate outer expression
	CWStringConst strNameT(GPOS_WSZ_LIT("Rel3"));

	CMDIdGPDB *pmdidT = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_TEST_REL_OID3, 1, 1);
	CTableDescriptor *ptabdescT = CTestUtils::PtabdescCreate(pmp, 3 /*ulCols*/, pmdidT, CName(&strNameT));
	CExpression *pexprT = CTestUtils::PexprLogicalGet(pmp, ptabdescT, &strNameT);
	CColRef *pcrInner = CDrvdPropRelational::Pdprel(pexprR->PdpDerive())->PcrsOutput()->PcrAny();
	CColRef *pcrOuter = CDrvdPropRelational::Pdprel(pexprT->PdpDerive())->PcrsOutput()->PcrAny();

	CExpression *pexprPred = NULL;
	if (fCorrelated)
	{
		// generate correlation predicate
		pexprPred = CUtils::PexprScalarEqCmp(pmp, pcrInner, pcrOuter);
	}
	else
	{
		pexprPred = CUtils::PexprScalarConstBool(pmp, true /*fVal*/);
	}

	// generate N-Ary join
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexpr->Append(pexprR);
	pdrgpexpr->Append(pexprS);
	pdrgpexpr->Append(pexprPred);
	CExpression *pexprJoin = CTestUtils::PexprLogicalNAryJoin(pmp, pdrgpexpr);

	CExpression *pexprSubq =
		GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarSubquery(pmp, pcrInner, false /*fGeneratedByExist*/, false /*fGeneratedByQuantified*/), pexprJoin);

	CExpression *pexprPredOuter = CUtils::PexprScalarEqCmp(pmp, pcrOuter, pexprSubq);
	return CUtils::PexprLogicalSelect(pmp, pexprT, pexprPredOuter);
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithAnySubquery
//
//	@doc:
//		Generate randomized Select expression with Any subquery predicate
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithAnySubquery
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	return PexprSelectWithSubqueryQuantified(pmp, COperator::EopScalarSubqueryAny, fCorrelated);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithAnySubqueryOverWindow
//
//	@doc:
//		Generate randomized Select expression with Any subquery predicate
//		over window operation
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithAnySubqueryOverWindow
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	return PexprSelectWithSubqueryQuantifiedOverWindow(pmp, COperator::EopScalarSubqueryAny, fCorrelated);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithSubqueryQuantifiedOverWindow
//
//	@doc:
//		Generate randomized Select expression with quantified subquery
//		predicate over window operations
//
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithSubqueryQuantifiedOverWindow
	(
	IMemoryPool *pmp,
	COperator::EOperatorId eopid,
	BOOL fCorrelated
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(COperator::EopScalarSubqueryAny == eopid || COperator::EopScalarSubqueryAll == eopid);

	CExpression *pexprOuter = CTestUtils::PexprLogicalGet(pmp);
	CExpression *pexprInner = CTestUtils::PexprOneWindowFunction(pmp);
	CExpression *pexprSubqueryQuantified = PexprSubqueryQuantified(pmp, eopid, pexprOuter, pexprInner, fCorrelated);

	return CUtils::PexprLogicalSelect(pmp, pexprOuter, pexprSubqueryQuantified);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithAllSubquery
//
//	@doc:
//		Generate randomized Select expression with All subquery predicate
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithAllSubquery
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	return PexprSelectWithSubqueryQuantified(pmp, COperator::EopScalarSubqueryAll, fCorrelated);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithAllSubqueryOverWindow
//
//	@doc:
//		Generate randomized Select expression with All subquery predicate
//		over window operation
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithAllSubqueryOverWindow
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	return PexprSelectWithSubqueryQuantifiedOverWindow(pmp, COperator::EopScalarSubqueryAll, fCorrelated);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithAnyAggSubquery
//
//	@doc:
//		Generate randomized Select expression with Any subquery whose inner
//		expression is a GbAgg
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithAnyAggSubquery
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	return PexprSelectWithQuantifiedAggSubquery(pmp, COperator::EopScalarSubqueryAny, fCorrelated);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithAllAggSubquery
//
//	@doc:
//		Generate randomized Select expression with All subquery whose inner
//		expression is a GbAgg
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithAllAggSubquery
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	return PexprSelectWithQuantifiedAggSubquery(pmp, COperator::EopScalarSubqueryAll, fCorrelated);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprProjectWithAnySubquery
//
//	@doc:
//		Generate randomized Project expression with Any subquery predicate
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprProjectWithAnySubquery
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	CExpression *pexprOuter = NULL;
	CExpression *pexprInner = NULL;
	GenerateGetExpressions(pmp, &pexprOuter, &pexprInner);

	return PexprProjectWithSubqueryQuantified(pmp, pexprOuter, pexprInner, COperator::EopScalarSubqueryAny, fCorrelated);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprProjectWithAllSubquery
//
//	@doc:
//		Generate randomized Project expression with All subquery predicate
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprProjectWithAllSubquery
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	CExpression *pexprOuter = NULL;
	CExpression *pexprInner = NULL;
	GenerateGetExpressions(pmp, &pexprOuter, &pexprInner);

	return PexprProjectWithSubqueryQuantified(pmp, pexprOuter, pexprInner, COperator::EopScalarSubqueryAll, fCorrelated);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSubqueriesInDifferentContexts
//
//	@doc:
//		Generate a randomized expression with subqueries in both value
//		and filter contexts
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSubqueriesInDifferentContexts
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	CExpression *pexprOuter = NULL;
	CExpression *pexprInner = NULL;
	GenerateGetExpressions(pmp, &pexprOuter, &pexprInner);
	CExpression *pexprSelect = PexprSelectWithAggSubquery(pmp, pexprOuter, pexprInner, fCorrelated);

	CExpression *pexprGet = CTestUtils::PexprLogicalGet(pmp);
	return PexprProjectWithSubqueryQuantified(pmp, pexprSelect, pexprGet, COperator::EopScalarSubqueryAny, fCorrelated);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSubqueriesInNullTestContext
//
//	@doc:
//		Generate a randomized expression expression with subquery in null
//		test context
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSubqueriesInNullTestContext
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	CExpression *pexprOuter = NULL;
	CExpression *pexprInner = NULL;
	GenerateGetExpressions(pmp, &pexprOuter, &pexprInner);

	// generate agg subquery
	CExpression *pexprSubq = PexprSubqueryAgg(pmp, pexprOuter, pexprInner, fCorrelated);

	// generate Is Not Null predicate
	CExpression *pexprPredicate = CUtils::PexprIsNotNull(pmp, pexprSubq);

	return CUtils::PexprLogicalSelect(pmp, pexprOuter, pexprPredicate);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithExistsSubquery
//
//	@doc:
//		Generate randomized Select expression with Exists subquery predicate
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithExistsSubquery
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	return PexprSelectWithSubqueryExistential(pmp, COperator::EopScalarSubqueryExists, fCorrelated);
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithNotExistsSubquery
//
//	@doc:
//		Generate randomized Select expression with Not Exists subquery predicate
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithNotExistsSubquery
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	return PexprSelectWithSubqueryExistential(pmp, COperator::EopScalarSubqueryNotExists, fCorrelated);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithSubqueryDisjuncts
//
//	@doc:
//		Generate randomized select expression with subquery predicates in
//		an OR tree
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithSubqueryDisjuncts
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	GPOS_ASSERT(NULL != pmp);

	CExpression *pexprOuter = NULL;
	CExpression *pexprInner = NULL;
	GenerateGetExpressions(pmp, &pexprOuter, &pexprInner);

	return PexprSelectWithSubqueryBoolOp(pmp, pexprOuter, pexprInner, fCorrelated, CScalarBoolOp::EboolopOr);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithTrimmableExists
//
//	@doc:
//		Generate randomized Select expression with trimmable Exists
//		subquery predicate
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithTrimmableExists
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	return PexprSelectWithTrimmableExistentialSubquery(pmp, COperator::EopScalarSubqueryExists, fCorrelated);
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithTrimmableNotExists
//
//	@doc:
//		Generate randomized Select expression with trimmable Not Exists
//		subquery predicate
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithTrimmableNotExists
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	return PexprSelectWithTrimmableExistentialSubquery(pmp, COperator::EopScalarSubqueryNotExists, fCorrelated);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprProjectWithExistsSubquery
//
//	@doc:
//		Generate randomized Project expression with Exists subquery predicate
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprProjectWithExistsSubquery
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	return PexprProjectWithSubqueryExistential(pmp, COperator::EopScalarSubqueryExists, fCorrelated);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprProjectWithNotExistsSubquery
//
//	@doc:
//		Generate randomized Project expression with Not Exists subquery predicate
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprProjectWithNotExistsSubquery
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	return PexprProjectWithSubqueryExistential(pmp, COperator::EopScalarSubqueryNotExists, fCorrelated);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithNestedCmpSubquery
//
//	@doc:
//		Generate randomized Select expression with nested comparisons
//		involving subqueries
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithNestedCmpSubquery
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	CExpression *pexprSelectWithSubquery = PexprSelectWithAggSubquery(pmp, fCorrelated);

	CExpression *pexprLogical = (*pexprSelectWithSubquery)[0];
	CExpression *pexprSubqueryPred = (*pexprSelectWithSubquery)[1];

	// generate a parent equality predicate
	pexprSubqueryPred->AddRef();
	CExpression *pexprPredicate1 =
		CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarConstBool(pmp, true /*fVal*/), pexprSubqueryPred);

	// add another nesting level
	CExpression *pexprPredicate =
		CUtils::PexprScalarEqCmp(pmp, CUtils::PexprScalarConstBool(pmp, true /*fVal*/), pexprPredicate1);

	pexprLogical->AddRef();
	pexprSelectWithSubquery->Release();

	return CUtils::PexprLogicalSelect(pmp, pexprLogical, pexprPredicate);
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithCmpSubqueries
//
//	@doc:
//		Generate randomized Select expression with comparison between
//		two subqueries
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithCmpSubqueries
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	GPOS_ASSERT(NULL != pmp);
	CExpression *pexprOuter = NULL;
	CExpression *pexprInner = NULL;
	GenerateGetExpressions(pmp, &pexprOuter, &pexprInner);

	// generate a scalar subquery
	CExpression *pexprScalarSubquery1 = PexprSubqueryAgg(pmp, pexprOuter, pexprInner, fCorrelated);

	// generate get expression
	CWStringConst strNameT(GPOS_WSZ_LIT("Rel3"));

	CMDIdGPDB *pmdidT = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_TEST_REL_OID3, 1, 1);
	CTableDescriptor *ptabdescT = CTestUtils::PtabdescCreate(pmp, 3 /*ulCols*/, pmdidT, CName(&strNameT));
	CExpression *pexprT = CTestUtils::PexprLogicalGet(pmp, ptabdescT, &strNameT);

	// generate another scalar subquery
	CExpression *pexprScalarSubquery2 = PexprSubqueryAgg(pmp, pexprOuter, pexprT, fCorrelated);

	// generate equality predicate between both subqueries
	CExpression *pexprPredicate =
		CUtils::PexprScalarEqCmp(pmp, pexprScalarSubquery1, pexprScalarSubquery2);

	return CUtils::PexprLogicalSelect(pmp, pexprOuter, pexprPredicate);
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithNestedSubquery
//
//	@doc:
//		Generate randomized Select expression with nested subquery predicate
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithNestedSubquery
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	CExpression *pexprInner = PexprSelectWithAggSubquery(pmp, fCorrelated);
	CColRef *pcrInner = CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->PcrsOutput()->PcrAny();

	CExpression *pexprOuter = CTestUtils::PexprLogicalGet(pmp);
	CColRef *pcrOuter = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput()->PcrAny();

	CExpression *pexprSubq = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarSubquery(pmp, pcrInner, false /*fGeneratedByExist*/, false /*fGeneratedByQuantified*/), pexprInner);

	CExpression *pexprPred = CUtils::PexprScalarEqCmp(pmp, pcrOuter, pexprSubq);

	return CUtils::PexprLogicalSelect(pmp, pexprOuter, pexprPred);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithNestedQuantifiedSubqueries
//
//	@doc:
//		Generate a random select expression with nested quantified subqueries
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithNestedQuantifiedSubqueries
	(
	IMemoryPool *pmp,
	COperator::EOperatorId eopid,
	BOOL fCorrelated
	)
{
	GPOS_ASSERT(COperator::EopScalarSubqueryAny == eopid || COperator::EopScalarSubqueryAll == eopid);

	CWStringConst strName1(GPOS_WSZ_LIT("Rel1"));
	CWStringConst strAlias1(GPOS_WSZ_LIT("Rel1Alias"));
	CExpression *pexprOuter1 = CTestUtils::PexprLogicalGetNullable(pmp, GPOPT_TEST_REL_OID1, &strName1, &strAlias1);

	CWStringConst strName2(GPOS_WSZ_LIT("Rel2"));
	CWStringConst strAlias2(GPOS_WSZ_LIT("Rel2Alias"));
	CExpression *pexprOuter2 = CTestUtils::PexprLogicalGetNullable(pmp, GPOPT_TEST_REL_OID2, &strName2, &strAlias2);

	CWStringConst strName3(GPOS_WSZ_LIT("Rel3"));
	CWStringConst strAlias3(GPOS_WSZ_LIT("Rel3Alias"));
	CExpression *pexprOuter3 = CTestUtils::PexprLogicalGetNullable(pmp, GPOPT_TEST_REL_OID3, &strName3, &strAlias3);

	CWStringConst strName4(GPOS_WSZ_LIT("Rel4"));
	CWStringConst strAlias4(GPOS_WSZ_LIT("Rel4Alias"));
	CExpression *pexprInner = CTestUtils::PexprLogicalGetNullable(pmp, GPOPT_TEST_REL_OID4, &strName4, &strAlias4);

	CExpression *pexprSubqueryQuantified1 = PexprSubqueryQuantified(pmp, eopid, pexprOuter3, pexprInner, fCorrelated);
	CExpression *pexprSelect1 = CUtils::PexprLogicalSelect(pmp, pexprOuter3, pexprSubqueryQuantified1);
	CExpression *pexprSubqueryQuantified2 = PexprSubqueryQuantified(pmp, eopid, pexprOuter2, pexprSelect1, fCorrelated);
	CExpression *pexprSelect2 = CUtils::PexprLogicalSelect(pmp, pexprOuter2, pexprSubqueryQuantified2);
	CExpression *pexprSubqueryQuantified3 = PexprSubqueryQuantified(pmp, eopid, pexprOuter1, pexprSelect2, fCorrelated);

	return CUtils::PexprLogicalSelect(pmp, pexprOuter1, pexprSubqueryQuantified3);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithNestedAnySubqueries
//
//	@doc:
//		Generate a random select expression with nested Any subqueries
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithNestedAnySubqueries
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	return PexprSelectWithNestedQuantifiedSubqueries(pmp, COperator::EopScalarSubqueryAny, fCorrelated);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithNestedAllSubqueries
//
//	@doc:
//		Generate a random select expression with nested All subqueries
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithNestedAllSubqueries
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	return PexprSelectWithNestedQuantifiedSubqueries(pmp, COperator::EopScalarSubqueryAll, fCorrelated);
}



//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWith2LevelsCorrSubquery
//
//	@doc:
//		Generate randomized select expression with 2-levels correlated subqueries
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWith2LevelsCorrSubquery
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	CExpression *pexpr = PexprSelectWithNestedSubquery(pmp, fCorrelated);
	if (fCorrelated)
	{
		// add a 2-level correlation
		CExpression *pexprOuterSubq = (*(*pexpr)[1])[1];
		CExpression *pexprInnerSubq = (*(*(*pexprOuterSubq)[0])[1])[1];
		CExpression *pexprInnerSelect = (*(*pexprInnerSubq)[0])[0];
		DrgPexpr *pdrgpexpr = (*pexprInnerSelect)[1]->PdrgPexpr();

		CColRef *pcrOuter = CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->PcrsOutput()->PcrAny();
		CColRef *pcrInner = CDrvdPropRelational::Pdprel(pexprInnerSelect->PdpDerive())->PcrsOutput()->PcrAny();
		CExpression *pexprPred = CUtils::PexprScalarEqCmp(pmp, pcrOuter, pcrInner);
		pdrgpexpr->Append(pexprPred);
	}

	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithSubqueryConjuncts
//
//	@doc:
//		Generate randomized select expression with subquery predicates in
//		an AND tree
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithSubqueryConjuncts
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	GPOS_ASSERT(NULL != pmp);

	CExpression *pexprOuter = NULL;
	CExpression *pexprInner = NULL;
	GenerateGetExpressions(pmp, &pexprOuter, &pexprInner);

	return PexprSelectWithSubqueryBoolOp(pmp, pexprOuter, pexprInner, fCorrelated, CScalarBoolOp::EboolopAnd);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprProjectWithSubqueries
//
//	@doc:
//		Generate randomized project expression with multiple subqueries
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprProjectWithSubqueries
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	GPOS_ASSERT(NULL != pmp);

	CExpression *pexprOuter = NULL;
	CExpression *pexprInner = NULL;
	GenerateGetExpressions(pmp, &pexprOuter, &pexprInner);

	return PexprProjectWithSubqueries(pmp, pexprOuter, pexprInner, fCorrelated);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSubquery
//
//	@doc:
//		Helper for generating a randomized Select expression with correlated
//		predicates to be used for building subquery examples:
//
//			SELECT inner_column
//			FROM inner_expression
//			WHERE inner_column = 5 [AND outer_column = inner_column]
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSubquery
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	BOOL fCorrelated // add a predicate to inner expression correlated with outer expression?
	)
{
	GPOS_ASSERT(NULL != pexprOuter);
	GPOS_ASSERT(NULL != pexprInner);

	// get a random column from inner expression
	CColRef *pcrInner = CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->PcrsOutput()->PcrAny();

	// generate a non-correlated predicate to be added to inner expression
	CExpression *pexprNonCorrelated = CUtils::PexprScalarEqCmp(pmp, pcrInner, CUtils::PexprScalarConstInt4(pmp, 5 /*iVal*/));

	// predicate for the inner expression
	CExpression *pexprPred = NULL;
	if (fCorrelated)
	{
		// get a random column from outer expression
		CColRef *pcrOuter = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput()->PcrAny();

		// generate correlated predicate
		CExpression *pexprCorrelated = CUtils::PexprScalarEqCmp(pmp, pcrOuter, pcrInner);

		// generate AND expression of correlated and non-correlated predicates
		DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
		pdrgpexpr->Append(pexprCorrelated);
		pdrgpexpr->Append(pexprNonCorrelated);
		pexprPred = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopAnd, pdrgpexpr);
	}
	else
	{
		pexprPred = pexprNonCorrelated;
	}

	// generate a select on top of inner expression
	return CUtils::PexprLogicalSelect(pmp, pexprInner, pexprPred);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSubqueryQuantified
//
//	@doc:
//		Generate a quantified subquery expression
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSubqueryQuantified
	(
	IMemoryPool *pmp,
	COperator::EOperatorId eopid,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	BOOL fCorrelated // add a predicate to inner expression correlated with outer expression?
	)
{
	GPOS_ASSERT(COperator::EopScalarSubqueryAny == eopid ||
				COperator::EopScalarSubqueryAll == eopid);
	GPOS_ASSERT(NULL != pexprOuter);
	GPOS_ASSERT(NULL != pexprInner);

	CExpression *pexprSelect = PexprSubquery(pmp, pexprOuter, pexprInner, fCorrelated);

	// get random columns from inner expression
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->PcrsOutput();
	const CColRef *pcrInner = pcrs->PcrAny();

	// get random columns from outer expression
	pcrs = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput();
	const CColRef *pcrOuter = pcrs->PcrAny();

	// return a quantified subquery expression
	if (COperator::EopScalarSubqueryAny == eopid)
	{
		const CWStringConst *pstr = GPOS_NEW(pmp) CWStringConst(GPOS_WSZ_LIT("="));
		return GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CScalarSubqueryAny(pmp, GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_EQ_OP), pstr, pcrInner),
			pexprSelect,
			CUtils::PexprScalarIdent(pmp, pcrOuter)
			);
	}

	const CWStringConst *pstr = GPOS_NEW(pmp) CWStringConst(GPOS_WSZ_LIT("<>"));
	return GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CScalarSubqueryAll(pmp, GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_NEQ_OP), pstr, pcrInner),
			pexprSelect,
			CUtils::PexprScalarIdent(pmp, pcrOuter)
			);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprUndecorrelatableSubquery
//
//	@doc:
//		Generate an expression with undecorrelatable quantified subquery
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprUndecorrelatableSubquery
	(
	IMemoryPool *pmp,
	COperator::EOperatorId eopid,
	BOOL fCorrelated
	)
{
	GPOS_ASSERT(COperator::EopScalarSubqueryAny == eopid ||
				COperator::EopScalarSubqueryAll == eopid ||
				COperator::EopScalarSubqueryExists == eopid ||
				COperator::EopScalarSubqueryNotExists == eopid);

	CWStringConst strNameR(GPOS_WSZ_LIT("Rel1"));
	CMDIdGPDB *pmdidR = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_TEST_REL_OID1, 1 /*ulVersionMajor*/, 1 /*ulVersionMinor*/);
	CTableDescriptor *ptabdescR = CTestUtils::PtabdescPlain(pmp, 3 /*ulCols*/, pmdidR, CName(&strNameR));
	CExpression *pexprOuter = CTestUtils::PexprLogicalGet(pmp, ptabdescR, &strNameR);

	// generate quantified subquery predicate
	CExpression *pexprInner = CTestUtils::PexprLogicalGet(pmp);
	CExpression *pexprSubquery = NULL;
	switch (eopid)
	{
		case COperator::EopScalarSubqueryAny:
		case COperator::EopScalarSubqueryAll:
			pexprSubquery = PexprSubqueryQuantified(pmp, eopid, pexprOuter, pexprInner, fCorrelated);
			break;

		case COperator::EopScalarSubqueryExists:
		case COperator::EopScalarSubqueryNotExists:
			pexprSubquery = PexprSubqueryExistential(pmp, eopid, pexprOuter, pexprInner, fCorrelated);
			break;

		default:
			GPOS_ASSERT(!"Invalid subquery type");
	}

	// generate a regular predicate
	CColRef *pcrOuter = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput()->PcrAny();
	CExpression *pexprPred = CUtils::PexprScalarEqCmp(pmp, pcrOuter, CUtils::PexprScalarConstInt4(pmp, 5 /*iVal*/));

	// generate OR expression of  predicates
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexpr->Append(pexprSubquery);
	pdrgpexpr->Append(pexprPred);

	return CUtils::PexprLogicalSelect(pmp, pexprOuter, CPredicateUtils::PexprDisjunction(pmp, pdrgpexpr));
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprUndecorrelatableAnySubquery
//
//	@doc:
//		Generate an expression with undecorrelatable ANY subquery
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprUndecorrelatableAnySubquery
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	return PexprUndecorrelatableSubquery(pmp, COperator::EopScalarSubqueryAny, fCorrelated);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprUndecorrelatableAllSubquery
//
//	@doc:
//		Generate an expression with undecorrelatable ALL subquery
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprUndecorrelatableAllSubquery
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	return PexprUndecorrelatableSubquery(pmp, COperator::EopScalarSubqueryAll, fCorrelated);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprUndecorrelatableExistsSubquery
//
//	@doc:
//		Generate an expression with undecorrelatable Exists subquery
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprUndecorrelatableExistsSubquery
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	return PexprUndecorrelatableSubquery(pmp, COperator::EopScalarSubqueryExists, fCorrelated);
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprUndecorrelatableNotExistsSubquery
//
//	@doc:
//		Generate an expression with undecorrelatable Not Exists subquery
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprUndecorrelatableNotExistsSubquery
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	return PexprUndecorrelatableSubquery(pmp, COperator::EopScalarSubqueryNotExists, fCorrelated);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprUndecorrelatableScalarSubquery
//
//	@doc:
//		Generate an expression with undecorrelatable Scalar subquery
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprUndecorrelatableScalarSubquery
	(
	IMemoryPool *pmp,
	BOOL fCorrelated
	)
{
	CExpression *pexprOuter = CTestUtils::PexprLogicalGet(pmp);
	CExpression *pexprInner = CTestUtils::PexprLogicalGet(pmp);
	CExpression *pexprSelect = PexprSubquery(pmp, pexprOuter, pexprInner, fCorrelated);

	// get a random column from inner expression
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->PcrsOutput();
	CColRef *pcrInner =  pcrs->PcrAny();

	CExpression *pexprSubquery = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarSubquery(pmp, pcrInner, false /*fGeneratedByExist*/, false /*fGeneratedByQuantified*/), pexprSelect);

	CColRef *pcrOuter = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput()->PcrAny();
	CExpression *pexprPred = CUtils::PexprScalarEqCmp(pmp, pcrOuter, pexprSubquery);

	return CUtils::PexprLogicalSelect(pmp, pexprOuter, pexprPred);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSubqueryExistential
//
//	@doc:
//		Generate an EXISTS/NOT EXISTS subquery expression
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSubqueryExistential
	(
	IMemoryPool *pmp,
	COperator::EOperatorId eopid,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	BOOL fCorrelated // add a predicate to inner expression correlated with outer expression?
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(COperator::EopScalarSubqueryExists == eopid ||
				COperator::EopScalarSubqueryNotExists == eopid);
	GPOS_ASSERT(NULL != pexprOuter);
	GPOS_ASSERT(NULL != pexprInner);

	CExpression *pexprSelect = PexprSubquery(pmp, pexprOuter, pexprInner, fCorrelated);

	// return a quantified subquery expression
	if (COperator::EopScalarSubqueryExists == eopid)
	{
		return GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarSubqueryExists(pmp), pexprSelect);
	}

	return GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarSubqueryNotExists(pmp), pexprSelect);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSubqueryAgg
//
//	@doc:
//		Generate a randomized ScalarSubquery aggregate expression for
//		the following query:
//
//			SELECT sum(inner_column)
//			FROM inner_expression
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSubqueryAgg
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	BOOL fCorrelated // add a predicate to inner expression correlated with outer expression?
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexprOuter);
	GPOS_ASSERT(NULL != pexprInner);

	CExpression *pexprSelect = PexprSubquery(pmp, pexprOuter, pexprInner, fCorrelated);

	// get a random column from inner expression
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->PcrsOutput();
	CColRef *pcrInner =  pcrs->PcrAny();

	// generate a SUM expression
	CExpression *pexprProjElem = CTestUtils::PexprPrjElemWithSum(pmp, pcrInner);
	CColRef *pcrComputed = CScalarProjectElement::PopConvert(pexprProjElem->Pop())->Pcr();

	// add SUM expression to a project list
	CExpression *pexprProjList = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp), pexprProjElem);

	// generate empty grouping columns list
	DrgPcr *pdrgpcr = GPOS_NEW(pmp) DrgPcr(pmp);

	// generate a group by on top of select expression
	CExpression *pexprLogicalGbAgg = CUtils::PexprLogicalGbAggGlobal(pmp, pdrgpcr, pexprSelect, pexprProjList);

	// return a subquery expression on top of group by
	return GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarSubquery(pmp, pcrComputed, false /*fGeneratedByExist*/, false /*fGeneratedByQuantified*/), pexprLogicalGbAgg);
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithSubqueryBoolOp
//
//	@doc:
//		Generate a Select expression with a BoolOp (AND/OR) predicate tree involving
//		subqueries
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithSubqueryBoolOp
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	BOOL fCorrelated,
	CScalarBoolOp::EBoolOperator eboolop
	)
{
	GPOS_ASSERT(NULL != pexprOuter);
	GPOS_ASSERT(NULL != pexprInner);

	GPOS_ASSERT(CScalarBoolOp::EboolopAnd == eboolop || CScalarBoolOp::EboolopOr == eboolop);

	// get any two columns
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput();
	CColRef *pcrLeft = pcrs->PcrAny();

	// generate agg subquery
	CExpression *pexprAggSubquery = PexprSubqueryAgg(pmp, pexprOuter, pexprInner, fCorrelated);

	// generate equality predicate involving a subquery
	CExpression *pexprPred1 = CUtils::PexprScalarEqCmp(pmp, pcrLeft, pexprAggSubquery);

	// generate a regular predicate
	CExpression *pexprPred2 = CUtils::PexprScalarEqCmp(pmp, pcrLeft, CUtils::PexprScalarConstInt4(pmp, 5 /*iVal*/));

	// generate ALL subquery
	CExpression *pexprGet = CTestUtils::PexprLogicalGet(pmp);
	CExpression *pexprSubqueryAll = PexprSubqueryQuantified(pmp, COperator::EopScalarSubqueryAll, pexprOuter, pexprGet, fCorrelated);

	// generate EXISTS subquery
	CExpression *pexprGet2 = CTestUtils::PexprLogicalGet(pmp);
	CExpression *pexprSubqueryExists = PexprSubqueryExistential(pmp, COperator::EopScalarSubqueryExists, pexprOuter, pexprGet2, fCorrelated);

	// generate AND expression of all predicates
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexpr->Append(pexprPred1);
	pdrgpexpr->Append(pexprPred2);
	pdrgpexpr->Append(pexprSubqueryExists);
	pdrgpexpr->Append(pexprSubqueryAll);

	CExpression *pexprPred = CUtils::PexprScalarBoolOp(pmp, eboolop, pdrgpexpr);

	return CUtils::PexprLogicalSelect(pmp, pexprOuter, pexprPred);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprProjectWithSubqueries
//
//	@doc:
//		Generate a Project expression with multiple subqueries in project list
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprProjectWithSubqueries
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	BOOL fCorrelated
	)
{
	GPOS_ASSERT(NULL != pexprOuter);
	GPOS_ASSERT(NULL != pexprInner);

	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	// generate an array of project elements holding subquery expressions
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);

	CColRef *pcrComputed = NULL;
	CExpression *pexprPrjElem = NULL;
	CExpression *pexprGet = NULL;

	const IMDTypeBool *pmdtypebool = pmda->PtMDType<IMDTypeBool>();

	// generate agg subquery
	CExpression *pexprAggSubquery = PexprSubqueryAgg(pmp, pexprOuter, pexprInner, fCorrelated);
	const CColRef *pcr = CScalarSubquery::PopConvert(pexprAggSubquery->Pop())->Pcr();
	pcrComputed = pcf->PcrCreate(pcr->Pmdtype(), pcr->ITypeModifier(), pcr->OidCollation());
	pexprPrjElem = CUtils::PexprScalarProjectElement(pmp, pcrComputed, pexprAggSubquery);
	pdrgpexpr->Append(pexprPrjElem);

	// generate ALL subquery
	pexprGet = CTestUtils::PexprLogicalGet(pmp);
	CExpression *pexprSubqueryAll = PexprSubqueryQuantified(pmp, COperator::EopScalarSubqueryAll, pexprOuter, pexprGet, fCorrelated);
	pcrComputed = pcf->PcrCreate(pmdtypebool, IDefaultTypeModifier, OidInvalidCollation);
	pexprPrjElem = CUtils::PexprScalarProjectElement(pmp, pcrComputed, pexprSubqueryAll);
	pdrgpexpr->Append(pexprPrjElem);

	// generate existential subquery
	pexprGet =CTestUtils::PexprLogicalGet(pmp);
	CExpression *pexprSubqueryExists = PexprSubqueryExistential(pmp, COperator::EopScalarSubqueryExists, pexprOuter, pexprGet, fCorrelated);
	pcrComputed = pcf->PcrCreate(pmdtypebool, IDefaultTypeModifier, OidInvalidCollation);
	pexprPrjElem = CUtils::PexprScalarProjectElement(pmp, pcrComputed, pexprSubqueryExists);
	pdrgpexpr->Append(pexprPrjElem);

	CExpression *pexprPrjList = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp), pdrgpexpr);

	return CUtils::PexprLogicalProject(pmp, pexprOuter, pexprPrjList, false /*fNewComputedCol*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithSubqueryQuantified
//
//	@doc:
//		Generate randomized Select expression with quantified subquery
//		predicate
//
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithSubqueryQuantified
	(
	IMemoryPool *pmp,
	COperator::EOperatorId eopid,
	BOOL fCorrelated
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(COperator::EopScalarSubqueryAny == eopid || COperator::EopScalarSubqueryAll == eopid);

	CExpression *pexprOuter = NULL;
	CExpression *pexprInner = NULL;
	GenerateGetExpressions(pmp, &pexprOuter, &pexprInner);
	CExpression *pexprSubqueryQuantified = PexprSubqueryQuantified(pmp, eopid, pexprOuter, pexprInner, fCorrelated);

	return CUtils::PexprLogicalSelect(pmp, pexprOuter, pexprSubqueryQuantified);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithQuantifiedAggSubquery
//
//	@doc:
//		Generate randomized Select expression with quantified subquery
//		whose inner expression is an aggregate
//
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithQuantifiedAggSubquery
	(
	IMemoryPool *pmp,
	COperator::EOperatorId eopid,
	BOOL fCorrelated
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(COperator::EopScalarSubqueryAny == eopid || COperator::EopScalarSubqueryAll == eopid);

	CExpression *pexprOuter = CTestUtils::PexprLogicalGet(pmp);
	CExpression *pexprSubq = PexprSubqueryAgg(pmp, pexprOuter, CTestUtils::PexprLogicalGet(pmp), fCorrelated);
	CExpression *pexprGb = (*pexprSubq)[0];
	pexprGb->AddRef();
	pexprSubq->Release();

	CColRef *pcrInner = CDrvdPropRelational::Pdprel(pexprGb->PdpDerive())->PcrsOutput()->PcrAny();
	CColRef *pcrOuter = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput()->PcrAny();
	CExpression *pexprSubqueryQuantified = NULL;
	if (COperator::EopScalarSubqueryAny == eopid)
	{
		const CWStringConst *pstr = GPOS_NEW(pmp) CWStringConst(GPOS_WSZ_LIT("="));
		pexprSubqueryQuantified = GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CScalarSubqueryAny(pmp, GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_EQ_OP), pstr, pcrInner),
			pexprGb,
			CUtils::PexprScalarIdent(pmp, pcrOuter)
			);
	}

	const CWStringConst *pstr = GPOS_NEW(pmp) CWStringConst(GPOS_WSZ_LIT("<>"));
	pexprSubqueryQuantified = GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CScalarSubqueryAll(pmp, GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_NEQ_OP), pstr, pcrInner),
			pexprGb,
			CUtils::PexprScalarIdent(pmp, pcrOuter)
			);


	return CUtils::PexprLogicalSelect(pmp, pexprOuter, pexprSubqueryQuantified);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprProjectWithSubqueryQuantified
//
//	@doc:
//		Generate a randomized Project expression with a quantified subquery
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprProjectWithSubqueryQuantified
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	COperator::EOperatorId eopid,
	BOOL fCorrelated
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(COperator::EopScalarSubqueryAny == eopid || COperator::EopScalarSubqueryAll == eopid);

	CExpression *pexprSubqueryQuantified = PexprSubqueryQuantified(pmp, eopid, pexprOuter, pexprInner, fCorrelated);

	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	// generate a computed column
	CScalarSubqueryQuantified *pop = CScalarSubqueryQuantified::PopConvert(pexprSubqueryQuantified->Pop());
	const IMDType *pmdtype = pmda->Pmdtype(pop->PmdidType());
	CColRef *pcrComputed = pcf->PcrCreate(pmdtype, pop->ITypeModifier(), OidInvalidCollation);

	// generate a scalar project list
	CExpression *pexprPrjElem =  CUtils::PexprScalarProjectElement(pmp, pcrComputed, pexprSubqueryQuantified);
	CExpression *pexprPrjList = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp), pexprPrjElem);

	return CUtils::PexprLogicalProject(pmp, pexprOuter, pexprPrjList, false /*fNewComputedCol*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithSubqueryExistential
//
//	@doc:
//		Generate randomized Select expression with existential subquery
//		predicate
//
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithSubqueryExistential
	(
	IMemoryPool *pmp,
	COperator::EOperatorId eopid,
	BOOL fCorrelated
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(COperator::EopScalarSubqueryExists == eopid || COperator::EopScalarSubqueryNotExists == eopid);

	CExpression *pexprOuter = NULL;
	CExpression *pexprInner = NULL;
	GenerateGetExpressions(pmp, &pexprOuter, &pexprInner);
	CExpression *pexprSubqueryExistential = PexprSubqueryExistential(pmp, eopid, pexprOuter, pexprInner, fCorrelated);

	return CUtils::PexprLogicalSelect(pmp, pexprOuter, pexprSubqueryExistential);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSelectWithTrimmableExistentialSubquery
//
//	@doc:
//		Generate randomized Select expression with existential subquery
//		predicate that can be trimmed
//
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSelectWithTrimmableExistentialSubquery
	(
	IMemoryPool *pmp,
	COperator::EOperatorId eopid,
	BOOL // fCorrelated
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(COperator::EopScalarSubqueryExists == eopid || COperator::EopScalarSubqueryNotExists == eopid);

	CExpression *pexprOuter = CTestUtils::PexprLogicalGet(pmp);
	CExpression *pexprInner = CTestUtils::PexprLogicalGbAggWithSum(pmp);

	// remove grouping columns
	(*pexprInner)[0]->AddRef();
	(*pexprInner)[1]->AddRef();
	CExpression *pexprGbAgg =
		CUtils::PexprLogicalGbAggGlobal(pmp, GPOS_NEW(pmp) DrgPcr(pmp), (*pexprInner)[0], (*pexprInner)[1]);
	pexprInner->Release();

	// create existential subquery
	CExpression *pexprSubqueryExistential = NULL;
	if (COperator::EopScalarSubqueryExists == eopid)
	{
		pexprSubqueryExistential = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarSubqueryExists(pmp), pexprGbAgg);
	}
	else
	{
		pexprSubqueryExistential = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarSubqueryNotExists(pmp), pexprGbAgg);
	}

	// generate a regular predicate
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput();
	CExpression *pexprEqPred = CUtils::PexprScalarEqCmp(pmp, pcrs->PcrAny(), CUtils::PexprScalarConstInt4(pmp, 5 /*iVal*/));

	CExpression *pexprConjunction =
		CPredicateUtils::PexprConjunction(pmp, pexprSubqueryExistential, pexprEqPred);
	pexprEqPred->Release();
	pexprSubqueryExistential->Release();

	return CUtils::PexprLogicalSelect(pmp, pexprOuter, pexprConjunction);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprProjectWithSubqueryExistential
//
//	@doc:
//		Generate randomized Project expression with existential subquery
//
//
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprProjectWithSubqueryExistential
	(
	IMemoryPool *pmp,
	COperator::EOperatorId eopid,
	BOOL fCorrelated
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(COperator::EopScalarSubqueryExists == eopid || COperator::EopScalarSubqueryNotExists == eopid);

	CExpression *pexprOuter = NULL;
	CExpression *pexprInner = NULL;
	GenerateGetExpressions(pmp, &pexprOuter, &pexprInner);
	CExpression *pexprSubqueryExistential = PexprSubqueryExistential(pmp, eopid, pexprOuter, pexprInner, fCorrelated);

	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	// generate a computed column
	CScalarSubqueryExistential *pop = CScalarSubqueryExistential::PopConvert(pexprSubqueryExistential->Pop());
	const IMDType *pmdtype = pmda->Pmdtype(pop->PmdidType());
	CColRef *pcrComputed = pcf->PcrCreate(pmdtype, pop->ITypeModifier(), OidInvalidCollation);

	// generate a scalar project list
	CExpression *pexprPrjElem = CUtils::PexprScalarProjectElement(pmp, pcrComputed, pexprSubqueryExistential);
	CExpression *pexprPrjList = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp), pexprPrjElem);

	return CUtils::PexprLogicalProject(pmp, pexprOuter, pexprPrjList, false /*fNewComputedCol*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSubqueryWithConstTableGet
//
//	@doc:
//		Generate Select expression with Any subquery predicate over a const
//		table get
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSubqueryWithConstTableGet
	(
	IMemoryPool *pmp,
	COperator::EOperatorId eopid
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(COperator::EopScalarSubqueryAny == eopid ||
				COperator::EopScalarSubqueryAll == eopid);

	CWStringConst strNameR(GPOS_WSZ_LIT("Rel1"));
	CMDIdGPDB *pmdidR = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_TEST_REL_OID1, 1, 1);
	CTableDescriptor *ptabdescR = CTestUtils::PtabdescCreate(pmp, 3 /*ulCols*/, pmdidR, CName(&strNameR));

	CExpression *pexprOuter = CTestUtils::PexprLogicalGet(pmp, ptabdescR, &strNameR);
	CExpression *pexprConstTableGet = CTestUtils::PexprConstTableGet(pmp, 3 /* ulElements */);

	// get random columns from inner expression
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprConstTableGet->PdpDerive())->PcrsOutput();
	const CColRef *pcrInner = pcrs->PcrAny();

	// get random columns from outer expression
	pcrs = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput();
	const CColRef *pcrOuter = pcrs->PcrAny();

	const CWStringConst *pstr = GPOS_NEW(pmp) CWStringConst(GPOS_WSZ_LIT("="));

	CExpression *pexprSubquery = NULL;
	if (COperator::EopScalarSubqueryAny == eopid)
	{
		// construct ANY subquery expression
		pexprSubquery = GPOS_NEW(pmp) CExpression
									(
									pmp,
									GPOS_NEW(pmp) CScalarSubqueryAny(pmp, GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_EQ_OP), pstr, pcrInner),
									pexprConstTableGet,
									CUtils::PexprScalarIdent(pmp, pcrOuter)
									);
	}
	else
	{
		// construct ALL subquery expression
		pexprSubquery = GPOS_NEW(pmp) CExpression
									(
									pmp,
									GPOS_NEW(pmp) CScalarSubqueryAll(pmp, GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_EQ_OP), pstr, pcrInner),
									pexprConstTableGet,
									CUtils::PexprScalarIdent(pmp, pcrOuter)
									);

	}

	return CUtils::PexprLogicalSelect(pmp, pexprOuter, pexprSubquery);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryTestUtils::PexprSubqueryWithDisjunction
//
//	@doc:
//		Generate Select expression with a disjunction of two Any subqueries over
//		const table get
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryTestUtils::PexprSubqueryWithDisjunction
	(
	IMemoryPool *pmp
	)
{
	GPOS_ASSERT(NULL != pmp);

	CWStringConst strNameR(GPOS_WSZ_LIT("Rel1"));
	CMDIdGPDB *pmdidR = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_TEST_REL_OID1, 1, 1);
	CTableDescriptor *ptabdescR = CTestUtils::PtabdescCreate(pmp, 3 /*ulCols*/, pmdidR, CName(&strNameR));

	CExpression *pexprOuter = CTestUtils::PexprLogicalGet(pmp, ptabdescR, &strNameR);
	CExpression *pexprConstTableGet = CTestUtils::PexprConstTableGet(pmp, 3 /* ulElements */);

	// get random columns from inner expression
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprConstTableGet->PdpDerive())->PcrsOutput();
	const CColRef *pcrInner = pcrs->PcrAny();

	// get random columns from outer expression
	pcrs = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput();
	const CColRef *pcrOuter = pcrs->PcrAny();

	const CWStringConst *pstr = GPOS_NEW(pmp) CWStringConst(GPOS_WSZ_LIT("="));

	CExpression *pexprSubquery = GPOS_NEW(pmp) CExpression
									(
									pmp,
									GPOS_NEW(pmp) CScalarSubqueryAny(pmp, GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_EQ_OP), pstr, pcrInner),
									pexprConstTableGet,
									CUtils::PexprScalarIdent(pmp, pcrOuter)
									);
	pexprSubquery->AddRef();

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexpr->Append(pexprSubquery);
	pdrgpexpr->Append(pexprSubquery);

	// generate a disjunction of the subquery with itself
	CExpression *pexprBoolOp = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopOr, pdrgpexpr);

	return CUtils::PexprLogicalSelect(pmp, pexprOuter, pexprBoolOp);
}

// EOF
