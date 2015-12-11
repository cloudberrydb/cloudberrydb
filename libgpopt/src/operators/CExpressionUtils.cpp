//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CExpressionUtils.cpp
//
//	@doc:
//		Utility routines for transforming expressions
//
//	@owner:
//		, 
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/CConstraintInterval.h"
#include "gpopt/exception.h"

#include "gpopt/operators/ops.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CExpressionUtils.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/statistics/CStatistics.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CExpressionUtils::UnnestChild
//
//	@doc:
//		Unnest a given expression's child and append unnested nodes to
//		the given expression array
//
//---------------------------------------------------------------------------
void CExpressionUtils::UnnestChild
	(
	IMemoryPool *pmp,
	CExpression *pexpr, // parent node
	ULONG ulChildIndex, // child index
	BOOL fAnd, // is expression an AND node?
	BOOL fOr, // is expression an OR node?
	BOOL fHasNegatedChild, // does expression have NOT child nodes?
	DrgPexpr *pdrgpexpr // array to append results to
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(ulChildIndex < pexpr->UlArity());
	GPOS_ASSERT(NULL != pdrgpexpr);

	CExpression *pexprChild = (*pexpr)[ulChildIndex];

	if ((fAnd && CPredicateUtils::FAnd(pexprChild)) ||
		(fOr && CPredicateUtils::FOr(pexprChild)))
	{
		// two cascaded AND nodes or two cascaded OR nodes, recursively
		// pull-up children
		AppendChildren(pmp, pexprChild, pdrgpexpr);

		return;
	}

	if (fHasNegatedChild &&
		CPredicateUtils::FNot(pexprChild) &&
		CPredicateUtils::FNot((*pexprChild)[0]))
	{
		// two cascaded Not nodes cancel each other
		CExpression *pexprNot = (*pexprChild)[0];
		pexprChild = (*pexprNot)[0];
	}
	CExpression *pexprUnnestedChild = PexprUnnest(pmp, pexprChild);
	pdrgpexpr->Append(pexprUnnestedChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionUtils::AppendChildren
//
//	@doc:
//		Append the unnested children of given expression to given array
//
//---------------------------------------------------------------------------
void CExpressionUtils::AppendChildren
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	DrgPexpr *pdrgpexpr
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pdrgpexpr);

	DrgPexpr *pdrgpexprChildren = PdrgpexprUnnestChildren(pmp, pexpr);
	CUtils::AddRefAppend<CExpression, CleanupRelease>(pdrgpexpr, pdrgpexprChildren);
	pdrgpexprChildren->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionUtils::PdrgpexprUnnestChildren
//
//	@doc:
//		Return an array of expression's children after unnesting nested
//		AND/OR/NOT subtrees
//
//---------------------------------------------------------------------------
DrgPexpr *
CExpressionUtils::PdrgpexprUnnestChildren
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	// compute flags for cases where we may have nested predicates
	BOOL fAnd = CPredicateUtils::FAnd(pexpr);
	BOOL fOr = CPredicateUtils::FOr(pexpr);
	BOOL fHasNegatedChild = CPredicateUtils::FHasNegatedChild(pexpr);

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	const ULONG ulArity = pexpr->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		UnnestChild(pmp, pexpr, ul, fAnd, fOr, fHasNegatedChild, pdrgpexpr);
	}

	return pdrgpexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionUtils::PexprUnnest
//
//	@doc:
//		Unnest AND/OR/NOT predicates
//
//---------------------------------------------------------------------------
CExpression *
CExpressionUtils::PexprUnnest
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	if (CPredicateUtils::FNot(pexpr))
	{
		CExpression *pexprChild = (*pexpr)[0];
		CExpression *pexprPushedNot = PexprPushNotOneLevel(pmp, pexprChild);

		COperator *pop = pexprPushedNot->Pop();
		DrgPexpr *pdrgpexpr = PdrgpexprUnnestChildren(pmp, pexprPushedNot);
		pop->AddRef();

		// clean up
		pexprPushedNot->Release();

		return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexpr);
	}

	COperator *pop = pexpr->Pop();
	DrgPexpr *pdrgpexpr = PdrgpexprUnnestChildren(pmp, pexpr);
	pop->AddRef();

	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionUtils::PexprPushNotOneLevel
//
//	@doc:
// 		Push not expression one level down the given expression. For example:
// 		1. AND of expressions into an OR a negation of these expression
// 		2. OR of expressions into an AND a negation of these expression
// 		3. EXISTS into NOT EXISTS and vice versa
//      4. Else, return NOT of given expression
//---------------------------------------------------------------------------
CExpression *
CExpressionUtils::PexprPushNotOneLevel
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	BOOL fAnd = CPredicateUtils::FAnd(pexpr);
	BOOL fOr = CPredicateUtils::FOr(pexpr);

	if (fAnd || fOr)
	{
		COperator *popNew = NULL;

		if (fOr)
		{
			popNew = GPOS_NEW(pmp) CScalarBoolOp(pmp, CScalarBoolOp::EboolopAnd);
		}
		else
		{
			popNew = GPOS_NEW(pmp) CScalarBoolOp(pmp, CScalarBoolOp::EboolopOr);
		}

		DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
		const ULONG ulArity = pexpr->UlArity();
		for (ULONG ul = 0; ul < ulArity; ul++)
		{
			CExpression *pexprChild = (*pexpr)[ul];
			pexprChild->AddRef();
			pdrgpexpr->Append(CUtils::PexprNegate(pmp, pexprChild));
		}

		return GPOS_NEW(pmp) CExpression(pmp, popNew, pdrgpexpr);
	}

	const COperator *pop = pexpr->Pop();
	if (COperator::EopScalarSubqueryExists == pop->Eopid())
	{
		pexpr->PdrgPexpr()->AddRef();
		return GPOS_NEW(pmp) CExpression
							(
							pmp,
							GPOS_NEW(pmp) CScalarSubqueryNotExists(pmp),
							pexpr->PdrgPexpr()
							);
	}

	if (COperator::EopScalarSubqueryNotExists == pop->Eopid())
	{
		pexpr->PdrgPexpr()->AddRef();
		return GPOS_NEW(pmp) CExpression
							(
							pmp,
							GPOS_NEW(pmp) CScalarSubqueryExists(pmp),
							pexpr->PdrgPexpr()
							);
	}

	// TODO: , Feb 4 2015, we currently only handling EXISTS/NOT EXISTS/AND/OR
	pexpr->AddRef();
	return GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CScalarBoolOp(pmp, CScalarBoolOp::EboolopNot),
					pexpr
					);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionUtils::PexprDedupChildren
//
//	@doc:
//		Remove duplicate AND/OR children
//
//---------------------------------------------------------------------------
CExpression *
CExpressionUtils::PexprDedupChildren
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	// recursively process children
	const ULONG ulArity = pexpr->UlArity();
	DrgPexpr *pdrgpexprChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = PexprDedupChildren(pmp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	if (CPredicateUtils::FAnd(pexpr) || CPredicateUtils::FOr(pexpr))
	{
		DrgPexpr *pdrgpexprNewChildren = CUtils::PdrgpexprDedup(pmp, pdrgpexprChildren);

		pdrgpexprChildren->Release();
		pdrgpexprChildren = pdrgpexprNewChildren;

		// Check if we end with one child, return that child
		if (1 == pdrgpexprChildren->UlLength())
		{
			CExpression *pexprChild = (*pdrgpexprChildren)[0];
			pexprChild->AddRef();
			pdrgpexprChildren->Release();

			return pexprChild;
		}
	}


	COperator *pop = pexpr->Pop();
	pop->AddRef();
	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexprChildren);
}

// EOF
