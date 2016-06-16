//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformExpandNAryJoin.cpp
//
//	@doc:
//		Implementation of n-ary join expansion
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/xforms/CXformExpandNAryJoin.h"
#include "gpopt/xforms/CJoinOrder.h"
#include "gpopt/xforms/CXformUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoin::CXformExpandNAryJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformExpandNAryJoin::CXformExpandNAryJoin
	(
	IMemoryPool *pmp
	)
	:
	CXformExploration
		(
		 // pattern
		GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalNAryJoin(pmp),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternMultiLeaf(pmp)),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))
					)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformExpandNAryJoin::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	if (exprhdl.Pdpscalar(exprhdl.UlArity() - 1)->FHasSubquery())
	{
		// subqueries must be unnested before applying xform
		return CXform::ExfpNone;
	}
#ifdef GPOS_DEBUG
	CAutoMemoryPool amp;
	GPOS_ASSERT(!CXformUtils::FJoinPredOnSingleChild(amp.Pmp(), exprhdl) &&
			"join predicates are not pushed down");
#endif // GPOS_DEBUG

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoin::AddSpecifiedJoinOrder
//
//	@doc:
//		Expand NAry join in the specified order of inputs
//
//---------------------------------------------------------------------------
void
CXformExpandNAryJoin::AddSpecifiedJoinOrder
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CXformResult *pxfres
	)
{
	GPOS_ASSERT(COperator::EopLogicalNAryJoin == pexpr->Pop()->Eopid());

	const ULONG ulArity = pexpr->UlArity();
	if (4 > ulArity)
	{
		return;
	}

	// create a join order with same order of given relations
	(*pexpr)[0]->AddRef();
	(*pexpr)[1]->AddRef();
	CExpression *pexprJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(pmp, (*pexpr)[0], (*pexpr)[1], CPredicateUtils::PexprConjunction(pmp, NULL));
	for (ULONG ul = 2; ul < ulArity - 1; ul++)
	{
		(*pexpr)[ul]->AddRef();
		pexprJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(pmp, pexprJoin, (*pexpr)[ul], CPredicateUtils::PexprConjunction(pmp, NULL));
	}
	CExpression *pexprScalar = (*pexpr)[ulArity - 1];
	pexprScalar->AddRef();
	CExpression *pexprSelect = CUtils::PexprLogicalSelect(pmp, pexprJoin, pexprScalar);
	CExpression *pexprNormalized = CNormalizer::PexprNormalize(pmp, pexprSelect);
	pexprSelect->Release();
	pxfres->Add(pexprNormalized);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoin::Transform
//
//	@doc:
//		Actual transformation of n-ary join to cluster of inner joins
//
//---------------------------------------------------------------------------
void
CXformExpandNAryJoin::Transform
	(
	CXformContext *pxfctxt,
	CXformResult *pxfres,
	CExpression *pexpr
	) 
	const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	IMemoryPool *pmp = pxfctxt->Pmp();

	const ULONG ulArity = pexpr->UlArity();
	GPOS_ASSERT(ulArity >= 3);

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulArity - 1; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		pexprChild->AddRef();
		pdrgpexpr->Append(pexprChild);
	}

	CExpression *pexprScalar = (*pexpr)[ulArity - 1];

	DrgPexpr *pdrgpexprPreds = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprScalar);

	// create a join order based on query-specified order of joins
	CJoinOrder jo(pmp, pdrgpexpr, pdrgpexprPreds);
	CExpression *pexprResult = jo.PexprExpand();

	// normalize resulting expression
	CExpression *pexprNormalized = CNormalizer::PexprNormalize(pmp, pexprResult);
	pexprResult->Release();
	pxfres->Add(pexprNormalized);

	AddSpecifiedJoinOrder(pmp, pexpr, pxfres);
}

// EOF
