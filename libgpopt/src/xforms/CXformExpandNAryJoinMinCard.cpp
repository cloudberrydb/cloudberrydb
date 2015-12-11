//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformExpandNAryJoinMinCard.cpp
//
//	@doc:
//		Implementation of n-ary join expansion based on cardinality
//		of intermediate results
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/xforms/CXformExpandNAryJoinMinCard.h"
#include "gpopt/xforms/CJoinOrderMinCard.h"
#include "gpopt/xforms/CXformUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinMinCard::CXformExpandNAryJoinMinCard
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformExpandNAryJoinMinCard::CXformExpandNAryJoinMinCard
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
//		CXformExpandNAryJoinMinCard::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformExpandNAryJoinMinCard::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	return CXformUtils::ExfpExpandJoinOrder(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinMinCard::Transform
//
//	@doc:
//		Actual transformation of n-ary join to cluster of inner joins
//
//---------------------------------------------------------------------------
void
CXformExpandNAryJoinMinCard::Transform
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

	// create a join order based on cardinality of intermediate results
	CJoinOrderMinCard jomc(pmp, pdrgpexpr, pdrgpexprPreds);
	CExpression *pexprResult = jomc.PexprExpand();

	// normalize resulting expression
	CExpression *pexprNormalized = CNormalizer::PexprNormalize(pmp, pexprResult);
	pexprResult->Release();
	pxfres->Add(pexprNormalized);
}

// EOF
