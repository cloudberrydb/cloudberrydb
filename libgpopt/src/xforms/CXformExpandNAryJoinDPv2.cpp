//---------------------------------------------------------------------------
// Greenplum Database
// Copyright (C) 2019 Pivotal Inc.
//
//	@filename:
//		CXformExpandNAryJoinDPv2.cpp
//
//	@doc:
//		Implementation of n-ary join expansion using dynamic programming
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/engine/CHint.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/xforms/CXformExpandNAryJoinDPv2.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/xforms/CJoinOrderDPv2.h"



using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinDPv2::CXformExpandNAryJoinDPv2
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformExpandNAryJoinDPv2::CXformExpandNAryJoinDPv2
	(
	CMemoryPool *mp
	)
	:
	CXformExploration
		(
		 // pattern
		GPOS_NEW(mp) CExpression
					(
					mp,
					GPOS_NEW(mp) CLogicalNAryJoin(mp),
					GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternMultiLeaf(mp)),
					GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))
					)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinDPv2::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformExpandNAryJoinDPv2::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	COptimizerConfig *optimizer_config = COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();
	const CHint *phint = optimizer_config->GetHint();

	const ULONG arity = exprhdl.Arity();

	// since the last child of the join operator is a scalar child
	// defining the join predicate, ignore it.
	const ULONG ulRelChild = arity - 1;

	if (ulRelChild > phint->UlJoinOrderDPLimit())
	{
		return CXform::ExfpNone;
	}

	return CXformUtils::ExfpExpandJoinOrder(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinDPv2::Transform
//
//	@doc:
//		Actual transformation of n-ary join to cluster of inner joins using
//		dynamic programming
//
//---------------------------------------------------------------------------
void
CXformExpandNAryJoinDPv2::Transform
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

	CMemoryPool *mp = pxfctxt->Pmp();

	const ULONG arity = pexpr->Arity();
	GPOS_ASSERT(arity >= 3);

	// Make an expression array with all the one-way-join childs
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity - 1; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		pexprChild->AddRef();
		pdrgpexpr->Append(pexprChild);
	}

	// Make an expression array with all the join conditions, one entry for
	// every conjunct (ANDed condition)
	CExpression *pexprScalar = (*pexpr)[arity - 1];
	CExpressionArray *pdrgpexprPreds = CPredicateUtils::PdrgpexprConjuncts(mp, pexprScalar);

	// create join order using dynamic programming v2, record topk results in jodp
	CJoinOrderDPv2 jodp(mp, pdrgpexpr, pdrgpexprPreds);
	jodp.PexprExpand();

	// Retrieve top K join orders from jodp and add as alternatives
	const ULONG UlTopKJoinOrders = jodp.PdrgpexprTopK()->Size();
	for (ULONG ul = 0; ul < UlTopKJoinOrders; ul++)
	{
		CExpression *pexprJoinOrder = (*jodp.PdrgpexprTopK())[ul];
		CExpression *pexprNormalized = CNormalizer::PexprNormalize(mp, pexprJoinOrder);

		pxfres->Add(pexprNormalized);
	}
}

// EOF
