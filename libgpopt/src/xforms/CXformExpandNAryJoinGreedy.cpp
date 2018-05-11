//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal Software Inc.
//
//	@filename:
//		CXformExpandNAryJoinGreedy.cpp
//
//	@doc:
//		Implementation of n-ary join expansion based on cardinality
//		of intermediate results and delay cross joins to
//		the end
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/engine/CHint.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/xforms/CXformExpandNAryJoinGreedy.h"
#include "gpopt/xforms/CJoinOrderGreedy.h"
#include "gpopt/xforms/CXformUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinGreedy::CXformExpandNAryJoinGreedy
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformExpandNAryJoinGreedy::CXformExpandNAryJoinGreedy
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
//		CXformExpandNAryJoinGreedy::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformExpandNAryJoinGreedy::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	COptimizerConfig *poconf = COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();
	const CHint *phint = poconf->GetHint();

	const ULONG ulArity = exprhdl.Arity();

	// since the last child of the join operator is a scalar child
	// defining the join predicate, ignore it.
	const ULONG ulRelChild = ulArity - 1;

	// This transform is used only when DP is disabled
	if (GPOPT_FENABLED_XFORM(CXform::ExfExpandNAryJoinDP) && ulRelChild < phint->UlJoinOrderDPLimit())
	{
		return CXform::ExfpNone;
	}

	return CXformUtils::ExfpExpandJoinOrder(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinGreedy::Transform
//
//	@doc:
//		Actual transformation of n-ary join to cluster of inner joins
//
//---------------------------------------------------------------------------
void
CXformExpandNAryJoinGreedy::Transform
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

	const ULONG ulArity = pexpr->Arity();
	GPOS_ASSERT(ulArity >= 3);

	CExpressionArray *pdrgpexpr = GPOS_NEW(pmp) CExpressionArray(pmp);
	for (ULONG ul = 0; ul < ulArity - 1; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		pexprChild->AddRef();
		pdrgpexpr->Append(pexprChild);
	}

	CExpression *pexprScalar = (*pexpr)[ulArity - 1];
	CExpressionArray *pdrgpexprPreds = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprScalar);

	// create a join order based on cardinality of intermediate results
	CJoinOrderGreedy jomc(pmp, pdrgpexpr, pdrgpexprPreds);
	CExpression *pexprResult = jomc.PexprExpand();

	// normalize resulting expression
	CExpression *pexprNormalized = CNormalizer::PexprNormalize(pmp, pexprResult);
	pexprResult->Release();
	pxfres->Add(pexprNormalized);
}

// EOF
