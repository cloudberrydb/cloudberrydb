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
//		CXformExpandNAryJoin::Transform
//
//	@doc:
//		Actual transformation of n-ary join to cluster of inner joins in
//		the order of the joins existing in the query
//
//---------------------------------------------------------------------------
//Example Input tree:
//	+--CLogicalNAryJoin
//	   |--CLogicalGet "t1"
//	   |--CLogicalGet "t2"
//	   |--CLogicalGet "t3"
//	   +--CScalarBoolOp (EboolopAnd)
//		  |--CScalarCmp (=)
//		  |  |--CScalarIdent "a" (0)
//		  |  +--CScalarIdent "a" (9)
//		  |--CScalarCmp (=)
//		  |  |--CScalarIdent "a" (9)
//		  |  +--CScalarIdent "a" (18)
//		  +--CScalarCmp (=)
//			 |--CScalarIdent "a" (0)
//			 +--CScalarIdent "a" (18)
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

	// create a cluster of inner joins with same order of given relations
	// and dummy join condition
	//	+--CLogicalInnerJoin
	//	   |--CLogicalInnerJoin
	//	   |  |--CLogicalGet "t1"
	//	   |  |--CLogicalGet "t2"
	//	   |  +--CScalarConst (1)
	//	   |--CLogicalGet "t3"
	//	   +--CScalarConst (1)
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

	// create a logical select with the join expression and scalar condition child
	CExpression *pexprSelect = CUtils::PexprLogicalSelect(pmp, pexprJoin, pexprScalar);

	// normalize the tree and push down the predicates
	CExpression *pexprNormalized = CNormalizer::PexprNormalize(pmp, pexprSelect);
	pexprSelect->Release();
	pxfres->Add(pexprNormalized);
}

// EOF
