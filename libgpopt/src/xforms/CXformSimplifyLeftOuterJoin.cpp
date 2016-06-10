//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformSimplifyLeftOuterJoin.cpp
//
//	@doc:
//		Simplify Left Outer Join with constant false predicate
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformSimplifyLeftOuterJoin.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifyLeftOuterJoin::CXformSimplifyLeftOuterJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSimplifyLeftOuterJoin::CXformSimplifyLeftOuterJoin
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
					GPOS_NEW(pmp) CLogicalLeftOuterJoin(pmp),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // left child
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)),  // right child
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))  // predicate tree
					)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifyLeftOuterJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSimplifyLeftOuterJoin::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	CExpression *pexprScalar = exprhdl.PexprScalarChild(2 /*ulChildIndex*/);
	if (CUtils::FScalarConstFalse(pexprScalar))
	{
		// if LOJ predicate is False, we can replace inner child with empty table
		return CXform::ExfpHigh;
	}

	return CXform::ExfpNone;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifyLeftOuterJoin::Transform
//
//	@doc:
//		Actual transformation to simplify left outer join
//
//---------------------------------------------------------------------------
void
CXformSimplifyLeftOuterJoin::Transform
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

	// extract components
	CExpression *pexprOuter = (*pexpr)[0];
	CExpression *pexprInner = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	pexprOuter->AddRef();
	pexprScalar->AddRef();
	CExpression *pexprResult = NULL;

	// inner child of LOJ can be replaced with empty table
	GPOS_ASSERT(CUtils::FScalarConstFalse(pexprScalar));

	// extract output columns of inner child
	DrgPcr *pdrgpcr = CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->PcrsOutput()->Pdrgpcr(pmp);

	// generate empty constant table with the same columns
	COperator *popCTG = GPOS_NEW(pmp) CLogicalConstTableGet(pmp, pdrgpcr, GPOS_NEW(pmp) DrgPdrgPdatum(pmp));
	pexprResult =
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CLogicalLeftOuterJoin(pmp),
			pexprOuter,
			GPOS_NEW(pmp) CExpression(pmp, popCTG),
			pexprScalar
			);

	pxfres->Add(pexprResult);
}

// EOF
