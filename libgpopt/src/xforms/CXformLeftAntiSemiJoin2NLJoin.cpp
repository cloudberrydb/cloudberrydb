//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformLeftAntiSemiJoin2NLJoin.cpp
//
//	@doc:
//		Implementation of transform
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformLeftAntiSemiJoin2NLJoin.h"
#include "gpopt/xforms/CXformUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftAntiSemiJoin2NLJoin::CXformLeftAntiSemiJoin2NLJoin
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXformLeftAntiSemiJoin2NLJoin::CXformLeftAntiSemiJoin2NLJoin
	(
	IMemoryPool *pmp
	)
	:
	// pattern
	CXformImplementation
		(
		GPOS_NEW(pmp) CExpression
					(pmp,
					 GPOS_NEW(pmp) CLogicalLeftAntiSemiJoin(pmp),
					 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // left child
					 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // right child
					 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))) // predicate
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftAntiSemiJoin2NLJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftAntiSemiJoin2NLJoin::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	return CXformUtils::ExfpLogicalJoin2PhysicalJoin(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftAntiSemiJoin2NLJoin::Transform
//
//	@doc:
//		actual transformation
//
//---------------------------------------------------------------------------
void
CXformLeftAntiSemiJoin2NLJoin::Transform
	(
	CXformContext *pxfctxt,
	CXformResult *pxfres,
	CExpression *pexpr
	)
	const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CXformUtils::ImplementNLJoin<CPhysicalLeftAntiSemiNLJoin>(pxfctxt, pxfres, pexpr);
}


// EOF

