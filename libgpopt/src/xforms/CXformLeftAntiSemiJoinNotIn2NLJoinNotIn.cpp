//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformLeftAntiSemiJoinNotIn2NLJoinNotIn.cpp
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
#include "gpopt/xforms/CXformLeftAntiSemiJoinNotIn2NLJoinNotIn.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftAntiSemiJoinNotIn2NLJoinNotIn::CXformLeftAntiSemiJoinNotIn2NLJoinNotIn
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXformLeftAntiSemiJoinNotIn2NLJoinNotIn::CXformLeftAntiSemiJoinNotIn2NLJoinNotIn
	(
	IMemoryPool *pmp
	)
	:
	// pattern
	CXformImplementation
		(
		GPOS_NEW(pmp) CExpression
					(pmp,
					 GPOS_NEW(pmp) CLogicalLeftAntiSemiJoinNotIn(pmp),
					 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // left child
					 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // right child
					 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))) // predicate
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftAntiSemiJoinNotIn2NLJoinNotIn::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftAntiSemiJoinNotIn2NLJoinNotIn::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	return CXformUtils::ExfpLogicalJoin2PhysicalJoin(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftAntiSemiJoinNotIn2NLJoinNotIn::Transform
//
//	@doc:
//		actual transformation
//
//---------------------------------------------------------------------------
void
CXformLeftAntiSemiJoinNotIn2NLJoinNotIn::Transform
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

	CXformUtils::ImplementNLJoin<CPhysicalLeftAntiSemiNLJoinNotIn>(pxfctxt, pxfres, pexpr);
}


// EOF

