//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoin2DynamicDynamicBitmapIndexGetApply.cpp
//
//	@doc:
//		Transform Inner Join to Dynamic Bitmap IndexGet Apply
//
//	@owner:
//		n
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformInnerJoin2DynamicBitmapIndexGetApply.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoin2DynamicBitmapIndexGetApply::CXformInnerJoin2DynamicBitmapIndexGetApply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformInnerJoin2DynamicBitmapIndexGetApply::CXformInnerJoin2DynamicBitmapIndexGetApply
	(
	IMemoryPool *pmp
	)
	:
	// pattern
	CXformInnerJoin2IndexApply
		(
		GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalInnerJoin(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // outer child
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalDynamicGet(pmp)), // inner child must be a DynamicGet operator
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))  // predicate tree
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoin2DynamicBitmapIndexGetApply::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformInnerJoin2DynamicBitmapIndexGetApply::Transform
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

	IMemoryPool *pmp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprOuter = (*pexpr)[0];
	CExpression *pexprInner = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	GPOS_ASSERT(COperator::EopLogicalDynamicGet == pexprInner->Pop()->Eopid());
	CLogicalDynamicGet *popDynamicGet = CLogicalDynamicGet::PopConvert(pexprInner->Pop());
	CreateHomogeneousIndexApplyAlternatives
		(
		pmp,
		pexpr->Pop()->UlOpId(),
		pexprOuter,
		pexprInner,
		pexprScalar,
		popDynamicGet->Ptabdesc(),
		popDynamicGet,
		pxfres,
		IMDIndex::EmdindBitmap
		);
}

// EOF
