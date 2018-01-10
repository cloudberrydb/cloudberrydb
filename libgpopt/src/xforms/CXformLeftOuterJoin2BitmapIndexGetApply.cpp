//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal, Inc.
//
//	Transform left outer Join to Bitmap IndexGet Apply
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformLeftOuterJoin2BitmapIndexGetApply.h"


using namespace gpopt;


CXformLeftOuterJoin2BitmapIndexGetApply::CXformLeftOuterJoin2BitmapIndexGetApply
	(
	IMemoryPool *pmp
	)
	:
	// pattern
	CXformLeftOuterJoin2IndexApply
		(
		GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalLeftOuterJoin(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // outer child
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalGet(pmp)), // inner child must be a Get operator
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))  // predicate tree
				)
		)
{}

void
CXformLeftOuterJoin2BitmapIndexGetApply::Transform
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

	GPOS_ASSERT(COperator::EopLogicalGet == pexprInner->Pop()->Eopid());
	CTableDescriptor *ptabdescInner = CLogicalGet::PopConvert(pexprInner->Pop())->Ptabdesc();
	CreateHomogeneousIndexApplyAlternatives
		(
		pmp,
		pexpr->Pop()->UlOpId(),
		pexprOuter,
		pexprInner,
		pexprScalar,
		ptabdescInner,
		NULL, // popDynamicGet
		pxfres,
		IMDIndex::EmdindBitmap
		);
}

// EOF
