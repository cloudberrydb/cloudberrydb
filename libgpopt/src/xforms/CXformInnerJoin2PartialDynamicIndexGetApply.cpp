//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoin2PartialDynamicIndexGetApply.cpp
//
//	@doc:
//		Transform inner join over partitioned table into a union-all of dynamic index get applies.
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformInnerJoin2PartialDynamicIndexGetApply.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoin2PartialDynamicIndexGetApply::CXformInnerJoin2PartialDynamicIndexGetApply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformInnerJoin2PartialDynamicIndexGetApply::CXformInnerJoin2PartialDynamicIndexGetApply
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
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp)), // outer child
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalDynamicGet(pmp)), // inner child must be a DynamicGet operator
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))  // predicate tree
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoin2PartialDynamicIndexGetApply::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformInnerJoin2PartialDynamicIndexGetApply::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	if (CXform::ExfpNone == CXformInnerJoin2IndexApply::Exfp(exprhdl))
	{
		return CXform::ExfpNone;
	}

	if (exprhdl.Pdprel(1 /*ulChildIndex*/)->FHasPartialIndexes())
	{
		return CXform::ExfpHigh;
	}

	return CXform::ExfpNone;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoin2PartialDynamicIndexGetApply::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformInnerJoin2PartialDynamicIndexGetApply::Transform
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
	CLogicalDynamicGet *popDynamicGet = CLogicalDynamicGet::PopConvert (pexprInner->Pop ());
	CTableDescriptor *ptabdescInner = popDynamicGet->Ptabdesc ();
	CreatePartialIndexApplyAlternatives
		(
		pmp,
		pexpr->Pop()->UlOpId(),
		pexprOuter,
		pexprInner,
		pexprScalar,
		ptabdescInner,
		popDynamicGet,
		pxfres
		);
}

// EOF
