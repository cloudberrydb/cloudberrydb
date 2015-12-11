//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply.cpp
//
//	@doc:
//		Transform inner join over Select over a partitioned table into a
//		union-all of dynamic index get applies
//
//	@owner:
//		n
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply::CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply::CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply
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
				GPOS_NEW(pmp) CExpression  // inner child must be a Select operator
						(
						pmp,
						GPOS_NEW(pmp) CLogicalSelect(pmp),
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalDynamicGet(pmp)),
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))  // predicate
						),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))  // predicate tree
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply::Exfp
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
//		CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply::Transform
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

	GPOS_ASSERT(COperator::EopLogicalSelect == pexprInner->Pop()->Eopid());
	CExpression *pexprGet = (*pexprInner)[0];
	GPOS_ASSERT(COperator::EopLogicalDynamicGet == pexprGet->Pop()->Eopid());

	CLogicalDynamicGet *popDynamicGet = CLogicalDynamicGet::PopConvert(pexprGet->Pop());
	CTableDescriptor *ptabdescInner = popDynamicGet->Ptabdesc();
	CExpression *pexprAllPredicates = CPredicateUtils::PexprConjunction(pmp, pexprScalar, (*pexprInner)[1]);
	CreatePartialIndexApplyAlternatives
		(
		pmp,
		pexpr->Pop()->UlOpId(),
		pexprOuter,
		pexprInner,
		pexprAllPredicates,
		ptabdescInner,
		popDynamicGet,
		pxfres
		);
	pexprAllPredicates->Release();
}

// EOF
