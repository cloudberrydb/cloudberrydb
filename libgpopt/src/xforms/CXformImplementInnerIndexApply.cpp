//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformImplementInnerIndexApply.cpp
//
//	@doc:
//		Implementation of Inner Index Apply operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/xforms/CXformImplementInnerIndexApply.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementInnerIndexApply::CXformImplementInnerIndexApply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementInnerIndexApply::CXformImplementInnerIndexApply
	(
	IMemoryPool *pmp
	)
	:
	// pattern
	CXformImplementation
		(
		GPOS_NEW(pmp) CExpression
						(
						pmp,
						GPOS_NEW(pmp) CLogicalInnerIndexApply(pmp),
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // outer child
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)),  // inner child
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))  // predicate
						)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementInnerIndexApply::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementInnerIndexApply::Transform
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
	DrgPcr *pdrgpcr = CLogicalInnerIndexApply::PopConvert(pexpr->Pop())->PdrgPcrOuterRefs();
	pdrgpcr->AddRef();

	// addref all components
	pexprOuter->AddRef();
	pexprInner->AddRef();
	pexprScalar->AddRef();

	// assemble physical operator
	CExpression *pexprResult =
		GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CPhysicalInnerIndexNLJoin(pmp, pdrgpcr),
					pexprOuter,
					pexprInner,
					pexprScalar
					);

	// add alternative to results
	pxfres->Add(pexprResult);
}


// EOF

