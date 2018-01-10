//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc.
//
//	Implementation of Left Outer Index Apply operator
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/xforms/CXformImplementLeftOuterIndexApply.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


CXformImplementLeftOuterIndexApply::CXformImplementLeftOuterIndexApply
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
						GPOS_NEW(pmp) CLogicalLeftOuterIndexApply(pmp),
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // outer child
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)),  // inner child
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))  // predicate
						)
		)
{}


void
CXformImplementLeftOuterIndexApply::Transform
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
	DrgPcr *pdrgpcr = CLogicalLeftOuterIndexApply::PopConvert(pexpr->Pop())->PdrgPcrOuterRefs();
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
					GPOS_NEW(pmp) CPhysicalLeftOuterIndexNLJoin(pmp, pdrgpcr),
					pexprOuter,
					pexprInner,
					pexprScalar
					);

	// add alternative to results
	pxfres->Add(pexprResult);
}


// EOF

