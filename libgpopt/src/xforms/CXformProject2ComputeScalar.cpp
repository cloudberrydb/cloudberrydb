//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformProject2ComputeScalar.cpp
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
#include "gpopt/xforms/CXformProject2ComputeScalar.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformProject2ComputeScalar::CXformProject2ComputeScalar
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformProject2ComputeScalar::CXformProject2ComputeScalar
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
						GPOS_NEW(pmp) CLogicalProject(pmp),
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // relational child
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))  // scalar child
						)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformProject2ComputeScalar::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformProject2ComputeScalar::Transform
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
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];
	
	// addref all children
	pexprRelational->AddRef();
	pexprScalar->AddRef();
	
	// assemble physical operator
	CExpression *pexprComputeScalar = 
		GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CPhysicalComputeScalar(pmp),
					pexprRelational,
					pexprScalar
					);
	
	// add alternative to results
	pxfres->Add(pexprComputeScalar);
}
	

// EOF

