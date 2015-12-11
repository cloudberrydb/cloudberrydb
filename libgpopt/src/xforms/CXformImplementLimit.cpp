//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformImplementLimit.cpp
//
//	@doc:
//		Implementation of limit operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/xforms/CXformImplementLimit.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementLimit::CXformImplementLimit
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementLimit::CXformImplementLimit
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
						GPOS_NEW(pmp) CLogicalLimit(pmp),
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // relational child
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)),  // scalar child for offset
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))  // scalar child for number of rows

						)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementLimit::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementLimit::Transform
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
	CLogicalLimit *popLimit = CLogicalLimit::PopConvert(pexpr->Pop());
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprScalarStart = (*pexpr)[1];
	CExpression *pexprScalarRows = (*pexpr)[2];
	COrderSpec *pos = popLimit->Pos();
	
	// addref all components
	pexprRelational->AddRef();
	pexprScalarStart->AddRef();
	pexprScalarRows->AddRef();
	popLimit->Pos()->AddRef();
	
	// assemble physical operator
	CExpression *pexprLimit = 
		GPOS_NEW(pmp) CExpression
					(
					pmp, 
					GPOS_NEW(pmp) CPhysicalLimit
						(
						pmp,
						pos,
						popLimit->FGlobal(),
						popLimit->FHasCount(),
						popLimit->FTopLimitUnderDML()
						),
					pexprRelational,
					pexprScalarStart,
					pexprScalarRows
					);
	
	// add alternative to results
	pxfres->Add(pexprLimit);
}
	

// EOF

