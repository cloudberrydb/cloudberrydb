//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementAssert.cpp
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
#include "gpopt/xforms/CXformImplementAssert.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementAssert::CXformImplementAssert
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementAssert::CXformImplementAssert
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
						GPOS_NEW(pmp) CLogicalAssert(pmp),
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // relational child
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))	// predicate
						)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementAssert::Exfp
//
//	@doc:
//		Compute xform promise level for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformImplementAssert::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	if(exprhdl.Pdpscalar(1)->FHasSubquery())
	{		
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementAssert::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementAssert::Transform
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
	CLogicalAssert *popAssert = CLogicalAssert::PopConvert(pexpr->Pop());
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];
	CException *pexc = popAssert->Pexc();
	
	// addref all children
	pexprRelational->AddRef();
	pexprScalar->AddRef();
	
	// assemble physical operator
	CPhysicalAssert *popPhysicalAssert = 
			GPOS_NEW(pmp) CPhysicalAssert
						(
						pmp, 
						GPOS_NEW(pmp) CException(pexc->UlMajor(), pexc->UlMinor(), pexc->SzFilename(), pexc->UlLine())
						);
	
	CExpression *pexprAssert = 
		GPOS_NEW(pmp) CExpression
					(
					pmp, 
					popPhysicalAssert,
					pexprRelational,
					pexprScalar
					);
	
	// add alternative to results
	pxfres->Add(pexprAssert);
}
	

// EOF

