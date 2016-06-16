//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementConstTableGet.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/xforms/CXformImplementConstTableGet.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementConstTableGet::CXformImplementConstTableGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementConstTableGet::CXformImplementConstTableGet
	(
	IMemoryPool *pmp
	)
	:
	CXformImplementation
		(
		 // pattern
		GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalConstTableGet(pmp)
				)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementConstTableGet::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementConstTableGet::Transform
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

	CLogicalConstTableGet *popConstTableGet = CLogicalConstTableGet::PopConvert(pexpr->Pop());
	IMemoryPool *pmp = pxfctxt->Pmp();

	// create/extract components for alternative
	DrgPcoldesc *pdrgpcoldesc = popConstTableGet->Pdrgpcoldesc();
	pdrgpcoldesc->AddRef();
	
	DrgPdrgPdatum *pdrgpdrgpdatum = popConstTableGet->Pdrgpdrgpdatum();
	pdrgpdrgpdatum->AddRef();
	
	DrgPcr *pdrgpcrOutput = popConstTableGet->PdrgpcrOutput();
	pdrgpcrOutput->AddRef();
		
	// create alternative expression
	CExpression *pexprAlt = 
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CPhysicalConstTableGet(pmp, pdrgpcoldesc, pdrgpdrgpdatum, pdrgpcrOutput)
			);
	
	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}


// EOF

