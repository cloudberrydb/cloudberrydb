//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementCTEConsumer.cpp
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
#include "gpopt/xforms/CXformImplementCTEConsumer.h"

#include "gpopt/operators/ops.h"
#include "gpopt/metadata/CTableDescriptor.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementCTEConsumer::CXformImplementCTEConsumer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementCTEConsumer::CXformImplementCTEConsumer
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
				GPOS_NEW(pmp) CLogicalCTEConsumer(pmp)
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementCTEConsumer::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformImplementCTEConsumer::Exfp
	(
	CExpressionHandle & // exprhdl
	)
	const
{
	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementCTEConsumer::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementCTEConsumer::Transform
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

	CLogicalCTEConsumer *popCTEConsumer = CLogicalCTEConsumer::PopConvert(pexpr->Pop());
	IMemoryPool *pmp = pxfctxt->Pmp();

	// extract components for alternative
	ULONG ulId = popCTEConsumer->UlCTEId();

	DrgPcr *pdrgpcr = popCTEConsumer->Pdrgpcr();
	pdrgpcr->AddRef();

	HMUlCr *phmulcr = popCTEConsumer->Phmulcr();
	GPOS_ASSERT(NULL != phmulcr);
	phmulcr->AddRef();

	// create physical CTE Consumer
	CExpression *pexprAlt =
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CPhysicalCTEConsumer(pmp, ulId, pdrgpcr, phmulcr)
			);

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF
