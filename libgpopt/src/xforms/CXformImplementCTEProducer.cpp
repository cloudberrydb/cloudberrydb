//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementCTEProducer.cpp
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
#include "gpopt/xforms/CXformImplementCTEProducer.h"

#include "gpopt/operators/ops.h"
#include "gpopt/metadata/CTableDescriptor.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementCTEProducer::CXformImplementCTEProducer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementCTEProducer::CXformImplementCTEProducer
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
				GPOS_NEW(pmp) CLogicalCTEProducer(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementCTEProducer::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformImplementCTEProducer::Exfp
	(
	CExpressionHandle & // exprhdl
	)
	const
{
	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementCTEProducer::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementCTEProducer::Transform
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

	CLogicalCTEProducer *popCTEProducer = CLogicalCTEProducer::PopConvert(pexpr->Pop());
	IMemoryPool *pmp = pxfctxt->Pmp();

	// extract components for alternative
	ULONG ulId = popCTEProducer->UlCTEId();

	DrgPcr *pdrgpcr = popCTEProducer->Pdrgpcr();
	pdrgpcr->AddRef();

	// child of CTEProducer operator
	CExpression *pexprChild = (*pexpr)[0];
	pexprChild->AddRef();

	// create physical CTE Producer
	CExpression *pexprAlt =
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CPhysicalCTEProducer(pmp, ulId, pdrgpcr),
			pexprChild
			);

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF
