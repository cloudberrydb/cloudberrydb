//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformImplementDynamicBitmapTableGet.cpp
//
//	@doc:
//		Implement DynamicBitmapTableGet
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformImplementDynamicBitmapTableGet.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/ops.h"

using namespace gpopt;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementDynamicBitmapTableGet::CXformImplementDynamicBitmapTableGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementDynamicBitmapTableGet::CXformImplementDynamicBitmapTableGet
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
				GPOS_NEW(pmp) CLogicalDynamicBitmapTableGet(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // predicate tree
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))  // bitmap index expression
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementDynamicBitmapTableGet::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementDynamicBitmapTableGet::Transform
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
	CLogicalDynamicBitmapTableGet *popLogical = CLogicalDynamicBitmapTableGet::PopConvert(pexpr->Pop());

	CTableDescriptor *ptabdesc = popLogical->Ptabdesc();
	ptabdesc->AddRef();

	CName *pname = GPOS_NEW(pmp) CName(pmp, popLogical->Name());

	DrgPcr *pdrgpcrOutput = popLogical->PdrgpcrOutput();

	GPOS_ASSERT(NULL != pdrgpcrOutput);
	pdrgpcrOutput->AddRef();

	DrgDrgPcr *pdrgpdrgpcrPart = popLogical->PdrgpdrgpcrPart();
	pdrgpdrgpcrPart->AddRef();

	CPartConstraint *ppartcnstr = popLogical->Ppartcnstr();
	ppartcnstr->AddRef();

	CPartConstraint *ppartcnstrRel = popLogical->PpartcnstrRel();
	ppartcnstrRel->AddRef();

	CPhysicalDynamicBitmapTableScan *popPhysical =
			GPOS_NEW(pmp) CPhysicalDynamicBitmapTableScan
					(
					pmp,
					popLogical->FPartial(),
					ptabdesc,
					pexpr->Pop()->UlOpId(),
					pname,
					popLogical->UlScanId(),
					pdrgpcrOutput,
					pdrgpdrgpcrPart,
					popLogical->UlSecondaryScanId(),
					ppartcnstr,
					ppartcnstrRel
					);

	CExpression *pexprCondition = (*pexpr)[0];
	CExpression *pexprIndexPath = (*pexpr)[1];
	pexprCondition->AddRef();
	pexprIndexPath->AddRef();

	CExpression *pexprPhysical =
			GPOS_NEW(pmp) CExpression(pmp, popPhysical, pexprCondition, pexprIndexPath);
	pxfres->Add(pexprPhysical);
}

// EOF
