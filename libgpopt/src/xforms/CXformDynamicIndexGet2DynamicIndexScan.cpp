//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformDynamicIndexGet2DynamicIndexScan.cpp
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
#include "gpopt/xforms/CXformDynamicIndexGet2DynamicIndexScan.h"

#include "gpopt/operators/ops.h"
#include "gpopt/metadata/CPartConstraint.h"
#include "gpopt/metadata/CTableDescriptor.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformDynamicIndexGet2DynamicIndexScan::CXformDynamicIndexGet2DynamicIndexScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformDynamicIndexGet2DynamicIndexScan::CXformDynamicIndexGet2DynamicIndexScan
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
				GPOS_NEW(pmp) CLogicalDynamicIndexGet(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))	// index lookup predicate
				)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformDynamicIndexGet2DynamicIndexScan::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformDynamicIndexGet2DynamicIndexScan::Transform
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

	CLogicalDynamicIndexGet *popIndexGet = CLogicalDynamicIndexGet::PopConvert(pexpr->Pop());
	IMemoryPool *pmp = pxfctxt->Pmp();

	// create/extract components for alternative
	CName *pname = GPOS_NEW(pmp) CName(pmp, popIndexGet->Name());
	
	CTableDescriptor *ptabdesc = popIndexGet->Ptabdesc();
	ptabdesc->AddRef();
	
	CIndexDescriptor *pindexdesc = popIndexGet->Pindexdesc();
	pindexdesc->AddRef();
	
	DrgPcr *pdrgpcrOutput = popIndexGet->PdrgpcrOutput();
	GPOS_ASSERT(NULL != pdrgpcrOutput);
	pdrgpcrOutput->AddRef();
	
	DrgDrgPcr *pdrgpdrgpcrPart = popIndexGet->PdrgpdrgpcrPart();
	pdrgpdrgpcrPart->AddRef();
	
	CPartConstraint *ppartcnstr = popIndexGet->Ppartcnstr();
	ppartcnstr->AddRef();
	
	CPartConstraint *ppartcnstrRel = popIndexGet->PpartcnstrRel();
	ppartcnstrRel->AddRef();
	
	COrderSpec *pos = popIndexGet->Pos();
	pos->AddRef();
	
	// extract components
	CExpression *pexprIndexCond = (*pexpr)[0];
	pexprIndexCond->AddRef();
	
	// create alternative expression
	CExpression *pexprAlt = 
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CPhysicalDynamicIndexScan
						(
						pmp,
						popIndexGet->FPartial(),
						pindexdesc,
						ptabdesc,
						pexpr->Pop()->UlOpId(),
						pname,  
						pdrgpcrOutput,
						popIndexGet->UlScanId(),
						pdrgpdrgpcrPart,
						popIndexGet->UlSecondaryScanId(),
						ppartcnstr,
						ppartcnstrRel,
						pos
						),
			pexprIndexCond
			);
	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}


// EOF

