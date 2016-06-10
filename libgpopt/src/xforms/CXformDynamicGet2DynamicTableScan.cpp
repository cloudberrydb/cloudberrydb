//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformDynamicGet2DynamicTableScan.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/xforms/CXformDynamicGet2DynamicTableScan.h"

#include "gpopt/operators/ops.h"
#include "gpopt/metadata/CTableDescriptor.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformDynamicGet2DynamicTableScan::CXformDynamicGet2DynamicTableScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformDynamicGet2DynamicTableScan::CXformDynamicGet2DynamicTableScan
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
				GPOS_NEW(pmp) CLogicalDynamicGet(pmp)
				)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformDynamicGet2DynamicTableScan::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformDynamicGet2DynamicTableScan::Transform
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

	CLogicalDynamicGet *popGet = CLogicalDynamicGet::PopConvert(pexpr->Pop());
	IMemoryPool *pmp = pxfctxt->Pmp();

	// create/extract components for alternative
	CName *pname = GPOS_NEW(pmp) CName(pmp, popGet->Name());
	
	CTableDescriptor *ptabdesc = popGet->Ptabdesc();
	ptabdesc->AddRef();
	
	DrgPcr *pdrgpcrOutput = popGet->PdrgpcrOutput();
	GPOS_ASSERT(NULL != pdrgpcrOutput);

	pdrgpcrOutput->AddRef();
	
	DrgDrgPcr *pdrgpdrgpcrPart = popGet->PdrgpdrgpcrPart();
	pdrgpdrgpcrPart->AddRef();
	
	popGet->Ppartcnstr()->AddRef();
	popGet->PpartcnstrRel()->AddRef();
	
	// create alternative expression
	CExpression *pexprAlt = 
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CPhysicalDynamicTableScan
						(
						pmp,
						popGet->FPartial(),
						pname, 
						ptabdesc,
						popGet->UlOpId(),
						popGet->UlScanId(), 
						pdrgpcrOutput,
						pdrgpdrgpcrPart,
						popGet->UlSecondaryScanId(),
						popGet->Ppartcnstr(),
						popGet->PpartcnstrRel()
						)
			);
	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}


// EOF

