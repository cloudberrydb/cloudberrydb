//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformImplementBitmapTableGet.cpp
//
//	@doc:
//		Implement BitmapTableGet
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformImplementBitmapTableGet.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/ops.h"

using namespace gpopt;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementBitmapTableGet::CXformImplementBitmapTableGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementBitmapTableGet::CXformImplementBitmapTableGet
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
				GPOS_NEW(pmp) CLogicalBitmapTableGet(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // predicate tree
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))  // bitmap index expression
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementBitmapTableGet::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementBitmapTableGet::Transform
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
	CLogicalBitmapTableGet *popLogical = CLogicalBitmapTableGet::PopConvert(pexpr->Pop());

	CTableDescriptor *ptabdesc = popLogical->Ptabdesc();
	ptabdesc->AddRef();

	DrgPcr *pdrgpcrOutput = popLogical->PdrgpcrOutput();
	pdrgpcrOutput->AddRef();

	CPhysicalBitmapTableScan *popPhysical =
			GPOS_NEW(pmp) CPhysicalBitmapTableScan
					(
					pmp,
					ptabdesc,
					pexpr->Pop()->UlOpId(),
					GPOS_NEW(pmp) CName(pmp, *popLogical->PnameTableAlias()),
					pdrgpcrOutput
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
