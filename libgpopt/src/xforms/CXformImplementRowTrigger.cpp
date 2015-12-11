//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementRowTrigger.cpp
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
#include "gpopt/xforms/CXformImplementRowTrigger.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementRowTrigger::CXformImplementRowTrigger
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementRowTrigger::CXformImplementRowTrigger
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
				GPOS_NEW(pmp) CLogicalRowTrigger(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementRowTrigger::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformImplementRowTrigger::Exfp
	(
	CExpressionHandle & // exprhdl
	)
	const
{
	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementRowTrigger::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementRowTrigger::Transform
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

	CLogicalRowTrigger *popRowTrigger = CLogicalRowTrigger::PopConvert(pexpr->Pop());
	IMemoryPool *pmp = pxfctxt->Pmp();

	// extract components for alternative
	IMDId *pmdidRel = popRowTrigger->PmdidRel();
	pmdidRel->AddRef();

	INT iType = popRowTrigger->IType();

	DrgPcr *pdrgpcrOld = popRowTrigger->PdrgpcrOld();
	if (NULL != pdrgpcrOld)
	{
		pdrgpcrOld->AddRef();
	}

	DrgPcr *pdrgpcrNew = popRowTrigger->PdrgpcrNew();
	if (NULL != pdrgpcrNew)
	{
		pdrgpcrNew->AddRef();
	}

	// child of RowTrigger operator
	CExpression *pexprChild = (*pexpr)[0];
	pexprChild->AddRef();

	// create physical RowTrigger
	CExpression *pexprAlt =
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CPhysicalRowTrigger(pmp, pmdidRel, iType, pdrgpcrOld, pdrgpcrNew),
			pexprChild
			);
	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF
