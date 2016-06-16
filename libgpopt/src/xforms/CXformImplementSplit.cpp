//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementSplit.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/xforms/CXformImplementSplit.h"

#include "gpopt/operators/ops.h"
#include "gpopt/metadata/CTableDescriptor.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementSplit::CXformImplementSplit
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementSplit::CXformImplementSplit
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
				GPOS_NEW(pmp) CLogicalSplit(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementSplit::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformImplementSplit::Exfp
	(
	CExpressionHandle & // exprhdl
	)
	const
{
	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementSplit::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementSplit::Transform
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

	CLogicalSplit *popSplit = CLogicalSplit::PopConvert(pexpr->Pop());
	IMemoryPool *pmp = pxfctxt->Pmp();

	// extract components for alternative
	DrgPcr *pdrgpcrDelete = popSplit->PdrgpcrDelete();
	pdrgpcrDelete->AddRef();

	DrgPcr *pdrgpcrInsert = popSplit->PdrgpcrInsert();
	pdrgpcrInsert->AddRef();

	CColRef *pcrAction = popSplit->PcrAction();
	CColRef *pcrCtid = popSplit->PcrCtid();
	CColRef *pcrSegmentId = popSplit->PcrSegmentId();
	CColRef *pcrTupleOid = popSplit->PcrTupleOid();

	// child of Split operator
	CExpression *pexprChild = (*pexpr)[0];
	CExpression *pexprProjList = (*pexpr)[1];
	pexprChild->AddRef();
	pexprProjList->AddRef();

	// create physical Split
	CExpression *pexprAlt =
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CPhysicalSplit(pmp, pdrgpcrDelete, pdrgpcrInsert, pcrCtid, pcrSegmentId, pcrAction, pcrTupleOid),
			pexprChild,
			pexprProjList
			);
	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF
