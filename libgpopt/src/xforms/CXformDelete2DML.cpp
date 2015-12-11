//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformDelete2DML.cpp
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
#include "gpopt/xforms/CXformDelete2DML.h"
#include "gpopt/xforms/CXformUtils.h"

#include "gpopt/operators/ops.h"
#include "gpopt/metadata/CTableDescriptor.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformDelete2DML::CXformDelete2DML
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformDelete2DML::CXformDelete2DML
	(
	IMemoryPool *pmp
	)
	:
	CXformExploration
		(
		 // pattern
		GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalDelete(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformDelete2DML::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformDelete2DML::Exfp
	(
	CExpressionHandle & // exprhdl
	)
	const
{
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformDelete2DML::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformDelete2DML::Transform
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

	CLogicalDelete *popDelete = CLogicalDelete::PopConvert(pexpr->Pop());
	IMemoryPool *pmp = pxfctxt->Pmp();

	// extract components for alternative

	CTableDescriptor *ptabdesc = popDelete->Ptabdesc();
	ptabdesc->AddRef();

	DrgPcr *pdrgpcr = popDelete->Pdrgpcr();
	pdrgpcr->AddRef();

	CColRef *pcrCtid = popDelete->PcrCtid();

	CColRef *pcrSegmentId = popDelete->PcrSegmentId();

	// child of delete operator
	CExpression *pexprChild = (*pexpr)[0];
	pexprChild->AddRef();

	// create logical DML
	CExpression *pexprAlt =
		CXformUtils::PexprLogicalDMLOverProject
						(
						pmp,
						pexprChild,
						CLogicalDML::EdmlDelete,
						ptabdesc,
						pdrgpcr,
						pcrCtid,
						pcrSegmentId
						);

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF
