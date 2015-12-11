//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformSelect2DynamicBitmapBoolOp.cpp
//
//	@doc:
//		Transform select over partitioned table into a dynamic bitmap table get 
//		over bitmap bool op
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformSelect2DynamicBitmapBoolOp.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpmd;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2DynamicBitmapBoolOp::CXformSelect2DynamicBitmapBoolOp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSelect2DynamicBitmapBoolOp::CXformSelect2DynamicBitmapBoolOp
	(
	IMemoryPool *pmp
	)
	:
	CXformExploration
		(
		GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalSelect(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalDynamicGet(pmp)),  // logical child
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))  		// predicate tree
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2DynamicBitmapBoolOp::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSelect2DynamicBitmapBoolOp::Exfp
	(
	CExpressionHandle &  // exprhdl
	)
	const
{
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2DynamicBitmapBoolOp::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformSelect2DynamicBitmapBoolOp::Transform
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
	CExpression *pexprResult = CXformUtils::PexprSelect2BitmapBoolOp(pmp, pexpr);

	if (NULL != pexprResult)
	{
		// create a redundant SELECT on top of DynamicIndexGet to be able to use predicate in partition elimination
		CExpression *pexprRedundantSelect = CXformUtils::PexprRedundantSelectForDynamicIndex(pmp, pexprResult);
		pexprResult->Release();

		pxfres->Add(pexprRedundantSelect);
	}
}

// EOF
