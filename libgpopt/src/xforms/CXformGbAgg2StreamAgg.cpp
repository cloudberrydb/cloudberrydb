//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformGbAgg2StreamAgg.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/ops.h"

#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/xforms/CXformGbAgg2StreamAgg.h"
#include "gpopt/xforms/CXformGbAgg2HashAgg.h"
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2StreamAgg::CXformGbAgg2StreamAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformGbAgg2StreamAgg::CXformGbAgg2StreamAgg
	(
	IMemoryPool *pmp
	)
	:
	CXformImplementation
		(
		 // pattern
		GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalGbAgg(pmp),
							 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)),
							 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)))
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2StreamAgg::CXformGbAgg2StreamAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformGbAgg2StreamAgg::CXformGbAgg2StreamAgg
	(
	CExpression *pexprPattern
	)
	:
	CXformImplementation(pexprPattern)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2StreamAgg::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		grouping columns must be non-empty
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformGbAgg2StreamAgg::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(exprhdl.Pop());
	if (0 == popAgg->Pdrgpcr()->UlLength() ||
		!CUtils::FComparisonPossible(popAgg->Pdrgpcr(), IMDType::EcmptL) ||
		exprhdl.Pdpscalar(1 /*ulChildIndex*/)->FHasSubquery())
	{
		// no grouping columns, or no sort operators are available for grouping columns, or
		// agg functions use subquery arguments
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2StreamAgg::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformGbAgg2StreamAgg::Transform
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
	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());
	IMemoryPool *pmp = pxfctxt->Pmp();
	DrgPcr *pdrgpcr = popAgg->Pdrgpcr();
	pdrgpcr->AddRef();
	
	// extract components
	CExpression *pexprRel = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];

	// addref children
	pexprRel->AddRef();
	pexprScalar->AddRef();

	DrgPcr *pdrgpcrArgDQA = popAgg->PdrgpcrArgDQA();
	if (pdrgpcrArgDQA != NULL && 0 != pdrgpcrArgDQA->UlLength())
	{
		pdrgpcrArgDQA->AddRef();
	}

	// create alternative expression
	CExpression *pexprAlt = 
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CPhysicalStreamAgg
						(
						pmp,
						pdrgpcr,
						popAgg->PdrgpcrMinimal(),
						popAgg->Egbaggtype(),
						popAgg->FGeneratesDuplicates(),
						pdrgpcrArgDQA,
						CXformUtils::FMultiStageAgg(pexpr)
						),
			pexprRel,
			pexprScalar
			);

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}


// EOF

