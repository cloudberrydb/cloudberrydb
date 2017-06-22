//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CXformGbAgg2HashAgg.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/ops.h"

#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/xforms/CXformGbAgg2HashAgg.h"
#include "naucrates/md/IMDAggregate.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2HashAgg::CXformGbAgg2HashAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformGbAgg2HashAgg::CXformGbAgg2HashAgg
	(
	IMemoryPool *pmp
	)
	:
	CXformImplementation
		(
		 // pattern
		GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalGbAgg(pmp),
							 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)),
							 // we need to extract deep tree in the project list to check
							 // for existence of distinct agg functions
							 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp)))
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2HashAgg::CXformGbAgg2HashAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformGbAgg2HashAgg::CXformGbAgg2HashAgg
	(
	CExpression *pexprPattern
	)
	:
	CXformImplementation(pexprPattern)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2HashAgg::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		grouping columns must be non-empty
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformGbAgg2HashAgg::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(exprhdl.Pop());
	DrgPcr *pdrgpcr = popAgg->Pdrgpcr();
	if (0 == pdrgpcr->UlLength() ||
		exprhdl.Pdpscalar(1 /*ulChildIndex*/)->FHasSubquery() ||
		!CUtils::FComparisonPossible(pdrgpcr, IMDType::EcmptEq) ||
		!CUtils::FHashable(pdrgpcr))
	{
		// no grouping columns, no equality or hash operators  are available for grouping columns, or
		// agg functions use subquery arguments
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2HashAgg::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformGbAgg2HashAgg::Transform
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
	
	// hash agg is not used with distinct agg functions
	// hash agg is not used if agg function does not have prelim func
	// hash agg is not used for ordered aggregate
	// evaluating these conditions needs a deep tree in the project list
	if (!FApplicable(pexpr))
	{
		return;
	}

	IMemoryPool *pmp = pxfctxt->Pmp();	
	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());
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
		GPOS_ASSERT(NULL != pdrgpcrArgDQA);
		pdrgpcrArgDQA->AddRef();
	}

	// create alternative expression
	CExpression *pexprAlt = 
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CPhysicalHashAgg
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

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2HashAgg::FApplicable
//
//	@doc:
//		Check if the transformation is applicable
//
//---------------------------------------------------------------------------
BOOL
CXformGbAgg2HashAgg::FApplicable
	(
	CExpression *pexpr
	)
	const
{
	CExpression *pexprPrjList = (*pexpr)[1];
	ULONG ulArity = pexprPrjList->UlArity();
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprPrjEl = (*pexprPrjList)[ul];
		CExpression *pexprAggFunc = (*pexprPrjEl)[0];
		CScalarAggFunc *popScAggFunc = CScalarAggFunc::PopConvert(pexprAggFunc->Pop());

		if (popScAggFunc->FDistinct() || !pmda->Pmdagg(popScAggFunc->Pmdid())->FHashAggCapable() )
		{
			return false;
		}
	}

	return true;
}

// EOF

