//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformInnerApplyWithOuterKey2InnerJoin.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CKeyCollection.h"
#include "gpopt/xforms/CXformInnerApplyWithOuterKey2InnerJoin.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformInnerApplyWithOuterKey2InnerJoin::CXformInnerApplyWithOuterKey2InnerJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformInnerApplyWithOuterKey2InnerJoin::CXformInnerApplyWithOuterKey2InnerJoin
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
				GPOS_NEW(pmp) CLogicalInnerApply(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // left child
				GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalGbAgg(pmp),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp)), // relational child of Gb
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)) // scalar project list of Gb
					), // right child
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))  // Apply predicate
				)
		)
{}



//---------------------------------------------------------------------------
//	@function:
//		CXformInnerApplyWithOuterKey2InnerJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformInnerApplyWithOuterKey2InnerJoin::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	// check if outer child has key and inner child has outer references
	if (NULL == exprhdl.Pdprel(0)->Pkc() ||
		0 == exprhdl.Pdprel(1)->PcrsOuter()->CElements())
	{
		return ExfpNone;
	}

	return ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformInnerApplyWithOuterKey2InnerJoin::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformInnerApplyWithOuterKey2InnerJoin::Transform
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

	// extract components
	CExpression *pexprOuter = (*pexpr)[0];
	CExpression *pexprGb = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	if (0 < CLogicalGbAgg::PopConvert(pexprGb->Pop())->Pdrgpcr()->UlLength())
	{
		// xform is not applicable if inner Gb has grouping columns
		return;
	}

	if (CUtils::FHasSubqueryOrApply((*pexprGb)[0]))
	{
		// Subquery/Apply must be unnested before reaching here
		return;
	}

	// decorrelate Gb's relational child
	(*pexprGb)[0]->ResetDerivedProperties();
	CExpression *pexprInner = NULL;
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	if (!CDecorrelator::FProcess(pmp, (*pexprGb)[0], false /*fEqualityOnly*/, &pexprInner, pdrgpexpr))
	{
		pdrgpexpr->Release();
		return;
	}

	GPOS_ASSERT(NULL != pexprInner);
	CExpression *pexprPredicate = CPredicateUtils::PexprConjunction(pmp, pdrgpexpr);

	// join outer child with Gb's decorrelated child
	pexprOuter->AddRef();
	CExpression *pexprInnerJoin =
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CLogicalInnerJoin(pmp),
			pexprOuter,
			pexprInner,
			pexprPredicate
			);

	// create grouping columns from the output of outer child
	DrgPcr *pdrgpcrKey = NULL;
	DrgPcr *pdrgpcr = CUtils::PdrgpcrGroupingKey(pmp, pexprOuter, &pdrgpcrKey);
	pdrgpcrKey->Release();  // key is not used here

	CLogicalGbAgg *popGbAgg = GPOS_NEW(pmp) CLogicalGbAgg(pmp, pdrgpcr, COperator::EgbaggtypeGlobal /*egbaggtype*/);
	CExpression *pexprPrjList = (*pexprGb)[1];
	pexprPrjList->AddRef();
	CExpression *pexprNewGb = GPOS_NEW(pmp) CExpression (pmp, popGbAgg, pexprInnerJoin, pexprPrjList);

	// add Apply predicate in a top Select node
	pexprScalar->AddRef();
	CExpression *pexprSelect = CUtils::PexprLogicalSelect(pmp, pexprNewGb, pexprScalar);

	pxfres->Add(pexprSelect);
}


// EOF

