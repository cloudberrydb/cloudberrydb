//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformLeftSemiJoin2InnerJoinUnderGb.cpp
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
#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformLeftSemiJoin2InnerJoinUnderGb.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiJoin2InnerJoinUnderGb::CXformLeftSemiJoin2InnerJoinUnderGb
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXformLeftSemiJoin2InnerJoinUnderGb::CXformLeftSemiJoin2InnerJoinUnderGb
	(
	IMemoryPool *pmp
	)
	:
	// pattern
	CXformExploration
		(
		GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalLeftSemiJoin(pmp),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // left child
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // right child
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))  // predicate
					)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiJoin2InnerJoinUnderGb::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftSemiJoin2InnerJoinUnderGb::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	CColRefSet *pcrsInnerOutput = exprhdl.Pdprel(1)->PcrsOutput();
	CExpression *pexprScalar = exprhdl.PexprScalarChild(2);
	CAutoMemoryPool amp;
	if (exprhdl.FHasOuterRefs() ||
		NULL == exprhdl.Pdprel(0)->Pkc() ||
		exprhdl.Pdpscalar(2)->FHasSubquery() ||
		CPredicateUtils::FSimpleEqualityUsingCols(amp.Pmp(), pexprScalar, pcrsInnerOutput))
	{
		return ExfpNone;
	}

	return ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiJoin2InnerJoinUnderGb::Transform
//
//	@doc:
//		actual transformation
//
//---------------------------------------------------------------------------
void
CXformLeftSemiJoin2InnerJoinUnderGb::Transform
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
	CExpression *pexprInner = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	pexprOuter->AddRef();
	pexprInner->AddRef();
	pexprScalar->AddRef();

	DrgPcr *pdrgpcrKeys = NULL;
	DrgPcr *pdrgpcrGrouping = CUtils::PdrgpcrGroupingKey(pmp, pexprOuter, &pdrgpcrKeys);
	GPOS_ASSERT(NULL != pdrgpcrKeys);

	CExpression *pexprInnerJoin =
		CUtils::PexprLogicalJoin<CLogicalInnerJoin>(pmp, pexprOuter, pexprInner, pexprScalar);

	CExpression *pexprGb =
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CLogicalGbAggDeduplicate(pmp, pdrgpcrGrouping, COperator::EgbaggtypeGlobal  /*egbaggtype*/, pdrgpcrKeys),
			pexprInnerJoin,
			GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp))
			);

	pxfres->Add(pexprGb);
}

// EOF
