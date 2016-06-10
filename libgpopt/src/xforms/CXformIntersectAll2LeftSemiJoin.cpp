//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformIntersectAll2LeftSemiJoin.cpp
//
//	@doc:
//		Implement the transformation of CLogicalIntersectAll into a left semi join
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/exception.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CColRefComputed.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/xforms/CXformIntersectAll2LeftSemiJoin.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformIntersectAll2LeftSemiJoin::CXformIntersectAll2LeftSemiJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformIntersectAll2LeftSemiJoin::CXformIntersectAll2LeftSemiJoin
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
					GPOS_NEW(pmp) CLogicalIntersectAll(pmp),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // left relational child
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)) // right relational child
					)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformIntersectAll2LeftSemiJoin::Transform
//
//	@doc:
//		Actual transformation that transforms a intersect all into a left semi
//		join over a window operation over the inputs
//
//---------------------------------------------------------------------------
void
CXformIntersectAll2LeftSemiJoin::Transform
	(
	CXformContext *pxfctxt,
	CXformResult *pxfres,
	CExpression *pexpr
	)
	const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	IMemoryPool *pmp = pxfctxt->Pmp();

	// TODO: we currently only handle intersect all operators that
	// have two children
	GPOS_ASSERT(2 == pexpr->UlArity());

	// extract components
	CExpression *pexprLeftChild = (*pexpr)[0];
	CExpression *pexprRightChild = (*pexpr)[1];

	CLogicalIntersectAll *popIntersectAll = CLogicalIntersectAll::PopConvert(pexpr->Pop());
	DrgDrgPcr *pdrgpdrgpcrInput = popIntersectAll->PdrgpdrgpcrInput();

	CExpression *pexprLeftWindow = CXformUtils::PexprWindowWithRowNumber(pmp, pexprLeftChild, (*pdrgpdrgpcrInput)[0]);
	CExpression *pexprRightWindow = CXformUtils::PexprWindowWithRowNumber(pmp, pexprRightChild, (*pdrgpdrgpcrInput)[1]);

	DrgDrgPcr *pdrgpdrgpcrInputNew = GPOS_NEW(pmp) DrgDrgPcr(pmp);
	DrgPcr *pdrgpcrLeftNew = CUtils::PdrgpcrExactCopy(pmp, (*pdrgpdrgpcrInput)[0]);
	pdrgpcrLeftNew->Append(CXformUtils::PcrProjectElement(pexprLeftWindow, 0 /* row_number window function*/));

	DrgPcr *pdrgpcrRightNew = CUtils::PdrgpcrExactCopy(pmp, (*pdrgpdrgpcrInput)[1]);
	pdrgpcrRightNew->Append(CXformUtils::PcrProjectElement(pexprRightWindow, 0 /* row_number window function*/));

	pdrgpdrgpcrInputNew->Append(pdrgpcrLeftNew);
	pdrgpdrgpcrInputNew->Append(pdrgpcrRightNew);

	CExpression *pexprScCond = CUtils::PexprConjINDFCond(pmp, pdrgpdrgpcrInputNew);

	// assemble the new logical operator
	CExpression *pexprLSJ = GPOS_NEW(pmp) CExpression
										(
										pmp,
										GPOS_NEW(pmp) CLogicalLeftSemiJoin(pmp),
										pexprLeftWindow,
										pexprRightWindow,
										pexprScCond
										);

	// clean up
	pdrgpdrgpcrInputNew->Release();

	pxfres->Add(pexprLSJ);
}

// EOF
