//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformDifferenceAll2LeftAntiSemiJoin.cpp
//
//	@doc:
//		Implementation of the transformation a logical difference all into LASJ
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/operators/ops.h"
#include "gpopt/exception.h"
#include "gpopt/xforms/CXformDifferenceAll2LeftAntiSemiJoin.h"
#include "gpopt/xforms/CXformIntersectAll2LeftSemiJoin.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformDifferenceAll2LeftAntiSemiJoin::CXformDifferenceAll2LeftAntiSemiJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformDifferenceAll2LeftAntiSemiJoin::CXformDifferenceAll2LeftAntiSemiJoin
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
					GPOS_NEW(pmp) CLogicalDifferenceAll(pmp),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternMultiLeaf(pmp))
					)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformDifferenceAll2LeftAntiSemiJoin::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformDifferenceAll2LeftAntiSemiJoin::Transform
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

	// TODO: , Jan 8th 2013, we currently only handle difference all
	//  operators with two children
	GPOS_ASSERT(2 == pexpr->UlArity());

	// extract components
	CExpression *pexprLeftChild = (*pexpr)[0];
	CExpression *pexprRightChild = (*pexpr)[1];

	CLogicalDifferenceAll *popDifferenceAll = CLogicalDifferenceAll::PopConvert(pexpr->Pop());
	DrgDrgPcr *pdrgpdrgpcrInput = popDifferenceAll->PdrgpdrgpcrInput();

	CExpression *pexprLeftWindow = CXformUtils::PexprWindowWithRowNumber(pmp, pexprLeftChild, (*pdrgpdrgpcrInput)[0]);
	CExpression *pexprRightWindow = CXformUtils::PexprWindowWithRowNumber(pmp, pexprRightChild, (*pdrgpdrgpcrInput)[1]);

	DrgDrgPcr *pdrgpdrgpcrInputNew = GPOS_NEW(pmp) DrgDrgPcr(pmp);
	DrgPcr *pdrgpcrLeftNew = CUtils::PdrgpcrExactCopy(pmp, (*pdrgpdrgpcrInput)[0]);
	pdrgpcrLeftNew->Append(CXformUtils::PcrProjectElement(pexprLeftWindow, 0 /* row_number window function*/));

	DrgPcr *pdrgpcrRightNew = CUtils::PdrgpcrExactCopy(pmp, (*pdrgpdrgpcrInput)[1]);
	pdrgpcrRightNew->Append(CXformUtils::PcrProjectElement(pexprRightWindow, 0 /* row_number window function*/));

	pdrgpdrgpcrInputNew->Append(pdrgpcrLeftNew);
	pdrgpdrgpcrInputNew->Append(pdrgpcrRightNew);

	// generate the scalar condition for the left anti-semi join
	CExpression *pexprScCond = CUtils::PexprConjINDFCond(pmp, pdrgpdrgpcrInputNew);

	// assemble the new left anti-semi join logical operator
	CExpression *pexprLASJ = GPOS_NEW(pmp) CExpression
										(
										pmp,
										GPOS_NEW(pmp) CLogicalLeftAntiSemiJoin(pmp),
										pexprLeftWindow,
										pexprRightWindow,
										pexprScCond
										);

	// clean up
	pdrgpdrgpcrInputNew->Release();

	// add alternative to results
	pxfres->Add(pexprLASJ);
}

// EOF
