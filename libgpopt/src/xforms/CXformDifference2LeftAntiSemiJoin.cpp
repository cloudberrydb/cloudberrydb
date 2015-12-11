//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformDifference2LeftAntiSemiJoin.cpp
//
//	@doc:
//		Implementation of the transformation that takes a logical difference and
//		converts it into an aggregate over a left anti-semi join
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/exception.h"
#include "gpopt/xforms/CXformDifference2LeftAntiSemiJoin.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/operators/ops.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformDifference2LeftAntiSemiJoin::CXformDifference2LeftAntiSemiJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformDifference2LeftAntiSemiJoin::CXformDifference2LeftAntiSemiJoin
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
					GPOS_NEW(pmp) CLogicalDifference(pmp),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternMultiLeaf(pmp))
					)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformDifference2LeftAntiSemiJoin::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformDifference2LeftAntiSemiJoin::Transform
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

	// TODO: Oct 24th 2012, we currently only handle difference all
	//  operators with two children
	GPOS_ASSERT(2 == pexpr->UlArity());

	// extract components
	CExpression *pexprLeftChild = (*pexpr)[0];
	CExpression *pexprRightChild = (*pexpr)[1];

	CLogicalDifference *popDifference = CLogicalDifference::PopConvert(pexpr->Pop());
	DrgPcr *pdrgpcrOutput = popDifference->PdrgpcrOutput();
	DrgDrgPcr *pdrgpdrgpcrInput = popDifference->PdrgpdrgpcrInput();

	// generate the scalar condition for the left anti-semi join
	CExpression *pexprScCond = CUtils::PexprConjINDFCond(pmp, pdrgpdrgpcrInput);

	pexprLeftChild->AddRef();
	pexprRightChild->AddRef();

	// assemble the new left anti-semi join logical operator
	CExpression *pexprLASJ = GPOS_NEW(pmp) CExpression
										(
										pmp,
										GPOS_NEW(pmp) CLogicalLeftAntiSemiJoin(pmp),
										pexprLeftChild,
										pexprRightChild,
										pexprScCond
										);

	// assemble the aggregate operator
	pdrgpcrOutput->AddRef();

	CExpression *pexprProjList = GPOS_NEW(pmp) CExpression
											(
											pmp,
											GPOS_NEW(pmp) CScalarProjectList(pmp),
											GPOS_NEW(pmp) DrgPexpr(pmp)
											);

	CExpression *pexprAgg = CUtils::PexprLogicalGbAggGlobal(pmp, pdrgpcrOutput, pexprLASJ, pexprProjList);

	// add alternative to results
	pxfres->Add(pexprAgg);
}

// EOF
