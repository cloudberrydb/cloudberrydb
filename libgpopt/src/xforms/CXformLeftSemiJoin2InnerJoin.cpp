//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformLeftSemiJoin2InnerJoin.cpp
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
#include "gpopt/xforms/CXformLeftSemiJoin2InnerJoin.h"



using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiJoin2InnerJoin::CXformLeftSemiJoin2InnerJoin
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXformLeftSemiJoin2InnerJoin::CXformLeftSemiJoin2InnerJoin
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
//		CXformLeftSemiJoin2InnerJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftSemiJoin2InnerJoin::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	if (exprhdl.FHasOuterRefs() || exprhdl.Pdpscalar(2)->FHasSubquery())
	{
		return ExfpNone;
	}

	CColRefSet *pcrsInnerOutput = exprhdl.Pdprel(1 /*ulChildIndex*/)->PcrsOutput();
	CExpression *pexprScalar = exprhdl.PexprScalarChild(2 /*ulChildIndex*/);
	CAutoMemoryPool amp;

	// examine join predicate to determine xform applicability
	if (!CPredicateUtils::FSimpleEqualityUsingCols(amp.Pmp(), pexprScalar, pcrsInnerOutput))
	{
		return ExfpNone;
	}

	return ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiJoin2InnerJoin::Transform
//
//	@doc:
//		actual transformation
//
//---------------------------------------------------------------------------
void
CXformLeftSemiJoin2InnerJoin::Transform
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

	// construct grouping columns by collecting used columns in the join predicate
	// that come from join's inner child
	CColRefSet *pcrsOuterOutput = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput();
	CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexprScalar->PdpDerive())->PcrsUsed();
	CColRefSet *pcrsGb = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsGb->Include(pcrsUsed);
	pcrsGb->Difference(pcrsOuterOutput);
	GPOS_ASSERT(0 < pcrsGb->CElements());

	CKeyCollection *pkc = CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->Pkc();
	if (NULL == pkc ||
		(NULL != pkc && !pkc->FKey(pcrsGb, false /*fExactMatch*/)))
	{
		// grouping columns do not cover a key on the inner side,
		// we need to create a group by on inner side
		DrgPcr *pdrgpcr = pcrsGb->Pdrgpcr(pmp);
		CExpression *pexprGb =
			GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalGbAgg(pmp, pdrgpcr, COperator::EgbaggtypeGlobal /*egbaggtype*/),
				pexprInner,
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp))
				);
		pexprInner = pexprGb;
	}

	CExpression *pexprInnerJoin =
		CUtils::PexprLogicalJoin<CLogicalInnerJoin>(pmp, pexprOuter, pexprInner, pexprScalar);

	pcrsGb->Release();
	pxfres->Add(pexprInnerJoin);
}


// EOF
