//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformInlineCTEConsumerUnderSelect.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/xforms/CXformInlineCTEConsumerUnderSelect.h"
#include "gpopt/xforms/CXformUtils.h"

#include "gpopt/operators/ops.h"
#include "gpopt/operators/CNormalizer.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformInlineCTEConsumerUnderSelect::CXformInlineCTEConsumerUnderSelect
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformInlineCTEConsumerUnderSelect::CXformInlineCTEConsumerUnderSelect
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
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalCTEConsumer(pmp)),  // relational child
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))	// predicate tree
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformInlineCTEConsumerUnderSelect::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformInlineCTEConsumerUnderSelect::Exfp
	(
	CExpressionHandle & //exprhdl
	)
	const
{
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformInlineCTEConsumerUnderSelect::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformInlineCTEConsumerUnderSelect::Transform
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

	CExpression *pexprConsumer = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];

	CLogicalCTEConsumer *popConsumer = CLogicalCTEConsumer::PopConvert(pexprConsumer->Pop());
	ULONG ulId = popConsumer->UlCTEId();
	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();
	// only continue if inlining is enabled or if this CTE has only 1 consumer
	if (!pcteinfo->FEnableInlining() && 1 < pcteinfo->UlConsumers(ulId))
	{
		return;
	}

	// don't push down selects with a const true or false, in case we end up
	// with a select(1) (coming from the anchor) right on top of the consumer
	if (CUtils::FScalarConstTrue(pexprScalar) || CUtils::FScalarConstFalse(pexprScalar) ||
		!CXformUtils::FInlinableCTE(ulId))
	{
		return;
	}

	IMemoryPool *pmp = pxfctxt->Pmp();

	// inline consumer
	GPOS_ASSERT(NULL != popConsumer->Phmulcr());
	CExpression *pexprInlinedConsumer = popConsumer->PexprInlined();
	pexprInlinedConsumer->AddRef();
	pexprScalar->AddRef();

	CExpression *pexprSelect = CUtils::PexprLogicalSelect(pmp, pexprInlinedConsumer, pexprScalar);

	CExpression *pexprNormalized = CNormalizer::PexprNormalize(pmp, pexprSelect);
	pexprSelect->Release();

	// add alternative to xform result
	pxfres->Add(pexprNormalized);
}

// EOF
