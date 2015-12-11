//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformCTEAnchor2Sequence.cpp
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
#include "gpopt/xforms/CXformCTEAnchor2Sequence.h"
#include "gpopt/xforms/CXformUtils.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformCTEAnchor2Sequence::CXformCTEAnchor2Sequence
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformCTEAnchor2Sequence::CXformCTEAnchor2Sequence
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
				GPOS_NEW(pmp) CLogicalCTEAnchor(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformCTEAnchor2Sequence::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformCTEAnchor2Sequence::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	ULONG ulId = CLogicalCTEAnchor::PopConvert(exprhdl.Pop())->UlId();
	const ULONG ulConsumers = COptCtxt::PoctxtFromTLS()->Pcteinfo()->UlConsumers(ulId);
	GPOS_ASSERT(0 < ulConsumers);

	if (1 == ulConsumers && CXformUtils::FInlinableCTE(ulId))
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformCTEAnchor2Sequence::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformCTEAnchor2Sequence::Transform
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

	CLogicalCTEAnchor *popCTEAnchor = CLogicalCTEAnchor::PopConvert(pexpr->Pop());
	IMemoryPool *pmp = pxfctxt->Pmp();

	ULONG ulId = popCTEAnchor->UlId();

	CExpression *pexprProducer = COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(ulId);
	GPOS_ASSERT(NULL != pexprProducer);

	pexprProducer->AddRef();

	// child of CTE anchor
	CExpression *pexprChild = (*pexpr)[0];
	pexprChild->AddRef();

	// create logical sequence
	CExpression *pexprSequence =
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CLogicalSequence(pmp),
			pexprProducer,
			pexprChild
			);

	pxfres->Add(pexprSequence);
}

// EOF
