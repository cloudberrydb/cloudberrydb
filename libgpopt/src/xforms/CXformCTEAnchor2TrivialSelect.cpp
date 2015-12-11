//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformCTEAnchor2TrivialSelect.cpp
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
#include "gpopt/xforms/CXformCTEAnchor2TrivialSelect.h"
#include "gpopt/xforms/CXformUtils.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformCTEAnchor2TrivialSelect::CXformCTEAnchor2TrivialSelect
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformCTEAnchor2TrivialSelect::CXformCTEAnchor2TrivialSelect
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
//		CXformCTEAnchor2TrivialSelect::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformCTEAnchor2TrivialSelect::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	ULONG ulId = CLogicalCTEAnchor::PopConvert(exprhdl.Pop())->UlId();
	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();
	const ULONG ulConsumers = pcteinfo->UlConsumers(ulId);
	GPOS_ASSERT(0 < ulConsumers);

	if ((pcteinfo->FEnableInlining() || 1 == ulConsumers) &&
		CXformUtils::FInlinableCTE(ulId))
	{
		return CXform::ExfpHigh;
	}

	return CXform::ExfpNone;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformCTEAnchor2TrivialSelect::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformCTEAnchor2TrivialSelect::Transform
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

	// child of CTE anchor
	CExpression *pexprChild = (*pexpr)[0];
	pexprChild->AddRef();

	CExpression *pexprSelect =
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CLogicalSelect(pmp),
			pexprChild,
			CUtils::PexprScalarConstBool(pmp, true /*fValue*/)
			);

	pxfres->Add(pexprSelect);
}

// EOF
