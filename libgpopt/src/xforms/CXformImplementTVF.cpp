//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementTVF.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/xforms/CXformImplementTVF.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementTVF::CXformImplementTVF
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementTVF::CXformImplementTVF
	(
	IMemoryPool *pmp
	)
	:
	CXformImplementation
		(
		 // pattern
		GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalTVF(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternMultiLeaf(pmp))
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementTVF::CXformImplementTVF
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementTVF::CXformImplementTVF
	(
	CExpression *pexpr
	)
	:
	CXformImplementation(pexpr)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementTVF::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformImplementTVF::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	const ULONG ulArity = exprhdl.UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		if (exprhdl.Pdpscalar(ul)->FHasSubquery())
		{
			// xform is inapplicable if TVF argument is a subquery
			return CXform::ExfpNone;
		}
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementTVF::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementTVF::Transform
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

	CLogicalTVF *popTVF = CLogicalTVF::PopConvert(pexpr->Pop());
	IMemoryPool *pmp = pxfctxt->Pmp();

	// create/extract components for alternative
	IMDId *pmdidFunc = popTVF->PmdidFunc();
	pmdidFunc->AddRef();

	IMDId *pmdidRetType = popTVF->PmdidRetType();
	pmdidRetType->AddRef();

	CWStringConst *pstr = GPOS_NEW(pmp) CWStringConst(popTVF->Pstr()->Wsz());

	DrgPcoldesc *pdrgpcoldesc = popTVF->Pdrgpcoldesc();
	pdrgpcoldesc->AddRef();

	DrgPcr *pdrgpcrOutput = popTVF->PdrgpcrOutput();
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrs->Include(pdrgpcrOutput);

	DrgPexpr *pdrgpexpr = pexpr->PdrgPexpr();

	CPhysicalTVF *pphTVF = GPOS_NEW(pmp) CPhysicalTVF(pmp, pmdidFunc, pmdidRetType, pstr, pdrgpcoldesc, pcrs);

	CExpression *pexprAlt = NULL;
	// create alternative expression
	if(NULL == pdrgpexpr || 0 == pdrgpexpr->UlLength())
	{
		pexprAlt = GPOS_NEW(pmp) CExpression(pmp, pphTVF);
	}
	else
	{
		pdrgpexpr->AddRef();
		pexprAlt = GPOS_NEW(pmp) CExpression(pmp, pphTVF, pdrgpexpr);
	}

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}


// EOF

