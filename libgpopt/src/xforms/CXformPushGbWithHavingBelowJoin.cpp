//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformPushGbWithHavingBelowJoin.cpp
//
//	@doc:
//		Implementation of pushing group by below join transform
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformPushGbWithHavingBelowJoin.h"
#include "gpopt/xforms/CXformUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbWithHavingBelowJoin::CXformPushGbWithHavingBelowJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformPushGbWithHavingBelowJoin::CXformPushGbWithHavingBelowJoin
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
			GPOS_NEW(pmp) CLogicalSelect(pmp),
			GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalGbAgg(pmp),
				GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalInnerJoin(pmp),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)),  // join outer child
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // join inner child
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp)) // join predicate
					),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))	// scalar project list
				),
			GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))	// Having clause
			)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbWithHavingBelowJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		we only push down global aggregates
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformPushGbWithHavingBelowJoin::Exfp
	(
	CExpressionHandle & // exprhdl
	)
	const
{
	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbWithHavingBelowJoin::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformPushGbWithHavingBelowJoin::Transform
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

	CExpression *pexprGb = (*pexpr)[0];
	CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(pexprGb->Pop());
	if (!popGbAgg->FGlobal())
	{
		// xform only applies to global aggs
		return;
	}

	CExpression *pexprResult = CXformUtils::PexprPushGbBelowJoin(pmp, pexpr);

	if (NULL != pexprResult)
	{
		// add alternative to results
		pxfres->Add(pexprResult);
	}
}


// EOF

