//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformPushGbBelowJoin.cpp
//
//	@doc:
//		Implementation of pushing group by below join transform
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
#include "gpopt/xforms/CXformPushGbBelowJoin.h"
#include "gpopt/xforms/CXformUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbBelowJoin::CXformPushGbBelowJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformPushGbBelowJoin::CXformPushGbBelowJoin
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
			)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbBelowJoin::CXformPushGbBelowJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformPushGbBelowJoin::CXformPushGbBelowJoin
	(
	CExpression *pexprPattern
	)
	:
	CXformExploration(pexprPattern)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbBelowJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		we only push down global aggregates
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformPushGbBelowJoin::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(exprhdl.Pop());
	if (!popGbAgg->FGlobal())
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbBelowJoin::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformPushGbBelowJoin::Transform
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

	CExpression *pexprResult = CXformUtils::PexprPushGbBelowJoin(pmp, pexpr);

	if (NULL != pexprResult)
	{
		// add alternative to results
		pxfres->Add(pexprResult);
	}
}


// EOF

