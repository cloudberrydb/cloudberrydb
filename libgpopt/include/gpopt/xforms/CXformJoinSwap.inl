//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformJoinSwap.inl
//
//	@doc:
//		Join swap transformation
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
#include "gpopt/xforms/CXformUtils.h"


using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformJoinSwap::CXformJoinSwap
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
template<class TJoinTop, class TJoinBottom>
CXformJoinSwap<TJoinTop, TJoinBottom>::CXformJoinSwap
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
					GPOS_NEW(pmp) TJoinTop(pmp),
					GPOS_NEW(pmp) CExpression  // left child is a join tree
					 	 (
					 	 pmp,
					 	 GPOS_NEW(pmp) TJoinBottom(pmp),
					 	 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // left child
					 	 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // right child
					 	 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)) // predicate
					 	 ),
					GPOS_NEW(pmp) CExpression // right child is a pattern leaf
								(
								pmp,
								GPOS_NEW(pmp) CPatternLeaf(pmp)
								),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)) // top-join predicate
					)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformJoinSwap::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
template<class TJoinTop, class TJoinBottom>
void
CXformJoinSwap<TJoinTop, TJoinBottom>::Transform
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

	CExpression *pexprResult = CXformUtils::PexprSwapJoins(pmp, pexpr, (*pexpr)[0]);
	if (NULL == pexprResult)
	{
		return;
	}

	pxfres->Add(pexprResult);
}


// EOF

