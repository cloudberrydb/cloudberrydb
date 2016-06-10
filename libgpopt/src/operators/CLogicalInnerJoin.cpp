//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalInnerJoin.cpp
//
//	@doc:
//		Implementation of inner join operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/operators/CPredicateUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerJoin::CLogicalInnerJoin
//
//	@doc:
//		ctor
//		Note: 04/09/2009 - ; so far inner join doesn't have any specific
//			members, hence, no need for a separate pattern ctor
//
//---------------------------------------------------------------------------
CLogicalInnerJoin::CLogicalInnerJoin
	(
	IMemoryPool *pmp
	)
	:
	CLogicalJoin(pmp)
{
	GPOS_ASSERT(NULL != pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerJoin::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalInnerJoin::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	return CLogical::Maxcard(exprhdl, 2 /*ulScalarIndex*/, MaxcardDef(exprhdl));
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerJoin::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalInnerJoin::PxfsCandidates
	(
	IMemoryPool *pmp
	) 
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	
	(void) pxfs->FExchangeSet(CXform::ExfInnerJoin2NLJoin);
	(void) pxfs->FExchangeSet(CXform::ExfInnerJoin2HashJoin);
	(void) pxfs->FExchangeSet(CXform::ExfSubqJoin2Apply);
	(void) pxfs->FExchangeSet(CXform::ExfInnerJoin2IndexGetApply);
	(void) pxfs->FExchangeSet(CXform::ExfInnerJoin2DynamicIndexGetApply);
	(void) pxfs->FExchangeSet(CXform::ExfInnerJoin2PartialDynamicIndexGetApply);
	(void) pxfs->FExchangeSet(CXform::ExfInnerJoin2BitmapIndexGetApply);
	(void) pxfs->FExchangeSet(CXform::ExfInnerJoinWithInnerSelect2IndexGetApply);
	(void) pxfs->FExchangeSet(CXform::ExfInnerJoinWithInnerSelect2DynamicIndexGetApply);
	(void) pxfs->FExchangeSet(CXform::ExfInnerJoinWithInnerSelect2PartialDynamicIndexGetApply);
	(void) pxfs->FExchangeSet(CXform::ExfInnerJoin2DynamicBitmapIndexGetApply);
	(void) pxfs->FExchangeSet(CXform::ExfInnerJoinWithInnerSelect2BitmapIndexGetApply);
	(void) pxfs->FExchangeSet(CXform::ExfInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply);

	(void) pxfs->FExchangeSet(CXform::ExfJoinCommutativity);
	(void) pxfs->FExchangeSet(CXform::ExfJoinAssociativity);
	(void) pxfs->FExchangeSet(CXform::ExfInnerJoinSemiJoinSwap);
	(void) pxfs->FExchangeSet(CXform::ExfInnerJoinAntiSemiJoinSwap);
	(void) pxfs->FExchangeSet(CXform::ExfInnerJoinAntiSemiJoinNotInSwap);
	
	return pxfs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerJoin::FFewerConj
//
//	@doc:
//		Compare two innerJoin group expressions, test whether the first one
//		has less join predicates than the second one. This is used to
//		prioritize innerJoin with less predicates for stats derivation
//
//---------------------------------------------------------------------------
BOOL
CLogicalInnerJoin::FFewerConj
	(
	IMemoryPool *pmp,
	CGroupExpression *pgexprFst,
	CGroupExpression *pgexprSnd
	)
{
	if (NULL == pgexprFst || NULL == pgexprSnd)
	{
		return false;
	}

	if (COperator::EopLogicalInnerJoin != pgexprFst->Pop()->Eopid() ||
		COperator::EopLogicalInnerJoin != pgexprSnd->Pop()->Eopid())
	{
		return false;
	}

	// third child must be the group for join conditions
	CGroup *pgroupScalarFst = (*pgexprFst)[2];
	CGroup *pgroupScalarSnd = (*pgexprSnd)[2];
	GPOS_ASSERT(pgroupScalarFst->FScalar());
	GPOS_ASSERT(pgroupScalarSnd->FScalar());

	DrgPexpr *pdrgpexprConjFst = CPredicateUtils::PdrgpexprConjuncts(pmp, pgroupScalarFst->PexprScalar());
	DrgPexpr *pdrgpexprConjSnd = CPredicateUtils::PdrgpexprConjuncts(pmp, pgroupScalarSnd->PexprScalar());

	ULONG ulConjFst = pdrgpexprConjFst->UlLength();
	ULONG ulConjSnd = pdrgpexprConjSnd->UlLength();

	pdrgpexprConjFst->Release();
	pdrgpexprConjSnd->Release();

	return ulConjFst < ulConjSnd;
}

// EOF

