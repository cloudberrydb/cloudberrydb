//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalSelect.cpp
//
//	@doc:
//		Implementation of select operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/COptCtxt.h"

#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalSelect.h"
#include "gpopt/operators/CPatternTree.h"
#include "gpopt/operators/CPredicateUtils.h"

#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSelect::CLogicalSelect
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalSelect::CLogicalSelect
	(
	IMemoryPool *pmp
	)
	:
	CLogicalUnary(pmp)
{}

	
//---------------------------------------------------------------------------
//	@function:
//		CLogicalSelect::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalSelect::PcrsDeriveOutput
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
{
	return PcrsDeriveOutputPassThru(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSelect::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalSelect::PkcDeriveKeys
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	return PkcDeriveKeysPassThru(exprhdl, 0 /* ulChild */);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSelect::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalSelect::PxfsCandidates
	(
	IMemoryPool *pmp
	) 
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);

	(void) pxfs->FExchangeSet(CXform::ExfSelect2Apply);
	(void) pxfs->FExchangeSet(CXform::ExfRemoveSubqDistinct);
	(void) pxfs->FExchangeSet(CXform::ExfInlineCTEConsumerUnderSelect);
	(void) pxfs->FExchangeSet(CXform::ExfPushGbWithHavingBelowJoin);
	(void) pxfs->FExchangeSet(CXform::ExfSelect2IndexGet);
	(void) pxfs->FExchangeSet(CXform::ExfSelect2DynamicIndexGet);
	(void) pxfs->FExchangeSet(CXform::ExfSelect2PartialDynamicIndexGet);
	(void) pxfs->FExchangeSet(CXform::ExfSelect2BitmapBoolOp);
	(void) pxfs->FExchangeSet(CXform::ExfSelect2DynamicBitmapBoolOp);
	(void) pxfs->FExchangeSet(CXform::ExfSimplifySelectWithSubquery);
	(void) pxfs->FExchangeSet(CXform::ExfSelect2Filter);

	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSelect::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalSelect::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	// in case of a false condition or a contradiction, maxcard should be zero
	CExpression *pexprScalar = exprhdl.PexprScalarChild(1);
	if ((NULL != pexprScalar && CUtils::FScalarConstFalse(pexprScalar)) ||
		CDrvdPropRelational::Pdprel(exprhdl.Pdp())->Ppc()->FContradiction())
	{
		return CMaxCard(0 /*ull*/);
	}

	// pass on max card of first child
	return exprhdl.Pdprel(0)->Maxcard();
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSelect::PstatsDerive
//
//	@doc:
//		Derive statistics based on filter predicates
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalSelect::PstatsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	DrgPstat *pdrgpstatCtxt
	)
	const
{
	GPOS_ASSERT(Esp(exprhdl) > EspNone);
	IStatistics *pstatsChild = exprhdl.Pstats(0);

	if (exprhdl.Pdpscalar(1 /*ulChildIndex*/)->FHasSubquery())
	{
		// in case of subquery in select predicate, we return child stats
		pstatsChild->AddRef();
		return pstatsChild;
	}

	// remove implied predicates from selection condition to avoid cardinality under-estimation
	CExpression *pexprScalar = exprhdl.PexprScalarChild(1 /*ulChildIndex*/);
	CExpression *pexprPredicate = CPredicateUtils::PexprRemoveImpliedConjuncts(pmp, pexprScalar, exprhdl);


	// split selection predicate into local predicate and predicate involving outer references
	CExpression *pexprLocal = NULL;
	CExpression *pexprOuterRefs = NULL;

	// get outer references from expression handle
	CColRefSet *pcrsOuter = exprhdl.Pdprel()->PcrsOuter();

	CPredicateUtils::SeparateOuterRefs(pmp, pexprPredicate, pcrsOuter, &pexprLocal, &pexprOuterRefs);
	pexprPredicate->Release();

	IStatistics *pstats = CStatisticsUtils::PstatsFilter(pmp, exprhdl, pstatsChild, pexprLocal, pexprOuterRefs, pdrgpstatCtxt);
	pexprLocal->Release();
	pexprOuterRefs->Release();

	return pstats;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSelect::PexprPartPred
//
//	@doc:
//		Compute partition predicate to pass down to n-th child
//
//---------------------------------------------------------------------------
CExpression *
CLogicalSelect::PexprPartPred
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CExpression *, //pexprInput
	ULONG
#ifdef GPOS_DEBUG
	ulChildIndex
#endif //GPOS_DEBUG
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);

	// in case of subquery in select predicate, we cannot extract the whole
	// predicate, and it would not be helpful anyway, so return NULL
	if (exprhdl.Pdpscalar(1 /*ulChildIndex*/)->FHasSubquery())
	{
		return NULL;
	}

	CExpression *pexprScalar = exprhdl.PexprScalarChild(1 /*ulChildIndex*/);
	GPOS_ASSERT(NULL != pexprScalar);

	// get partition keys
	CPartInfo *ppartinfo = exprhdl.Pdprel()->Ppartinfo();
	GPOS_ASSERT(NULL != ppartinfo);

	// we assume that the select is right on top of the dynamic get, so there
	// should be only one part consumer. If there is more, then we are higher up so
	// we do not push down any predicates
	if (1 != ppartinfo->UlConsumers())
	{
		return NULL;
	}

	CExpression *pexprPredOnPartKey = NULL;

	DrgPpartkeys *pdrgppartkeys = ppartinfo->Pdrgppartkeys(0 /*ulPos*/);
	const ULONG ulKeySets = pdrgppartkeys->UlLength();
	for (ULONG ul = 0; NULL == pexprPredOnPartKey && ul < ulKeySets; ul++)
	{
		pexprPredOnPartKey = CPredicateUtils::PexprExtractPredicatesOnPartKeys
												(
												pmp,
												pexprScalar,
												(*pdrgppartkeys)[ul]->Pdrgpdrgpcr(),
												NULL, //pcrsAllowedRefs
												true //fUseConstraints
												);
	}

	return pexprPredOnPartKey;
}

// EOF

