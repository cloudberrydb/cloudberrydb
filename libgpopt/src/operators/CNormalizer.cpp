//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2012 EMC Corp.
//
//	@filename:
//		CNormalizer.cpp
//
//	@doc:
//		Implementation of expression tree normalizer
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

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/ops.h"

using namespace gpopt;


// initialization of push through handler
const CNormalizer::SPushThru CNormalizer::m_rgpt[] =
{
	{COperator::EopLogicalSelect, PushThruSelect},
	{COperator::EopLogicalProject, PushThruUnaryWithScalarChild},
	{COperator::EopLogicalSequenceProject, PushThruSeqPrj},
	{COperator::EopLogicalGbAgg, PushThruUnaryWithScalarChild},
	{COperator::EopLogicalCTEAnchor, PushThruUnaryWithoutScalarChild},
	{COperator::EopLogicalUnion, PushThruSetOp},
	{COperator::EopLogicalUnionAll, PushThruSetOp},
	{COperator::EopLogicalIntersect, PushThruSetOp},
	{COperator::EopLogicalIntersectAll, PushThruSetOp},
	{COperator::EopLogicalDifference, PushThruSetOp},
	{COperator::EopLogicalDifferenceAll, PushThruSetOp},
	{COperator::EopLogicalInnerJoin,PushThruJoin},
	{COperator::EopLogicalNAryJoin, PushThruJoin},
	{COperator::EopLogicalInnerApply, PushThruJoin},
	{COperator::EopLogicalInnerCorrelatedApply, PushThruJoin},
	{COperator::EopLogicalLeftOuterJoin, PushThruJoin},
	{COperator::EopLogicalLeftOuterApply, PushThruJoin},
	{COperator::EopLogicalLeftOuterCorrelatedApply, PushThruJoin},
	{COperator::COperator::EopLogicalLeftSemiApply, PushThruJoin},
	{COperator::COperator::EopLogicalLeftSemiApplyIn, PushThruJoin},
	{COperator::COperator::EopLogicalLeftSemiCorrelatedApplyIn, PushThruJoin},
	{COperator::COperator::EopLogicalLeftAntiSemiApply, PushThruJoin},
	{COperator::COperator::EopLogicalLeftAntiSemiApplyNotIn, PushThruJoin},
	{COperator::COperator::EopLogicalLeftAntiSemiCorrelatedApplyNotIn, PushThruJoin},
};


//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::FPushThruOuterChild
//
//	@doc:
//		Check if we should push predicates through expression's outer child
//
//
//---------------------------------------------------------------------------
BOOL
CNormalizer::FPushThruOuterChild
	(
	CExpression *pexprLogical
	)
{
	GPOS_ASSERT(NULL != pexprLogical);

	COperator::EOperatorId eopid = pexprLogical->Pop()->Eopid();

	return
		COperator::EopLogicalLeftOuterJoin == eopid ||
		COperator::EopLogicalLeftOuterApply == eopid ||
		COperator::EopLogicalLeftOuterCorrelatedApply == eopid ||
		CUtils::FLeftAntiSemiApply(pexprLogical->Pop()) ||
		CUtils::FLeftSemiApply(pexprLogical->Pop());
}


//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::FPushableThruSeqPrjChild
//
//	@doc:
//		Check if a predicate can be pushed through the child of a sequence
//		project expression
//
//---------------------------------------------------------------------------
BOOL
CNormalizer::FPushableThruSeqPrjChild
	(
	CExpression *pexprSeqPrj,
	CExpression *pexprPred
	)
{
	GPOS_ASSERT(NULL != pexprSeqPrj);
	GPOS_ASSERT(NULL != pexprPred);
	GPOS_ASSERT(CLogical::EopLogicalSequenceProject == pexprSeqPrj->Pop()->Eopid());

	CDistributionSpec *pds = CLogicalSequenceProject::PopConvert(pexprSeqPrj->Pop())->Pds();
	BOOL fPushable = false;
	if (CDistributionSpec::EdtHashed == pds->Edt())
	{
		CAutoMemoryPool amp;
		IMemoryPool *pmp = amp.Pmp();
		CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexprPred->PdpDerive())->PcrsUsed();
		CColRefSet *pcrsPartCols = CUtils::PcrsExtractColumns(pmp, CDistributionSpecHashed::PdsConvert(pds)->Pdrgpexpr());
		if (pcrsPartCols->FSubset(pcrsUsed))
		{
			// predicate is pushable if used columns are included in partition-by expression
			fPushable = true;
		}
		pcrsPartCols->Release();
	}

	return fPushable;
}


//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::FPushable
//
//	@doc:
//		Check if a predicate can be pushed through a logical expression
//
//
//---------------------------------------------------------------------------
BOOL
CNormalizer::FPushable
	(
	CExpression *pexprLogical,
	CExpression *pexprPred
	)
{
	GPOS_ASSERT(NULL != pexprLogical);
	GPOS_ASSERT(NULL != pexprPred);

	CColRefSet *pcrsUsed =
		CDrvdPropScalar::Pdpscalar(pexprPred->PdpDerive())->PcrsUsed();
	CColRefSet *pcrsOutput =
		CDrvdPropRelational::Pdprel(pexprLogical->PdpDerive())->PcrsOutput();

	return pcrsOutput->FSubset(pcrsUsed);
}


//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::PexprRecursiveNormalize
//
//	@doc:
//		Call normalizer recursively on children array
//
//
//---------------------------------------------------------------------------
CExpression *
CNormalizer::PexprRecursiveNormalize
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	const ULONG ulArity = pexpr->UlArity();
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = PexprNormalize(pmp, (*pexpr)[ul]);
		pdrgpexpr->Append(pexprChild);
	}

	COperator *pop = pexpr->Pop();
	pop->AddRef();

	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::SplitConjunct
//
//	@doc:
//		Split the given conjunct into pushable and unpushable predicates
//
//
//---------------------------------------------------------------------------
void
CNormalizer::SplitConjunct
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CExpression *pexprConj,
	DrgPexpr **ppdrgpexprPushable,
	DrgPexpr **ppdrgpexprUnpushable
	)
{
	GPOS_ASSERT(pexpr->Pop()->FLogical());
	GPOS_ASSERT(pexprConj->Pop()->FScalar());
	GPOS_ASSERT(NULL != ppdrgpexprPushable);
	GPOS_ASSERT(NULL != ppdrgpexprUnpushable);

	// collect pushable predicates from given conjunct
	*ppdrgpexprPushable =  GPOS_NEW(pmp) DrgPexpr(pmp);
	*ppdrgpexprUnpushable =  GPOS_NEW(pmp) DrgPexpr(pmp);
	DrgPexpr *pdrgpexprConjuncts = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprConj);
	const ULONG ulSize = pdrgpexprConjuncts->UlLength();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CExpression *pexprScalar = (*pdrgpexprConjuncts)[ul];
		pexprScalar->AddRef();
		if (FPushable(pexpr, pexprScalar))
		{
			(*ppdrgpexprPushable)->Append(pexprScalar);
		}
		else
		{
			(*ppdrgpexprUnpushable)->Append(pexprScalar);
		}
	}
	pdrgpexprConjuncts->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::PushThruOuterChild
//
//	@doc:
//		Push scalar expression through left outer join children;
//		this only handles the case of a SELECT on top of LEFT OUTER JOIN;
//		pushing down join predicates is handled in PushThruJoin();
//		here, we push predicates of the top SELECT node through LEFT OUTER JOIN's
//		outer child
//
//---------------------------------------------------------------------------
void
CNormalizer::PushThruOuterChild
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CExpression *pexprConj,
	CExpression **ppexprResult
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(FPushThruOuterChild(pexpr));
	GPOS_ASSERT(NULL != pexprConj);
	GPOS_ASSERT(NULL != ppexprResult);

	if (0 == pexpr->UlArity())
	{
		// end recursion early for leaf patterns extracted from memo
		pexpr->AddRef();
		pexprConj->AddRef();
		*ppexprResult = CUtils::PexprSafeSelect(pmp, pexpr, pexprConj);

		return;
	}

	CExpression *pexprOuter = (*pexpr)[0];
	CExpression *pexprInner = (*pexpr)[1];
	CExpression *pexprPred = (*pexpr)[2];

	DrgPexpr *pdrgpexprPushable = NULL;
	DrgPexpr *pdrgpexprUnpushable = NULL;
	SplitConjunct(pmp, pexprOuter, pexprConj, &pdrgpexprPushable, &pdrgpexprUnpushable);

	if (0 < pdrgpexprPushable->UlLength())
	{
		pdrgpexprPushable->AddRef();
		CExpression *pexprNewConj = CPredicateUtils::PexprConjunction(pmp, pdrgpexprPushable);

		// create a new select node on top of the outer child
		pexprOuter->AddRef();
		CExpression *pexprNewSelect =
			GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalSelect(pmp), pexprOuter, pexprNewConj);

		// push predicate through the new select to create a new outer child
		CExpression *pexprNewOuter = NULL;
		PushThru(pmp, pexprNewSelect, pexprNewConj, &pexprNewOuter);
		pexprNewSelect->Release();

		// create a new outer join using the new outer child and the new inner child
		COperator *pop = pexpr->Pop();
		pop->AddRef();
		pexprInner->AddRef();
		pexprPred->AddRef();
		CExpression *pexprNew = GPOS_NEW(pmp) CExpression(pmp, pop, pexprNewOuter, pexprInner, pexprPred);

		// call push down predicates on the new outer join
		CExpression *pexprConstTrue = CUtils::PexprScalarConstBool(pmp, true /*fVal*/);
		PushThru(pmp, pexprNew, pexprConstTrue, ppexprResult);
		pexprConstTrue->Release();
		pexprNew->Release();
	}

	if (0 < pdrgpexprUnpushable->UlLength())
	{
		CExpression *pexprOuterJoin = pexpr;
		if (0 < pdrgpexprPushable->UlLength())
		{
			pexprOuterJoin = *ppexprResult;
			GPOS_ASSERT(NULL != pexprOuterJoin);
		}

		// call push down on the outer join predicates
		CExpression *pexprNew = NULL;
		CExpression *pexprConstTrue = CUtils::PexprScalarConstBool(pmp, true /*fVal*/);
		PushThru(pmp, pexprOuterJoin, pexprConstTrue, &pexprNew);
		if (pexprOuterJoin != pexpr)
		{
			pexprOuterJoin->Release();
		}
		pexprConstTrue->Release();

		// create a SELECT on top of the new outer join
		pdrgpexprUnpushable->AddRef();
		*ppexprResult = PexprSelect(pmp, pexprNew, pdrgpexprUnpushable);
	}

	pdrgpexprPushable->Release();
	pdrgpexprUnpushable->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::FSimplifySelectOnOuterJoin
//
//	@doc:
//		A SELECT on top of LOJ, where SELECT's predicate is NULL-filtering and
//		uses columns from LOJ's inner child, is simplified as Inner-Join
//
//		Example:
//
//			select * from (select * from R left join S on r1=s1) as foo where foo.s1>0;
//
//			is converted to:
//
//			select * from R inner join S on r1=s1 and s1>0;
//
//
//
//---------------------------------------------------------------------------
BOOL
CNormalizer::FSimplifySelectOnOuterJoin
	(
	IMemoryPool *pmp,
	CExpression *pexprOuterJoin,
	CExpression *pexprPred, // selection predicate
	CExpression **ppexprResult
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(COperator::EopLogicalLeftOuterJoin == pexprOuterJoin->Pop()->Eopid());
	GPOS_ASSERT(pexprPred->Pop()->FScalar());
	GPOS_ASSERT(NULL != ppexprResult);

	if (0 == pexprOuterJoin->UlArity())
	{
		// exit early for leaf patterns extracted from memo
		*ppexprResult = NULL;
		return false;
	}

	CExpression *pexprOuterJoinOuterChild = (*pexprOuterJoin)[0];
	CExpression *pexprOuterJoinInnerChild = (*pexprOuterJoin)[1];
	CExpression *pexprOuterJoinPred = (*pexprOuterJoin)[2];

	CColRefSet *pcrsOutput = CDrvdPropRelational::Pdprel(pexprOuterJoinInnerChild->PdpDerive())->PcrsOutput();
	if (!GPOS_FTRACE(EopttraceDisableOuterJoin2InnerJoinRewrite) &&
		CPredicateUtils::FNullRejecting(pmp, pexprPred, pcrsOutput))
	{
		// we have a predicate on top of LOJ that uses LOJ's inner child,
		// if the predicate filters-out nulls, we can add it to the join
		// predicate and turn LOJ into Inner-Join
		pexprOuterJoinOuterChild->AddRef();
		pexprOuterJoinInnerChild->AddRef();

		*ppexprResult = GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalInnerJoin(pmp),
					pexprOuterJoinOuterChild,
					pexprOuterJoinInnerChild,
					CPredicateUtils::PexprConjunction(pmp, pexprPred, pexprOuterJoinPred)
					);

		return true;
	}

	// failed to convert LOJ to inner-join
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::PushThruSelect
//
//	@doc:
//		Push a conjunct through a select
//
//---------------------------------------------------------------------------
void
CNormalizer::PushThruSelect
	(
	IMemoryPool *pmp,
	CExpression *pexprSelect,
	CExpression *pexprConj,
	CExpression **ppexprResult
	)
{
	GPOS_ASSERT(NULL != pexprConj);
	GPOS_ASSERT(NULL != ppexprResult);

	CExpression *pexprLogicalChild = (*pexprSelect)[0];
	CExpression *pexprScalarChild = (*pexprSelect)[1];
	CExpression *pexprPred =  CPredicateUtils::PexprConjunction(pmp, pexprScalarChild, pexprConj);

	if (CUtils::FScalarConstTrue(pexprPred))
	{
		pexprPred->Release();
		*ppexprResult = PexprNormalize(pmp, pexprLogicalChild);

		return;
	}

	COperator::EOperatorId eopid = pexprLogicalChild->Pop()->Eopid();
	if (COperator::EopLogicalLeftOuterJoin == eopid)
	{
		CExpression *pexprSimplified = NULL;
		if (FSimplifySelectOnOuterJoin(pmp, pexprLogicalChild, pexprPred, &pexprSimplified))
		{
			// simplification succeeded, normalize resulting expression
			*ppexprResult = PexprNormalize(pmp, pexprSimplified);
			pexprPred->Release();
			pexprSimplified->Release();

			return;
		}
	}

	if (FPushThruOuterChild(pexprLogicalChild))
	{
		PushThruOuterChild(pmp, pexprLogicalChild, pexprPred, ppexprResult);
	}
	else
	{
		// logical child may not pass all predicates through, we need to collect
		// unpushable predicates, if any, into a top Select node
		DrgPexpr *pdrgpexprConjuncts = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprPred);
		DrgPexpr *pdrgpexprRemaining = NULL;
		CExpression *pexpr = NULL;
		PushThru(pmp, pexprLogicalChild, pdrgpexprConjuncts, &pexpr, &pdrgpexprRemaining);
		*ppexprResult = PexprSelect(pmp, pexpr, pdrgpexprRemaining);
		pdrgpexprConjuncts->Release();
	}

	pexprPred->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::PexprSelect
//
//	@doc:
//		Return a Select expression, if needed, with a scalar condition made of
//		given array of conjuncts
//
//---------------------------------------------------------------------------
CExpression *
CNormalizer::PexprSelect
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	DrgPexpr *pdrgpexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pdrgpexpr);

	if (0 == pdrgpexpr->UlLength())
	{
		// no predicate, return given expression
		pdrgpexpr->Release();
		return pexpr;
	}

	// result expression is a select over predicates
	CExpression *pexprConjunction = CPredicateUtils::PexprConjunction(pmp, pdrgpexpr);
	CExpression *pexprSelect = CUtils::PexprSafeSelect(pmp, pexpr, pexprConjunction);
	if (COperator::EopLogicalSelect != pexprSelect->Pop()->Eopid())
	{
		// Select node was pruned, return created expression
		return pexprSelect;
	}

	CExpression *pexprLogicalChild = (*pexprSelect)[0];
	COperator::EOperatorId eopidChild = pexprLogicalChild->Pop()->Eopid();
	if (COperator::EopLogicalLeftOuterJoin != eopidChild)
	{
		// child of Select is not an outer join, return created Select expression
		return pexprSelect;
	}

	// we have a Select on top of Outer Join expression, attempt simplifying expression into InnerJoin
	CExpression *pexprSimplified = NULL;
	if (FSimplifySelectOnOuterJoin(pmp, pexprLogicalChild, (*pexprSelect)[1], &pexprSimplified))
	{
		// simplification succeeded, normalize resulting expression
		pexprSelect->Release();
		CExpression *pexprResult = PexprNormalize(pmp, pexprSimplified);
		pexprSimplified->Release();

		return pexprResult;
	}

	// simplification failed, return created Select expression
	return pexprSelect;
}


//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::PushThruUnaryWithoutScalarChild
//
//	@doc:
//		Push a conjunct through unary operator without scalar child
//
//---------------------------------------------------------------------------
void
CNormalizer::PushThruUnaryWithoutScalarChild
	(
	IMemoryPool *pmp,
	CExpression *pexprLogical,
	CExpression *pexprConj,
	CExpression **ppexprResult
	)
{
	GPOS_ASSERT(NULL != pexprLogical);
	GPOS_ASSERT(1 == pexprLogical->UlArity());
	GPOS_ASSERT(NULL != pexprConj);
	GPOS_ASSERT(NULL != ppexprResult);

	// break scalar expression to conjuncts
	DrgPexpr *pdrgpexprConjuncts = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprConj);

	// get logical child
	CExpression *pexprLogicalChild = (*pexprLogical)[0];

	// push conjuncts through the logical child
	CExpression *pexprNewLogicalChild = NULL;
	DrgPexpr *pdrgpexprUnpushable = NULL;
	PushThru(pmp, pexprLogicalChild, pdrgpexprConjuncts, &pexprNewLogicalChild, &pdrgpexprUnpushable);
	pdrgpexprConjuncts->Release();

	// create a new logical expression based on recursion results
	COperator *pop = pexprLogical->Pop();
	pop->AddRef();
	CExpression *pexprNewLogical = GPOS_NEW(pmp) CExpression(pmp, pop, pexprNewLogicalChild);
	*ppexprResult = PexprSelect(pmp, pexprNewLogical, pdrgpexprUnpushable);
}


//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::PushThruUnaryWithScalarChild
//
//	@doc:
//		Push a conjunct through a unary operator with scalar child
//
//---------------------------------------------------------------------------
void
CNormalizer::PushThruUnaryWithScalarChild
	(
	IMemoryPool *pmp,
	CExpression *pexprLogical,
	CExpression *pexprConj,
	CExpression **ppexprResult
	)
{
	GPOS_ASSERT(NULL != pexprLogical);
	GPOS_ASSERT(2 == pexprLogical->UlArity());
	GPOS_ASSERT(NULL != pexprConj);
	GPOS_ASSERT(NULL != ppexprResult);

	// get logical and scalar children
	CExpression *pexprLogicalChild = (*pexprLogical)[0];
	CExpression *pexprScalarChild = (*pexprLogical)[1];

	// push conjuncts through the logical child
	CExpression *pexprNewLogicalChild = NULL;
	DrgPexpr *pdrgpexprUnpushable = NULL;

	// break scalar expression to conjuncts
	DrgPexpr *pdrgpexprConjuncts = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprConj);

	PushThru(pmp, pexprLogicalChild, pdrgpexprConjuncts, &pexprNewLogicalChild, &pdrgpexprUnpushable);
	pdrgpexprConjuncts->Release();

	// create a new logical expression based on recursion results
	COperator *pop = pexprLogical->Pop();
	pop->AddRef();
	pexprScalarChild->AddRef();
	CExpression *pexprNewLogical = GPOS_NEW(pmp) CExpression(pmp, pop, pexprNewLogicalChild, pexprScalarChild);
	*ppexprResult = PexprSelect(pmp, pexprNewLogical, pdrgpexprUnpushable);
}

//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::SplitConjunctForSeqPrj
//
//	@doc:
//		Split the given conjunct into pushable and unpushable predicates
//		for a sequence project expression
//
//---------------------------------------------------------------------------
void
CNormalizer::SplitConjunctForSeqPrj
	(
	IMemoryPool *pmp,
	CExpression *pexprSeqPrj,
	CExpression *pexprConj,
	DrgPexpr **ppdrgpexprPushable,
	DrgPexpr **ppdrgpexprUnpushable
	)
{
	GPOS_ASSERT(NULL != pexprSeqPrj);
	GPOS_ASSERT(NULL != pexprConj);
	GPOS_ASSERT(NULL != ppdrgpexprPushable);
	GPOS_ASSERT(NULL != ppdrgpexprUnpushable);

	*ppdrgpexprPushable =  GPOS_NEW(pmp) DrgPexpr(pmp);
	*ppdrgpexprUnpushable = GPOS_NEW(pmp) DrgPexpr(pmp);
	DrgPexpr *pdrgpexprPreds = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprConj);
	const ULONG ulPreds = pdrgpexprPreds->UlLength();
	for (ULONG ul = 0; ul < ulPreds; ul++)
	{
		CExpression *pexprPred = (*pdrgpexprPreds)[ul];
		pexprPred->AddRef();
		if (FPushableThruSeqPrjChild(pexprSeqPrj, pexprPred))
		{
			(*ppdrgpexprPushable)->Append(pexprPred);
		}
		else
		{
			(*ppdrgpexprUnpushable)->Append(pexprPred);
		}
	}
	pdrgpexprPreds->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::PushThruSeqPrj
//
//	@doc:
//		Push a conjunct through a sequence project expression
//
//---------------------------------------------------------------------------
void
CNormalizer::PushThruSeqPrj
	(
	IMemoryPool *pmp,
	CExpression *pexprSeqPrj,
	CExpression *pexprConj,
	CExpression **ppexprResult
	)
{
	GPOS_ASSERT(NULL != pexprSeqPrj);
	GPOS_ASSERT(CLogical::EopLogicalSequenceProject == pexprSeqPrj->Pop()->Eopid());
	GPOS_ASSERT(NULL != pexprConj);
	GPOS_ASSERT(NULL != ppexprResult);

	// get logical and scalar children
	CExpression *pexprLogicalChild = (*pexprSeqPrj)[0];
	CExpression *pexprScalarChild = (*pexprSeqPrj)[1];

	// break scalar expression to pushable and unpushable conjuncts
	DrgPexpr *pdrgpexprPushable = NULL;
	DrgPexpr *pdrgpexprUnpushable = NULL;
	SplitConjunctForSeqPrj(pmp, pexprSeqPrj, pexprConj, &pdrgpexprPushable, &pdrgpexprUnpushable);

	CExpression *pexprNewLogicalChild = NULL;
	if (0 < pdrgpexprPushable->UlLength())
	{
		CExpression *pexprPushableConj = CPredicateUtils::PexprConjunction(pmp, pdrgpexprPushable);
		PushThru(pmp, pexprLogicalChild, pexprPushableConj, &pexprNewLogicalChild);
		pexprPushableConj->Release();
	}
	else
	{
		// no pushable predicates on top of sequence project,
		// we still need to process child recursively to push-down child's own predicates
		pdrgpexprPushable->Release();
		pexprNewLogicalChild = PexprNormalize(pmp, pexprLogicalChild);
	}

	// create a new logical expression based on recursion results
	COperator *pop = pexprSeqPrj->Pop();
	pop->AddRef();
	pexprScalarChild->AddRef();
	CExpression *pexprNewLogical = GPOS_NEW(pmp) CExpression(pmp, pop, pexprNewLogicalChild, pexprScalarChild);

	// create a select node for remaining predicates, if any
	*ppexprResult = PexprSelect(pmp, pexprNewLogical, pdrgpexprUnpushable);
}


//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::PushThruSetOp
//
//	@doc:
//		Push a conjunct through set operation
//
//---------------------------------------------------------------------------
void
CNormalizer::PushThruSetOp
	(
	IMemoryPool *pmp,
	CExpression *pexprSetOp,
	CExpression *pexprConj,
	CExpression **ppexprResult
	)
{
	GPOS_ASSERT(NULL != pexprSetOp);
	GPOS_ASSERT(CUtils::FLogicalSetOp(pexprSetOp->Pop()));
	GPOS_ASSERT(NULL != pexprConj);
	GPOS_ASSERT(NULL != ppexprResult);

	CLogicalSetOp *popSetOp = CLogicalSetOp::PopConvert(pexprSetOp->Pop());
	DrgPcr *pdrgpcrOutput = popSetOp->PdrgpcrOutput();
	CColRefSet *pcrsOutput = GPOS_NEW(pmp) CColRefSet(pmp, pdrgpcrOutput);
	DrgDrgPcr *pdrgpdrgpcrInput = popSetOp->PdrgpdrgpcrInput();
	DrgPexpr *pdrgpexprNewChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
	const ULONG ulArity = pexprSetOp->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = (*pexprSetOp)[ul];
		DrgPcr *pdrgpcrChild = (*pdrgpdrgpcrInput)[ul];
		CColRefSet *pcrsChild =  GPOS_NEW(pmp) CColRefSet(pmp, pdrgpcrChild);

		pexprConj->AddRef();
		CExpression *pexprRemappedConj = pexprConj;
		if (!pcrsChild->FEqual(pcrsOutput))
		{
			// child columns are different from SetOp output columns,
			// we need to fix conjunct by mapping output columns to child columns,
			// columns that are not in the output of SetOp child need also to be re-mapped
			// to new columns,
			//
			// for example, if the conjunct looks like 'x > (select max(y) from T)'
			// and the SetOp child produces only column x, we need to create a new
			// conjunct that looks like 'x1 > (select max(y1) from T)'
			// where x1 is a copy of x, and y1 is a copy of y
			//
			// this is achieved by passing (fMustExist = True) flag below, which enforces
			// creating column copies for columns not already in the given map
			HMUlCr *phmulcr = CUtils::PhmulcrMapping(pmp, pdrgpcrOutput, pdrgpcrChild);
			pexprRemappedConj->Release();
			pexprRemappedConj = pexprConj->PexprCopyWithRemappedColumns(pmp, phmulcr, true /*fMustExist*/);
			phmulcr->Release();
		}

		CExpression *pexprNewChild = NULL;
		PushThru(pmp, pexprChild, pexprRemappedConj, &pexprNewChild);
		pdrgpexprNewChildren->Append(pexprNewChild);

		pexprRemappedConj->Release();
		pcrsChild->Release();
	}

	pcrsOutput->Release();
	popSetOp->AddRef();
	*ppexprResult = GPOS_NEW(pmp) CExpression(pmp, popSetOp, pdrgpexprNewChildren);
}


//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::PushThruJoin
//
//	@doc:
//		Push a conjunct through a join
//
//
//---------------------------------------------------------------------------
void
CNormalizer::PushThruJoin
	(
	IMemoryPool *pmp,
	CExpression *pexprJoin,
	CExpression *pexprConj,
	CExpression **ppexprResult
	)
{
	GPOS_ASSERT(NULL != pexprConj);
	GPOS_ASSERT(NULL != ppexprResult);

	COperator *pop = pexprJoin->Pop();
	const ULONG ulArity = pexprJoin->UlArity();
	BOOL fLASApply = CUtils::FLeftAntiSemiApply(pop);
	COperator::EOperatorId eopid = pop->Eopid();
	BOOL fOuterJoin =
		COperator::EopLogicalLeftOuterJoin == eopid ||
		COperator::EopLogicalLeftOuterApply == eopid ||
		COperator::EopLogicalLeftOuterCorrelatedApply == eopid;

	if (fOuterJoin && !CUtils::FScalarConstTrue(pexprConj))
	{
		// whenever possible, push incoming predicate through outer join's outer child,
		// recursion will eventually reach the rest of PushThruJoin() to process join predicates
		PushThruOuterChild(pmp, pexprJoin, pexprConj, ppexprResult);

		return;
	}

	// combine conjunct with join predicate
	CExpression *pexprScalar = (*pexprJoin)[ulArity - 1];
	CExpression *pexprPred =  CPredicateUtils::PexprConjunction(pmp, pexprScalar, pexprConj);

	// break predicate to conjuncts
	DrgPexpr *pdrgpexprConjuncts = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprPred);
	pexprPred->Release();

	// push predicates through children and compute new child expressions
	DrgPexpr *pdrgpexprChildren =  GPOS_NEW(pmp) DrgPexpr(pmp);

	for (ULONG ul = 0; ul < ulArity - 1; ul++)
	{
		CExpression *pexprChild = (*pexprJoin)[ul];
		CExpression *pexprNewChild = NULL;
		if (fLASApply)
		{
			// do not push anti-semi-apply predicates to any of the children
			pexprNewChild = PexprNormalize(pmp, pexprChild);
			pdrgpexprChildren->Append(pexprNewChild);
			continue;
		}

		if (0 == ul && fOuterJoin)
		{
			// do not push outer join predicates through outer child
			// otherwise, we will throw away outer child's tuples that should
			// be part of the join result
			pexprNewChild = PexprNormalize(pmp, pexprChild);
			pdrgpexprChildren->Append(pexprNewChild);
			continue;
		}

		DrgPexpr *pdrgpexprRemaining = NULL;
		PushThru(pmp, pexprChild, pdrgpexprConjuncts, &pexprNewChild, &pdrgpexprRemaining);
		pdrgpexprChildren->Append(pexprNewChild);

		pdrgpexprConjuncts->Release();
		pdrgpexprConjuncts = pdrgpexprRemaining;
	}

	// remaining conjuncts become the new join predicate
	CExpression *pexprNewScalar = CPredicateUtils::PexprConjunction(pmp, pdrgpexprConjuncts);
	pdrgpexprChildren->Append(pexprNewScalar);

	// create a new join expression
	pop->AddRef();
	*ppexprResult = GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexprChildren);
}

//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::FChild
//
//	@doc:
//		Return true if second expression is a child of first expression
//
//---------------------------------------------------------------------------
BOOL
CNormalizer::FChild
	(
	CExpression *pexpr,
	CExpression *pexprChild
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pexprChild);

	BOOL fFound = false;
	const ULONG ulArity = pexpr->UlArity();
	for (ULONG ul = 0; !fFound && ul < ulArity; ul++)
	{
		fFound = ((*pexpr)[ul] == pexprChild);
	}

	return fFound;
}


//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::PushThru
//
//	@doc:
//		Hub for pushing a conjunct through a logical expression
//
//---------------------------------------------------------------------------
void
CNormalizer::PushThru
	(
	IMemoryPool *pmp,
	CExpression *pexprLogical,
	CExpression *pexprConj,
	CExpression **ppexprResult
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexprLogical);
	GPOS_ASSERT(NULL != pexprConj);
	GPOS_ASSERT(NULL != ppexprResult);

	// TODO: 01/13/2012 - ; predicate push down with set returning functions

	if (0 == pexprLogical->UlArity())
	{
		// end recursion early for leaf patterns extracted from memo
		pexprLogical->AddRef();
		pexprConj->AddRef();
		*ppexprResult = CUtils::PexprSafeSelect(pmp, pexprLogical, pexprConj);
		return;
	}

	FnPushThru *pfnpt = NULL;
	COperator::EOperatorId eopid = pexprLogical->Pop()->Eopid();
	const ULONG ulSize = GPOS_ARRAY_SIZE(m_rgpt);
	// find the push thru function corresponding to the given operator
	for (ULONG ul = 0; pfnpt == NULL && ul < ulSize; ul++)
	{
		if (eopid == m_rgpt[ul].m_eopid)
		{
			pfnpt = m_rgpt[ul].m_pfnpt;
		}
	}

	if (NULL != pfnpt)
	{
		pfnpt(pmp, pexprLogical, pexprConj, ppexprResult);
		return;
	}

	// can't push predicates through, start a new normalization path
	CExpression *pexprNormalized = PexprRecursiveNormalize(pmp, pexprLogical);
	*ppexprResult = pexprNormalized;
	if (!FChild(pexprLogical, pexprConj))
	{
		// add select node on top of the result for the given predicate
		pexprConj->AddRef();
		*ppexprResult = CUtils::PexprSafeSelect(pmp, pexprNormalized, pexprConj);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::PushThru
//
//	@doc:
//		Push an array of conjuncts through a logical expression;
//		compute an array of unpushable conjuncts
//
//---------------------------------------------------------------------------
void
CNormalizer::PushThru
	(
	IMemoryPool *pmp,
	CExpression *pexprLogical,
	DrgPexpr *pdrgpexprConjuncts,
	CExpression **ppexprResult,
	DrgPexpr **ppdrgpexprRemaining
	)
{
	GPOS_ASSERT(NULL != pexprLogical);
	GPOS_ASSERT(NULL != pdrgpexprConjuncts);
	GPOS_ASSERT(NULL != ppexprResult);

	DrgPexpr *pdrgpexprPushable =  GPOS_NEW(pmp) DrgPexpr(pmp);
	DrgPexpr *pdrgpexprUnpushable =  GPOS_NEW(pmp) DrgPexpr(pmp);

	const ULONG ulSize = pdrgpexprConjuncts->UlLength();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CExpression *pexprConj = (*pdrgpexprConjuncts)[ul];
		pexprConj->AddRef();
		if (FPushable(pexprLogical, pexprConj))
		{
			pdrgpexprPushable->Append(pexprConj);
		}
		else
		{
			pdrgpexprUnpushable->Append(pexprConj);
		}
	}

	// push through a conjunction of all pushable predicates
	CExpression *pexprPred = CPredicateUtils::PexprConjunction(pmp, pdrgpexprPushable);
	if (FPushThruOuterChild(pexprLogical))
	{
		PushThruOuterChild(pmp, pexprLogical, pexprPred, ppexprResult);
	}
	else
	{
		PushThru(pmp, pexprLogical, pexprPred, ppexprResult);
	}
	pexprPred->Release();

	*ppdrgpexprRemaining = pdrgpexprUnpushable;
}


//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::PexprNormalize
//
//	@doc:
//		Main driver
//
//---------------------------------------------------------------------------
CExpression *
CNormalizer::PexprNormalize
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexpr);

	if (0 == pexpr->UlArity())
	{
		// end recursion early for leaf patterns extracted from memo
		pexpr->AddRef();
		return pexpr;
	}

	CExpression *pexprResult = NULL;
	COperator *pop = pexpr->Pop();
	if (pop->FLogical() && CLogical::PopConvert(pop)->FSelectionOp())
	{
		if (FPushThruOuterChild(pexpr))
		{
			CExpression *pexprConstTrue = CUtils::PexprScalarConstBool(pmp, true /*fVal*/);
			PushThru(pmp, pexpr, pexprConstTrue, &pexprResult);
			pexprConstTrue->Release();
		}
		else
		{
			// add-ref all children except scalar predicate
			const ULONG ulArity = pexpr->UlArity();
			DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
			for (ULONG ul = 0; ul < ulArity - 1; ul++)
			{
				CExpression *pexprChild = (*pexpr)[ul];
				pexprChild->AddRef();
				pdrgpexpr->Append(pexprChild);
			}

			// normalize scalar predicate and construct a new expression
			CExpression *pexprPred = (*pexpr)[pexpr->UlArity() - 1];
			CExpression *pexprPredNormalized = PexprRecursiveNormalize(pmp, pexprPred);
			pdrgpexpr->Append(pexprPredNormalized);
			COperator *pop = pexpr->Pop();
			pop->AddRef();
			CExpression *pexprNew = GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexpr);

			// push normalized predicate through
			PushThru(pmp, pexprNew, pexprPredNormalized, &pexprResult);
			pexprNew->Release();
		}
	}
	else
	{
		pexprResult = PexprRecursiveNormalize(pmp, pexpr);
	}
	GPOS_ASSERT(NULL != pexprResult);

	return pexprResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::PexprPullUpAndCombineProjects
//
//	@doc:
//		Pulls up logical projects as far as possible, and combines consecutive
//		projects if possible
//
//---------------------------------------------------------------------------
CExpression *
CNormalizer::PexprPullUpAndCombineProjects
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	BOOL *pfSuccess		// output to indicate whether anything was pulled up
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pfSuccess);

	COperator *pop = pexpr->Pop();
	const ULONG ulArity = pexpr->UlArity();
	if (!pop->FLogical() || 0 == ulArity)
	{
		pexpr->AddRef();
		return pexpr;
	}

	DrgPexpr *pdrgpexprChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
	DrgPexpr *pdrgpexprPrElPullUp = GPOS_NEW(pmp) DrgPexpr(pmp);
	CExpressionHandle exprhdl(pmp);
	exprhdl.Attach(pexpr);

	CColRefSet *pcrsOutput = CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->PcrsOutput();

	// extract the columns used by the scalar expression and the operator itself (for grouping, sorting, etc.)
	CColRefSet *pcrsUsed = exprhdl.PcrsUsedColumns(pmp);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = PexprPullUpAndCombineProjects(pmp, (*pexpr)[ul], pfSuccess);
		if (pop->FLogical() && CLogical::PopConvert(pop)->FCanPullProjectionsUp(ul) &&
			COperator::EopLogicalProject == pexprChild->Pop()->Eopid())
		{
			// this child is a project - see if any project elements can be pulled up
			CExpression *pexprNewChild = PexprPullUpProjectElements
											(
											pmp,
											pexprChild,
											pcrsUsed,
											pcrsOutput,
											&pdrgpexprPrElPullUp
											);

			pexprChild->Release();
			pexprChild = pexprNewChild;
		}

		pdrgpexprChildren->Append(pexprChild);
	}

	pcrsUsed->Release();
	pop->AddRef();

	if (0 < pdrgpexprPrElPullUp->UlLength() && COperator::EopLogicalProject == pop->Eopid())
	{
		// some project elements have been pulled up and the original expression
		// was a project - combine its project list with the pulled up project elements
		GPOS_ASSERT(2 == pdrgpexprChildren->UlLength());
		*pfSuccess = true;
		CExpression *pexprRelational = (*pdrgpexprChildren)[0];
		CExpression *pexprPrLOld = (*pdrgpexprChildren)[1];
		pexprRelational->AddRef();

		CUtils::AddRefAppend(pdrgpexprPrElPullUp, pexprPrLOld->PdrgPexpr());
		pdrgpexprChildren->Release();
		CExpression *pexprPrjList = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp), pdrgpexprPrElPullUp);
		GPOS_ASSERT(CDrvdPropRelational::Pdprel(pexprRelational->PdpDerive())->PcrsOutput()->FSubset(CDrvdPropScalar::Pdpscalar(pexprPrjList->PdpDerive())->PcrsUsed()));

		return GPOS_NEW(pmp) CExpression(pmp, pop, pexprRelational, pexprPrjList);
	}

	CExpression *pexprOutput = GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexprChildren);

	if (0 == pdrgpexprPrElPullUp->UlLength())
	{
		// no project elements were pulled up
		pdrgpexprPrElPullUp->Release();
		return pexprOutput;
	}

	// some project elements were pulled - add a project on top of output expression
	*pfSuccess = true;
	CExpression *pexprPrjList = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp), pdrgpexprPrElPullUp);
	GPOS_ASSERT(CDrvdPropRelational::Pdprel(pexprOutput->PdpDerive())->PcrsOutput()->FSubset(CDrvdPropScalar::Pdpscalar(pexprPrjList->PdpDerive())->PcrsUsed()));

	return GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalProject(pmp), pexprOutput, pexprPrjList);
}

//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::PexprPullUpProjectElements
//
//	@doc:
//		Pull up project elements from the given projection expression that do not
//		exist in the given used columns set. The pulled up project elements must only
//		use columns that are in the output columns of the parent operator. Returns
//		a new expression that does not have the pulled up project elements. These
//		project elements are appended to the given array.
//
//---------------------------------------------------------------------------
CExpression *
CNormalizer::PexprPullUpProjectElements
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CColRefSet *pcrsUsed,
	CColRefSet *pcrsOutput,
	DrgPexpr **ppdrgpexprPrElPullUp	// output: the pulled-up project elements
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(COperator::EopLogicalProject == pexpr->Pop()->Eopid());
	GPOS_ASSERT(NULL != pcrsUsed);
	GPOS_ASSERT(NULL != ppdrgpexprPrElPullUp);
	GPOS_ASSERT(NULL != *ppdrgpexprPrElPullUp);

	if (2 != pexpr->UlArity())
	{
		// the project's children were not extracted as part of the pattern in this xform
		GPOS_ASSERT(0 == pexpr->UlArity());
		pexpr->AddRef();
		return pexpr;
	}

	DrgPexpr *pdrgpexprPrElNoPullUp = GPOS_NEW(pmp) DrgPexpr(pmp);
	CExpression *pexprPrL = (*pexpr)[1];

	const ULONG ulProjElements = pexprPrL->UlArity();
	for (ULONG ul = 0; ul < ulProjElements; ul++)
	{
		CExpression *pexprPrEl = (*pexprPrL)[ul];
		CScalarProjectElement *popPrEl = CScalarProjectElement::PopConvert(pexprPrEl->Pop());
		CColRef *pcrDefined = popPrEl->Pcr();
		CColRefSet *pcrsUsedByProjElem = CDrvdPropScalar::Pdpscalar(pexprPrEl->PdpDerive())->PcrsUsed();

		// a proj elem can be pulled up only if the defined column is not in
		// pcrsUsed and its used columns are in pcrOutput
		pexprPrEl->AddRef();
		if (!pcrsUsed->FMember(pcrDefined) && pcrsOutput->FSubset(pcrsUsedByProjElem))
		{
			(*ppdrgpexprPrElPullUp)->Append(pexprPrEl);
		}
		else
		{
			pdrgpexprPrElNoPullUp->Append(pexprPrEl);
		}
	}

	CExpression *pexprNew = (*pexpr)[0];
	pexprNew->AddRef();
	if (0 == pdrgpexprPrElNoPullUp->UlLength())
	{
		pdrgpexprPrElNoPullUp->Release();
	}
	else
	{
		// some project elements could not be pulled up - need a project here
		CExpression *pexprPrjList = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp), pdrgpexprPrElNoPullUp);
		pexprNew = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalProject(pmp), pexprNew, pexprPrjList);
	}

	return pexprNew;
}

//---------------------------------------------------------------------------
//	@function:
//		CNormalizer::PexprPullUpProjections
//
//	@doc:
//		Pulls up logical projects as far as possible, and combines consecutive
//		projects if possible
//
//---------------------------------------------------------------------------
CExpression *
CNormalizer::PexprPullUpProjections
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	BOOL fPullUp = true;
	pexpr->AddRef();
	CExpression *pexprOutput = pexpr;

	while (fPullUp)
	{
		fPullUp = false;

		CExpression *pexprOutputNew = PexprPullUpAndCombineProjects(pmp, pexprOutput, &fPullUp);
		pexprOutput->Release();
		pexprOutput = pexprOutputNew;
	}

	GPOS_ASSERT(FLocalColsSubsetOfInputCols(pmp, pexprOutput));

	return pexprOutput;
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//     @function:
//             CNormalizer::FLocalColsSubsetOfInputCols
//
//     @doc:
//             Check if the columns used by the operator are a subset of its input columns
//
//---------------------------------------------------------------------------
BOOL
CNormalizer::FLocalColsSubsetOfInputCols
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_CHECK_STACK_SIZE;

	CExpressionHandle exprhdl(pmp);
	if (NULL != pexpr->Pgexpr())
	{
		exprhdl.Attach(pexpr->Pgexpr());
	}
	else
	{
		exprhdl.Attach(pexpr);
	}
	exprhdl.DeriveProps(NULL /*pdpctxt*/);

	BOOL fValid = true;
	if (pexpr->Pop()->FLogical())
	{
		if (0 == exprhdl.UlNonScalarChildren())
		{
			return true;
		}

		CColRefSet *pcrsInput = GPOS_NEW(pmp) CColRefSet(pmp);

		const ULONG ulArity = exprhdl.UlArity();
		for (ULONG ul = 0; ul < ulArity; ul++)
		{
			if (!exprhdl.FScalarChild(ul))
			{
				CDrvdPropRelational *pdprelChild = exprhdl.Pdprel(ul);
				pcrsInput->Include(pdprelChild->PcrsOutput());
			}
		}

		// check if the operator's locally used columns are a subset of the input columns
		CColRefSet *pcrsUsedOp = exprhdl.PcrsUsedColumns(pmp);
		pcrsUsedOp->Exclude(exprhdl.Pdprel()->PcrsOuter());

		fValid = pcrsInput->FSubset(pcrsUsedOp);

		// release
		pcrsInput->Release();
		pcrsUsedOp->Release();
	}

	// check if its children are valid
	const ULONG ulExprArity = pexpr->UlArity();
	for (ULONG ulChildIdx = 0; ulChildIdx < ulExprArity && fValid; ulChildIdx++)
	{
		CExpression *pexprChild = (*pexpr)[ulChildIdx];
		fValid = FLocalColsSubsetOfInputCols(pmp, pexprChild);
	}

	return fValid;
}

#endif //GPOS_DEBUG

// EOF
