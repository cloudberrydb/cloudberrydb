//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CJoinOrderDP.cpp
//
//	@doc:
//		Implementation of dynamic programming-based join order generation
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpos/common/clibwrapper.h"
#include "gpos/common/CBitSet.h"
#include "gpos/common/CBitSetIter.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/xforms/CJoinOrderDP.h"

#include "gpopt/exception.h"

using namespace gpopt;

#define GPOPT_DP_JOIN_ORDERING_TOPK	10

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::SComponentPair::SComponentPair
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderDP::SComponentPair::SComponentPair
	(
	CBitSet *pbsFst,
	CBitSet *pbsSnd
	)
	:
	m_pbsFst(pbsFst),
	m_pbsSnd(pbsSnd)
{
	GPOS_ASSERT(NULL != pbsFst);
	GPOS_ASSERT(NULL != pbsSnd);
	GPOS_ASSERT(pbsFst->FDisjoint(pbsSnd));
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::SComponentPair::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CJoinOrderDP::SComponentPair::UlHash
	(
	const SComponentPair *pcomppair
	)
{
	GPOS_ASSERT(NULL != pcomppair);

	return UlCombineHashes
			(
			pcomppair->m_pbsFst->UlHash(),
			pcomppair->m_pbsSnd->UlHash()
			);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::SComponentPair::FEqual
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CJoinOrderDP::SComponentPair::FEqual
	(
	const SComponentPair *pcomppairFst,
	const SComponentPair *pcomppairSnd
	)
{
	GPOS_ASSERT(NULL != pcomppairFst);
	GPOS_ASSERT(NULL != pcomppairSnd);

	return pcomppairFst->m_pbsFst->FEqual(pcomppairSnd->m_pbsFst) &&
		pcomppairFst->m_pbsSnd->FEqual(pcomppairSnd->m_pbsSnd);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::SComponentPair::~SComponentPair
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderDP::SComponentPair::~SComponentPair()
{
	m_pbsFst->Release();
	m_pbsSnd->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::CJoinOrderDP
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderDP::CJoinOrderDP
	(
	IMemoryPool *pmp,
	DrgPexpr *pdrgpexprComponents,
	DrgPexpr *pdrgpexprConjuncts
	)
	:
	CJoinOrder(pmp, pdrgpexprComponents, pdrgpexprConjuncts)
{
	m_phmcomplink = GPOS_NEW(pmp) HMCompLink(pmp);
	m_phmbsexpr = GPOS_NEW(pmp) HMBSExpr(pmp);
	m_phmexprcost = GPOS_NEW(pmp) HMExprCost(pmp);
	m_pdrgpexprTopKOrders = GPOS_NEW(pmp) DrgPexpr(pmp);
	m_pexprDummy = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp));

#ifdef GPOS_DEBUG
	for (ULONG ul = 0; ul < m_ulComps; ul++)
	{
		GPOS_ASSERT(NULL != m_rgpcomp[ul]->m_pexpr->Pstats() &&
				"stats were not derived on input component");
	}
#endif // GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::~CJoinOrderDP
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderDP::~CJoinOrderDP()
{
#ifdef GPOS_DEBUG
	// in optimized build, we flush-down memory pools without leak checking,
	// we can save time in optimized build by skipping all de-allocations here,
	// we still have all de-llocations enabled in debug-build to detect any possible leaks
	m_phmcomplink->Release();
	m_phmbsexpr->Release();
	m_phmexprcost->Release();
	m_pdrgpexprTopKOrders->Release();
	m_pexprDummy->Release();
#endif // GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::AddJoinOrder
//
//	@doc:
//		Add given join order to top k join orders
//
//---------------------------------------------------------------------------
void
CJoinOrderDP::AddJoinOrder
	(
	CExpression *pexprJoin,
	CDouble dCost
	)
{
	GPOS_ASSERT(NULL != pexprJoin);
	GPOS_ASSERT(NULL != m_pdrgpexprTopKOrders);

	const ULONG ulResults = m_pdrgpexprTopKOrders->UlLength();
	INT iReplacePos = -1;
	BOOL fAddJoinOrder = false;
	if (ulResults < GPOPT_DP_JOIN_ORDERING_TOPK)
	{
		// we have less than K results, always add the given expression
		fAddJoinOrder = true;
	}
	else
	{
		// we have stored K expressions, evict worst expression
		for (ULONG ul = 0; !fAddJoinOrder && ul < ulResults; ul++)
		{
			CExpression *pexpr = (*m_pdrgpexprTopKOrders)[ul];
			CDouble *pd = m_phmexprcost->PtLookup(pexpr);
			GPOS_ASSERT(NULL != pd);

			if (dCost < *pd)
			{
				// found a worse expression
				fAddJoinOrder = true;
				iReplacePos = ul;
			}
		}
	}

	if (fAddJoinOrder)
	{
		pexprJoin->AddRef();
		if (iReplacePos > -1)
		{
			m_pdrgpexprTopKOrders->Replace((ULONG) iReplacePos, pexprJoin);
		}
		else
		{
			m_pdrgpexprTopKOrders->Append(pexprJoin);
		}

		InsertExpressionCost(pexprJoin, dCost, false /*fValidateInsert*/);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::PexprLookup
//
//	@doc:
//		Lookup best join order for given set
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDP::PexprLookup
	(
	CBitSet *pbs
	)
{
	// if set has size 1, return expression directly
	if (1 == pbs->CElements())
	{
		CBitSetIter bsi(*pbs);
		(void) bsi.FAdvance();

		return m_rgpcomp[bsi.UlBit()]->m_pexpr;
	}

	// otherwise, return expression by looking up DP table
	return m_phmbsexpr->PtLookup(pbs);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::PexprPred
//
//	@doc:
//		Extract predicate joining the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDP::PexprPred
	(
	CBitSet *pbsFst,
	CBitSet *pbsSnd
	)
{
	GPOS_ASSERT(NULL != pbsFst);
	GPOS_ASSERT(NULL != pbsSnd);

	if (!pbsFst->FDisjoint(pbsSnd) || 0 == pbsFst->CElements() || 0 == pbsSnd->CElements())
	{
		// components must be non-empty and disjoint
		return NULL;
	}

	CExpression *pexprPred = NULL;
	SComponentPair *pcomppair = NULL;

	// lookup link map
	for (ULONG ul = 0; ul < 2; ul++)
	{
		pbsFst->AddRef();
		pbsSnd->AddRef();
		pcomppair = GPOS_NEW(m_pmp) SComponentPair(pbsFst, pbsSnd);
		pexprPred = m_phmcomplink->PtLookup(pcomppair);
		if (NULL != pexprPred)
		{
			pcomppair->Release();
			if (m_pexprDummy == pexprPred)
			{
				return NULL;
			}
			return pexprPred;
		}

		// try again after swapping sets
		if (0 == ul)
		{
			pcomppair->Release();
			std::swap(pbsFst, pbsSnd);
		}
	}

	// could not find link in the map, construct it from edge set
	pexprPred = PexprBuildPred(pbsFst, pbsSnd);
	if (NULL == pexprPred)
	{
		m_pexprDummy->AddRef();
		pexprPred = m_pexprDummy;
	}

	// store predicate in link map
#ifdef GPOS_DEBUG
	BOOL fInserted =
#endif // GPOS_DEBUG
		m_phmcomplink->FInsert(pcomppair, pexprPred);
	GPOS_ASSERT(fInserted);

	if (m_pexprDummy != pexprPred)
	{
		return pexprPred;
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::PexprJoin
//
//	@doc:
//		Join expressions in the given two sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDP::PexprJoin
	(
	CBitSet *pbsFst,
	CBitSet *pbsSnd
	)
{
	GPOS_ASSERT(NULL != pbsFst);
	GPOS_ASSERT(NULL != pbsSnd);

	CExpression *pexprFst = PexprLookup(pbsFst);
	GPOS_ASSERT(NULL != pexprFst);

	CExpression *pexprSnd = PexprLookup(pbsSnd);
	GPOS_ASSERT(NULL != pexprSnd);

	CExpression *pexprScalar = PexprPred(pbsFst, pbsSnd);
	GPOS_ASSERT(NULL != pexprScalar);

	pexprFst->AddRef();
	pexprSnd->AddRef();
	pexprScalar->AddRef();

	return CUtils::PexprLogicalJoin<CLogicalInnerJoin>(m_pmp, pexprFst, pexprSnd, pexprScalar);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::DeriveStats
//
//	@doc:
//		Derive stats on given expression
//
//---------------------------------------------------------------------------
void
CJoinOrderDP::DeriveStats
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	if (m_pexprDummy != pexpr && NULL == pexpr->Pstats())
	{
		CExpressionHandle exprhdl(m_pmp);
		exprhdl.Attach(pexpr);
		exprhdl.DeriveStats(m_pmp, m_pmp, NULL /*prprel*/, NULL /*pdrgpstatCtxt*/);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::InsertExpressionCost
//
//	@doc:
//		Add expression to cost map
//
//---------------------------------------------------------------------------
void
CJoinOrderDP::InsertExpressionCost
	(
	CExpression *pexpr,
	CDouble dCost,
	BOOL fValidateInsert // if true, insertion must succeed
	)
{
	GPOS_ASSERT(NULL != pexpr);

	if (m_pexprDummy == pexpr)
	{
		// ignore dummy expression
		return;
	}

	if (!fValidateInsert && NULL != m_phmexprcost->PtLookup(pexpr))
	{
		// expression already exists in cost map
		return;
	}

	pexpr->AddRef();
#ifdef GPOS_DEBUG
	BOOL fInserted =
#endif // GPOS_DEBUG
		m_phmexprcost->FInsert(pexpr, GPOS_NEW(m_pmp) CDouble(dCost));
	GPOS_ASSERT(fInserted);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::PexprJoin
//
//	@doc:
//		Join expressions in the given set
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDP::PexprJoin
	(
	CBitSet *pbs
	)
{
	GPOS_ASSERT(2 == pbs->CElements());

	CBitSetIter bsi(*pbs);
	(void) bsi.FAdvance();
	ULONG ulCompFst = bsi.UlBit();
	(void) bsi.FAdvance();
	ULONG ulCompSnd = bsi.UlBit();
	GPOS_ASSERT(!bsi.FAdvance());

	CBitSet *pbsFst = GPOS_NEW(m_pmp) CBitSet(m_pmp);
	(void) pbsFst->FExchangeSet(ulCompFst);
	CBitSet *pbsSnd = GPOS_NEW(m_pmp) CBitSet(m_pmp);
	(void) pbsSnd->FExchangeSet(ulCompSnd);
	CExpression *pexprScalar = PexprPred(pbsFst, pbsSnd);
	pbsFst->Release();
	pbsSnd->Release();

	if (NULL == pexprScalar)
	{
		return NULL;
	}

	CExpression *pexprLeft = m_rgpcomp[ulCompFst]->m_pexpr;
	CExpression *pexprRight = m_rgpcomp[ulCompSnd]->m_pexpr;
	pexprLeft->AddRef();
	pexprRight->AddRef();
	pexprScalar->AddRef();
	CExpression *pexprJoin =
		CUtils::PexprLogicalJoin<CLogicalInnerJoin>(m_pmp, pexprLeft, pexprRight, pexprScalar);

	DeriveStats(pexprJoin);
	// store solution in DP table
	pbs->AddRef();
#ifdef GPOS_DEBUG
	BOOL fInserted =
#endif // GPOS_DEBUG
		m_phmbsexpr->FInsert(pbs, pexprJoin);
	GPOS_ASSERT(fInserted);

	return pexprJoin;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::PexprBestJoinOrderDP
//
//	@doc:
//		Find the best join order of a given set of elements using dynamic
//		programming;
//		given a set of elements (e.g., {A, B, C}), we find all possible splits
//		of the set (e.g., {A}, {B, C}) where at least one edge connects the
//		two subsets resulting from the split,
//		for each split, we find the best join orders of left and right subsets
//		recursively,
//		the function finds the split with the least cost, and stores the join
//		of its two subsets as the best join order of the given set
//
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDP::PexprBestJoinOrderDP
	(
	CBitSet *pbs // set of elements to be joined
	)
{
	CDouble dMinCost(0.0);
	CExpression *pexprResult = NULL;

	DrgPbs *pdrgpbsSubsets = PdrgpbsSubsets(m_pmp, pbs);
	const ULONG ulSubsets = pdrgpbsSubsets->UlLength();
	for (ULONG ul = 0; ul < ulSubsets; ul++)
	{
		CBitSet *pbsCurrent = (*pdrgpbsSubsets)[ul];
		CBitSet *pbsRemaining = GPOS_NEW(m_pmp) CBitSet(m_pmp, *pbs);
		pbsRemaining->Difference(pbsCurrent);

		// check if subsets are connected with one or more edges
		CExpression *pexprPred = PexprPred(pbsCurrent, pbsRemaining);
		if (NULL != pexprPred)
		{
			// compute solutions of left and right subsets recursively
			CExpression *pexprLeft = PexprBestJoinOrder(pbsCurrent);
			CExpression *pexprRight = PexprBestJoinOrder(pbsRemaining);

			if (NULL != pexprLeft && NULL != pexprRight)
			{
				// we found solutions of left and right subsets, we check if
				// this gives a better solution for the input set
				CExpression *pexprJoin = PexprJoin(pbsCurrent, pbsRemaining);
				CDouble dCost = DCost(pexprJoin);

				if (NULL == pexprResult || dCost < dMinCost)
				{
					// this is the first solution, or we found a better solution
					dMinCost = dCost;
					CRefCount::SafeRelease(pexprResult);
					pexprJoin->AddRef();
					pexprResult = pexprJoin;
				}

				if (m_ulComps == pbs->CElements())
				{
					AddJoinOrder(pexprJoin, dCost);
				}

				pexprJoin->Release();
			}
		}
		pbsRemaining->Release();
	}
	pdrgpbsSubsets->Release();

	// store solution in DP table
	if (NULL == pexprResult)
	{
		m_pexprDummy->AddRef();
		pexprResult = m_pexprDummy;
	}

	DeriveStats(pexprResult);
	pbs->AddRef();
#ifdef GPOS_DEBUG
	BOOL fInserted =
#endif // GPOS_DEBUG
		m_phmbsexpr->FInsert(pbs, pexprResult);
	GPOS_ASSERT(fInserted);

	// add expression cost to cost map
	InsertExpressionCost(pexprResult, dMinCost, false /*fValidateInsert*/);

	return pexprResult;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::GenerateSubsets
//
//	@doc:
//		Generate all subsets of given array of elements
//
//---------------------------------------------------------------------------
void
CJoinOrderDP::GenerateSubsets
	(
	IMemoryPool *pmp,
	CBitSet *pbsCurrent,
	ULONG *pulElems,
	ULONG ulSize,
	ULONG ulIndex,
	DrgPbs *pdrgpbsSubsets
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_CHECK_ABORT;

	GPOS_ASSERT(ulIndex <= ulSize);
	GPOS_ASSERT(NULL != pbsCurrent);
	GPOS_ASSERT(NULL != pulElems);
	GPOS_ASSERT(NULL != pdrgpbsSubsets);

	if (ulIndex == ulSize)
	{
		pdrgpbsSubsets->Append(pbsCurrent);
		return;
	}

	CBitSet *pbsCopy = GPOS_NEW(pmp) CBitSet(pmp, *pbsCurrent);
#ifdef GPOS_DEBUG
	BOOL fSet =
#endif // GPOS_DEBUG
		pbsCopy->FExchangeSet(pulElems[ulIndex]);
	GPOS_ASSERT(!fSet);

	GenerateSubsets(pmp, pbsCopy, pulElems, ulSize, ulIndex + 1, pdrgpbsSubsets);
	GenerateSubsets(pmp, pbsCurrent, pulElems, ulSize, ulIndex + 1, pdrgpbsSubsets);
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::PdrgpbsSubsets
//
//	@doc:
//		 Driver of subset generation
//
//---------------------------------------------------------------------------
DrgPbs *
CJoinOrderDP::PdrgpbsSubsets
	(
	IMemoryPool *pmp,
	CBitSet *pbs
	)
{
	const ULONG ulSize = pbs->CElements();
	ULONG *pulElems = GPOS_NEW_ARRAY(pmp, ULONG, ulSize);
	ULONG ul = 0;
	CBitSetIter bsi(*pbs);
	while (bsi.FAdvance())
	{
		pulElems[ul++] = bsi.UlBit();
	}

	CBitSet *pbsCurrent = GPOS_NEW(pmp) CBitSet(pmp);
	DrgPbs *pdrgpbsSubsets = GPOS_NEW(pmp) DrgPbs(pmp);
	GenerateSubsets(pmp, pbsCurrent, pulElems, ulSize, 0, pdrgpbsSubsets);
	GPOS_DELETE_ARRAY(pulElems);

	return pdrgpbsSubsets;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::DCost
//
//	@doc:
//		Primitive costing of join expressions;
//		cost of a join expression is the summation of the costs of its
//		children plus its local cost;
//		cost of a leaf expression is the estimated number of rows
//
//---------------------------------------------------------------------------
CDouble
CJoinOrderDP::DCost
	(
	CExpression *pexpr
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexpr);

	CDouble *pd = m_phmexprcost->PtLookup(pexpr);
	if (NULL != pd)
	{
		// stop recursion if cost was already cashed
		return *pd;
	}

	CDouble dCost(0.0);
	const ULONG ulArity = pexpr->UlArity();
	if (0 == ulArity)
	{
		// leaf operator, use its estimated number of rows as cost
		dCost = CDouble(pexpr->Pstats()->DRows());
	}
	else
	{
		// inner join operator, sum-up cost of its children
		DOUBLE rgdRows[2] = {0.0,  0.0};
		for (ULONG ul = 0; ul < ulArity - 1; ul++)
		{
			CExpression *pexprChild = (*pexpr)[ul];

			// call function recursively to find child cost
			dCost = dCost + DCost(pexprChild);
			DeriveStats(pexprChild);
			rgdRows[ul] = pexprChild->Pstats()->DRows().DVal();
		}

		// add inner join local cost
		dCost = dCost + (rgdRows[0] + rgdRows[1]);
	}

	return dCost;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::PbsCovered
//
//	@doc:
//		Return a subset of the given set covered by one or more edges
//
//---------------------------------------------------------------------------
CBitSet *
CJoinOrderDP::PbsCovered
	(
	CBitSet *pbsInput
	)
{
	GPOS_ASSERT(NULL != pbsInput);
	CBitSet *pbs = GPOS_NEW(m_pmp) CBitSet(m_pmp);

	for (ULONG ul = 0; ul < m_ulEdges; ul++)
	{
		SEdge *pedge = m_rgpedge[ul];
		if (pbsInput->FSubset(pedge->m_pbs))
		{
			pbs->Union(pedge->m_pbs);
		}
	}

	return pbs;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::PexprCross
//
//	@doc:
//		Generate cross product for the given components
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDP::PexprCross
	(
	CBitSet *pbs
	)
{
	GPOS_ASSERT(NULL != pbs);

	CExpression *pexpr = PexprLookup(pbs);
	if (NULL != pexpr)
	{
		// join order is already created
		return pexpr;
	}

	CBitSetIter bsi(*pbs);
	(void) bsi.FAdvance();
	CExpression *pexprComp = m_rgpcomp[bsi.UlBit()]->m_pexpr;
	pexprComp->AddRef();
	CExpression *pexprCross = pexprComp;
	while (bsi.FAdvance())
	{
		pexprComp =  m_rgpcomp[bsi.UlBit()]->m_pexpr;
		pexprComp->AddRef();
		pexprCross = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(m_pmp, pexprComp, pexprCross, CPredicateUtils::PexprConjunction(m_pmp, NULL /*pdrgpexpr*/));
	}

	pbs->AddRef();
#ifdef GPOS_DEBUG
		BOOL fInserted =
#endif // GPOS_DEBUG
			m_phmbsexpr->FInsert(pbs, pexprCross);
		GPOS_ASSERT(fInserted);

	return pexprCross;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::PexprJoinCoveredSubsetWithUncoveredSubset
//
//	@doc:
//		Join a covered subset with uncovered subset
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDP::PexprJoinCoveredSubsetWithUncoveredSubset
	(
	CBitSet *pbs,
	CBitSet *pbsCovered,
	CBitSet *pbsUncovered
	)
{
	GPOS_ASSERT(NULL != pbs);
	GPOS_ASSERT(NULL != pbsCovered);
	GPOS_ASSERT(NULL != pbsUncovered);
	GPOS_ASSERT(pbsCovered->FDisjoint(pbsUncovered));
	GPOS_ASSERT(pbs->FSubset(pbsCovered));
	GPOS_ASSERT(pbs->FSubset(pbsUncovered));

	// find best join order for covered subset
	CExpression *pexprJoin = PexprBestJoinOrder(pbsCovered);
	if (NULL == pexprJoin)
	{
		return NULL;
	}

	// create a cross product for uncovered subset
	CExpression *pexprCross = PexprCross(pbsUncovered);

	// join the results with a cross product
	pexprJoin->AddRef();
	pexprCross->AddRef();
	CExpression *pexprResult = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(m_pmp, pexprJoin, pexprCross, CPredicateUtils::PexprConjunction(m_pmp, NULL));
	pbs->AddRef();
#ifdef GPOS_DEBUG
	BOOL fInserted =
#endif // GPOS_DEBUG
		m_phmbsexpr->FInsert(pbs, pexprResult);
	GPOS_ASSERT(fInserted);

	return pexprResult;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::PexprBestJoinOrder
//
//	@doc:
//		find best join order for a given set of elements;
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDP::PexprBestJoinOrder
	(
	CBitSet *pbs
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_CHECK_ABORT;

	GPOS_ASSERT(NULL != pbs);

	// start by looking-up cost in the DP map
	CExpression *pexpr = PexprLookup(pbs);

	if (pexpr == m_pexprDummy)
	{
		// no join order could be created
		return NULL;
	}

	if (NULL != pexpr)
	{
		// join order is found by looking up map
		return pexpr;
	}

	// find maximal covered subset
	CBitSet *pbsCovered = PbsCovered(pbs);
	if (0 == pbsCovered->CElements())
	{
		// set is not covered, return a cross product
		pbsCovered->Release();

		return PexprCross(pbs);
	}

	if (!pbsCovered->FEqual(pbs))
	{
		// create a cross product for uncovered subset
		CBitSet *pbsUncovered = GPOS_NEW(m_pmp) CBitSet(m_pmp, *pbs);
		pbsUncovered->Difference(pbsCovered);
		CExpression *pexprResult =
			PexprJoinCoveredSubsetWithUncoveredSubset(pbs, pbsCovered, pbsUncovered);
		pbsCovered->Release();
		pbsUncovered->Release();

		return pexprResult;
	}
	pbsCovered->Release();

	// if set has size 2, there is only one possible solution
	if (2 == pbs->CElements())
	{
		return PexprJoin(pbs);
	}

	// otherwise, compute best join order using dynamic programming
	CExpression *pexprBestJoinOrder = PexprBestJoinOrderDP(pbs);
	if (pexprBestJoinOrder == m_pexprDummy)
	{
		// no join order could be created
		return NULL;
	}

	return pexprBestJoinOrder;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::PexprBuildPred
//
//	@doc:
//		Build predicate connecting the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDP::PexprBuildPred
	(
	CBitSet *pbsFst,
	CBitSet *pbsSnd
	)
{
	// collect edges connecting the given sets
	CBitSet *pbsEdges = GPOS_NEW(m_pmp) CBitSet(m_pmp);
	CBitSet *pbs = GPOS_NEW(m_pmp) CBitSet(m_pmp, *pbsFst);
	pbs->Union(pbsSnd);

	for (ULONG ul = 0; ul < m_ulEdges; ul++)
	{
		SEdge *pedge = m_rgpedge[ul];
		if (
			pbs->FSubset(pedge->m_pbs) &&
			!pbsFst->FDisjoint(pedge->m_pbs) &&
			!pbsSnd->FDisjoint(pedge->m_pbs)
			)
		{
#ifdef GPOS_DEBUG
		BOOL fSet =
#endif // GPOS_DEBUG
			pbsEdges->FExchangeSet(ul);
			GPOS_ASSERT(!fSet);
		}
	}
	pbs->Release();

	CExpression *pexprPred = NULL;
	if (0 < pbsEdges->CElements())
	{
		DrgPexpr *pdrgpexpr = GPOS_NEW(m_pmp) DrgPexpr(m_pmp);
		CBitSetIter bsi(*pbsEdges);
		while (bsi.FAdvance())
		{
			ULONG ul = bsi.UlBit();
			SEdge *pedge = m_rgpedge[ul];
			pedge->m_pexpr->AddRef();
			pdrgpexpr->Append(pedge->m_pexpr);
		}

		pexprPred = CPredicateUtils::PexprConjunction(m_pmp, pdrgpexpr);
	}

	pbsEdges->Release();
	return pexprPred;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::PexprExpand
//
//	@doc:
//		Create join order
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDP::PexprExpand()
{
	CBitSet *pbs = GPOS_NEW(m_pmp) CBitSet(m_pmp);
	for (ULONG ul = 0; ul < m_ulComps; ul++)
	{
		(void) pbs->FExchangeSet(ul);
	}

	CExpression *pexprResult = PexprBestJoinOrder(pbs);
	if (NULL != pexprResult)
	{
		pexprResult->AddRef();
	}
	pbs->Release();

	return pexprResult;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::OsPrint
//
//	@doc:
//		Print created join order
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrderDP::OsPrint
	(
	IOstream &os
	)
	const
{
	return os;
}

// EOF
