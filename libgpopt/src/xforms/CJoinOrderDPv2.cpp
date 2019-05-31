//---------------------------------------------------------------------------
// Greenplum Database
// Copyright (C) 2019 Pivotal Inc.
//
//	@filename:
//		CJoinOrderDPv2.cpp
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
#include "gpopt/xforms/CJoinOrderDPv2.h"

#include "gpopt/exception.h"

using namespace gpopt;

// Communitivity will generate an additional 5 alternatives. This value should be 1/2
// of GPOPT_DP_JOIN_ORDERING_TOPK to generate equivalent alternatives as the DP xform
#define GPOPT_DP_JOIN_ORDERING_TOPK	5

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::CJoinOrderDPv2
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderDPv2::CJoinOrderDPv2
	(
	CMemoryPool *mp,
	CExpressionArray *pdrgpexprComponents,
	CExpressionArray *pdrgpexprConjuncts
	)
	:
	CJoinOrder(mp, pdrgpexprComponents, pdrgpexprConjuncts, false /* m_include_loj_childs */)
{
	m_join_levels = GPOS_NEW(mp) ComponentInfoArrayLevels(mp);
	// put a NULL entry at index 0, because there are no 0-way joins
	m_join_levels->Append(GPOS_NEW(mp) ComponentInfoArray(mp));

	m_topKOrders = GPOS_NEW(mp) CExpressionArray(mp);
	m_topKCosts  = GPOS_NEW(mp) CDoubleArray(mp);
	m_pexprDummy = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp));
	m_mp = mp;

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
//		CJoinOrderDPv2::~CJoinOrderDPv2
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderDPv2::~CJoinOrderDPv2()
{
#ifdef GPOS_DEBUG
	// in optimized build, we flush-down memory pools without leak checking,
	// we can save time in optimized build by skipping all de-allocations here,
	// we still have all de-llocations enabled in debug-build to detect any possible leaks
	m_topKOrders->Release();
	m_topKCosts->Release();
	m_pexprDummy->Release();
	m_join_levels->Release();
#endif // GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::AddJoinOrderToTopK
//
//	@doc:
//		Add given join order to top k join orders
//
//---------------------------------------------------------------------------

void
CJoinOrderDPv2::AddJoinOrderToTopK
	(
	CExpression *pexprJoin,
	CDouble dCost
	)
{
	GPOS_ASSERT(NULL != pexprJoin);
	GPOS_ASSERT(NULL != m_topKOrders);

	// length of the array will not be more than 10
	INT ulResults = m_topKOrders->Size();
	INT iReplacePos = -1;
	BOOL fAddJoinOrder = false;
	if (ulResults < GPOPT_DP_JOIN_ORDERING_TOPK)
	{
		// we have less than K results, always add the given expression
		fAddJoinOrder = true;
	}
	else
	{

		// TODO: Can also store max cost, and if dcost > max cost, then we don't even need to iterate through the topK. This is only needed if we spend a fair amount of time here.
		CDouble dmaxCost = 0.0;
		// we have stored K expressions, evict worst expression
		for (INT ul = 0; ul < ulResults; ul++)
		{
			CDouble *pd = (*m_topKCosts)[ul];
			GPOS_ASSERT(NULL != pd);

			if (dmaxCost < *pd && dCost < *pd)
			{
				// found a worse expression
				dmaxCost = *pd;
				fAddJoinOrder = true;
				iReplacePos = ul;
			}
		}
	}

	if (fAddJoinOrder)
	{
		pexprJoin->AddRef();
		CDouble *saved_cost = GPOS_NEW(m_mp) CDouble(dCost);
		if (iReplacePos > -1)
		{
			m_topKOrders->Replace((ULONG) iReplacePos, pexprJoin);
			m_topKCosts->Replace((ULONG) iReplacePos, saved_cost);

		}
		else
		{
			m_topKOrders->Append(pexprJoin);
			m_topKCosts->Append(saved_cost);
		}

	}
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::PexprPred
//
//	@doc:
//		Extract predicate joining the two given sets or NULL for cross joins
//		or overlapping or empty sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPv2::PexprPred
	(
	CBitSet *pbsFst,
	CBitSet *pbsSnd
	)
{
	GPOS_ASSERT(NULL != pbsFst);
	GPOS_ASSERT(NULL != pbsSnd);

	if (!pbsFst->IsDisjoint(pbsSnd) || 0 == pbsFst->Size() || 0 == pbsSnd->Size())
	{
		// components must be non-empty and disjoint
		return NULL;
	}

	CExpression *pexprPred = NULL;

	// could not find link in the map, construct it from edge set
	pexprPred = PexprBuildPred(pbsFst, pbsSnd);
	if (NULL == pexprPred)
	{
		pexprPred = m_pexprDummy;
	}

	if (m_pexprDummy != pexprPred)
	{
		return pexprPred;
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::DeriveStats
//
//	@doc:
//		Derive stats on given expression
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::DeriveStats
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	if (m_pexprDummy != pexpr && NULL == pexpr->Pstats())
	{
		CExpressionHandle exprhdl(m_mp);
		exprhdl.Attach(pexpr);
		exprhdl.DeriveStats(m_mp, m_mp, NULL /*prprel*/, NULL /*stats_ctxt*/);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::DCost
//
//	@doc:
//		Primitive costing of join expressions;
//		Cost of a join expression is the "internal data flow" of the join
//		tree, the sum of all the rows flowing from the leaf nodes up to
//		the root. This does not include the number of result rows of joins,
//		for a simple reason: To compare two equivalent joins, we would have
//		to derive the stats twice, which would be expensive and run the risk
//		that we get different rowcounts.
//		NOTE: We could consider the width of the rows as well, if we had
//		a reliable way of determining the actual width.
//
//---------------------------------------------------------------------------
CDouble
CJoinOrderDPv2::DCost
	(
	CExpression *pexpr
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexpr);

	CDouble dCost(0.0);
	const ULONG arity = pexpr->Arity();
	if (0 == arity)
	{
		// leaf operator, use its estimated number of rows as cost
		dCost = CDouble(pexpr->Pstats()->Rows());
	}
	else
	{
		// inner join operator, sum-up cost of its children
		DOUBLE rgdRows[2] = {0.0,  0.0};
		for (ULONG ul = 0; ul < arity - 1; ul++)
		{
			CExpression *pexprChild = (*pexpr)[ul];

			// call function recursively to find child cost
			// NOTE: This will count cost of leaves twice
			dCost = dCost + DCost(pexprChild);
			DeriveStats(pexprChild);
			rgdRows[ul] = pexprChild->Pstats()->Rows().Get();
		}

		// add inner join local cost
		dCost = dCost + (rgdRows[0] + rgdRows[1]);
	}

	return dCost;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::PexprBuildPred
//
//	@doc:
//		Build predicate connecting the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPv2::PexprBuildPred
	(
	CBitSet *pbsFst,
	CBitSet *pbsSnd
	)
{
	// collect edges connecting the given sets
	CBitSet *pbsEdges = GPOS_NEW(m_mp) CBitSet(m_mp);
	CBitSet *pbs = GPOS_NEW(m_mp) CBitSet(m_mp, *pbsFst);
	pbs->Union(pbsSnd);

	for (ULONG ul = 0; ul < m_ulEdges; ul++)
	{
		SEdge *pedge = m_rgpedge[ul];
		if (
			pbs->ContainsAll(pedge->m_pbs) &&
			!pbsFst->IsDisjoint(pedge->m_pbs) &&
			!pbsSnd->IsDisjoint(pedge->m_pbs)
			)
		{
#ifdef GPOS_DEBUG
		BOOL fSet =
#endif // GPOS_DEBUG
			pbsEdges->ExchangeSet(ul);
			GPOS_ASSERT(!fSet);
		}
	}
	pbs->Release();

	CExpression *pexprPred = NULL;
	if (0 < pbsEdges->Size())
	{
		CExpressionArray *pdrgpexpr = GPOS_NEW(m_mp) CExpressionArray(m_mp);
		CBitSetIter bsi(*pbsEdges);
		while (bsi.Advance())
		{
			ULONG ul = bsi.Bit();
			SEdge *pedge = m_rgpedge[ul];
			pedge->m_pexpr->AddRef();
			pdrgpexpr->Append(pedge->m_pexpr);
		}

		pexprPred = CPredicateUtils::PexprConjunction(m_mp, pdrgpexpr);
	}

	pbsEdges->Release();
	return pexprPred;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::GetJoinExpr
//
//	@doc:
//		Build a CExpression joining the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPv2::GetJoinExpr
	(
	SComponentInfo *left_child,
	SComponentInfo *right_child
	)
{
	CExpression *scalar_expr = PexprPred(left_child->component, right_child->component);

	if (NULL == scalar_expr)
	{
		scalar_expr = CPredicateUtils::PexprConjunction(m_mp, NULL /*pdrgpexpr*/);
	}

	CExpression *left_expr = left_child->best_expr;
	CExpression *right_expr = right_child->best_expr;

	left_expr->AddRef();
	right_expr->AddRef();

	CExpression *join_expr = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(m_mp, left_expr, right_expr, scalar_expr);

	return join_expr;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::AddJoinExprAlternativeForBitSet
//
//	@doc:
//		Add the given expression to BitSetToExpressionArrayMap map
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::AddJoinExprAlternativeForBitSet
	(
	CBitSet *join_bitset,
	CExpression *join_expr,
	BitSetToExpressionArrayMap *map
	)
{
	GPOS_ASSERT(NULL != join_bitset);
	GPOS_ASSERT(NULL != join_expr);

	join_expr->AddRef();
	CExpressionArray *existing_join_exprs = map->Find(join_bitset);
	if (NULL != existing_join_exprs)
	{
		existing_join_exprs->Append(join_expr);
	}
	else
	{
		CExpressionArray *exprs = GPOS_NEW(m_mp) CExpressionArray(m_mp);
		exprs->Append(join_expr);
		join_bitset->AddRef();
		BOOL success = map->Insert(join_bitset, exprs);
		if (!success)
			GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedPred);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::SearchJoinOrders
//
//	@doc:
//		Enumerate all the possible joins between two lists of components
//
//---------------------------------------------------------------------------
CJoinOrderDPv2::BitSetToExpressionArrayMap *
CJoinOrderDPv2::SearchJoinOrders
	(
	ComponentInfoArray *join_pair_components,
	ComponentInfoArray *other_join_pair_components
	)
{
	GPOS_ASSERT(join_pair_components);
	GPOS_ASSERT(other_join_pair_components);

	ULONG join_pairs_size = join_pair_components->Size();
	ULONG other_join_pairs_size = other_join_pair_components->Size();
	BitSetToExpressionArrayMap *join_pairs_map = GPOS_NEW(m_mp) BitSetToExpressionArrayMap(m_mp);

	for (ULONG join_pair_id = 0; join_pair_id < join_pairs_size; join_pair_id++)
	{
		SComponentInfo *left_component_info = (*join_pair_components)[join_pair_id];
		CBitSet *left_bitset = left_component_info->component;

		// if pairs from the same level, start from the next
		// entry to avoid duplicate join combinations
		// i.e a join b and b join a, just try one
		// commutativity will take care of the other
		ULONG other_pair_start_id = 0;
		if (join_pair_components == other_join_pair_components)
			other_pair_start_id = join_pair_id + 1;

		for (ULONG other_pair_id = other_pair_start_id; other_pair_id < other_join_pairs_size; other_pair_id++)
		{
			CBitSet *join_bitset = GPOS_NEW(m_mp) CBitSet(m_mp, *left_bitset);
			SComponentInfo *right_component_info = (*other_join_pair_components)[other_pair_id];
			CBitSet *right_bitset = right_component_info->component;
			if (!left_bitset->IsDisjoint(right_bitset))
			{
				join_bitset->Release();
				continue;
			}

			CExpression *join_expr = GetJoinExpr(left_component_info, right_component_info);

			join_bitset->Union(right_bitset);
			AddJoinExprAlternativeForBitSet(join_bitset, join_expr, join_pairs_map);
			join_expr->Release();
			join_bitset->Release();
		}
	}
	return join_pairs_map;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::AddExprs
//
//	@doc:
//		Add the given expressions to a CExpressionArray
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::AddExprs
	(
	const CExpressionArray *candidate_join_exprs,
	CExpressionArray *result_join_exprs
	)
{
	for (ULONG ul = 0; ul < candidate_join_exprs->Size(); ul++)
	{
		CExpression *join_expr = (*candidate_join_exprs)[ul];
		join_expr->AddRef();
		result_join_exprs->Append(join_expr);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::AddJoinExprsForBitSet
//
//	@doc:
//		Add candidate expressions to BitSetToExpressionArrayMap result_map if
//      not already present, merge expression arrays otherwise
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::AddJoinExprsForBitSet
	(
	BitSetToExpressionArrayMap *result_map,
	BitSetToExpressionArrayMap *candidate_map
	)
{
	if (NULL == candidate_map)
		return;

	BitSetToExpressionArrayMapIter iter(candidate_map);
	while (iter.Advance())
	{
		const CBitSet *join_bitset = iter.Key();
		CExpressionArray *existing_join_exprs = result_map->Find(join_bitset);
		const CExpressionArray *candidate_join_exprs = iter.Value();
		if (NULL == existing_join_exprs)
		{
			CBitSet *join_bitset_entry = GPOS_NEW(m_mp) CBitSet(m_mp, *join_bitset);
			CExpressionArray *join_exprs = GPOS_NEW(m_mp) CExpressionArray(m_mp);
			AddExprs(candidate_join_exprs, join_exprs);
			result_map->Insert(join_bitset_entry, join_exprs);
		}
		else
		{
			AddExprs(candidate_join_exprs, existing_join_exprs);
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::MergeJoinExprsForBitSet
//
//	@doc:
//		Create a new map that contains the union of two provided maps
//
//---------------------------------------------------------------------------
CJoinOrderDPv2::BitSetToExpressionArrayMap *
CJoinOrderDPv2::MergeJoinExprsForBitSet
	(
	BitSetToExpressionArrayMap *map,
	BitSetToExpressionArrayMap *other_map
	)
{
	BitSetToExpressionArrayMap *result_map = GPOS_NEW(m_mp) BitSetToExpressionArrayMap(m_mp);
	AddJoinExprsForBitSet(result_map, map);
	AddJoinExprsForBitSet(result_map, other_map);
	return result_map;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::AddJoinExprFromMapToTopK
//
//	@doc:
//		Iterate over the map and try to add each entry to the TopK list
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::AddJoinExprFromMapToTopK
	(
	BitSetToExpressionArrayMap *bitset_joinexpr_map
	)
{
	BitSetToExpressionArrayMapIter iter(bitset_joinexpr_map);

	while (iter.Advance())
	{
		const CExpressionArray *join_exprs = iter.Value();
		for (ULONG id = 0; id < join_exprs->Size(); id++)
		{
			CExpression *join_expr = (*join_exprs)[id];
			CDouble join_cost = DCost(join_expr);
			AddJoinOrderToTopK(join_expr, join_cost);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::SearchBushyJoinOrders
//
//	@doc:
//		Generate all bushy join trees of level current_level,
//		given an array of an array of bit sets (components), arranged by level
//
//---------------------------------------------------------------------------
CJoinOrderDPv2::BitSetToExpressionArrayMap *
CJoinOrderDPv2::SearchBushyJoinOrders
	(
	ULONG current_level
	)
{
	BitSetToExpressionArrayMap *final_bushy_join_exprs_map = NULL;

	// join trees of level 3 and below are never bushy
	if (current_level > 3)
	{
		// try all joins of bitsets of level x and y, where
		// x + y = current_level and x > 1 and y > 1
		for (ULONG k = 3; k <= current_level; k++)
		{
			ULONG join_level = k - 1;
			ULONG other_join_level = current_level - join_level;
			if (join_level > other_join_level)
				// we've already considered the commutated join
				break;
			ComponentInfoArray *join_component_infos = (*m_join_levels)[join_level];
			ComponentInfoArray *other_join_component_infos = (*m_join_levels)[other_join_level];
			BitSetToExpressionArrayMap *bitset_bushy_join_exprs_map = SearchJoinOrders(join_component_infos,
																					   other_join_component_infos);
			BitSetToExpressionArrayMap *interim_map = final_bushy_join_exprs_map;
			final_bushy_join_exprs_map = MergeJoinExprsForBitSet(bitset_bushy_join_exprs_map, interim_map);
			CRefCount::SafeRelease(interim_map);
			bitset_bushy_join_exprs_map->Release();
		}
	}
	return final_bushy_join_exprs_map;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::PExprExpand
//
//	@doc:
//		Main driver for join order enumeration, called by xform
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPv2::PexprExpand()
{
	ComponentInfoArray *non_join_vertex_component_infos = GPOS_NEW(m_mp) ComponentInfoArray(m_mp);
	// put the "non join vertices", the nodes of the join tree that
	// are not joins themselves, at the first level
	for (ULONG relation_id = 0; relation_id < m_ulComps; relation_id++)
	{
		CBitSet *non_join_vertex_bitset = GPOS_NEW(m_mp) CBitSet(m_mp);
		non_join_vertex_bitset->ExchangeSet(relation_id);
		CExpression *pexpr_relation = m_rgpcomp[relation_id]->m_pexpr;
		pexpr_relation->AddRef();
		SComponentInfo *non_join_component_info = GPOS_NEW(m_mp) SComponentInfo(non_join_vertex_bitset,
																				pexpr_relation,
																				0.0);
		non_join_vertex_component_infos->Append(non_join_component_info);
	}

	m_join_levels->Append(non_join_vertex_component_infos);

	for (ULONG current_join_level = 2; current_join_level <= m_ulComps; current_join_level++)
	{
		ULONG previous_level = current_join_level - 1;
		ComponentInfoArray *prev_lev_comps = (*m_join_levels)[previous_level];
		// build linear "current_join_level" joins, with a "previous_level"-way join on one
		// side and a non-join vertex on the other side
		BitSetToExpressionArrayMap *bitset_join_exprs_map = SearchJoinOrders(prev_lev_comps, non_join_vertex_component_infos);
		// build bushy trees - joins between two other joins
		BitSetToExpressionArrayMap *bitset_bushy_join_exprs_map = SearchBushyJoinOrders(current_join_level);

		// A set of different components/bit sets, each with a set of equivalent expressions,
		// for current_join_level
		BitSetToExpressionArrayMap *all_join_exprs_map = MergeJoinExprsForBitSet(bitset_join_exprs_map, bitset_bushy_join_exprs_map);
		if (current_join_level == m_ulComps)
		{
			// top-level join: Keep the top K expressions
			AddJoinExprFromMapToTopK(all_join_exprs_map);
		}
		else
		{
			// levels below the top level: Keep the best expression for each bit set
			ComponentInfoArray *cheapest_bitset_join_expr_map = GetCheapestJoinExprForBitSet(all_join_exprs_map);
			m_join_levels->Append(cheapest_bitset_join_expr_map);
		}
		all_join_exprs_map->Release();
		CRefCount::SafeRelease(bitset_bushy_join_exprs_map);
		bitset_join_exprs_map->Release();
	}
	return NULL;
}



//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::GetCheapestJoinExprForBitSet
//
//	@doc:
//		Convert a BitSetToExpressionArrayMap to a BitSetToExpressionMap
//		by selecting the cheapest expression from each array
//
//---------------------------------------------------------------------------
CJoinOrderDPv2::ComponentInfoArray *
CJoinOrderDPv2::GetCheapestJoinExprForBitSet
	(
	BitSetToExpressionArrayMap *bitset_exprs_map
	)
{
	ComponentInfoArray *cheapest_join_array = GPOS_NEW(m_mp) ComponentInfoArray(m_mp);
	BitSetToExpressionArrayMapIter iter(bitset_exprs_map);

	while (iter.Advance())
	{
		const CBitSet *join_bitset = iter.Key();
		const CExpressionArray *join_exprs = iter.Value();
		CDouble min_join_cost(0.0);
		CExpression *best_join_expr = NULL;
		for (ULONG id = 0; id < join_exprs->Size(); id++)
		{
			CExpression *join_expr = (*join_exprs)[id];
			CDouble join_cost = DCost(join_expr);
			if (min_join_cost == 0.0 || join_cost < min_join_cost)
			{
				best_join_expr = join_expr;
				min_join_cost = join_cost;
			}
		}
		CBitSet *join_bitset_entry = GPOS_NEW(m_mp) CBitSet(m_mp, *join_bitset);
		best_join_expr->AddRef();

		SComponentInfo *component_info = GPOS_NEW(m_mp) SComponentInfo(join_bitset_entry, best_join_expr, min_join_cost);

		cheapest_join_array->Append(component_info);

	}
	return cheapest_join_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::OsPrint
//
//	@doc:
//		Print created join order
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrderDPv2::OsPrint
	(
	IOstream &os
	)
	const
{
	ULONG num_levels = m_join_levels->Size();
	ULONG num_bitsets = 0;

	for (ULONG lev=0; lev<num_levels; lev++)
	{
		ComponentInfoArray *bitsets_this_level = (*m_join_levels)[lev];
		ULONG num_bitsets_this_level = bitsets_this_level->Size();

		os << "CJoinOrderDPv2 - Level: " << lev << std::endl;

		for (ULONG c=0; c<num_bitsets_this_level; c++)
		{
			SComponentInfo *ci = (*bitsets_this_level)[c];
			num_bitsets++;
			os << "   Bitset: ";
			ci->component->OsPrint(os);
			os << std::endl;
			os << "   Expression: ";
			if (NULL == ci->best_expr)
			{
				os << "NULL" << std::endl;
			}
			else
			{
				ci->best_expr->OsPrint(os);
			}
			os << "   Cost: ";
			ci->cost.OsPrint(os);
			os << std::endl;
		}
	}

	os << "CJoinOrderDPv2 - total number of bitsets: " << num_bitsets << std::endl;

	return os;
}

// EOF
