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
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CScalarNAryJoinPredList.h"
#include "gpopt/xforms/CJoinOrderDPv2.h"

#include "gpopt/exception.h"

#include "naucrates/statistics/CJoinStatsProcessor.h"


using namespace gpopt;

// return 10 alternatives, same as in CJoinOrderDP
#define GPOPT_DPV2_JOIN_ORDERING_TOPK 10
// a somewhat arbitrary penalty for cross joins, this will be refined in a future PR
#define GPOPT_DPV2_CROSS_JOIN_PENALTY 100


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
	CExpressionArray *pdrgpexprAtoms,
	CExpressionArray *innerJoinConjuncts,
	CExpressionArray *onPredConjuncts,
	ULongPtrArray *childPredIndexes
	)
	:
	CJoinOrder(mp, pdrgpexprAtoms, innerJoinConjuncts, onPredConjuncts, childPredIndexes),
	m_expression_to_edge_map(NULL),
	m_on_pred_conjuncts(onPredConjuncts),
	m_child_pred_indexes(childPredIndexes),
	m_non_inner_join_dependencies(NULL)
{
	m_join_levels = GPOS_NEW(mp) DPv2Levels(mp, m_ulComps+1);
	// populate levels array with n+1 levels for an n-way join
	// level 0 remains unused, so index i corresponds to level i,
	// making it easier for humans to read the code
	for (ULONG l=0; l<= m_ulComps; l++)
	{
		m_join_levels->Append(GPOS_NEW(mp) SLevelInfo(l, GPOS_NEW(mp) SGroupInfoArray(mp)));
	}

	m_bitset_to_group_info_map = GPOS_NEW(mp) BitSetToGroupInfoMap(mp);

	m_top_k_expressions = GPOS_NEW(mp) KHeap<SExpressionInfoArray, SExpressionInfo>
										(
										 mp,
										 this,
										 GPOPT_DPV2_JOIN_ORDERING_TOPK
										);

	m_mp = mp;
	if (0 < m_on_pred_conjuncts->Size())
	{
		// we have non-inner joins, add dependency info
		ULONG numNonInnerJoins = m_on_pred_conjuncts->Size();

		m_non_inner_join_dependencies = GPOS_NEW(mp) CBitSetArray(mp, numNonInnerJoins);
		for (ULONG ul=0; ul<numNonInnerJoins; ul++)
		{
			m_non_inner_join_dependencies->Append(GPOS_NEW(mp) CBitSet(mp));
		}

		// compute dependencies of the NIJ right children
		// (those components must appear on the left of the NIJ)
		// Note: NIJ = Non-inner join, e.g. LOJ
		for (ULONG en = 0; en < m_ulEdges; en++)
		{
			SEdge *pedge = m_rgpedge[en];

			if (0 < pedge->m_loj_num)
			{
				// edge represents a non-inner join pred
				ULONG logicalChildNum = FindLogicalChildByNijId(pedge->m_loj_num);
				CBitSet * nijBitSet = (*m_non_inner_join_dependencies)[pedge->m_loj_num-1];

				GPOS_ASSERT(0 < logicalChildNum);
				nijBitSet->Union(pedge->m_pbs);
				// clear the bit representing the right side of the NIJ, we only
				// want to track the components needed on the left side
				nijBitSet->ExchangeClear(logicalChildNum);
			}
		}
		PopulateExpressionToEdgeMapIfNeeded();
	}
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
	// we still have all de-allocations enabled in debug-build to detect any possible leaks
	CRefCount::SafeRelease(m_non_inner_join_dependencies);
	CRefCount::SafeRelease(m_child_pred_indexes);
	m_bitset_to_group_info_map->Release();
	CRefCount::SafeRelease(m_expression_to_edge_map);
	m_top_k_expressions->Release();
	m_join_levels->Release();
	m_on_pred_conjuncts->Release();
#endif // GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::DCost
//
//	@doc:
//		Primitive costing of join expressions;
//		Cost of a join expression is the "internal data flow" of the join
//		tree, the sum of all the rows flowing from the leaf nodes up to
//		the root.
//		NOTE: We could consider the width of the rows as well, if we had
//		a reliable way of determining the actual width.
//
//---------------------------------------------------------------------------
CDouble
CJoinOrderDPv2::DCost
	(
	 SGroupInfo *group,
	 const SGroupInfo *leftChildGroup,
	 const SGroupInfo *rightChildGroup
	)
{
	CDouble dCost(group->m_expr_for_stats->Pstats()->Rows());

	if (NULL != leftChildGroup)
	{
		GPOS_ASSERT(NULL != rightChildGroup);
		dCost = dCost + leftChildGroup->m_best_expr_info->m_cost;
		dCost = dCost + rightChildGroup->m_best_expr_info->m_cost;
	}

	if (CUtils::FCrossJoin(group->m_best_expr_info->m_expr))
	{
		dCost = dCost * GPOPT_DPV2_CROSS_JOIN_PENALTY;
	}

	return dCost;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::PexprBuildInnerJoinPred
//
//	@doc:
//		Build predicate connecting the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPv2::PexprBuildInnerJoinPred
	(
	CBitSet *pbsFst,
	CBitSet *pbsSnd
	)
{
	GPOS_ASSERT(pbsFst->IsDisjoint(pbsSnd));
	// collect edges connecting the given sets
	CBitSet *pbsEdges = GPOS_NEW(m_mp) CBitSet(m_mp);
	CBitSet *pbs = GPOS_NEW(m_mp) CBitSet(m_mp, *pbsFst);
	pbs->Union(pbsSnd);

	for (ULONG ul = 0; ul < m_ulEdges; ul++)
	{
		SEdge *pedge = m_rgpedge[ul];
		if (
			// edge represents an inner join pred
			0 == pedge->m_loj_num &&
			// all columns referenced in the edge pred are provided
			pbs->ContainsAll(pedge->m_pbs) &&
			// the edge represents a true join predicate between the two components
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

void CJoinOrderDPv2::DeriveStats(CExpression *pexpr)
{
	try {
		// We want to let the histogram code compute the join selectivity and the number of NDVs based
		// on actual histogram buckets, taking into account the overlap of the data ranges. It helps
		// with getting more consistent and accurate cardinality estimates for DP.
		// Eventually, this should probably become the default method.
		CJoinStatsProcessor::SetComputeScaleFactorFromHistogramBuckets(true);
		CJoinOrder::DeriveStats(pexpr);
		CJoinStatsProcessor::SetComputeScaleFactorFromHistogramBuckets(false);
	} catch (...) {
		CJoinStatsProcessor::SetComputeScaleFactorFromHistogramBuckets(false);
		throw;
	}
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
	 SGroupInfo *left_child,
	 SGroupInfo *right_child,
	 BOOL use_stats_expr
	)
{
	CExpression *scalar_expr = NULL;
	CBitSet *required_on_left = NULL;
	// for now we just handle LOJs here
	BOOL isLOJ = IsRightChildOfNIJ(right_child, &scalar_expr, &required_on_left);

	if (!isLOJ)
	{
		// inner join, compute the predicate from the join graph
		GPOS_ASSERT(NULL == scalar_expr);
		scalar_expr = PexprBuildInnerJoinPred(left_child->m_atoms, right_child->m_atoms);
	}
	else
	{
		// check whether scalar_expr can be computed from left_child and right_child,
		// otherwise this is not a valid join
		GPOS_ASSERT(NULL != scalar_expr && NULL != required_on_left);
		if (!left_child->m_atoms->ContainsAll(required_on_left))
		{
			// the left child does not produce all the values needed in the ON
			// predicate, so this is not a valid join
			return NULL;
		}
		scalar_expr->AddRef();
	}

	if (NULL == scalar_expr)
	{
		// this is a cross product

		if (right_child->IsAnAtom())
		{
			// generate a TRUE boolean expression as the join predicate of the cross product
			scalar_expr = CUtils::PexprScalarConstBool(m_mp, true);
		}
		else
		{
			// we don't do bushy cross products, any mandatory or optional cross products
			// are linear trees
			return NULL;
		}
	}

	CExpression *left_expr = left_child->m_best_expr_info->m_expr;
	CExpression *right_expr = right_child->m_best_expr_info->m_expr;
	CExpression *join_expr = NULL;

	if (use_stats_expr)
	{
		left_expr = left_child->m_expr_for_stats;
		right_expr = right_child->m_expr_for_stats;
	}

	left_expr->AddRef();
	right_expr->AddRef();

	if (isLOJ)
	{
		join_expr = CUtils::PexprLogicalJoin<CLogicalLeftOuterJoin>(m_mp, left_expr, right_expr, scalar_expr);
	}
	else
	{
		join_expr = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(m_mp, left_expr, right_expr, scalar_expr);
	}

	return join_expr;
}


void
CJoinOrderDPv2::PopulateExpressionToEdgeMapIfNeeded()
{
	// In some cases we may not place all of the predicates in the NAry join in
	// the resulting tree of binary joins. If we see expressions that could cause
	// this situation, we'll create a map from expressions to edges, so that we
	// can find any unused edges to be placed in a select node on top of the join.
	//
	// Example:
	// select * from foo left join bar on foo.a=bar.a where coalesce(bar.b, 0) < 10;
	if (0 == m_child_pred_indexes->Size())
	{
		// all inner joins, all predicates will be placed
		return;
	}

	BOOL populate = false;
	// make a bitset b with all the LOJ right children
	CBitSet *loj_right_children = GPOS_NEW(m_mp) CBitSet(m_mp);

	for (ULONG c=0; c<m_child_pred_indexes->Size(); c++)
	{
		if (0 < *((*m_child_pred_indexes)[c]))
		{
			loj_right_children->ExchangeSet(c);
		}
	}

	for (ULONG en1 = 0; en1 < m_ulEdges; en1++)
	{
		SEdge *pedge = m_rgpedge[en1];

		if (pedge->m_loj_num == 0)
		{
			// check whether this inner join (WHERE) predicate refers to any LOJ right child
			// (whether its bitset overlaps with b)
			// or whether we see any local predicates (this should be uncommon)
			if (!loj_right_children->IsDisjoint(pedge->m_pbs) || 1 == pedge->m_pbs->Size())
			{
				populate = true;
				break;
			}
		}
	}

	if (populate)
	{
		m_expression_to_edge_map = GPOS_NEW(m_mp) ExpressionToEdgeMap(m_mp);

		for (ULONG en2 = 0; en2 < m_ulEdges; en2++)
		{
			SEdge *pedge = m_rgpedge[en2];

			pedge->AddRef();
			pedge->m_pexpr->AddRef();
			m_expression_to_edge_map->Insert(pedge->m_pexpr, pedge);
		}
	}

	loj_right_children->Release();
}

// add a select node with any remaining edges (predicates) that have
// not been incorporated in the join tree
CExpression *
CJoinOrderDPv2::AddSelectNodeForRemainingEdges(CExpression *join_expr)
{
	if (NULL == m_expression_to_edge_map)
	{
		return join_expr;
	}

	CExpressionArray *exprArray = GPOS_NEW(m_mp) CExpressionArray(m_mp);
	RecursivelyMarkEdgesAsUsed(join_expr);

	// find any unused edges and add them to a select
	for (ULONG en = 0; en < m_ulEdges; en++)
	{
		SEdge *pedge = m_rgpedge[en];

		if (pedge->m_fUsed)
		{
			// mark the edge as unused for the next alternative, where
			// we will have to repeat this check
			pedge->m_fUsed = false;
		}
		else
		{
			// found an unused edge, this one will need to go into
			// a select node on top of the join
			pedge->m_pexpr->AddRef();
			exprArray->Append(pedge->m_pexpr);
		}
	}

	if (0 < exprArray->Size())
	{
		CExpression *conj = CPredicateUtils::PexprConjunction(m_mp, exprArray);

		return GPOS_NEW(m_mp) CExpression(m_mp, GPOS_NEW(m_mp) CLogicalSelect(m_mp), join_expr, conj);
	}

	exprArray->Release();

	return join_expr;
}


void CJoinOrderDPv2::RecursivelyMarkEdgesAsUsed(CExpression *expr)
{
	GPOS_CHECK_STACK_SIZE;

	if (expr->Pop()->FLogical())
	{
		for (ULONG ul=0; ul< expr->Arity(); ul++)
		{
			RecursivelyMarkEdgesAsUsed((*expr)[ul]);
		}
	}
	else
	{
		GPOS_ASSERT(expr->Pop()->FScalar());
		const SEdge *edge = m_expression_to_edge_map->Find(expr);
		if (NULL != edge)
		{
			// we found the edge belonging to this expression, terminate the recursion
			const_cast<SEdge *>(edge)->m_fUsed = true;
			return;
		}

		// we should not reach the leaves of the tree without finding an edge
		GPOS_ASSERT(0 < expr->Arity());

		// this is an AND of multiple edges
		for (ULONG ul = 0; ul < expr->Arity(); ul++)
		{
			RecursivelyMarkEdgesAsUsed((*expr)[ul]);
		}
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
void
CJoinOrderDPv2::SearchJoinOrders
	(
	 ULONG left_level,
	 ULONG right_level
	)
{
	GPOS_ASSERT(left_level > 0 &&
				right_level > 0 &&
				left_level + right_level <= m_ulComps);

	SGroupInfoArray *left_group_info_array = GetGroupsForLevel(left_level);
	SGroupInfoArray *right_group_info_array = GetGroupsForLevel(right_level);
	SLevelInfo *current_level_info = Level(left_level+right_level);

	ULONG left_size = left_group_info_array->Size();
	ULONG right_size = right_group_info_array->Size();
	BOOL is_top_level = (left_level + right_level == m_ulComps);

	for (ULONG left_ix=0; left_ix<left_size; left_ix++)
	{
		SGroupInfo *left_group_info = (*left_group_info_array)[left_ix];
		CBitSet *left_bitset = left_group_info->m_atoms;
		ULONG right_ix = 0;

		// if pairs from the same level, start from the next
		// entry to avoid duplicate join combinations
		// i.e a join b and b join a, just try one
		// commutativity will take care of the other
		if (left_level == right_level)
		{
			right_ix = left_ix + 1;
		}

		for (; right_ix<right_size; right_ix++)
		{
			SGroupInfo *right_group_info = (*right_group_info_array)[right_ix];
			CBitSet *right_bitset = right_group_info->m_atoms;
			BOOL release_join_expr = false;

			if (!left_bitset->IsDisjoint(right_bitset))
			{
				// not a valid join, left and right tables must not overlap
				continue;
			}

			CExpression *join_expr = GetJoinExpr(left_group_info, right_group_info, false /*use the best expr*/);

			if (NULL != join_expr)
			{
				// we have a valid join
				CBitSet *join_bitset = GPOS_NEW(m_mp) CBitSet(m_mp, *left_bitset);

				// TODO: Reduce non-mandatory cross products

				join_bitset->Union(right_bitset);

				SGroupInfo *group_info = m_bitset_to_group_info_map->Find(join_bitset);

				if (NULL != group_info)
				{
					// we are dealing with a group that already has an existing expression in it
					CDouble newCost = DCost(group_info, left_group_info, right_group_info);

					if (newCost < group_info->m_best_expr_info->m_cost)
					{
						// this new expression is better than the one currently in the group,
						// so release the current expression and replace it with the new one
						SExpressionInfo *expr_info = group_info->m_best_expr_info;

						expr_info->m_expr->Release();
						expr_info->m_expr = join_expr;
						expr_info->m_left_child_group = left_group_info;
						expr_info->m_right_child_group = right_group_info;
						expr_info->m_cost = newCost;
					}
					else
					{
						// this new expression is not better than what we have in the group already,
						// mark it for release (it may still be used if we are at the top level)
						release_join_expr = true;
					}
					join_bitset->Release();
				}
				else
				{
					// this is a new group, insert it if we have not yet exceeded the maximum number of groups for this level
					group_info = GPOS_NEW(m_mp) SGroupInfo(join_bitset,
														   GPOS_NEW(m_mp) SExpressionInfo(join_expr,
																						  left_group_info,
																						  right_group_info));
					AddGroupInfo(current_level_info, group_info);
				}

				if (is_top_level)
				{
					// At the top level, we have only one group. To be able to return multiple results
					// for the xform, we keep the top k expressions (all from the same group) in a KHeap
					SExpressionInfo *top_k_expr = GPOS_NEW(m_mp) SExpressionInfo(join_expr,
																				 left_group_info,
																				 right_group_info);

					join_expr->AddRef();
					top_k_expr->m_cost = DCost(group_info, left_group_info, right_group_info);
					m_top_k_expressions->Insert(top_k_expr);
				}

				if (release_join_expr)
				{
					join_expr->Release();
				}
			}
		}
	}
}


void
CJoinOrderDPv2::AddGroupInfo(SLevelInfo *levelInfo, SGroupInfo *groupInfo)
{
	// derive stats and cost
	if (1 < levelInfo->m_level)
	{
		groupInfo->m_expr_for_stats = GetJoinExpr
										(
										 groupInfo->m_best_expr_info->m_left_child_group,
										 groupInfo->m_best_expr_info->m_right_child_group,
										 true /* use children's stats expressions */
										);
		DeriveStats(groupInfo->m_expr_for_stats);
	}
	else
	{
		groupInfo->m_expr_for_stats = groupInfo->m_best_expr_info->m_expr;
		groupInfo->m_expr_for_stats->AddRef();
		// stats at level 1 must have been derived before we enter DPv2
		GPOS_ASSERT(NULL != groupInfo->m_expr_for_stats->Pstats());
	}
	groupInfo->m_best_expr_info->m_cost = DCost
				(
				 groupInfo,
				 groupInfo->m_best_expr_info->m_left_child_group,
				 groupInfo->m_best_expr_info->m_right_child_group
				);

	if (NULL == levelInfo->m_top_k_groups)
	{
		// no limits, just add the group to the array
		// note that the groups won't be sorted by cost in this case
		levelInfo->m_groups->Append(groupInfo);
	}
	else
	{
		// insert into the KHeap for now, the best groups will be transferred to
		// levelInfo->m_groups when we call FinalizeLevel()
		levelInfo->m_top_k_groups->Insert(groupInfo);
	}

	if (1 < levelInfo->m_level)
	{
		// also insert into the bitset to group map
		groupInfo->m_atoms->AddRef();
		groupInfo->AddRef();
		m_bitset_to_group_info_map->Insert(groupInfo->m_atoms, groupInfo);
	}
}


void
CJoinOrderDPv2::FinalizeLevel(ULONG level)
{
	GPOS_ASSERT(level >= 2);
	SLevelInfo *level_info = Level(level);

	if (NULL != level_info->m_top_k_groups)
	{
		SGroupInfo *winner;

		while (NULL != (winner = level_info->m_top_k_groups->RemoveBestElement()))
		{
			// add the next best group to the level array, sorted by ascending cost
			level_info->m_groups->Append(winner);
		}

		// release the remaining groups at this time, they won't be needed anymore
		level_info->m_top_k_groups->Clear();
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
void
CJoinOrderDPv2::SearchBushyJoinOrders
	(
	 ULONG current_level
	)
{
	// try bushy joins of bitsets of level x and y, where
	// x + y = current_level and x > 1 and y > 1
	// note that join trees of level 3 and below are never bushy,
	// so this loop only executes at current_level >= 4
	for (ULONG left_level = 2; left_level < current_level-1; left_level++)
	{
		if (LevelIsFull(current_level))
		{
			// we've exceeded the number of joins for which we generate bushy trees
			// TODO: Transition off of bushy joins more gracefully, note that bushy
			// trees usually do't add any more groups, they just generate more
			// expressions for existing groups
			return;
		}

		ULONG right_level = current_level - left_level;
		if (left_level > right_level)
			// we've already considered the commuted join
			break;
		SearchJoinOrders(left_level, right_level);
	}

	return;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::PExprExpand
//
//	@doc:
//		Main driver for join order enumeration, called by xform
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::PexprExpand()
{
	// put the "atoms", the nodes of the join tree that
	// are not joins themselves, at the first level
	SLevelInfo *atom_level = Level(1);

	for (ULONG atom_id = 0; atom_id < m_ulComps; atom_id++)
	{
		CBitSet *atom_bitset = GPOS_NEW(m_mp) CBitSet(m_mp);
		atom_bitset->ExchangeSet(atom_id);
		CExpression *pexpr_atom = m_rgpcomp[atom_id]->m_pexpr;
		pexpr_atom->AddRef();
		SGroupInfo *atom_info = GPOS_NEW(m_mp) SGroupInfo(atom_bitset,
														  GPOS_NEW(m_mp) SExpressionInfo(pexpr_atom,
																						 NULL,
																						 NULL));
		AddGroupInfo(atom_level, atom_info);
	}

	COptimizerConfig *optimizer_config = COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();
	const CHint *phint = optimizer_config->GetHint();
	ULONG join_order_exhaustive_limit = phint->UlJoinOrderDPLimit();

	// for larger joins, compute the limit for the number of groups at each level, this
	// follows the number of groups for the largest join for which we do exhaustive search
	if (join_order_exhaustive_limit < m_ulComps)
	{
		for (ULONG l=2; l<=m_ulComps; l++)
		{
			ULONG number_of_allowed_groups = 0;

			if (join_order_exhaustive_limit < l)
			{
				// at lower levels, limit the number of groups to that of an
				// <join_order_exhaustive_limit>-way join
				number_of_allowed_groups = NChooseK(join_order_exhaustive_limit, l);
			}
			else
			{
				// beyond that, use greedy (keep only one group per level)
				number_of_allowed_groups = 1;
			}

			// add a KHeap to this level, so that we can collect the k best expressions
			// while we are building the level
			Level(l)->m_top_k_groups = GPOS_NEW(m_mp) KHeap<SGroupInfoArray, SGroupInfo>
																	 (
																	  m_mp,
																	  this,
																	  number_of_allowed_groups
																	 );
		}
	}

	// build n-ary joins from the bottom up, starting with 2-way, 3-way up to m_ulComps-way
	for (ULONG current_join_level = 2; current_join_level <= m_ulComps; current_join_level++)
	{
		// build linear joins, with a "current_join_level-1"-way join on one
		// side and an atom on the other side
		SearchJoinOrders(current_join_level-1, 1);

		// build bushy trees - joins between two other joins
		SearchBushyJoinOrders(current_join_level);

		// finalize level, enforce limit for groups
		FinalizeLevel(current_join_level);
	}
}


CExpression*
CJoinOrderDPv2::GetNextOfTopK()
{
	SExpressionInfo *join_result_info = m_top_k_expressions->RemoveBestElement();

	if (NULL == join_result_info)
	{
		return NULL;
	}

	CExpression *join_result = join_result_info->m_expr;

	join_result->AddRef();
	join_result_info->Release();

	return AddSelectNodeForRemainingEdges(join_result);
}


BOOL
CJoinOrderDPv2::IsRightChildOfNIJ
	(SGroupInfo *groupInfo,
	 CExpression **onPredToUse,
	 CBitSet **requiredBitsOnLeft
	)
{
	*onPredToUse = NULL;
	*requiredBitsOnLeft = NULL;

	if (1 != groupInfo->m_atoms->Size() || 0 == m_on_pred_conjuncts->Size())
	{
		// this is not a non-join vertex component (and only those can be right
		// children of NIJs), or the entire NAry join doesn't contain any NIJs
		return false;
	}

	// get the child predicate index for the non-join vertex component represented
	// by this component
	CBitSetIter iter(*groupInfo->m_atoms);

	// there is only one bit set for this component
	iter.Advance();

	ULONG childPredIndex = *(*m_child_pred_indexes)[iter.Bit()];

	if (GPOPT_ZERO_INNER_JOIN_PRED_INDEX != childPredIndex)
	{
		// this non-join vertex component is the right child of an
		// NIJ, return the ON predicate to use and also return TRUE
		*onPredToUse = (*m_on_pred_conjuncts)[childPredIndex-1];
		// also return the required minimal component on the left side of the join
		*requiredBitsOnLeft = (*m_non_inner_join_dependencies)[childPredIndex-1];
		return true;
	}

	// this is a non-join vertex component that is not the right child of an NIJ
	return false;
}


ULONG
CJoinOrderDPv2::FindLogicalChildByNijId(ULONG nij_num)
{
	GPOS_ASSERT(NULL != m_child_pred_indexes);

	for (ULONG c=0; c<m_child_pred_indexes->Size(); c++)
	{
		if (*(*m_child_pred_indexes)[c] == nij_num)
		{
			return c;
		}
	}

	return 0;
}


ULONG CJoinOrderDPv2::NChooseK(ULONG n, ULONG k)
{
	ULLONG numerator = 1;
	ULLONG denominator = 1;

	for (ULONG i=1; i<=k; i++)
	{
		numerator *= n+1-i;
		denominator *= i;
	}

	return (ULONG) (numerator / denominator);
}


BOOL
CJoinOrderDPv2::LevelIsFull(ULONG level)
{
	SLevelInfo *li = Level(level);

	if (NULL == li->m_top_k_groups)
	{
		return false;
	}

	return li->m_top_k_groups->IsLimitExceeded();
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
	// increase GPOS_LOG_MESSAGE_BUFFER_SIZE in file ILogger.h if the output of this method gets truncated
	ULONG num_levels = m_join_levels->Size();
	ULONG num_bitsets = 0;
	CPrintPrefix pref(NULL, "      ");

	for (ULONG lev=1; lev<num_levels; lev++)
	{
		SGroupInfoArray *bitsets_this_level = GetGroupsForLevel(lev);
		ULONG num_bitsets_this_level = bitsets_this_level->Size();

		os << "CJoinOrderDPv2 - Level: " << lev << " (" << bitsets_this_level->Size() << " group(s))" << std::endl;

		for (ULONG c=0; c<num_bitsets_this_level; c++)
		{
			SGroupInfo *gi = (*bitsets_this_level)[c];
			num_bitsets++;
			os << "   Group: ";
			gi->m_atoms->OsPrint(os);
			os << std::endl;
			if (!gi->IsAnAtom())
			{
				os << "   Child groups: ";
				gi->m_best_expr_info->m_left_child_group->m_atoms->OsPrint(os);
				if (COperator::EopLogicalLeftOuterJoin == gi->m_best_expr_info->m_expr->Pop()->Eopid())
				{
					os << " left";
				}
				os << " join ";
				gi->m_best_expr_info->m_right_child_group->m_atoms->OsPrint(os);
				os << std::endl;
			}
			if (NULL != gi->m_expr_for_stats)
			{
				os << "   Rows: " << gi->m_expr_for_stats->Pstats()->Rows() << std::endl;
			}
			os << "   Cost: ";
			gi->m_best_expr_info->m_cost.OsPrint(os);
			os << std::endl;
			if (NULL == gi->m_best_expr_info)
			{
				os << "Expression: None";
			}
			else
			{
				if (lev == 1)
				{
					os << "   Atom: " << std::endl;
					gi->m_best_expr_info->m_expr->OsPrint(os, &pref);
				}
				else if (lev < num_levels-1)
				{
					os << "   Join predicate: " << std::endl;
					(*gi->m_best_expr_info->m_expr)[2]->OsPrint(os, &pref);
				}
				else
				{
					os << "   Top-level expression: " << std::endl;
					gi->m_best_expr_info->m_expr->OsPrint(os, &pref);
				}
			}
			os << std::endl;
		}
	}

	os << "CJoinOrderDPv2 - total number of groups: " << num_bitsets << std::endl;

	return os;
}


#ifdef GPOS_DEBUG
void
CJoinOrderDPv2::DbgPrint()
{
	CAutoTrace at(m_mp);

	OsPrint(at.Os());
}
#endif


// EOF
