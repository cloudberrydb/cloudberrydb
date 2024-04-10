#include "postgres.h"

#include "access/htup_details.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/instrument.h"	/* Instrumentation */
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "utils/arrow.h"
#include "utils/faultinjector.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "vecexecutor/nodeHash.h"
#include "vecexecutor/nodeHashjoin.h"
#include "vecexecutor/executor.h"
#include "vecexecutor/execslot.h"
#include "executor/nodeRuntimeFilter.h"

#include "cdb/cdbvars.h"
#include "miscadmin.h"			/* work_mem */

#include "vecexecutor/execAmi.h"
static List *to_exprlist(List *exprstatelist);
static bool isNotDistinctJoin(List *qualList);
extern bool Test_print_prefetch_joinqual;

/* ----------------------------------------------------------------
 *		ExecInitHashJoin
 *
 *		Init routine for HashJoin node.
 * ----------------------------------------------------------------
 */
HashJoinState *
ExecInitVecHashJoin(HashJoin *node, EState *estate, int eflags)
{
	HashJoinState *hjstate;
	VecHashJoinState *vhjstate;
	PlanState  *outerState;
	HashState  *hstate;
	Plan	   *outerNode;
	Hash	   *hashNode;
	TupleDesc	outerDesc,
				innerDesc,
				hashkeysDesc;
	const TupleTableSlotOps *ops;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	vhjstate = (VecHashJoinState*) palloc0(sizeof(VecHashJoinState));
	hjstate = (HashJoinState *) vhjstate;
	NodeSetTag(hjstate, T_HashJoinState);
	hjstate->js.ps.plan = (Plan *) node;
	hjstate->js.ps.state = estate;
	hjstate->reuse_hashtable = (eflags & EXEC_FLAG_REWIND) != 0;

	/*
	 * If eflag contains EXEC_FLAG_REWIND,
	 * then this node is not eager free safe.
	 */
	hjstate->delayEagerFree = (eflags & EXEC_FLAG_REWIND) != 0;
	/*
	 * See ExecHashJoinInitializeDSM() and ExecHashJoinInitializeWorker()
	 * where this function may be replaced with a parallel version, if we
	 * managed to launch a parallel query.
	 */
	hjstate->js.ps.ExecProcNode = ExecVecHashJoin;
	hjstate->js.jointype = node->join.jointype;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &hjstate->js.ps);

	if (node->hashqualclauses != NIL)
	{
		/* CDB: This must be an IS NOT DISTINCT join!  */
		if (isNotDistinctJoin(node->hashqualclauses))
			hjstate->hj_nonequijoin = true;
	}
	else
		hjstate->hj_nonequijoin = false;

	/*
	 * MPP-3300, we only pre-build hashtable if we need to (this is relaxing
	 * the fix to MPP-989)
	 */
	hjstate->prefetch_inner = node->join.prefetch_inner;
	hjstate->prefetch_joinqual = node->join.prefetch_joinqual;
	hjstate->prefetch_qual = node->join.prefetch_qual;

	if (Test_print_prefetch_joinqual && hjstate->prefetch_joinqual)
		elog(NOTICE,
			 "prefetch join qual in slice %d of plannode %d",
			 currentSliceId, ((Plan *) node)->plan_node_id);

	/*
	 * reuse GUC Test_print_prefetch_joinqual to output debug information for
	 * prefetching non join qual
	 */
	if (Test_print_prefetch_joinqual && hjstate->prefetch_qual)
		elog(NOTICE,
			 "prefetch non join qual in slice %d of plannode %d",
			 currentSliceId, ((Plan *) node)->plan_node_id);

	/*
	 * initialize child nodes
	 *
	 * Note: we could suppress the REWIND flag for the inner input, which
	 * would amount to betting that the hash will be a single batch.  Not
	 * clear if this would be a win or not.
	 */
	outerNode = outerPlan(node);
	hashNode = (Hash *) innerPlan(node);

	/*
	 * XXX The following order are significant.  We init Hash first, then the outerNode
	 * this is the same order as we execute (in the sense of the first exec called).
	 * Until we have a better way to uncouple, share input needs this to be true.  If the
	 * order is wrong, when both hash and outer node have share input and (both ?) have 
	 * a subquery node, share input will fail because the estate of the nodes can not be
	 * set up correctly.
	 */
    innerPlanState(hjstate) = VecExecInitNode((Plan *) hashNode, estate, eflags);
    innerDesc = ExecGetResultType(innerPlanState(hjstate));
	((HashState *) innerPlanState(hjstate))->hs_keepnull = hjstate->hj_nonequijoin;

	outerPlanState(hjstate) = VecExecInitNode(outerNode, estate, eflags);
	outerDesc = ExecGetResultType(outerPlanState(hjstate));

	/*
	 * Initialize result slot, type and projection.
	 */
	ExecInitResultTupleSlotTL(&hjstate->js.ps, &TTSOpsVecTuple);
	ExecAssignProjectionInfo(&hjstate->js.ps, NULL);

	/*
	 * tuple table initialization
	 */
	ops = ExecGetResultSlotOps(outerPlanState(hjstate), NULL);
	hjstate->hj_OuterTupleSlot = ExecInitExtraTupleSlot(estate, outerDesc,
														ops);

	/*
	 * detect whether we need only consider the first matching inner tuple
	 */
	hjstate->js.single_match = (node->join.inner_unique ||
								node->join.jointype == JOIN_SEMI);

	/* set up null tuples for outer joins, if needed */
	switch (node->join.jointype)
	{
		case JOIN_INNER:
		case JOIN_SEMI:
			break;
		case JOIN_LEFT:
		case JOIN_ANTI:
		case JOIN_LASJ_NOTIN:
			hjstate->hj_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate, innerDesc, &TTSOpsVecTuple);
			break;
		case JOIN_RIGHT:
			hjstate->hj_NullOuterTupleSlot =
				ExecInitNullTupleSlot(estate, outerDesc, &TTSOpsVecTuple);
			break;
		case JOIN_FULL:
			hjstate->hj_NullOuterTupleSlot =
				ExecInitNullTupleSlot(estate, outerDesc, &TTSOpsVecTuple);
			hjstate->hj_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate, innerDesc, &TTSOpsVecTuple);
			break;
		default:
			elog(ERROR, "unrecognized join type: %d",
				 (int) node->join.jointype);
	}

	/*
	 * initialize child expressions
	 */
	hjstate->js.ps.qual =
		ExecInitQual(node->join.plan.qual, (PlanState *) hjstate);
	hjstate->js.joinqual =
		ExecInitQual(node->join.joinqual, (PlanState *) hjstate);
	hjstate->hashclauses =
		ExecInitQual(node->hashclauses, (PlanState *) hjstate);

	if (node->hashqualclauses != NIL)
	{
		hjstate->hashqualclauses =
			ExecInitQual(node->hashqualclauses, (PlanState *) hjstate);
	}
	else
	{
		hjstate->hashqualclauses = hjstate->hashclauses;
	}

	/*
	 * initialize hash-specific info
	 */
	hjstate->hj_HashTable = NULL;
	hjstate->hj_FirstOuterTupleSlot = NULL;

	hjstate->hj_CurHashValue = 0;
	hjstate->hj_CurBucketNo = 0;
	hjstate->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
	hjstate->hj_CurTuple = NULL;

	hjstate->hj_OuterHashKeys = ExecInitExprList(node->hashkeys,
												 (PlanState *) hjstate);
	hjstate->hj_HashOperators = node->hashoperators;
	hjstate->hj_Collations = node->hashcollations;

	hjstate->hj_MatchedOuter = false;
	hjstate->hj_OuterNotEmpty = false;

	hashkeysDesc = ExecTypeFromExprList(to_exprlist(hjstate->hj_OuterHashKeys));

	/* Setup the relationship of HashJoin, Hash and RuntimeFilter node. */
	hstate = (HashState *) innerPlanState(hjstate);
	outerState = outerPlanState(hjstate);
	if (IsA(outerState, RuntimeFilterState))
	{
		RuntimeFilterState *rfstate = (RuntimeFilterState *) outerState;
		rfstate->hjstate = hjstate;
		hstate->rfstate = rfstate;
		ExecInitRuntimeFilterFinish(rfstate, hstate->ps.plan->plan_rows);
	}

	/* Init Arrow execute plan */
	PostBuildVecPlan((PlanState *)vhjstate, &vhjstate->estate);
	return hjstate;
}

TupleTableSlot *
ExecVecHashJoin(PlanState *pstate)
{
	HashJoinState *node = castNode(HashJoinState, pstate);
	VecHashJoinState *vnode = (VecHashJoinState *)node;

	int rows, rest, length;
	g_autoptr(GArrowRecordBatch) slice_rb = NULL;

	if (vnode->offset >= vnode->nrows)
	{
		if (vnode->result_slot)
			ExecClearTuple(vnode->result_slot);
		if (vnode->origin_rb)
			ARROW_FREE_BATCH(&vnode->origin_rb);
		vnode->result_slot = NULL;
		vnode->offset      = 0;
		vnode->nrows       = 0;

		TupleTableSlot *slot = ExecuteVecPlan(&vnode->estate);
		if (TupIsNull(slot))
		{
			if (!((HashJoinState *) pstate)->reuse_hashtable
			&&  !((HashJoinState *) pstate)->delayEagerFree) 
			{
				ExecVecSquelchNode(pstate);
			}	
			return slot;
		}	

		rows = GetNumRows(slot);
		if (rows <= max_batch_size)
			return slot;

		vnode->result_slot = slot;
		vnode->origin_rb   = g_object_ref(GetBatch(slot));
		vnode->offset      = 0;
		vnode->nrows       = GetNumRows(slot);
	}

	ExecClearTuple(vnode->result_slot);

	rest   = vnode->nrows - vnode->offset;
	length = rest > max_batch_size ? max_batch_size : rest;

	slice_rb = garrow_record_batch_slice(vnode->origin_rb, vnode->offset, length);

	vnode->offset += length;

	ExecStoreBatch(vnode->result_slot, slice_rb);
	RebuildRuntimeSchema(vnode->result_slot);

	return vnode->result_slot;
}

static void
ExecEagerFreeVecHashJoin(VecHashJoinState *vnode)
{
	vnode->hashkeys_econtext = NULL;

	if (vnode->origin_rb)
		ARROW_FREE_BATCH(&vnode->origin_rb);
	FreeVecExecuteState(&vnode->estate);
}

void
ExecEndVecHashJoin(HashJoinState *node)
{
	VecHashJoinState *vnode = (VecHashJoinState *) node;
	ExecEagerFreeVecHashJoin(vnode);
	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->js.ps);
	/*
	 * clean out the tuple table
	 */
	if (vnode->result_slot)
		ExecClearTuple(vnode->result_slot);
	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);

	/*
	 * clean up subtrees
	 */
	VecExecEndNode(outerPlanState(node));
	VecExecEndNode(innerPlanState(node));
}

static List*
to_exprlist(List *exprstatelist)
{
	List *expr_list = NIL;

	ListCell *l;
	foreach(l, exprstatelist)
	{
		ExprState *state = lfirst(l);
		expr_list = lappend(expr_list, state->expr);
	}

	return expr_list;
}

static bool
isNotDistinctJoin(List *qualList)
{
	ListCell   *lc;

	foreach(lc, qualList)
	{
		BoolExpr   *bex = (BoolExpr *) lfirst(lc);
		DistinctExpr *dex;

		if (IsA(bex, BoolExpr) &&bex->boolop == NOT_EXPR)
		{
			dex = (DistinctExpr *) linitial(bex->args);

			if (IsA(dex, DistinctExpr))
				return true;	/* We assume the rest follow suit! */
		}
	}
	return false;
}

void
ExecSquelchVecHashJoin(HashJoinState* node)
{	
	VecHashJoinState* vnode = (VecHashJoinState *) node;
	ExecEagerFreeVecHashJoin(vnode);
	ExecVecSquelchNode(outerPlanState(node));
	ExecVecSquelchNode(innerPlanState(node));
}