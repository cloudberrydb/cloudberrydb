/*-------------------------------------------------------------------------
 *
 * nodeLimit.c
 *	  Routines to handle limiting of query results where appropriate
 *
 * Portions Copyright (c) 2005-2008, Cloudberry inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeLimit.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecLimit		- extract a limited range of tuples
 *		ExecInitLimit	- initialize node and subnodes..
 *		ExecEndLimit	- shutdown node and subnodes
 */

#include "postgres.h"

#include "cdb/cdbvars.h"
#include "executor/execdebug.h"
#include "executor/executor.h"
#include "executor/nodeLimit.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"

#include "vecexecutor/nodeLimit.h"
#include "vecexecutor/executor.h"
#include "vecexecutor/execnodes.h"
#include "vecexecutor/execAmi.h"
#include "vecexecutor/execslot.h"
#include "utils/wrapper.h"

static void recompute_limits(LimitState *node);
static int64 compute_tuples_needed(LimitState *node);

/* ----------------------------------------------------------------
 *		ExecLimit
 *
 *		This is a very simple node which just performs LIMIT/OFFSET
 *		filtering on the stream of tuples returned by a subplan.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *			/* return: a tuple or NULL */
ExecVecLimit_guts(PlanState *pstate)
{
	TupleTableSlot *slot;
	VecLimitState *vnode = (VecLimitState *)pstate;

	CHECK_FOR_INTERRUPTS();

	/*
	 * get information from the node
	 */
	slot = ExecuteVecPlan(&vnode->estate);

	return slot;
}

static TupleTableSlot *
ExecVecLimit(PlanState *node)
{
	TupleTableSlot *result;

	result = ExecVecLimit_guts(node);

	if (TupIsNull(result) && ScanDirectionIsForward(node->state->es_direction) &&
		!((LimitState *) node)->expect_rescan)
	{
		/*
		 * CDB: We'll read no more from inner subtree. To keep our sibling
		 * QEs from being starved, tell source QEs not to clog up the
		 * pipeline with our never-to-be-consumed data.
		 */
		ExecVecSquelchNode(node);
	}

	return result;
}

/*
 * Evaluate the limit/offset expressions --- done at startup or rescan.
 *
 * This is also a handy place to reset the current-position state info.
 */
static void
recompute_limits(LimitState *node)
{
	ExprContext *econtext = node->ps.ps_ExprContext;
	Datum		val;
	bool		isNull;

	if (node->limitOffset)
	{
		val = ExecEvalExprSwitchContext(node->limitOffset,
										econtext,
										&isNull);
		/* Interpret NULL offset as no offset */
		if (isNull)
			node->offset = 0;
		else
		{
			node->offset = DatumGetInt64(val);
			if (node->offset < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE),
						 errmsg("OFFSET must not be negative")));
		}
	}
	else
	{
		/* No OFFSET supplied */
		node->offset = 0;
	}

	if (node->limitCount)
	{
		val = ExecEvalExprSwitchContext(node->limitCount,
										econtext,
										&isNull);
		/* Interpret NULL count as no count (LIMIT ALL) */
		if (isNull)
		{
			node->count = 0;
			node->noCount = true;
		}
		else
		{
			node->count = DatumGetInt64(val);
			if (node->count < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE),
						 errmsg("LIMIT must not be negative")));
			node->noCount = false;
		}
	}
	else
	{
		/* No COUNT supplied */
		node->count = 0;
		node->noCount = true;
	}

	/* Reset position to start-of-scan */
	node->position = 0;
	node->subSlot = NULL;

	/* Set state-machine state */
	node->lstate = LIMIT_RESCAN;

	/*
	 * Notify child node about limit.  Note: think not to "optimize" by
	 * skipping ExecSetTupleBound if compute_tuples_needed returns < 0.  We
	 * must update the child node anyway, in case this is a rescan and the
	 * previous time we got a different result.
	 */
	ExecSetTupleBound(compute_tuples_needed(node), outerPlanState(node));
}

/*
 * Compute the maximum number of tuples needed to satisfy this Limit node.
 * Return a negative value if there is not a determinable limit.
 */
static int64
compute_tuples_needed(LimitState *node)
{
	if ((node->noCount) || (node->limitOption == LIMIT_OPTION_WITH_TIES))
		return -1;
	/* Note: if this overflows, we'll return a negative value, which is OK */
	return node->count + node->offset;
}

/* ----------------------------------------------------------------
 *		ExecInitLimit
 *
 *		This initializes the limit node state structures and
 *		the node's subplan.
 * ----------------------------------------------------------------
 */
LimitState *
ExecInitVecLimit(Limit *node, EState *estate, int eflags)
{
	LimitState *limitstate;
	VecLimitState *vlimitstate;
	Plan	   *outerPlan;
	PlanState *child_node;

	/* check for unsupported flags */
	Assert(!(eflags & EXEC_FLAG_MARK));

	/*
	 * create state structure
	 */
	vlimitstate = (VecLimitState *) palloc0(sizeof(VecLimitState));
	limitstate  = (LimitState *)vlimitstate;
	NodeSetTag(limitstate, T_LimitState);
	limitstate->ps.plan = (Plan *) node;
	limitstate->ps.state = estate;
	limitstate->ps.ExecProcNode = ExecVecLimit;

	limitstate->lstate = LIMIT_INITIAL;

	/*
	 * Miscellaneous initialization
	 *
	 * Limit nodes never call ExecQual or ExecProject, but they need an
	 * exprcontext anyway to evaluate the limit/offset parameters in.
	 */
	ExecAssignExprContext(estate, &limitstate->ps);

	/*
 	 * initialize child expressions
	 */
	limitstate->limitOffset = ExecInitExpr((Expr *) node->limitOffset,
										   (PlanState *) limitstate);
	limitstate->limitCount = ExecInitExpr((Expr *) node->limitCount,
										  (PlanState *) limitstate);
	/*
	 * initialize outer plan
	 */
	outerPlan = outerPlan(node);
	outerPlanState(limitstate) = VecExecInitNode(outerPlan, estate, eflags);
	child_node = outerPlanState(limitstate);

	/*
	 * initialize child expressions
	 * FIXME:  expression needs refactoring
	 */
	 
	/*
	 * Initialize result type.
	 */
	ExecInitResultTupleSlotTL(&limitstate->ps, &TTSOpsVecTuple);

	limitstate->ps.resultopsset = true;
	limitstate->ps.resultops = ExecGetResultSlotOps(outerPlanState(limitstate),
													&limitstate->ps.resultopsfixed);

	/*
	 * limit nodes do no projections, so initialize projection info for this
	 * node appropriately
	 */
	limitstate->ps.ps_ProjInfo = NULL;

	limitstate->expect_rescan = ((eflags & EXEC_FLAG_REWIND) != 0);

	/*
	 * Initialize the equality evaluation, to detect ties.
	 */
	if (node->limitOption == LIMIT_OPTION_WITH_TIES)
	{
		TupleDesc	desc;
		const TupleTableSlotOps *ops;

		desc = ExecGetResultType(outerPlanState(limitstate));
		ops = ExecGetResultSlotOps(outerPlanState(limitstate),  NULL);

		limitstate->last_slot = ExecInitExtraTupleSlot(estate, desc, ops);
		limitstate->eqfunction = execTuplesMatchPrepare(desc,
														node->uniqNumCols,
														node->uniqColIdx,
														node->uniqOperators,
														node->uniqCollations,
														&limitstate->ps);
	}

	/*
	 * It's necessary to call recompute_limits() before BuildVecPlan() to get
	 * offset, count and noCount.
	 */
	recompute_limits(limitstate);
	BuildVecPlan((PlanState *)vlimitstate, &vlimitstate->estate);

	return limitstate;
}

/* ----------------------------------------------------------------
 *		ExecEndLimit
 *
 *		This shuts down the subplan and frees resources allocated
 *		to this node.
 * ----------------------------------------------------------------
 */
void
ExecEndVecLimit(LimitState *node)
{
	if (node->ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ps.ps_ResultTupleSlot);
	if (node->last_slot)
		ExecClearTuple(node->last_slot);
	ExecFreeExprContext(&node->ps);
	VecExecEndNode(outerPlanState(node));
}


void
ExecReScanVecLimit(LimitState *node)
{
	/*
	 * Recompute limit/offset in case parameters changed, and reset the state
	 * machine.  We must do this before rescanning our child node, in case
	 * it's a Sort that we are passing the parameters down to.
	 */
	recompute_limits(node);

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (node->ps.lefttree->chgParam == NULL)
		ExecReScan(node->ps.lefttree);
}
