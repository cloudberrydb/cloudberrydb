/*-------------------------------------------------------------------------
 *
 * nodeNestloop.c
 *	  routines to support nest-loop joins
 *
 * Portions Copyright (c) 2005-2008, Cloudberry inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeNestloop.c
 *
 *-------------------------------------------------------------------------
 */
/*
 *	 INTERFACE ROUTINES
 *		ExecVecNestLoop	 - process a nestloop join of two plans
 *		ExecInitVecNestLoop - initialize the join
 *		ExecEndVecNestLoop  - shut down the join
 */

#include "postgres.h"

#include "cdb/cdbvars.h"
#include "executor/execdebug.h"
#include "optimizer/clauses.h"
#include "utils/lsyscache.h"
#include "miscadmin.h"
#include "utils/memutils.h"

#include "vecexecutor/nodeNestloop.h"
#include "vecexecutor/executor.h"
#include "vecexecutor/execAmi.h"

static void splitJoinQualExpr(List *joinqual, List **inner_join_keys_p, List **outer_join_keys_p);
static void extractFuncExprArgs(Expr *clause, List **lclauses, List **rclauses);

static TupleTableSlot *
ExecVecNestLoop(PlanState *pstate)
{
	TupleTableSlot *result;
	NestLoopState *node = castNode(NestLoopState, pstate);
	VecNestLoopState *vnode = (VecNestLoopState *)node;

	/* set shortcut flag if sub node is material
	 * the nestloop join hold the whole inner table
	 * in memory, so not need store tuples any more */

	PlanState  *innerNode;
	innerNode = innerPlanState(node);

	if(IsA(innerNode, MaterialState))
	{
		VecMaterialState *vms = (VecMaterialState*)innerNode;
		vms->is_skip = true;
	}

	result = ExecuteVecPlan(&vnode->estate);
	if (TupIsNull(result))
	{
		/*
		 * CDB: We'll read no more from inner subtree. To keep our
		 * sibling QEs from being starved, tell source QEs not to
		 * clog up the pipeline with our never-to-be-consumed
		 * data.
		 */
		ExecVecSquelchNode(pstate);
	}
	return result;
}

/* ----------------------------------------------------------------
 *		ExecInitVecNestLoop
 * ----------------------------------------------------------------
 */
NestLoopState *
ExecInitVecNestLoop(NestLoop *node, EState *estate, int eflags)
{
	VecNestLoopState *vnlstate;
	NestLoopState *nlstate;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	NL1_printf("ExecInitNestLoop: %s\n",
			   "initializing node");

	/*
	 * create state structure
	 */
	vnlstate = (VecNestLoopState *)palloc0(sizeof(VecNestLoopState));
	nlstate = (NestLoopState *) vnlstate;
	NodeSetTag(nlstate, T_NestLoopState);
	nlstate->js.ps.plan = (Plan *) node;
	nlstate->js.ps.state = estate;
	nlstate->js.ps.ExecProcNode = ExecVecNestLoop;

	nlstate->shared_outer = node->shared_outer;

	nlstate->prefetch_inner = node->join.prefetch_inner;
	nlstate->prefetch_joinqual = node->join.prefetch_joinqual;
	nlstate->prefetch_qual = node->join.prefetch_qual;

	/*CDB-OLAP*/
	nlstate->reset_inner = false;
	nlstate->require_inner_reset = !node->singleton_outer;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &nlstate->js.ps);

	/*
	 * initialize child nodes
	 *
	 * If we have no parameters to pass into the inner rel from the outer,
	 * tell the inner child that cheap rescans would be good.  If we do have
	 * such parameters, then there is no point in REWIND support at all in the
	 * inner child, because it will always be rescanned with fresh parameter
	 * values.
	 */

	/*
	 * XXX ftian: Because share input need to make the whole thing into a tree,
	 * we can put the underlying share only under one shareinputscan.  During execution,
	 * we need the shareinput node that has underlying subtree be inited/executed first.
	 * This means, 
	 * 	1. Init and first ExecProcNode call must be in the same order
	 *	2. Init order above is the same as the tree walking order in cdbmutate.c
	 * For nest loop join, it is more strange than others.  Depends on prefetch_inner,
	 * the execution order may change.  Handle this correctly here.
	 * 
	 * Until we find a better way to handle the dependency of ShareInputScan on 
	 * execution order, this is pretty much what we have to deal with.
	 */
	if (node->nestParams == NIL)
		eflags |= EXEC_FLAG_REWIND;
	else
		eflags &= ~EXEC_FLAG_REWIND;
	if (nlstate->prefetch_inner)
	{
		innerPlanState(nlstate) = VecExecInitNode(innerPlan(node), estate, eflags);
		if (!node->shared_outer)
			outerPlanState(nlstate) = VecExecInitNode(outerPlan(node), estate, eflags);
	}
	else
	{
		if (!node->shared_outer)
			outerPlanState(nlstate) = VecExecInitNode(outerPlan(node), estate, eflags);
		innerPlanState(nlstate) = VecExecInitNode(innerPlan(node), estate, eflags);
	}

	/*
	 * Initialize result slot, type and projection.
	 */
	ExecInitResultTupleSlotTL(&nlstate->js.ps, &TTSOpsVecTuple);
	ExecAssignProjectionInfo(&nlstate->js.ps, NULL);

	/*
	 * initialize child expressions
	 */
	nlstate->js.ps.qual =
		ExecInitQual(node->join.plan.qual, (PlanState *) nlstate);
	nlstate->js.jointype = node->join.jointype;

	if (node->join.jointype == JOIN_LASJ_NOTIN)
	{
		List	   *inner_join_keys;
		List	   *outer_join_keys;
		ListCell   *lc;

		/* not initialized yet */
		Assert(nlstate->nl_InnerJoinKeys == NIL);
		Assert(nlstate->nl_OuterJoinKeys == NIL);

		splitJoinQualExpr(node->join.joinqual,
						  &inner_join_keys,
						  &outer_join_keys);
		foreach(lc, inner_join_keys)
		{
			Expr	   *expr = (Expr *) lfirst(lc);
			ExprState  *exprstate;

			exprstate = ExecInitExpr(expr, (PlanState *) nlstate);

			nlstate->nl_InnerJoinKeys = lappend(nlstate->nl_InnerJoinKeys,
												exprstate);
		}
		foreach(lc, outer_join_keys)
		{
			Expr	   *expr = (Expr *) lfirst(lc);
			ExprState  *exprstate;

			exprstate = ExecInitExpr(expr, (PlanState *) nlstate);

			nlstate->nl_OuterJoinKeys = lappend(nlstate->nl_OuterJoinKeys,
												exprstate);
		}

		/*
		 * For LASJ_NOTIN, when we evaluate the join condition, we want to
		 * return true when one of the conditions is NULL, so we exclude
		 * that tuple from the output.
		 */
		nlstate->nl_qualResultForNull = true;
	}
	else
	{
		nlstate->nl_qualResultForNull = false;
	}

	if (nlstate->nl_qualResultForNull)
		nlstate->js.joinqual =
			ExecInitCheck(node->join.joinqual, (PlanState *) nlstate);
	else
		nlstate->js.joinqual =
			ExecInitQual(node->join.joinqual, (PlanState *) nlstate);

	/*
	 * detect whether we need only consider the first matching inner tuple
	 */
	nlstate->js.single_match = (node->join.inner_unique ||
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
			nlstate->nl_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate,
									  ExecGetResultType(innerPlanState(nlstate)),
									  &TTSOpsVecTuple);
			break;
		default:
			elog(ERROR, "unrecognized join type: %d",
				 (int) node->join.jointype);
	}

	/*
	 * finally, wipe the current outer tuple clean.
	 */
	nlstate->nl_NeedNewOuter = true;
	nlstate->nl_MatchedOuter = false;

	BuildVecPlan((PlanState *) vnlstate, &vnlstate->estate);

	NL1_printf("ExecInitVecNestLoop: %s\n",
			   "node initialized");

	return nlstate;
}

static void
ExecEagerFreeVecNestLoop(VecNestLoopState *node)
{
	FreeVecExecuteState(&node->estate);
}

/* ----------------------------------------------------------------
 *		ExecEndVecNestLoop
 *
 *		closes down scans and frees allocated storage
 * ----------------------------------------------------------------
 */
void
ExecEndVecNestLoop(NestLoopState *node)
{
	NL1_printf("ExecEndVecNestLoop: %s\n",
			   "ending node processing");
	VecNestLoopState *vnode = (VecNestLoopState *) node;
	ExecEagerFreeVecNestLoop(vnode);
    /*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->js.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);

	/*
	 * close down subplans
	 */
	if (!node->shared_outer)
		VecExecEndNode(outerPlanState(node));
	VecExecEndNode(innerPlanState(node));

	NL1_printf("ExecEndNestLoop: %s\n",
			   "node processing ended");
}

/* ----------------------------------------------------------------
 * splitJoinQualExpr
 *
 * Deconstruct the join clauses into outer and inner argument values, so
 * that we can evaluate those subexpressions separately. Note: for constant
 * expression we don't need to split (MPP-21294). However, if constant expressions
 * have peer splittable expressions we *do* split those.
 *
 * This is used for NOTIN joins, as we need to look for NULLs on both
 * inner and outer side.
 *
 * XXX: This would be more appropriate in the planner.
 * ----------------------------------------------------------------
 */
static void
splitJoinQualExpr(List *joinqual, List **inner_join_keys_p, List **outer_join_keys_p)
{
	List *lclauses = NIL;
	List *rclauses = NIL;
	ListCell   *lc;

	foreach(lc, joinqual)
	{
		Expr	   *expr = (Expr *) lfirst(lc);

		switch (expr->type)
		{
			case T_FuncExpr:
			case T_OpExpr:
				extractFuncExprArgs(expr, &lclauses, &rclauses);
				break;

			case T_BoolExpr:
				{
					BoolExpr   *bexpr = (BoolExpr *) expr;
					ListCell   *argslc;

					foreach(argslc, bexpr->args)
					{
						extractFuncExprArgs(lfirst(argslc), &lclauses, &rclauses);
					}
				}
				break;

			case T_Const:
				/*
				 * Constant expressions do not need to be splitted into left and
				 * right as they don't need to be considered for NULL value special
				 * cases
				 */
				break;

			default:
				elog(ERROR, "unexpected expression type in NestLoopJoin qual");
		}
	}

	*inner_join_keys_p = rclauses;
	*outer_join_keys_p = lclauses;
}


/* ----------------------------------------------------------------
 * extractFuncExprArgs
 *
 * Extract the arguments of a FuncExpr or an OpExpr and append them into two
 * given lists:
 *   - lclauses for the left side of the expression,
 *   - rclauses for the right side
 *
 * This function is only used for LASJ. Once we find a NULL from inner side, we
 * can skip the join and just return an empty set as result. This is only true
 * if the equality operator is strict, that is, if a tuple from inner side is
 * NULL then the equality operator returns NULL.
 *
 * If the number of arguments is not two, we just return leaving lclauses and
 * rclauses remaining NULL. In this case, the LASJ join would be actually
 * performed.
 * ----------------------------------------------------------------
 */
static void
extractFuncExprArgs(Expr *clause, List **lclauses, List **rclauses)
{
	if (IsA(clause, OpExpr))
	{
		OpExpr	   *opexpr = (OpExpr *) clause;

		if (list_length(opexpr->args) != 2)
			return;

		if (!op_strict(opexpr->opno))
			return;

		*lclauses = lappend(*lclauses, linitial(opexpr->args));
		*rclauses = lappend(*rclauses, lsecond(opexpr->args));
	}
	else if (IsA(clause, FuncExpr))
	{
		FuncExpr   *fexpr = (FuncExpr *) clause;

		if (list_length(fexpr->args) != 2)
			return;

		if (!func_strict(fexpr->funcid))
			return;

		*lclauses = lappend(*lclauses, linitial(fexpr->args));
		*rclauses = lappend(*rclauses, lsecond(fexpr->args));
	}
	else
		elog(ERROR, "unexpected join qual in JOIN_LASJ_NOTIN join");
}

void
ExecSquelchVecNestLoop(NestLoopState* node)
{	
	VecNestLoopState *vnode = (VecNestLoopState *) node;
	ExecEagerFreeVecNestLoop(vnode);
	ExecVecSquelchNode(outerPlanState(node));
	ExecVecSquelchNode(innerPlanState(node));
}
