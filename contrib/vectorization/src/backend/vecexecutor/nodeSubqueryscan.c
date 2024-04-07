/*-------------------------------------------------------------------------
 *
 * nodeSubqueryscan.c
 *	  Support routines for scanning subqueries (subselects in rangetable).
 *
 * This is just enough different from sublinks (nodeSubplan.c) to mean that
 * we need two sets of code.  Ought to look at trying to unify the cases.
 *
 *
 * Portions Copyright (c) 2006-2008, Cloudberry inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/vecexecutor/nodeSubqueryscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecVecSubqueryScan			scans a subquery.
 *		ExecInitVecSubqueryScan		creates and initializes a subqueryscan node.
 *		ExecEndVecSubqueryScan			releases any storage allocated.
 *		ExecReScanSubqueryScan		rescans the relation
 *
 */
#include "postgres.h"

#include "executor/execdebug.h"
#include "vecexecutor/nodeSubqueryscan.h"
#include "vecexecutor/executor.h"
#include "vecexecutor/execAmi.h"

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 *		ExecSubqueryScan(node)
 *
 *		Scans the subquery sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecVecSubqueryScan(PlanState *pstate)
{
	SubqueryScanState *node = castNode(SubqueryScanState, pstate);
	VecSubqueryScanState *vnode = (VecSubqueryScanState*) node;
	return ExecuteVecPlan(&vnode->estate);
}

/* ----------------------------------------------------------------
 *		ExecInitSubqueryScan
 * ----------------------------------------------------------------
 */
SubqueryScanState *
ExecInitVecSubqueryScan(SubqueryScan *node, EState *estate, int eflags)
{
	SubqueryScanState *subquerystate;
	VecSubqueryScanState *vecsubquerystate;

	/* check for unsupported flags */
	Assert(!(eflags & EXEC_FLAG_MARK));

	/* SubqueryScan should not have any "normal" children */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	vecsubquerystate = (VecSubqueryScanState*) palloc0(sizeof(VecSubqueryScanState));
	subquerystate = (SubqueryScanState *) vecsubquerystate;
	NodeSetTag(subquerystate, T_SubqueryScanState);
	subquerystate->ss.ps.plan = (Plan *) node;
	subquerystate->ss.ps.state = estate;
	subquerystate->ss.ps.ExecProcNode = ExecVecSubqueryScan;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &subquerystate->ss.ps);

	/*
	 * initialize subquery
	 */
	subquerystate->subplan = VecExecInitNode(node->subplan, estate, eflags);

	/*
	 * Initialize scan slot and type (needed by ExecAssignScanProjectionInfo)
	 */
	ExecInitScanTupleSlot(estate, &subquerystate->ss,
						  ExecGetResultType(subquerystate->subplan),
						  ExecGetResultSlotOps(subquerystate->subplan, NULL));

	/*
	 * The slot used as the scantuple isn't the slot above (outside of EPQ),
	 * but the one from the node below.
	 */
	subquerystate->ss.ps.scanopsset = true;
	subquerystate->ss.ps.scanops = ExecGetResultSlotOps(subquerystate->subplan,
														&subquerystate->ss.ps.scanopsfixed);
	subquerystate->ss.ps.resultopsset = true;
	subquerystate->ss.ps.resultops = subquerystate->ss.ps.scanops;
	subquerystate->ss.ps.resultopsfixed = subquerystate->ss.ps.scanopsfixed;

	/*
	 * Initialize result type and projection.
	 */
	ExecInitResultTypeTL(&subquerystate->ss.ps);
	ExecInitResultTupleSlotTL(&subquerystate->ss.ps, &TTSOpsVecTuple);

	/*
	 * initialize child expressions
	 */
	subquerystate->ss.ps.qual =
		ExecInitQual(node->scan.plan.qual, (PlanState *) subquerystate);

	PostBuildVecPlan((PlanState *) subquerystate, &vecsubquerystate->estate);

	return subquerystate;
}

static void 
ExecEagerFreeVecSubqueryScan(VecSubqueryScanState *node)
{
	FreeVecExecuteState(&node->estate);
}

/* ----------------------------------------------------------------
 *		ExecEndVecSubqueryScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndVecSubqueryScan(SubqueryScanState *node)
{
	VecSubqueryScanState *vnode = (VecSubqueryScanState *) node;
	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the upper tuple table
	 */
	if (node->ss.ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * close down subquery
	 */
	VecExecEndNode(node->subplan);
	ExecEagerFreeVecSubqueryScan(vnode);
}

/* ----------------------------------------------------------------
 *		ExecReScanSubqueryScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanSubqueryScan(SubqueryScanState *node)
{
	ExecScanReScan(&node->ss);

	/*
	 * ExecReScan doesn't know about my subplan, so I have to do
	 * changed-parameter signaling myself.  This is just as well, because the
	 * subplan has its own memory context in which its chgParam state lives.
	 */
	if (node->ss.ps.chgParam != NULL)
		UpdateChangedParamSet(node->subplan, node->ss.ps.chgParam);

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (node->subplan->chgParam == NULL)
		ExecReScan(node->subplan);
}

void
ExecSquelchVecSubqueryScan(SubqueryScanState *node)
{
	VecSubqueryScanState *vnode = (VecSubqueryScanState *) node;
	ExecEagerFreeVecSubqueryScan(vnode);
	/* Recurse to subquery */
	ExecVecSquelchNode(node->subplan);
}
