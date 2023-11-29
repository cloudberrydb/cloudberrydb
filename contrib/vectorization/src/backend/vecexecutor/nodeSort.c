/*-------------------------------------------------------------------------
 *
 * nodeSort.c
 *	  Routines to handle sorting of relations.
 *
 * Portions Copyright (c) 2007-2008, Cloudberry inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeSort.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/parallel.h"
#include "executor/execdebug.h"
#include "executor/nodeSort.h"
#include "lib/stringinfo.h"             /* StringInfo */
#include "miscadmin.h"
#include "utils/tuplesort.h"
#include "cdb/cdbvars.h" /* CDB *//* gp_sort_flags */
#include "utils/workfile_mgr.h"
#include "executor/instrument.h"
#include "utils/faultinjector.h"

#include "vecexecutor/nodeSort.h"
#include "vecexecutor/execnodes.h"
#include "vecexecutor/executor.h"
#include "vecexecutor/execslot.h"
#include "utils/tuptable_vec.h"
#include "utils/vecsort.h"
#include "vecexecutor/execAmi.h"

static void ExecVecSortExplainEnd(PlanState *planstate, struct StringInfoData *buf);
static void ExecEagerFreeVecSort(SortState *node);

/* ----------------------------------------------------------------
 *		ExecSort
 *
 *		Sorts tuples from the outer subtree of the node using tuplesort,
 *		which saves the results in a temporary file or memory. After the
 *		initial call, returns a tuple from the file with each call.
 *
 *		Conditions:
 *		  -- none.
 *
 *		Initial States:
 *		  -- the outer child is prepared to return the first tuple.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecVecSort(PlanState *pstate)
{
	SortState  *node = castNode(SortState, pstate);
    VecSortState *vnode;

	CHECK_FOR_INTERRUPTS();

    vnode = (VecSortState *) node;

	if (!vnode->started)
	{
		BuildVecPlan((PlanState *)vnode, &vnode->estate);
		vnode->started = true;
	}

	/* FIXME: always false. may be we can delete skip field. */
    if (vnode->skip)
	{
		PlanState *outerNode = outerPlanState(node);
		TupleTableSlot *slot = ExecProcNode(outerNode);

        if (TupIsNull(slot))
			return NULL;

		return slot;
	}

	TupleTableSlot *slot = ExecuteVecPlan(&vnode->estate);

	if (TupIsNull(slot) && !node->delayEagerFree)
	{
		ExecEagerFreeVecSort(node);
	}

	return slot;
}

/* ----------------------------------------------------------------
 *		ExecInitVecSort
 *
 *		Creates the run-time state information for the sort node
 *		produced by the planner and initializes its outer subtree.
 * ----------------------------------------------------------------
 */
SortState *
ExecInitVecSort(Sort *node, EState *estate, int eflags)
{
	SortState       *sortstate;
    VecSortState    *vsortstate;

    SO1_printf("ExecInitVecSort: %s\n",
               "initializing sort node");

    /*
	 * create state structure
	 */
	vsortstate = (VecSortState*) palloc0(sizeof(VecSortState));
	sortstate = (SortState*) vsortstate;
	NodeSetTag(sortstate, T_SortState);
	sortstate->ss.ps.plan = (Plan *) node;
	sortstate->ss.ps.state = estate;
	sortstate->ss.ps.ExecProcNode = ExecVecSort;
	vsortstate->started = false;

	/*
	 * We must have random access to the sort output to do backward scan or
	 * mark/restore.  We also prefer to materialize the sort output if we
	 * might be called on to rewind and replay it many times.
	 */
	sortstate->randomAccess = (eflags & (EXEC_FLAG_REWIND |
										 EXEC_FLAG_BACKWARD |
										 EXEC_FLAG_MARK)) != 0;

	sortstate->bounded = false;
	sortstate->sort_Done = false;
	sortstate->tuplesortstate = NULL;

	/* CDB */

	/* BUT:
	 * The LIMIT optimizations requires exprcontext in which to
	 * evaluate the limit/offset parameters.
	 */
	ExecAssignExprContext(estate, &sortstate->ss.ps);

	/* CDB */ /* evaluate a limit as part of the sort */
	{
		sortstate->noduplicates = node->noduplicates;
	}

	/*
	 * Miscellaneous initialization
	 *
	 * Sort nodes don't initialize their ExprContexts because they never call
	 * ExecQual or ExecProject.
	 */

	/*
	 * CDB: Offer extra info for EXPLAIN ANALYZE.
	 */
	if (estate->es_instrument && (estate->es_instrument & INSTRUMENT_CDB))
	{
		/* Allocate string buffer. */
		sortstate->ss.ps.cdbexplainbuf = makeStringInfo();

		/* Request a callback at end of query. */
		sortstate->ss.ps.cdbexplainfun = ExecVecSortExplainEnd;
	}

	/*
	 * If eflag contains EXEC_FLAG_REWIND or EXEC_FLAG_BACKWARD or EXEC_FLAG_MARK,
	 * then this node is not eager free safe.
	 */
	sortstate->delayEagerFree =
		((eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) != 0);

	/*
	 * initialize child nodes
	 *
	 * We shield the child node from the need to support BACKWARD, or
	 * MARK/RESTORE.
	 */

	eflags &= ~(EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

	/*
	 * If Sort does not have any external parameters, then it
	 * can shield the child node from being rescanned as well, hence
	 * we can clear the EXEC_FLAG_REWIND as well. If there are parameters,
	 * don't clear the REWIND flag, as the child will be rewound.
	 */

	if (node->plan.allParam == NULL || node->plan.extParam == NULL)
	{
		eflags &= ~EXEC_FLAG_REWIND;
	}

	outerPlanState(sortstate) = VecExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * If the child node of a Material is a Motion, then this Material node is
	 * not eager free safe.
	 */
	if (IsA(outerPlan((Plan *)node), Motion))
	{
		sortstate->delayEagerFree = true;
	}

	/*
	 * Initialize scan slot and type.
	 */
	ExecCreateScanSlotFromOuterPlan(estate, &sortstate->ss, &TTSOpsVecTuple);

	/*
	 * Initialize return slot and type. No need to initialize projection info
	 * because this node doesn't do projections.
	 */
	ExecInitResultTupleSlotTL(&sortstate->ss.ps, &TTSOpsVecTuple);
	sortstate->ss.ps.ps_ProjInfo = NULL;

	SO1_printf("ExecInitVecSort: %s\n",
			   "sort node initialized");

	return sortstate;
}

/* ----------------------------------------------------------------
 *		ExecEndVecSort(node)
 * ----------------------------------------------------------------
 */
void
ExecEndVecSort(SortState *node)
{
	SO1_printf("ExecEndVecSort: %s\n",
			   "shutting down sort node");

	/* release tuple schema */
	FreeVecSchema(node->ss.ps.ps_ResultTupleSlot);

	ExecEagerFreeVecSort(node);

	/*
	 * shut down the subplan
	 */
	VecExecEndNode(outerPlanState(node));

	SO1_printf("ExecEndVecSort: %s\n",
			   "sort node shutdown");
}

/* ----------------------------------------------------------------
 *		ExecVecSortMarkPos
 *
 *		Calls tuplesort to save the current position in the sorted file.
 * ----------------------------------------------------------------
 */
void
ExecVecSortMarkPos(SortState *node)
{
	/*
	 * if we haven't sorted yet, just return
	 */
	if (!node->sort_Done)
		return;

	tuplesort_markpos((Tuplesortstate *) node->tuplesortstate);
}

/* ----------------------------------------------------------------
 *		ExecVecSortRestrPos
 *
 *		Calls tuplesort to restore the last saved sort file position.
 * ----------------------------------------------------------------
 */
void
ExecVecSortRestrPos(SortState *node)
{
	/*
	 * if we haven't sorted yet, just return.
	 */
	if (!node->sort_Done)
		return;
    
	/*
	 * restore the scan to the previously marked position
	 */
	tuplesort_restorepos((Tuplesortstate *) node->tuplesortstate);
}

/*
 * ExecVecSortExplainEnd
 *      Called before ExecutorEnd to finish EXPLAIN ANALYZE reporting.
 */
void
ExecVecSortExplainEnd(PlanState *planstate, struct StringInfoData *buf)
{
	/* FIXME: implement statistic */
	return;
}                               /* ExecVecSortExplainEnd */

static void
ExecEagerFreeVecSort(SortState *node)
{
	VecSortState *vnode = (VecSortState *) node;
	/* clean out the tuple table */
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/* must drop pointer to sort result tuple */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	FreeVecExecuteState(&vnode->estate);
}

void
ExecSquelchVecSort(SortState *node)
{
	if (!node->delayEagerFree)
	{
		ExecEagerFreeVecSort(node);
		ExecVecSquelchNode(outerPlanState(node));
	}
}
