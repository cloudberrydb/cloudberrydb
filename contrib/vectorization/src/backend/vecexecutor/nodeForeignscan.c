/*-------------------------------------------------------------------------
 *
 * nodeForeignscan.c
 *	  Routines to support scans of foreign tables
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeForeignscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *
 *		ExecForeignScan			scans a foreign table.
 *		ExecInitForeignScan		creates and initializes state info.
 *		ExecReScanForeignScan	rescans the foreign relation.
 *		ExecEndForeignScan		releases any resources allocated.
 */
#include "postgres.h"

#include "vecexecutor/executor.h"
#include "vecexecutor/nodeForeignscan.h"
#include "foreign/fdwapi.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/* ----------------------------------------------------------------
 *		ForeignNext
 *
 *		This is a workhorse for ExecForeignScan
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ForeignNext(ForeignScanState *node)
{
	TupleTableSlot *slot;
	ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	MemoryContext oldcontext;

	/* Call the Iterate function in short-lived context */
	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	if (plan->operation != CMD_SELECT)
	{
		/*
		 * direct modifications cannot be re-evaluated, so shouldn't get here
		 * during EvalPlanQual processing
		 */
		Assert(node->ss.ps.state->es_epq_active == NULL);

		slot = node->fdwroutine->IterateDirectModify(node);
	}
	else
		slot = node->fdwroutine->IterateForeignScan(node);
	Assert(TTS_IS_VIRTUAL(slot));
	MemoryContextSwitchTo(oldcontext);

	/*
	 * Insert valid value into tableoid, the only actually-useful system
	 * column.
	 */
	if (plan->fsSystemCol && !TupIsNull(slot))
		slot->tts_tableOid = RelationGetRelid(node->ss.ss_currentRelation);

	return slot;
}

/*
 * ForeignRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
bool
ForeignRecheck(ForeignScanState *node, TupleTableSlot *slot)
{
	FdwRoutine *fdwroutine = node->fdwroutine;
	ExprContext *econtext;

	/*
	 * extract necessary information from foreign scan node
	 */
	econtext = node->ss.ps.ps_ExprContext;

	/* Does the tuple meet the remote qual condition? */
	econtext->ecxt_scantuple = slot;

	ResetExprContext(econtext);

	/*
	 * If an outer join is pushed down, RecheckForeignScan may need to store a
	 * different tuple in the slot, because a different set of columns may go
	 * to NULL upon recheck.  Otherwise, it shouldn't need to change the slot
	 * contents, just return true or false to indicate whether the quals still
	 * pass.  For simple cases, setting fdw_recheck_quals may be easier than
	 * providing this callback.
	 */
	if (fdwroutine->RecheckForeignScan &&
		!fdwroutine->RecheckForeignScan(node, slot))
		return false;

	/* FIXME: Vector recheck filter is not implemented now.*/
	Assert(false);
	return ExecQual(node->fdw_recheck_quals, econtext);
}

/* ----------------------------------------------------------------
 *		ExecForeignScan(node)
 *
 *		Fetches the next tuple from the FDW, checks local quals, and
 *		returns it.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecVecForeignScan(PlanState *pstate)
{
	VecForeignScanState *node = (VecForeignScanState *) pstate;
	ForeignScan *plan = (ForeignScan *) node->base.ss.ps.plan;
	EState	   *estate = node->base.ss.ps.state;

	/*
	 * Ignore direct modifications when EvalPlanQual is active --- they are
	 * irrelevant for EvalPlanQual rechecking
	 */
	if (estate->es_epq_active != NULL && plan->operation != CMD_SELECT)
		return NULL;

	return ExecVecScan(&node->base.ss,
					   &node->vestate,
					   node->vscanslot,
					   (ExecScanAccessMtd) ForeignNext,
					   (ExecScanRecheckMtd) ForeignRecheck);
}


/* ----------------------------------------------------------------
 *		ExecInitForeignScan
 * ----------------------------------------------------------------
 */
VecForeignScanState *
ExecInitVecForeignScan(ForeignScan *node, EState *estate, int eflags)
{
	VecForeignScanState *vscanstate;
	ForeignScanState *scanstate;
	Relation	currentRelation = NULL;
	Index		scanrelid = node->scan.scanrelid;
	Index		tlistvarno;
	FdwRoutine *fdwroutine;
	TupleDesc	scan_tupdesc;
	g_autoptr(GArrowSchema) schema = NULL;


	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	vscanstate =(VecForeignScanState *) palloc0(sizeof(VecForeignScanState));
	NodeSetTag(vscanstate, T_ForeignScanState);
	scanstate = &vscanstate->base;
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->ss.ps.ExecProcNode = ExecVecForeignScan;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * open the scan relation, if any; also acquire function pointers from the
	 * FDW's handler
	 */
	if (scanrelid > 0)
	{
		currentRelation = ExecOpenScanRelation(estate, scanrelid, eflags);
		scanstate->ss.ss_currentRelation = currentRelation;
		fdwroutine = GetFdwRoutineForRelation(currentRelation, true);
	}
	else
	{
		/* We can't use the relcache, so get fdwroutine the hard way */
		fdwroutine = GetFdwRoutineByServerId(node->fs_server);
	}

	/* As a vector interface. Init Scan slot as VirtualTupleSlot contains
	 * abi style ArrowRecordBatch* . Init Result slot as VectorTupleSlot. */

	/*
	 * Determine the scan tuple type.  If the FDW provided a targetlist
	 * describing the scan tuples, use that; else use base relation's rowtype.
	 */
	if (node->fdw_scan_tlist != NIL || currentRelation == NULL)
	{
		scan_tupdesc = ExecTypeFromTL(node->fdw_scan_tlist);
		/* Node's targetlist will contain Vars with varno = INDEX_VAR */
		tlistvarno = INDEX_VAR;
	}
	else
	{
		/* don't trust FDWs to return tuples fulfilling NOT NULL constraints */
		scan_tupdesc = CreateTupleDescCopy(RelationGetDescr(currentRelation));
		/* Node's targetlist will contain Vars with varno = scanrelid */
		tlistvarno = scanrelid;
	}

	vscanstate->columnmap = palloc0(scan_tupdesc->natts * sizeof(int));
	ExecInitScanTupleSlotVec(estate, &scanstate->ss, &vscanstate->vscanslot, scan_tupdesc, vscanstate->columnmap);
	ExecInitResultTupleSlotTL(&scanstate->ss.ps, &TTSOpsVecTuple);
	schema = TupDescToSchema(scanstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
	garrow_store_ptr(vscanstate->schema, schema);

	/* Don't know what an FDW might return */
	scanstate->ss.ps.scanopsfixed = false;
	scanstate->ss.ps.scanopsset = true;

	/*
	 * Initialize result slot, type and projection.
	 */
	ExecAssignScanProjectionInfoWithVarno(&scanstate->ss, tlistvarno);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.qual =
		ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);
	scanstate->fdw_recheck_quals =
		ExecInitQual(node->fdw_recheck_quals, (PlanState *) scanstate);

	/*
	 * Determine whether to scan the foreign relation asynchronously or not;
	 * this has to be kept in sync with the code in ExecInitAppend().
	 */
	scanstate->ss.ps.async_capable = (((Plan *) node)->async_capable &&
									  estate->es_epq_active == NULL);

	/*
	 * Initialize FDW-related state.
	 */
	scanstate->fdwroutine = fdwroutine;
	scanstate->fdw_state = NULL;

	/*
	 * For the FDW's convenience, look up the modification target relation's
	 * ResultRelInfo.  The ModifyTable node should have initialized it for us,
	 * see ExecInitModifyTable.
	 *
	 * Don't try to look up the ResultRelInfo when EvalPlanQual is active,
	 * though.  Direct modifications cannot be re-evaluated as part of
	 * EvalPlanQual.  The lookup wouldn't work anyway because during
	 * EvalPlanQual processing, EvalPlanQual only initializes the subtree
	 * under the ModifyTable, and doesn't run ExecInitModifyTable.
	 */
	if (node->resultRelation > 0 && estate->es_epq_active == NULL)
	{
		if (estate->es_result_relations == NULL ||
			estate->es_result_relations[node->resultRelation - 1] == NULL)
		{
			elog(ERROR, "result relation not initialized");
		}
		scanstate->resultRelInfo = estate->es_result_relations[node->resultRelation - 1];
	}

	/* Initialize any outer plan. */
	if (outerPlan(node))
		outerPlanState(scanstate) =
			VecExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * Tell the FDW to initialize the scan.
	 */
	if (node->operation != CMD_SELECT)
	{
		/*
		 * Direct modifications cannot be re-evaluated by EvalPlanQual, so
		 * don't bother preparing the FDW.
		 *
		 * In case of an inherited UPDATE/DELETE with foreign targets there
		 * can be direct-modify ForeignScan nodes in the EvalPlanQual subtree,
		 * so we need to ignore such ForeignScan nodes during EvalPlanQual
		 * processing.  See also ExecForeignScan/ExecReScanForeignScan.
		 */
		if (estate->es_epq_active == NULL)
			fdwroutine->BeginDirectModify(scanstate, eflags);
	}
	else
		fdwroutine->BeginForeignScan(scanstate, eflags);

	BuildVecPlan((PlanState *)vscanstate, &vscanstate->vestate);
	return vscanstate;
}

static void
ExecEagerFreeVecForeignScan(VecForeignScanState *node)
{
	ARROW_FREE(GArrowSchema, &node->schema);
	FreeVecExecuteState(&(node->vestate));
}

/* ----------------------------------------------------------------
 *		ExecEndForeignScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndVecForeignScan(VecForeignScanState *vnode)
{
	ForeignScanState *node = (ForeignScanState *)vnode;
	ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
	EState	   *estate = node->ss.ps.state;

	ExecEagerFreeVecForeignScan(vnode);
	/* Let the FDW shut down */
	if (plan->operation != CMD_SELECT)
	{
		if (estate->es_epq_active == NULL)
			node->fdwroutine->EndDirectModify(node);
	}
	else
		node->fdwroutine->EndForeignScan(node);

	/* Shut down any outer plan. */
	if (outerPlanState(node))
		VecExecEndNode(outerPlanState(node));

	/* Free the exprcontext */
	ExecFreeExprContext(&node->ss.ps);

	if (vnode->columnmap != NULL)
	{
		pfree(vnode->columnmap);
	}
	/* clean out the tuple table */
	if (node->ss.ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

}

void
ExecReScanVecForeignScan(VecForeignScanState *vnode)
{
	ForeignScanState *node = (ForeignScanState *)vnode;
	ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
	EState	   *estate = node->ss.ps.state;
	PlanState  *outerPlan = outerPlanState(node);

	/*
	 * Ignore direct modifications when EvalPlanQual is active --- they are
	 * irrelevant for EvalPlanQual rechecking
	 */
	if (estate->es_epq_active != NULL && plan->operation != CMD_SELECT)
		return;

	node->fdwroutine->ReScanForeignScan(node);

	/*
	 * If chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.  outerPlan may also be NULL, in which case there is
	 * nothing to rescan at all.
	 */
	if (outerPlan != NULL && outerPlan->chgParam == NULL)
		ExecReScan(outerPlan);

	ExecScanReScan(&node->ss);
}

/* ----------------------------------------------------------------
 *		ExecShutdownForeignScan
 *
 *		Gives FDW chance to stop asynchronous resource consumption
 *		and release any resources still held.
 * ----------------------------------------------------------------
 */
void
ExecShutdownForeignScan(ForeignScanState *node)
{
	FdwRoutine *fdwroutine = node->fdwroutine;

	if (fdwroutine->ShutdownForeignScan)
		fdwroutine->ShutdownForeignScan(node);
}

void
ExecSquelchVecForeignScan(ForeignScanState *node)
{
	VecForeignScanState *vnode = (VecForeignScanState *) node;
	ExecEagerFreeVecForeignScan(vnode);
	ExecShutdownForeignScan(node);
}
