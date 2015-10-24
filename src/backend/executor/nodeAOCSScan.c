/* 
 * nodeAOCSScan.c
 *
 *      Copyright (c) 2009, Greenplum Inc.
 */

#include "postgres.h"
#include "storage/gp_compress.h"
#include "cdb/cdbaocsam.h"
#include "cdb/cdbvars.h"
#include "executor/nodeAOCSScan.h"
#include "executor/executor.h"

#include "parser/parsetree.h"

static TupleTableSlot *AOCSNext(AOCSScanState *node)
{
        aocs_getnext(node->scandesc, node->ss.ps.state->es_direction, node->ss.ss_ScanTupleSlot);
        return node->ss.ss_ScanTupleSlot;
}

/* 
 * Open/close underlying table.
 * Resource acquire and release 
 */
static void OpenAOCSScanRelation(AOCSScanState *node)
{
	Snapshot appendOnlyMetaDataSnapshot;

	Assert(node->ss.scan_state == SCAN_INIT || node->ss.scan_state == SCAN_DONE);
	Assert(!node->scandesc);

	appendOnlyMetaDataSnapshot = node->ss.ps.state->es_snapshot;
	if (appendOnlyMetaDataSnapshot == SnapshotAny)
	{
		/* 
		 * the append-only meta data should never be fetched with
		 * SnapshotAny as bogus results are returned.
		 */
		appendOnlyMetaDataSnapshot = GetTransactionSnapshot();
	}

	node->scandesc = aocs_beginscan(
			node->ss.ss_currentRelation, 
			node->ss.ps.state->es_snapshot,
			appendOnlyMetaDataSnapshot,
			NULL /* relationTupleDesc */,
			node->proj);

	node->ss.scan_state = SCAN_SCAN;
}
 
static void CloseAOCSScanRelation(AOCSScanState *node)
{
	Assert((node->ss.scan_state & SCAN_SCAN) != 0);
        aocs_endscan(node->scandesc);
        
	node->scandesc = NULL;
	node->ss.scan_state = SCAN_INIT;
}

/* ----------------------------------------------------------------
 *		ExecAOCSScan
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecAOCSScan(AOCSScanState *node)
{
	/*
	 * use AppendOnlyNext as access method
	 */
	TupleTableSlot *slot;
	
	if((node->ss.scan_state & SCAN_SCAN) == 0)
		OpenAOCSScanRelation(node);
			
	slot = ExecScan((ScanState *) node, (ExecScanAccessMtd) AOCSNext);
     	if (!TupIsNull(slot))
        {
     		Gpmon_M_Incr_Rows_Out(GpmonPktFromAOCSScanState(node));
                CheckSendPlanStateGpmonPkt(&node->ss.ps);
        }

	Assert((node->ss.scan_state & SCAN_MARKPOS) == 0);
	if(TupIsNull(slot))
		CloseAOCSScanRelation(node);

	return slot;
}

/* ----------------------------------------------------------------
 *		ExecInitAOCSScan
 * ----------------------------------------------------------------
 */
AOCSScanState *
ExecInitAOCSScan(AOCSScan *node, EState *estate, int eflags)
{
	AOCSScanState	*state;
	Relation	currentRelation;
	
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	state = makeNode(AOCSScanState);
	state->ss.ps.plan = (Plan *) node;
	state->ss.ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &state->ss.ps);

	/*
	 * initialize child expressions
	 */
	state->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->scan.plan.targetlist,
					 (PlanState *) state);
	state->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->scan.plan.qual,
					 (PlanState *) state);

#define AOSCAN_NSLOTS 2

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &state->ss.ps);
	ExecInitScanTupleSlot(estate, &state->ss);

	/*
	 * get the relation object id from the relid'th entry in the range table
	 * and open that relation.
	 */
	currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid);	
	state->ss.ss_currentRelation = currentRelation;
	ExecAssignScanType(&state->ss, RelationGetDescr(currentRelation));
	
	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&state->ss.ps);
	ExecAssignScanProjectionInfo(&state->ss);

	/* init aocs prj */
	{
		int i;
		state->ncol = currentRelation->rd_att->natts;
		state->proj = palloc0(sizeof(bool) * state->ncol);
		GetNeededColumnsForScan((Node *) node->scan.plan.targetlist, state->proj, state->ncol);
		GetNeededColumnsForScan((Node *) node->scan.plan.qual, state->proj, state->ncol);
        
		/* At least project one col */
		for(i=0; i<state->ncol; ++i)
		{
			if(state->proj[i])
				break;
		}
		
		if(i==state->ncol)
			state->proj[0] = true;
	}
	
	initGpmonPktForAOCSScan((Plan *)node, &state->ss.ps.gpmon_pkt, estate);

	return state;
}


int
ExecCountSlotsAOCSScan(AOCSScan *node)
{
	return ExecCountSlotsNode(outerPlan(node)) +
		ExecCountSlotsNode(innerPlan(node)) +
		AOSCAN_NSLOTS;
}

/* ----------------------------------------------------------------
 *		ExecEndAppendOnlyScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndAOCSScan(AOCSScanState *node)
{
	Relation	relation;
	/*
	 * get information from node
	 */
	relation = node->ss.ss_currentRelation;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	ExecEagerFreeAOCSScan(node);

	/*
	 * close the heap relation.
	 */
	ExecCloseScanRelation(relation);

	EndPlanStateGpmonPkt(&node->ss.ps);
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecAppendOnlyReScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecAOCSReScan(AOCSScanState *node, ExprContext *exprCtxt)
{
	EState				*estate;
	Index				scanrelid;

	estate = node->ss.ps.state;
	scanrelid = ((AppendOnlyScan *) node->ss.ps.plan)->scan.scanrelid;

	/* If this is re-scanning of PlanQual ... */
	if (estate->es_evTuple != NULL &&
		estate->es_evTuple[scanrelid - 1] != NULL)
	{
		estate->es_evTupleNull[scanrelid - 1] = false;
		return;
	}

	if((node->ss.scan_state & SCAN_SCAN) == 0)
		OpenAOCSScanRelation(node);

	aocs_rescan(node->scandesc); 

	Gpmon_M_Incr(GpmonPktFromAOCSScanState(node), GPMON_AOCSSCAN_RESCAN);
	CheckSendPlanStateGpmonPkt(&node->ss.ps);
}

void
initGpmonPktForAOCSScan(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(planNode != NULL && gpmon_pkt != NULL && IsA(planNode, AOCSScan));

	{
		RangeTblEntry *rte = rt_fetch(((AOCSScan *)planNode)->scan.scanrelid,
									  estate->es_range_table);
		char schema_rel_name[SCAN_REL_NAME_BUF_SIZE] = {0};
		
		Assert(GPMON_AOCSSCAN_TOTAL <= (int)GPMON_QEXEC_M_COUNT);
		InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, PMNT_AppendOnlyColumnarScan,
							 (int64)planNode->plan_rows,
							 GetScanRelNameGpmon(rte->relid, schema_rel_name));
	}
}

void
ExecEagerFreeAOCSScan(AOCSScanState *node)
{
	if((node->ss.scan_state & SCAN_SCAN) != 0)
		CloseAOCSScanRelation(node);
}
