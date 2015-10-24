/*-------------------------------------------------------------------------
 *
 * nodeAppendOnlyscan.c
 *	  Support routines for sequential scans of append-only relations.
 *
 * Portions Copyright (c) 2008, greenplum inc
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecAppendOnlyScan				sequentially scans a relation.
 *		ExecAppendOnlyNext				retrieve next tuple in sequential order.
 *		ExecInitAppendOnlyScan			creates and initializes a seqscan node.
 *		ExecEndAppendOnlyScan			releases any storage allocated.
 *		ExecAppendOnlyReScan			rescans the relation
 */
#include "postgres.h"

#include "access/heapam.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbvars.h"
#include "executor/execdebug.h"
#include "executor/nodeAppendOnlyscan.h"

#include "parser/parsetree.h"

static TupleTableSlot *AppendOnlyNext(AppendOnlyScanState *node);

/* 
 * Open/Close underlying AO table.
 * Open/Close means resource acquisition and release.
 */
static void OpenAOScanRelation(AppendOnlyScanState *node);
static void CloseAOScanRelation(AppendOnlyScanState *node);

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 *		SeqNext
 *
 *		This is a workhorse for ExecSeqScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
AppendOnlyNext(AppendOnlyScanState *node)
{
	AppendOnlyScanDesc scandesc;
	Index		scanrelid;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;

	Assert((node->ss.scan_state & SCAN_SCAN) != 0);
	/*
	 * get information from the estate and scan state
	 */
	estate = node->ss.ps.state;
	scandesc = node->aos_ScanDesc;
	scanrelid = ((AppendOnlyScan *) node->ss.ps.plan)->scan.scanrelid;
	direction = estate->es_direction;
	slot = node->ss.ss_ScanTupleSlot;

	/*
	 * put the next tuple from the access methods in our tuple slot
	 */
	appendonly_getnext(scandesc, direction, slot);

	return slot;
}

/* ----------------------------------------------------------------
 *		ExecAppendOnlyScan(node)
 *
 *		Scans the relation sequentially and returns the next qualifying
 *		tuple.
 *		It calls the ExecScan() routine and passes it the access method
 *		which retrieve tuples sequentially.
 *
 */

TupleTableSlot *
ExecAppendOnlyScan(AppendOnlyScanState *node)
{
	/*
	 * use AppendOnlyNext as access method
	 */
	TupleTableSlot *slot;
	
	if((node->ss.scan_state & SCAN_SCAN) == 0)
		OpenAOScanRelation(node);
			
	slot = ExecScan((ScanState *) node, (ExecScanAccessMtd) AppendOnlyNext);
     	if (!TupIsNull(slot))
        {
     		Gpmon_M_Incr_Rows_Out(GpmonPktFromAppOnlyScanState(node));
                CheckSendPlanStateGpmonPkt(&node->ss.ps);
        }

	Assert((node->ss.scan_state & SCAN_MARKPOS) == 0);
	if(TupIsNull(slot))
		CloseAOScanRelation(node);

	return slot;
}


/* ----------------------------------------------------------------
 *		ExecInitAppendOnlyScan
 * ----------------------------------------------------------------
 */
AppendOnlyScanState *
ExecInitAppendOnlyScan(AppendOnlyScan *node, EState *estate, int eflags)
{
	AppendOnlyScanState	*appendonlystate;
	Relation			currentRelation;
	
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	appendonlystate = makeNode(AppendOnlyScanState);
	appendonlystate->ss.ps.plan = (Plan *) node;
	appendonlystate->ss.ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &appendonlystate->ss.ps);

	/*
	 * initialize child expressions
	 */
	appendonlystate->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->scan.plan.targetlist,
					 (PlanState *) appendonlystate);
	appendonlystate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->scan.plan.qual,
					 (PlanState *) appendonlystate);

#define AOSCAN_NSLOTS 2

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &appendonlystate->ss.ps);
	ExecInitScanTupleSlot(estate, &appendonlystate->ss);

	/*
	 * get the relation object id from the relid'th entry in the range table
	 * and open that relation.
	 */
	currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid);	
	appendonlystate->ss.ss_currentRelation = currentRelation;
	ExecAssignScanType(&appendonlystate->ss, RelationGetDescr(currentRelation));
	
	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&appendonlystate->ss.ps);
	ExecAssignScanProjectionInfo(&appendonlystate->ss);

	initGpmonPktForAppendOnlyScan((Plan *)node, &appendonlystate->ss.ps.gpmon_pkt, estate);

	return appendonlystate;
}

/*
 * Open/Close underlying relation for AO Scan.
 * 
 * Open/Close means resource allocation/release.  Here, it is file descriptors
 * and the buffers in the aos_ScanDesc.
 * 
 * See nodeSeqscan.c as well.
 */
static void OpenAOScanRelation(AppendOnlyScanState *node)
{
	Snapshot appendOnlyMetaDataSnapshot;

	Assert(node->ss.scan_state == SCAN_INIT || node->ss.scan_state == SCAN_DONE);
	Assert(!node->aos_ScanDesc);
	
	appendOnlyMetaDataSnapshot = node->ss.ps.state->es_snapshot;
	if (appendOnlyMetaDataSnapshot == SnapshotAny)
	{
		/* 
		 * the append-only meta data should never be fetched with
		 * SnapshotAny as bogus results are returned.
		 */
		appendOnlyMetaDataSnapshot = GetTransactionSnapshot();
	}
	
	node->aos_ScanDesc = appendonly_beginscan(
			node->ss.ss_currentRelation, 
			node->ss.ps.state->es_snapshot,
			appendOnlyMetaDataSnapshot,
			0, NULL);
	node->ss.scan_state = SCAN_SCAN;
}

static void CloseAOScanRelation(AppendOnlyScanState *node)
{
	Assert((node->ss.scan_state & SCAN_SCAN) != 0);
	appendonly_endscan(node->aos_ScanDesc);
	node->aos_ScanDesc = NULL;
	node->ss.scan_state = SCAN_INIT;
}


int
ExecCountSlotsAppendOnlyScan(AppendOnlyScan *node)
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
ExecEndAppendOnlyScan(AppendOnlyScanState *node)
{
	Relation			relation;
	AppendOnlyScanDesc	scanDesc;

	/*
	 * get information from node
	 */
	relation = node->ss.ss_currentRelation;
	scanDesc = node->aos_ScanDesc;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	ExecEagerFreeAppendOnlyScan(node);

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
ExecAppendOnlyReScan(AppendOnlyScanState *node, ExprContext *exprCtxt)
{
	EState				*estate;
	Index				scanrelid;
	AppendOnlyScanDesc	scan;

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
		OpenAOScanRelation(node);

	scan = node->aos_ScanDesc;

	appendonly_rescan(scan,			/* scan desc */
					  NULL);		/* new scan keys */

	Gpmon_M_Incr(GpmonPktFromAppOnlyScanState(node), GPMON_APPONLYSCAN_RESCAN);
	CheckSendPlanStateGpmonPkt(&node->ss.ps);
}

void
initGpmonPktForAppendOnlyScan(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(planNode != NULL && gpmon_pkt != NULL && IsA(planNode, AppendOnlyScan));

	{
		RangeTblEntry *rte = rt_fetch(((AppendOnlyScan *)planNode)->scan.scanrelid,
									  estate->es_range_table);
		char schema_rel_name[SCAN_REL_NAME_BUF_SIZE] = {0};
		
		Assert(GPMON_APPONLYSCAN_TOTAL <= (int)GPMON_QEXEC_M_COUNT);
		InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, PMNT_AppendOnlyScan,
							 (int64)planNode->plan_rows,
							 GetScanRelNameGpmon(rte->relid, schema_rel_name));
	}
}

void
ExecEagerFreeAppendOnlyScan(AppendOnlyScanState *node)
{
	if((node->ss.scan_state & SCAN_SCAN) != 0)
		CloseAOScanRelation(node);
}
