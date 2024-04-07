/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.c
 *	  Support routines for sequential scans of relations.
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/backend/vecexecutor/nodeSeqscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecVecSeqScan				sequentially scans a relation.
 *		ExecVecSeqNext				retrieve next tuple in sequential order.
 *		ExecInitVecSeqScan			creates and initializes a seqscan node.
 *		ExecEndVecSeqScan			releases any storage allocated.
 *		ExecReScanVecSeqScan		rescans the relation
 */
#include "postgres.h"

#include "access/relscan.h"
#include "access/tableam.h"
#include "executor/execdebug.h"
#include "executor/nodeSeqscan.h"
#include "utils/rel.h"
#include "nodes/nodeFuncs.h"

#include "utils/aocs_vec.h"
#include "utils/am_vec.h"
#include "utils/fmgr_vec.h"
#include "utils/vecfuncs.h"
#include "vecexecutor/nodeSeqscan.h"
#include "vecexecutor/execslot.h"
#include "vecexecutor/executor.h"
#include "vecexecutor/execnodes.h"
#include "vecnodes/nodes.h"

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		VecSeqNext
 *
 *		This is a workhorse for ExecVecSeqScan
 * ----------------------------------------------------------------
 */
TupleTableSlot *
VecSeqNext(VecSeqScanState *node)
{
	ScanState *ss = (ScanState *) node;
	TableScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;
	g_autoptr(GError) error = NULL;

	/*
	 * get information from the estate and scan state
	 */
	scandesc = ss->ss_currentScanDesc;
	estate = ss->ps.state;
	direction = estate->es_direction;
	slot = ss->ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		VecAOCSScanDescData* vaocs;


		/*
		 * We reach here if the scan is not parallel, or if we're serially
		 * executing a scan that was planned to be parallel.
		 */
		/*
		 * GPDB: we are using table_beginscan_es in order to also initialize the
		 * scan state with the column info needed for AOCO relations. Check the
		 * comment in table_beginscan_es() for more info.
		 */
		scandesc = table_beginscan_es_vec(ss->ss_currentRelation,
										  estate->es_snapshot,
										  0, NULL,
										  (struct PlanState *)node,
										  SO_TYPE_VECTOR);


		ss->ss_currentScanDesc = scandesc;

		vaocs = (VecAOCSScanDescData*) scandesc;

		if (RelationIsAoCols(ss->ss_currentRelation))
			vaocs->vecdes = node->vecdesc;
	}

	/*
	 * get the next tuple from the table
	 */

	if (table_scan_getnextslot(scandesc, direction, slot))
	{
		return slot;
	}
	return NULL;
}


/*
 * VecSeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
bool
VecSeqRecheck(VecSeqScanState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, SeqScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecVecSeqScan(node)
 *
 *		Scans the relation sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecVecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecVecSeqScan(PlanState *pstate)
{
	TupleTableSlot *slot = NULL;
	VecSeqScanState *vsstate = (VecSeqScanState *) pstate;

	slot = ExecVecScan(&vsstate->base.ss,
					   &vsstate->vestate,
					   vsstate->vscanslot,
					   (ExecScanAccessMtd)VecSeqNext,
					   (ExecScanRecheckMtd)VecSeqRecheck);
	return slot;
}

/* ----------------------------------------------------------------
 *		ExecInitVecSeqScan
 * ----------------------------------------------------------------
 */
VecSeqScanState *
ExecInitVecSeqScan(SeqScan *node, EState *estate, int eflags)
{
	Relation	currentRelation;

	/*
	 * get the relation object id from the relid'th entry in the range table,
	 * open that relation and acquire appropriate lock on it.
	 */
	currentRelation = ExecOpenScanRelation(estate, node->scanrelid, eflags);

	return ExecInitVecSeqScanForPartition(node, estate, currentRelation);
}

VecSeqScanState *
ExecInitVecSeqScanForPartition(SeqScan *node, EState *estate,
							   Relation currentRelation)
{
	VecSeqScanState *vscanstate;
	ScanState 	*scanstate;

	/*
	 * Once upon a time it was possible to have an outerPlan of a SeqScan, but
	 * not any more.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	vscanstate = (VecSeqScanState*) palloc0(sizeof(VecSeqScanState));
	scanstate = (ScanState *)vscanstate;
	NodeSetTag(scanstate, T_SeqScanState);
	scanstate->ps.plan = (Plan *) node;
	scanstate->ps.state = estate;
	scanstate->ps.ExecProcNode = ExecVecSeqScan;
	TupleDesc currenttupledesc = RelationGetDescr(currentRelation);
	vscanstate->columnmap = palloc0(currenttupledesc->natts * sizeof(int));

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ps);

	/*
	 * open the scan relation
	 */
	scanstate->ss_currentRelation = currentRelation;

	/* and create slot with the appropriate rowtype */
	ExecInitScanTupleSlotVec(estate,
							 scanstate,
							 &vscanstate->vscanslot,
							 RelationGetDescr(currentRelation),
							 vscanstate->columnmap);

	/*
	 * Initialize result type、result tuple slot and projection 
	 */
	ExecInitResultTupleSlotTL(&scanstate->ps, &TTSOpsVecTuple);
	ExecAssignScanProjectionInfo(scanstate);

	vscanstate->vecdesc = TupleDescToVecDesc(scanstate->ps.scandesc);

	PostBuildVecPlan((PlanState *)vscanstate, &vscanstate->vestate);

	return vscanstate;
}

static void
ExecEagerFreeVecSeqScan(VecSeqScanState *node)
{
	FreeVecExecuteState(&node->vestate);
}

/* ----------------------------------------------------------------
 *		ExecEndVecSeqScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndVecSeqScan(VecSeqScanState *node)
{
	TableScanDesc scanDesc;
	ScanState *ss = (ScanState *) node;

	/*
	 * get information from node
	 */
	scanDesc = ss->ss_currentScanDesc;

	/*
	 * release tuple schema
	 */
	if (ss->ps.ps_ResultTupleSlot)
		FreeVecSchema(ss->ps.ps_ResultTupleSlot);

	FreeVecSchema(ss->ss_ScanTupleSlot);

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&ss->ps);

	/*
	 * clean out the tuple table
	 */
	if (node->vecdesc != NULL)
	{
		FreeVecDesc(node->vecdesc);
		node->vecdesc = NULL;
	}

	if (node->resdesc != NULL)
	{
		FreeResDesc(node->resdesc);
		node->resdesc = NULL;
	}

	if (node->columnmap != NULL)
	{
		pfree(node->columnmap);
	}
	/*
	 * clean out the tuple table
	 */
	if (ss->ps.ps_ResultTupleSlot)
		ExecClearTuple(ss->ps.ps_ResultTupleSlot);
	if (ss->ss_ScanTupleSlot)
		ExecClearTuple(ss->ss_ScanTupleSlot);
	if (node->vscanslot)
		ExecClearTuple(node->vscanslot);

	/*
	 * close heap scan
	 */
	if (scanDesc != NULL)
		table_endscan(scanDesc);
	
	FreeDummySchema();
	ExecEagerFreeVecSeqScan(node);
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecReScanSeqScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanVecSeqScan(VecSeqScanState *node)
{
	TableScanDesc scan;
	ScanState *ss = (ScanState *) node;

	scan = ss->ss_currentScanDesc;

	if (scan != NULL)
		table_rescan(scan,		/* scan desc */
					 NULL);		/* new scan keys */

	ExecScanReScan((ScanState *) node);
}

void
ExecSquelchVecSeqScan(SeqScanState *node)
{
	VecSeqScanState *vnode = (VecSeqScanState *) node;
	ExecEagerFreeVecSeqScan(vnode);
}
