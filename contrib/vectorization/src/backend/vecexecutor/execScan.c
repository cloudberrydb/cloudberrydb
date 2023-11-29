/*-------------------------------------------------------------------------
 *
 * execScan.c
 *	  This code provides support for vectorial relation scans.
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/backend/vecexecutor/execScan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "executor/executor.h"
#include "utils/faultinjector.h"
#include "catalog/pg_attribute.h"

#include "vecexecutor/executor.h"
#include "vecexecutor/execslot.h"
#include "vecexecutor/execnodes.h"

static TupleTableSlot *
ExecVecScanFetchInternal(ScanState *node,
						 ExecScanAccessMtd accessMtd,
						 ExecScanRecheckMtd recheckMtd);
/*
 * ExecScanFetch -- check interrupts & fetch next potential tuple
 *
 * This routine is concerned with substituting a test tuple if we are
 * inside an EvalPlanQual recheck.  If we aren't, just execute
 * the access method's next-tuple routine.
 */
TupleTableSlot *
ExecVecScanFetch(ScanState *node,
				 ExecScanAccessMtd accessMtd,
				 ExecScanRecheckMtd recheckMtd,
				 TupleTableSlot *vscanslot)
{
	TupleTableSlot *slot;

	slot = ExecVecScanFetchInternal(node, accessMtd, recheckMtd);
	return ExecImportABISlot(slot, vscanslot);
}

static TupleTableSlot *
ExecVecScanFetchInternal(ScanState *node,
						 ExecScanAccessMtd accessMtd,
						 ExecScanRecheckMtd recheckMtd)
{
	EState	   *estate = node->ps.state;

	CHECK_FOR_INTERRUPTS();

	if (QueryFinishPending)
		return NULL;

	if (estate->es_epq_active != NULL)
	{
		EPQState   *epqstate = estate->es_epq_active;

		/*
		 * We are inside an EvalPlanQual recheck.  Return the test tuple if
		 * one is available, after rechecking any access-method-specific
		 * conditions.
		 */
		Index		scanrelid = ((Scan *) node->ps.plan)->scanrelid;

		if (scanrelid == 0)
		{
			/*
			 * This is a ForeignScan or CustomScan which has pushed down a
			 * join to the remote side.  The recheck method is responsible not
			 * only for rechecking the scan/join quals but also for storing
			 * the correct tuple in the slot.
			 */

			TupleTableSlot *slot = node->ss_ScanTupleSlot;

			if (!(*recheckMtd) (node, slot))
				ExecClearTuple(slot);	/* would not be returned by scan */
			return slot;
		}
		else if (epqstate->relsubs_done[scanrelid - 1])
		{
			/*
			 * Return empty slot, as we already performed an EPQ substitution
			 * for this relation.
			 */

			TupleTableSlot *slot = node->ss_ScanTupleSlot;

			/* Return empty slot, as we already returned a tuple */
			return ExecClearTuple(slot);
		}
		else if (epqstate->relsubs_slot[scanrelid - 1] != NULL)
		{
			/*
			 * Return replacement tuple provided by the EPQ caller.
			 */

			TupleTableSlot *slot = epqstate->relsubs_slot[scanrelid - 1];

			Assert(epqstate->relsubs_rowmark[scanrelid - 1] == NULL);

			/* Mark to remember that we shouldn't return more */
			epqstate->relsubs_done[scanrelid - 1] = true;

			/* Return empty slot if we haven't got a test tuple */
			if (TupIsNull(slot))
				return NULL;

			/* Check if it meets the access-method conditions */
			if (!(*recheckMtd) (node, slot))
				return ExecClearTuple(slot);	/* would not be returned by
												 * scan */
			return slot;
		}
		else if (epqstate->relsubs_rowmark[scanrelid - 1] != NULL)
		{
			/*
			 * Fetch and return replacement tuple using a non-locking rowmark.
			 */

			TupleTableSlot *slot = node->ss_ScanTupleSlot;

			/* Mark to remember that we shouldn't return more */
			epqstate->relsubs_done[scanrelid - 1] = true;

			if (!EvalPlanQualFetchRowMark(epqstate, scanrelid, slot))
				return NULL;

			/* Return empty slot if we haven't got a test tuple */
			if (TupIsNull(slot))
				return NULL;

			/* Check if it meets the access-method conditions */
			if (!(*recheckMtd) (node, slot))
				return ExecClearTuple(slot);	/* would not be returned by
												 * scan */
			return slot;
		}
	}

	/*
	 * Run the node-type-specific access method function to get the next tuple
	 */
	return (*accessMtd) (node);
}


/* ----------------------------------------------------------------
 *		ExecVecScan
 *
 *		Scans the relation using the 'access method' indicated and
 *		returns the next qualifying tuple.
 *		The access method returns the next tuple and ExecVecScan() is
 *		responsible for checking the tuple returned against the qual-clause.
 *
 *		A 'recheck method' must also be provided that can check an
 *		arbitrary tuple of the relation against any qual conditions
 *		that are implemented internal to the access method.
 *
 *		Conditions:
 *		  -- the "cursor" maintained by the AMI is positioned at the tuple
 *			 returned previously.
 *
 *		Initial States:
 *		  -- the relation indicated is opened for scanning so that the
 *			 "cursor" is positioned before the first qualifying tuple.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecVecScan(ScanState *node,
			VecExecuteState *vestate,
			TupleTableSlot *vscanslot,
			ExecScanAccessMtd accessMtd, /* function returning a tuple */
			ExecScanRecheckMtd recheckMtd)
{
	ExprContext *econtext;
	List *targetlist = node->ps.plan->targetlist;
	TupleTableSlot *slot;

	SIMPLE_FAULT_INJECTOR("before_exec_scan");

	/*
	 * Fetch data from node
	 */
	econtext = node->ps.ps_ExprContext;

	/* interrupt checks are in ExecScanFetch */

	/* scan to get a non-empty slot */
	while (true)
	{
		/*
		 * If we have neither a qual to check nor a projection to do, just skip
		 * all the overhead and return the raw scan tuple.
		 */
		if (!vestate->plan)
		{
			ResetExprContext(econtext);
			slot = ExecVecScanFetch(node, accessMtd, recheckMtd, vscanslot);
		}
		else
			slot = ExecuteVecPlan(vestate);

		if(TupIsNull(slot))
			return NULL;

		/* Empty targetlist, which means a count(). Return batch contains
		 * empty tuples*/
		if (!targetlist)
		{
			int numrows;

			numrows = GetNumRows(slot);
			/* no valid tuple in this batch, continue to scan*/
			if (numrows == 0)
				continue;
			/* found tuple, store number of rows and return */
			else
			{
				StoreEmptyTuples(slot, numrows);
			}
		}
		break;
	}

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	return slot;
}
