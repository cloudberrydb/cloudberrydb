#include "postgres.h"

#include "access/htup_details.h"
#include "access/parallel.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "common/hashfn.h"
#include "executor/execExpr.h"
#include "executor/executor.h"
#include "executor/nodeAgg.h"
#include "lib/hyperloglog.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/dynahash.h"
#include "utils/expandeddatum.h"
#include "utils/faultinjector.h"
#include "utils/logtape.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"

#include "cdb/cdbexplain.h"
#include "lib/stringinfo.h"             /* StringInfo */
#include "optimizer/walkers.h"
#include "cdb/cdbvars.h"


#include "utils/tuptable_vec.h"
#include "utils/guc_vec.h"
#include "utils/fmgr_vec.h"
#include "vecexecutor/execnodes.h"
#include "vecexecutor/executor.h"
#include "vecexecutor/execslot.h"
#include "vecexecutor/nodeAgg.h"
#include "vecexecutor/execAmi.h"

#define STREAMING_ROWS (8*1024)

static TupleTableSlot *ExecVecAgg(PlanState *pstate);

static TupleTableSlot *
ExecVecAgg(PlanState *pstate)
{
	AggState   *node = castNode(AggState, pstate);
	VecAggState *vnode = (VecAggState*) node;

	int rows, rest, length;
	g_autoptr(GArrowRecordBatch) slice_rb = NULL;

	if (vnode->offset >= vnode->rows)
	{
		if (vnode->result_slot)
			ExecClearTuple(vnode->result_slot);
		if (vnode->origin_rb)
			ARROW_FREE_BATCH(&vnode->origin_rb);
		vnode->result_slot = NULL;
		vnode->offset      = 0;
		vnode->rows        = 0;

		TupleTableSlot *slot = ExecuteVecPlan(&vnode->estate);
		if (TupIsNull(slot))
		{
			return slot;
		}

		rows = GetNumRows(slot);
		if (rows <= max_batch_size)
		{
			return slot;
		}

		vnode->result_slot = slot;
		vnode->origin_rb   = g_object_ref(GetBatch(slot));
		vnode->offset      = 0;
		vnode->rows        = GetNumRows(slot);
	}

	ExecClearTuple(vnode->result_slot);

	rest   = vnode->rows - vnode->offset;
	length = rest > max_batch_size ? max_batch_size : rest;

	slice_rb = garrow_record_batch_slice(vnode->origin_rb, vnode->offset, length);

	vnode->offset += length;
	g_autoptr(GError) error = NULL;
	ExecStoreBatch(vnode->result_slot,  slice_rb);

	return vnode->result_slot;
}

/* -----------------
 * ExecInitVecAgg
 *
 *	Creates the run-time information for the agg node produced by the
 *	planner and initializes its outer subtree.
 *
 * -----------------
 */
AggState *
ExecInitVecAgg(Agg *node, EState *estate, int eflags)
{
	AggState    *aggstate;
    VecAggState *vaggstate;
	Plan	   *outerPlan;
	TupleDesc	scanDesc;
	int			numGroupingSets = 1;
	int			i = 0;
	bool		use_hashing = (node->aggstrategy == AGG_HASHED ||
							   node->aggstrategy == AGG_MIXED);

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	vaggstate = (VecAggState*) palloc0(sizeof(VecAggState));
	aggstate = (AggState*) vaggstate;
	NodeSetTag(aggstate, T_AggState);
	aggstate->ss.ps.plan = (Plan *) node;
	aggstate->ss.ps.state = estate;
	aggstate->ss.ps.ExecProcNode = ExecVecAgg;

	aggstate->aggs = NIL;
	aggstate->numaggs = 0;
	aggstate->numtrans = 0;
	aggstate->aggstrategy = node->aggstrategy;
	aggstate->aggsplit = node->aggsplit;
	aggstate->maxsets = 0;
	aggstate->projected_set = -1;
	aggstate->current_set = 0;
	aggstate->peragg = NULL;
	aggstate->pertrans = NULL;
	aggstate->curperagg = NULL;
	aggstate->curpertrans = NULL;
	aggstate->input_done = false;
	aggstate->agg_done = false;
	aggstate->grp_firstTuple = NULL;
	aggstate->sort_in = NULL;
	aggstate->sort_out = NULL;
	aggstate->aggs_used = NULL;
	vaggstate->plan = NULL;
	vaggstate->state = NULL;
	vaggstate->rbs = NULL;
	vaggstate->source_schema = NULL;

	aggstate->aggcontexts = (ExprContext **)
		palloc0(sizeof(ExprContext *) * numGroupingSets);

	/*
	 * Create expression contexts.  We need three or more, one for
	 * per-input-tuple processing, one for per-output-tuple processing, one
	 * for all the hashtables, and one for each grouping set.  The per-tuple
	 * memory context of the per-grouping-set ExprContexts (aggcontexts)
	 * replaces the standalone memory context formerly used to hold transition
	 * values.  We cheat a little by using ExecAssignExprContext() to build
	 * all of them.
	 *
	 * NOTE: the details of what is stored in aggcontexts and what is stored
	 * in the regular per-query memory context are driven by a simple
	 * decision: we want to reset the aggcontext at group boundaries (if not
	 * hashing) and in ExecReScanAgg to recover no-longer-wanted space.
	 */
	ExecAssignExprContext(estate, &aggstate->ss.ps);
	aggstate->tmpcontext = aggstate->ss.ps.ps_ExprContext;

	for (i = 0; i < numGroupingSets; ++i)
	{
		ExecAssignExprContext(estate, &aggstate->ss.ps);
		aggstate->aggcontexts[i] = aggstate->ss.ps.ps_ExprContext;
	}

	if (use_hashing)
		aggstate->hashcontext = CreateWorkExprContext(estate);

	ExecAssignExprContext(estate, &aggstate->ss.ps);
	/* veccontext for ExecVecProject */
	vaggstate->veccontext = aggstate->ss.ps.ps_ExprContext;

	ExecAssignExprContext(estate, &aggstate->ss.ps);

	/*
	 * Initialize child nodes.
	 *
	 * If we are doing a hashed aggregation then the child plan does not need
	 * to handle REWIND efficiently; see ExecReScanAgg.
	 */
	if (node->aggstrategy == AGG_HASHED)
		eflags &= ~EXEC_FLAG_REWIND;
	outerPlan = outerPlan(node);
	outerPlanState(aggstate) = VecExecInitNode(outerPlan, estate, eflags);

	if (node->aggstrategy == AGG_SORTED && IsA(outerPlan, Sort))
	{
		VecSortState *state = (VecSortState *)outerPlanState(aggstate);
		Sort       *sortnode = (Sort *) state->base.ss.ps.plan;
		bool found = false;

		for (int i = 0; i < node->numCols; ++i)
		{
			found = false;
			for (int j = 0; j < sortnode->numCols; j++)
			{
				if (node->grpColIdx[i] == sortnode->sortColIdx[i])
				{
					found = true;
					break;
				}
			}
			if (!found)
				break;
		}
		if (found)
			state->skip = true;
	}

	/*
	 * initialize source tuple type.
	 */
	aggstate->ss.ps.outerops =
		ExecGetResultSlotOps(outerPlanState(&aggstate->ss),
							 &aggstate->ss.ps.outeropsfixed);
	aggstate->ss.ps.outeropsset = true;

	ExecCreateScanSlotFromOuterPlan(estate, &aggstate->ss,
									aggstate->ss.ps.outerops);
	scanDesc = aggstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;

	for (int col = 0; col < scanDesc->natts; ++col)
	{
		aggstate->ss.ss_ScanTupleSlot->tts_values[col] = (Datum)0;
		aggstate->ss.ss_ScanTupleSlot->tts_isnull[col] = true;
	}
	ExecStoreVirtualTuple(aggstate->ss.ss_ScanTupleSlot);

	/*
	 * Initialize result type, slot and projection.
	 */
	ExecInitResultTupleSlotTL(&aggstate->ss.ps, &TTSOpsVecTuple);
	ExecAssignProjectionInfo(&aggstate->ss.ps, NULL);

	/* Init Arrow execute plan */
	PostBuildVecPlan((PlanState *)vaggstate, &vaggstate->estate);

	return aggstate;
}


static void
ExecEagerFreeVecAgg(AggState *node)
{
	VecAggState *vnode;
	int			aggno;
	VecAggStatePerAgg vperagg;

	vnode = (VecAggState *) node;
	vperagg = (VecAggStatePerAgg) node->peragg;

	if (vnode->proj)
		pfree(vnode->proj);	

	if (vnode->plan)
		ARROW_FREE(GArrowExecutePlan, &vnode->plan);
	if (vnode->state)
		ARROW_FREE(GArrowAggregationNodeState, &vnode->state);
	if (vnode->origin_rb)
		ARROW_FREE(GArrowRecordBatch, &vnode->origin_rb);

	if (vnode->int64_dt)
		ARROW_FREE(GArrowDataType, &vnode->int64_dt);
	if (vnode->decimal256_dt)
		ARROW_FREE(GArrowDataType, &vnode->decimal256_dt);
	if (vnode->source_schema)
		ARROW_FREE(GArrowSchema, &vnode->source_schema);
	if (VECSLOT(node->ss.ps.ps_ResultTupleSlot)->vec_schema.schema)
		ARROW_FREE(GArrowSchema, &VECSLOT(node->ss.ps.ps_ResultTupleSlot)->vec_schema.schema);
	
	for (aggno = 0; aggno < node->numaggs; ++aggno)
	{
		if (vnode->veccontext->ecxt_aggvalues[aggno])
			ARROW_FREE(GArrowArray, &vnode->veccontext->ecxt_aggvalues[aggno]);
	}
	FreeVecExecuteState(&vnode->estate);

	int			transno;
	int         numGroupingSets = Max(node->maxsets, 1);
	int			setno;

	/*
	 * When ending a parallel worker, copy the statistics gathered by the
	 * worker back into shared memory so that it can be picked up by the main
	 * process to report in EXPLAIN ANALYZE.
	 */
	if (node->shared_info && IsParallelWorker())
	{
		AggregateInstrumentation *si;

		Assert(ParallelWorkerNumber <= node->shared_info->num_workers);
		si = &node->shared_info->sinstrument[ParallelWorkerNumber];
		si->hash_batches_used = node->hash_batches_used;
		si->hash_disk_used = node->hash_disk_used;
		si->hash_mem_peak = node->hash_mem_peak;
	}

	if (node->hash_metacxt != NULL)
	{
		MemoryContextDelete(node->hash_metacxt);
		node->hash_metacxt = NULL;
	}

	for (transno = 0; transno < node->numtrans; transno++)
	{
		if (!bms_is_member(transno, node->aggs_used))
			continue;
		for (setno = 0; setno < numGroupingSets; setno++)
		{
			AggStatePerTrans pertrans = &node->pertrans[transno];

			if (pertrans->sortstates[setno])
			{
				tuplesort_end(pertrans->sortstates[setno]);
				pertrans->sortstates[setno] = NULL;
			}
		}
	}

	/* And ensure any agg shutdown callbacks have been called */
	for (setno = 0; setno < numGroupingSets; setno++)
		ReScanExprContext(node->aggcontexts[setno]);
	if (node->hashcontext)
		ReScanExprContext(node->hashcontext);
	if (node->ss.ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	if (vnode->result_slot)
		ExecClearTuple(vnode->result_slot);
}

void
ExecEndVecAgg(AggState *node)
{
	PlanState  	*outerPlan;

	ExecEagerFreeVecAgg(node);

	/*
	 * We don't actually free any ExprContexts here (see comment in
	 * ExecFreeExprContext), just unlinking the output one from the plan node
	 * suffices.
	 */
	ExecFreeExprContext(&node->ss.ps);

	/* clean up tuple table */
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	outerPlan = outerPlanState(node);
	VecExecEndNode(outerPlan);
}

void
ExecSquelchVecAgg(AggState *node)
{
	/*
	 * Sometimes, ExecSquelchAgg() is called, but the node is rescanned anyway.
	 * If we destroy the hash table here, then we need to rebuild it later.
	 * ExecReScanAgg() will try to reuse the hash table if params is not changing
	 * or affect input expressions, it will rescan the existing hash table.
	 * Therefore, don't destroy the hash table if reusing hashtable during rescan.
	 */

	if (!ReuseHashTable(node))
	{
		ExecEagerFreeVecAgg(node);
	}

	ExecVecSquelchNode(outerPlanState(node));
}