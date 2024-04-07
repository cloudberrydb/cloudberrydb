/*-------------------------------------------------------------------------
 *
 * nodeResult.c
 *	  support for constant nodes needing special code.
 *
 * DESCRIPTION
 *
 *		Result nodes are used in queries where no relations are scanned.
 *		Examples of such queries are:
 *
 *				select 1 * 2
 *
 *				insert into emp values ('mike', 15000)
 *
 *		(Remember that in an INSERT or UPDATE, we need a plan tree that
 *		generates the new rows.)
 *
 *		Result nodes are also used to optimise queries with constant
 *		qualifications (ie, quals that do not depend on the scanned data),
 *		such as:
 *
 *				select * from emp where 2 > 1
 *
 *		In this case, the plan generated is
 *
 *						Result	(with 2 > 1 qual)
 *						/
 *				   SeqScan (emp.*)
 *
 *		At runtime, the Result node evaluates the constant qual once,
 *		which is shown by EXPLAIN as a One-Time Filter.  If it's
 *		false, we can return an empty result set without running the
 *		controlled plan at all.  If it's true, we run the controlled
 *		plan normally and pass back the results.
 *
 *
 * Portions Copyright (c) 2005-2008, Cloudberry inc.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeResult.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/execdebug.h"
#include "executor/executor.h"

#include "executor/nodeResult.h"
#include "miscadmin.h"
#include "utils/memutils.h"

#include "cdb/cdbhash.h"
#include "cdb/veccdbhash.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/memquota.h"
#include "executor/spi.h"

#include "vecexecutor/nodeResult.h"
#include "vecexecutor/executor.h"
#include "vecexecutor/execnodes.h"
#include "vecexecutor/execAmi.h"
#include "vecexecutor/execslot.h"
#include "utils/fmgr_vec.h"

/* ----------------------------------------------------------------
 *		ExecResult(node)
 *
 *		returns the tuples from the outer plan which satisfy the
 *		qualification clause.  Since result nodes with right
 *		subtrees are never planned, we ignore the right subtree
 *		entirely (for now).. -cim 10/7/89
 *
 *		The qualification containing only constant clauses are
 *		checked first before any processing is done. It always returns
 *		'nil' if the constant qualification is not satisfied.
 * ----------------------------------------------------------------
 */

static Datum
get_segment_filter(int32 seg, Datum seg_array)
{
	g_autoptr(GArrowInt32Scalar)   seg_scalar = NULL;
	g_autoptr(GArrowDatum)         seg_scalar_datum = NULL;
	g_autoptr(GArrowDatum)         filter = NULL;
	g_autoptr(GArrowDatum)         segs_array_datum = NULL;

	Assert(seg_array);
	segs_array_datum = garrow_copy_ptr(GARROW_DATUM(DatumGetPointer(seg_array)));
	seg_scalar = garrow_int32_scalar_new(seg);
	seg_scalar_datum = GARROW_DATUM(garrow_scalar_datum_new(GARROW_SCALAR(seg_scalar)));
	garrow_store_func(filter,
			FunctionCall2Args("equal", segs_array_datum, seg_scalar_datum))
	PG_VEC_RETURN_POINTER(filter);
}

static TupleTableSlot *
ExecVecResult(PlanState *pstate)
{
	VecResultState *vnode = (VecResultState *)pstate;
	ResultState *node = castNode(ResultState, pstate);
	PlanState  *outerPlan;
	ExprContext *econtext;

	CHECK_FOR_INTERRUPTS();

	econtext = node->ps.ps_ExprContext;

	/*
	 * check constant qualifications like (2 > 1), if not already done
	 */
	if (node->rs_checkqual)
	{
		bool		qualResult = ExecQual(node->resconstantqual, econtext);

		node->rs_checkqual = false;
		if (!qualResult)
		{
			node->rs_done = true;
			return NULL;
		}
	}

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * if rs_done is true then it means that we were asked to return a
	 * constant tuple and we already did the last time ExecResult() was
	 * called, OR that we failed the constant qual check. Either way, now we
	 * are through.
	 */
	if (!node->rs_done)
	{
		outerPlan = outerPlanState(node);

		if (outerPlan == NULL)
		{
			/*
			 * if we don't have an outer plan, then we are just generating the
			 * results from a constant target list.  Do it only once.
			 */
			node->rs_done = true;
		}

		TupleTableSlot *candidateOutputSlot;

		candidateOutputSlot = ExecuteVecPlan(&vnode->estate);

		/*
		 * If there was a GPDB hash filter, check that too. Note that
		 * the hash filter is expressed in terms of *result* slot, so
		 * we must do this after projecting.
		 */
		if (node->hashFilter && !TupIsNull(candidateOutputSlot))
		{	
			GError *error = NULL;
			g_autoptr(GArrowArrayDatum) filter_datum = NULL;
			g_autoptr(GArrowBooleanArray) filter_array = NULL;
			g_autoptr(GArrowRecordBatch)  filter_batch = NULL;
			Datum hval = (Datum)0;
			Datum hval_reduce = (Datum)0;
			g_autoptr(GArrowRecordBatch) result_batch = GetBatch(candidateOutputSlot);
			hval = evalHashKeyVec(vnode->hash_projector, result_batch, (VecCdbHash *) node->hashFilter, GetNumRows(candidateOutputSlot));
			hval_reduce =  get_segment_filter(GpIdentity.segindex, hval);
			filter_datum = GARROW_ARRAY_DATUM(DatumGetPointer(hval_reduce));
			filter_array = GARROW_BOOLEAN_ARRAY(garrow_datum_get_array(filter_datum));
			filter_batch = garrow_record_batch_filter(result_batch, filter_array, NULL, &error);
			ExecStoreBatch(candidateOutputSlot, garrow_move_ptr(filter_batch));
		}
		return candidateOutputSlot;
	}

	return NULL;
}

/* ----------------------------------------------------------------
 *		ExecResultMarkPos
 * ----------------------------------------------------------------
 */
void
ExecVecResultMarkPos(ResultState *node)
{
	PlanState  *outerPlan = outerPlanState(node);

	if (outerPlan != NULL)
		ExecMarkPos(outerPlan);
	else
		elog(DEBUG2, "Result nodes do not support mark/restore");
}

/* ----------------------------------------------------------------
 *		ExecResultRestrPos
 * ----------------------------------------------------------------
 */
void
ExecVecResultRestrPos(ResultState *node)
{
	PlanState  *outerPlan = outerPlanState(node);

	if (outerPlan != NULL)
		ExecRestrPos(outerPlan);
	else
		elog(ERROR, "Result nodes do not support mark/restore");
}

static void 
ExecInitHashFilter(ResultState *node)
{
	g_autoptr(GArrowSchema) out_schema = NULL;
	g_autoptr(GError) error = NULL;
	g_autoptr(GGandivaCondition) condition = NULL;
	g_autoptr(GGandivaNode) hash_node = NULL;
	List *hash_keys = NIL;

	Result	   *resultNode = (Result *) node->ps.plan;
	out_schema = GetSchemaFromSlot(((PlanState *) node)->ps_ResultTupleSlot);
	for (int i = 0; i < resultNode->numHashFilterCols; i++)
	{
		hash_keys = lappend_int(hash_keys, resultNode->hashFilterColIdx[i] - 1);
	}
	hash_node = make_result_cols_hash_fnode(out_schema, hash_keys);
	if (hash_node)
	{
		GError *error = NULL;
		g_autoptr(GArrowInt32DataType) dt_int32 = garrow_int32_data_type_new();
		g_autoptr(GArrowField) retfield = garrow_field_new("hash_result", GARROW_DATA_TYPE(dt_int32));
		g_autoptr(GGandivaExpression) expr = ggandiva_expression_new(hash_node, retfield);
		/* projector */
		GList *exprs = garrow_list_append_ptr(NULL, expr);
		g_autoptr(GGandivaProjector) projector = ggandiva_projector_new(out_schema, exprs, &error);
		if (error)
			elog(ERROR, "Failed to initialize hash projector, cause: %s", error->message);
		VecResultState *vnode = (VecResultState *) node;
		garrow_store_ptr(vnode->hash_projector, projector);
		garrow_list_free_ptr(&exprs);
	}
	if (hash_keys != NIL)
	{
		list_free(hash_keys);
		hash_keys = NIL;
	}
}

/* ----------------------------------------------------------------
 *		ExecInitResult
 *
 *		Creates the run-time state information for the result node
 *		produced by the planner and initializes outer relations
 *		(child nodes).
 * ----------------------------------------------------------------
 */
ResultState *
ExecInitVecResult(Result *node, EState *estate, int eflags)
{
	VecResultState *vresstate;
	ResultState *resstate;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD)) ||
		   outerPlan(node) != NULL);

	/*
	 * create state structure
	 */
	vresstate = (VecResultState*) palloc0(sizeof(VecResultState));
	resstate = (ResultState *)vresstate;
	NodeSetTag(resstate, T_ResultState);
	resstate->ps.plan = (Plan *) node;
	resstate->ps.state = estate;
	resstate->ps.ExecProcNode = ExecVecResult;

	resstate->rs_done = false;
	resstate->rs_checkqual = (node->resconstantqual == NULL) ? false : true;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &resstate->ps);

	/*
	 * initialize child nodes
	 */
	outerPlanState(resstate) = VecExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * we don't use inner plan
	 */
	Assert(innerPlan(node) == NULL);

	/*
	 * Initialize result slot, type and projection.
	 */
	ExecInitResultTupleSlotTL(&resstate->ps, &TTSOpsVecTuple);

	ExecAssignProjectionInfo(&resstate->ps, NULL);

	/*
	 * initialize hash filter
	 */
	if (node->numHashFilterCols > 0)
	{
		int		currentSliceId = estate->currentSliceId;
		ExecSlice *currentSlice = &estate->es_sliceTable->slices[currentSliceId];
		resstate->hashFilter = (CdbHash *) makeVecCdbHash(currentSlice->planNumSegments, node->numHashFilterCols, node->hashFilterFuncs);
		ExecInitHashFilter(resstate);
	}

	if (!IsResManagerMemoryPolicyNone()
			&& IsResultMemoryIntensive(node))
	{
		SPI_ReserveMemory(((Plan *)node)->operatorMemKB * 1024L);
	}

	PostBuildVecPlan((PlanState *)vresstate, &vresstate->estate);

	return resstate;
}

static void
ExecEagerFreeVecResult(VecResultState *vnode)
{
	FreeVecExecuteState(&vnode->estate);
}

/* ----------------------------------------------------------------
 *		ExecEndResult
 *
 *		frees up storage allocated through C routines
 * ----------------------------------------------------------------
 */
void
ExecEndVecResult(ResultState *node)
{

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ps.ps_ResultTupleSlot);

	ExecEagerFreeVecResult((VecResultState *)node);

	/*
	 * shut down subplans
	 */
	VecExecEndNode(outerPlanState(node));
}

void
ExecReScanVecResult(ResultState *node)
{
	node->rs_done = false;
	node->rs_checkqual = (node->resconstantqual == NULL) ? false : true;

	/*
	 * If chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (node->ps.lefttree &&
		node->ps.lefttree->chgParam == NULL)
		ExecReScan(node->ps.lefttree);
}

void
ExecSquelchVecResult(ResultState *node)
{
	VecResultState *vnode = (VecResultState *) node;
	ExecEagerFreeVecResult(vnode);
	ExecVecSquelchNode(outerPlanState(node));
}
