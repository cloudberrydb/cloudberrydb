/*
 * FIXME: This file will be deleted in the future
 */
#include "hook/hook.h"

#include "cdb/cdbsubplan.h"
#include "cdb/cdbdisp_query.h"
#include "catalog/oid_dispatch.h"
#include "access/xact.h"
#include "executor/execUtils.h"
#include "utils/snapmgr.h"
#include "cdb/memquota.h"
#include "cdb/cdbvars.h"
#include "nodes/print.h"
#include "commands/trigger.h"
#include "cdb/cdbmotion.h"
#include "cdb/ml_ipc.h"
#include "utils/metrics_utils.h"
#include "commands/copy.h"
#include "commands/createas.h"
#include "commands/matview.h"
#include "foreign/fdwapi.h"
#include "executor/nodeSubplan.h"
#include "cdb/cdbexplain.h"
#include "utils/lsyscache.h"
#include "utils/probes.h"
#include "catalog/namespace.h"
#include "tcop/cmdtag.h"
#include "tcop/utility.h"

#include "access/printtup_vec.h"
#include "vecexecutor/nodeAssertOp.h"
#include "vecexecutor/execnodes.h"
#include "vecexecutor/execslot.h"
#include "vecexecutor/executor.h"
#include "vecexecutor/nodeAgg.h"
#include "vecexecutor/nodeAppend.h"
#include "vecexecutor/nodeForeignscan.h"
#include "vecexecutor/nodeHash.h"
#include "vecexecutor/nodeHashjoin.h"
#include "vecexecutor/nodeLimit.h"
#include "vecexecutor/nodeMaterial.h"
#include "vecexecutor/nodeMotion.h"
#include "vecexecutor/nodeNestloop.h"
#include "vecexecutor/nodeResult.h"
#include "vecexecutor/nodeSeqscan.h"
#include "vecexecutor/nodeSequence.h"
#include "vecexecutor/nodeShareInputScan.h"
#include "vecexecutor/nodeSort.h"
#include "vecexecutor/nodeSubqueryscan.h"
#include "vecexecutor/nodeWindowAgg.h"
#include "vecnodes/nodes.h"

/* In the future this hook will be cleaned up. */
ExplainOneQuery_hook_type   vec_explain_prev;
ExecutorStart_hook_type     vec_exec_start_prev;
ExecutorRun_hook_type       vec_exec_run_prev;
ExecutorEnd_hook_type       vec_exec_end_prev;

/* decls for local routines only used within this module */
static void InitPlan(QueryDesc *queryDesc, int eflags);
static void CheckValidRowMarkRel(Relation rel, RowMarkType markType);
static void AdjustReplicatedTableCounts(EState *estate);
static void ExecEndPlan(PlanState *planstate, EState *estate);
static void ExecCheckXactReadOnly(PlannedStmt *plannedstmt);

static TupleTableSlot *
VecExecProcNodeGPDB(PlanState *node)
{
	TupleTableSlot *result;
	MemoryContext oldcxt = NULL;

	/*
	 * Even if we are requested to finish query, Motion has to do its work
	 * to tell End of Stream message to upper slice.  He will probably get
	 * NULL tuple from underlying operator by calling another ExecProcNode,
	 * so one additional operator execution should not be a big hit.
	 */
	if (QueryFinishPending && !IsA(node, MotionState))
		return NULL;

	if (node->plan)
		TRACE_POSTGRESQL_EXECPROCNODE_ENTER(GpIdentity.segindex, currentSliceId, nodeTag(node), node->plan->plan_node_id);

	if (node->squelched)
		elog(ERROR, "cannot execute squelched plan node of type: %d",
			 (int) nodeTag(node));

	if (!node->fHadSentNodeStart)
	{
		/* GPDB hook for collecting query info */
		if (query_info_collect_hook)
			(*query_info_collect_hook)(METRICS_PLAN_NODE_EXECUTING, node);
		node->fHadSentNodeStart = true;
	}

	if (node->instrument)
		InstrStartNode(node->instrument);

	if ((node->state->es_instrument & INSTRUMENT_MEMORY_DETAIL) != 0)
		oldcxt = MemoryContextSwitchTo(node->node_context);

	result = node->ExecProcNodeReal(node);

	if ((node->state->es_instrument & INSTRUMENT_MEMORY_DETAIL) != 0)
	{
		Assert(CurrentMemoryContext == node->node_context);
		MemoryContextSwitchTo(oldcxt);
	}

	if (node->instrument)
		InstrStopNode(node->instrument, TupIsNull(result) ? 0.0 : GetNumRows(result));

	if (node->plan)
		TRACE_POSTGRESQL_EXECPROCNODE_EXIT(GpIdentity.segindex, currentSliceId, nodeTag(node), node->plan->plan_node_id);

	return result;
}

/*
 * ExecProcNode wrapper that performs some one-time checks, before calling
 * the relevant node method (possibly via an instrumentation wrapper).
 */
static TupleTableSlot *
VecExecProcNodeFirst(PlanState *node)
{
	/*
	 * Perform stack depth check during the first execution of the node.  We
	 * only do so the first time round because it turns out to not be cheap on
	 * some common architectures (eg. x86).  This relies on the assumption
	 * that ExecProcNode calls for a given plan node will always be made at
	 * roughly the same stack depth.
	 */
	check_stack_depth();

	/*
	 * If instrumentation is required, change the wrapper to one that just
	 * does instrumentation.  Otherwise we can dispense with all wrappers and
	 * have ExecProcNode() directly call the relevant function from now on.
	 *
	 * GPDB: Unfortunately, GPDB has a bunch of extra stuff that we need
	 * to do on every node, so we cannot make use of this upstream optimization.
	 * ExecProcNodeGPDB() is a wrapper that does all the Cloudberry-specific
	 * extra stuff.
	 */
#if 0
	if (node->instrument)
		node->ExecProcNode = ExecProcNodeInstr;
	else
		node->ExecProcNode = node->ExecProcNodeReal;
#endif
	node->ExecProcNode = VecExecProcNodeGPDB;

	return node->ExecProcNode(node);
}

/*
 * If a node wants to change its ExecProcNode function after ExecInitNode()
 * has finished, it should do so with this function.  That way any wrapper
 * functions can be reinstalled, without the node having to know how that
 * works.
 */
static void
VecExecSetExecProcNode(PlanState *node, ExecProcNodeMtd function)
{
	/*
	 * Add a wrapper around the ExecProcNode callback that checks stack depth
	 * during the first execution and maybe adds an instrumentation wrapper.
	 * When the callback is changed after execution has already begun that
	 * means we'll superfluously execute ExecProcNodeFirst, but that seems ok.
	 */
	node->ExecProcNodeReal = function;
	node->ExecProcNode = VecExecProcNodeFirst;
}

void
ExecutorStartWrapper(QueryDesc *queryDesc, int eflags)
{
	bool vec_type = find_extension_context(queryDesc->plannedstmt->extensionContext);

	if (vec_type)
	{
		init_vector_types();
		/* Set vector executor flag, start vector executation. */
		eflags |= EXEC_FLAG_VECTOR;
		if (vec_exec_start_prev)
			(*vec_exec_start_prev) (queryDesc, eflags);
		VecExecutorStart(queryDesc, eflags);
		return;
	}

	if (vec_exec_start_prev)
		(*vec_exec_start_prev) (queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
} 

void
ExecutorRunWrapper(QueryDesc *queryDesc,
				   ScanDirection direction,
				   uint64 count,
				   bool execute_once)
{
	if (queryDesc->estate->es_top_eflags & EXEC_FLAG_VECTOR)
		set_printtup_wrapper(&queryDesc->dest);

	if (vec_exec_run_prev)
		(*vec_exec_run_prev) (queryDesc, direction, count, execute_once);
	else
		standard_ExecutorRun(queryDesc, direction, count, execute_once);
} 

void
ExecutorEndWrapper(QueryDesc *queryDesc)
{
	if (queryDesc->estate->es_top_eflags & EXEC_FLAG_VECTOR) 
	{
		if (vec_exec_end_prev)
			(*vec_exec_end_prev)(queryDesc);
		VecExecutorEnd(queryDesc);
		return;
	}

	if (vec_exec_end_prev)
		(*vec_exec_end_prev) (queryDesc);
	else
		standard_ExecutorEnd(queryDesc);

}

void
VecExecutorStart(QueryDesc *queryDesc, int eflags)
{
	EState	   *estate;
	MemoryContext oldcontext;
	GpExecIdentity exec_identity;
	bool		shouldDispatch;
	bool		needDtx;
	List 		*toplevelOidCache = NIL;

	/* sanity checks: queryDesc must not be started already */
	Assert(queryDesc != NULL);
	Assert(queryDesc->estate == NULL);
	Assert(queryDesc->plannedstmt != NULL);

	Assert(queryDesc->plannedstmt->intoPolicy == NULL ||
		GpPolicyIsPartitioned(queryDesc->plannedstmt->intoPolicy) ||
		GpPolicyIsReplicated(queryDesc->plannedstmt->intoPolicy));

	/* GPDB hook for collecting query info */
	if (query_info_collect_hook)
		(*query_info_collect_hook)(METRICS_QUERY_START, queryDesc);

	/**
	 * Distribute memory to operators.
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		if (!IsResManagerMemoryPolicyNone() &&
			LogResManagerMemory())
		{
			elog(GP_RESMANAGER_MEMORY_LOG_LEVEL, "query requested %.0fKB of memory",
				 (double) queryDesc->plannedstmt->query_mem / 1024.0);
		}

		/**
		 * There are some statements that do not go through the resource queue, so we cannot
		 * put in a strong assert here. Someday, we should fix resource queues.
		 */
		if (queryDesc->plannedstmt->query_mem > 0)
		{
			switch(*gp_resmanager_memory_policy)
			{
				case RESMANAGER_MEMORY_POLICY_AUTO:
					PolicyAutoAssignOperatorMemoryKB(queryDesc->plannedstmt,
													 queryDesc->plannedstmt->query_mem);
					break;
				case RESMANAGER_MEMORY_POLICY_EAGER_FREE:
					PolicyEagerFreeAssignOperatorMemoryKB(queryDesc->plannedstmt,
														  queryDesc->plannedstmt->query_mem);
					break;
				default:
					Assert(IsResManagerMemoryPolicyNone());
					break;
			}
		}
	}

	/*
	 * If the transaction is read-only, we need to check if any writes are
	 * planned to non-temporary tables.  EXPLAIN is considered read-only.
	 *
	 * Don't allow writes in parallel mode.  Supporting UPDATE and DELETE
	 * would require (a) storing the combo CID hash in shared memory, rather
	 * than synchronizing it just once at the start of parallelism, and (b) an
	 * alternative to heap_update()'s reliance on xmax for mutual exclusion.
	 * INSERT may have no such troubles, but we forbid it to simplify the
	 * checks.
	 *
	 * We have lower-level defenses in CommandCounterIncrement and elsewhere
	 * against performing unsafe operations in parallel mode, but this gives a
	 * more user-friendly error message.
	 *
	 * In GPDB, we must call ExecCheckXactReadOnly() in the QD even if the
	 * transaction is not read-only, because ExecCheckXactReadOnly() also
	 * determines if two-phase commit is needed.
	 */
	if ((XactReadOnly || IsInParallelMode() || Gp_role == GP_ROLE_DISPATCH) &&
		!(eflags & EXEC_FLAG_EXPLAIN_ONLY))
		ExecCheckXactReadOnly(queryDesc->plannedstmt);

	/*
	 * Build EState, switch into per-query memory context for startup.
	 */
	estate = CreateExecutorState();
	queryDesc->estate = estate;

	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	/**
	 * Attached the plannedstmt from queryDesc
	 */
	estate->es_plannedstmt = queryDesc->plannedstmt;

	/*
	 * Fill in external parameters, if any, from queryDesc; and allocate
	 * workspace for internal parameters
	 */
	estate->es_param_list_info = queryDesc->params;

	if (queryDesc->plannedstmt->paramExecTypes != NIL)
	{
		int			nParamExec;

		nParamExec = list_length(queryDesc->plannedstmt->paramExecTypes);
		estate->es_param_exec_vals = (ParamExecData *)
			palloc0(nParamExec * sizeof(ParamExecData));
	}

	/* We now require all callers to provide sourceText */
	Assert(queryDesc->sourceText != NULL);
	estate->es_sourceText = queryDesc->sourceText;

	/*
	 * Fill in the query environment, if any, from queryDesc.
	 */
	estate->es_queryEnv = queryDesc->queryEnv;

	/*
	 * If non-read-only query, set the command ID to mark output tuples with
	 */
	switch (queryDesc->operation)
	{
		case CMD_SELECT:

			/*
			 * SELECT FOR [KEY] UPDATE/SHARE and modifying CTEs need to mark
			 * tuples
			 */
			if (queryDesc->plannedstmt->rowMarks != NIL ||
				queryDesc->plannedstmt->hasModifyingCTE)
				estate->es_output_cid = GetCurrentCommandId(true);

			/*
			 * A SELECT without modifying CTEs can't possibly queue triggers,
			 * so force skip-triggers mode. This is just a marginal efficiency
			 * hack, since AfterTriggerBeginQuery/AfterTriggerEndQuery aren't
			 * all that expensive, but we might as well do it.
			 */
			if (!queryDesc->plannedstmt->hasModifyingCTE)
				eflags |= EXEC_FLAG_SKIP_TRIGGERS;
			break;

		case CMD_INSERT:
		case CMD_DELETE:
		case CMD_UPDATE:
			estate->es_output_cid = GetCurrentCommandId(true);
			break;

		default:
			elog(ERROR, "unrecognized operation code: %d",
				 (int) queryDesc->operation);
			break;
	}

	/*
	 * Copy other important information into the EState
	 */
	estate->es_snapshot = RegisterSnapshot(queryDesc->snapshot);
	estate->es_crosscheck_snapshot = RegisterSnapshot(queryDesc->crosscheck_snapshot);
	estate->es_top_eflags = eflags;
	estate->es_instrument = queryDesc->instrument_options;
	estate->es_jit_flags = queryDesc->plannedstmt->jitFlags;
	estate->showstatctx = queryDesc->showstatctx;

	/*
	 * Shared input info is needed when ROLE_EXECUTE or sequential plan
	 */
	estate->es_sharenode = NIL;

	/*
	 * Handling of the Slice table depends on context.
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		/* Set up the slice table. */
		SliceTable *sliceTable;

		sliceTable = InitSliceTable(estate, queryDesc->plannedstmt);
		estate->es_sliceTable = sliceTable;

		if (sliceTable->slices[0].gangType != GANGTYPE_UNALLOCATED ||
			sliceTable->hasMotions)
		{
			if (queryDesc->ddesc == NULL)
			{
				queryDesc->ddesc = makeNode(QueryDispatchDesc);;
				queryDesc->ddesc->useChangedAOOpts = true;
			}

			/* Pass EXPLAIN ANALYZE flag to qExecs. */
			estate->es_sliceTable->instrument_options = queryDesc->instrument_options;

			/* set our global sliceid variable for elog. */
			currentSliceId = LocallyExecutingSliceIndex(estate);

			/* InitPlan() will acquire locks by walking the entire plan
			 * tree -- we'd like to avoid acquiring the locks until
			 * *after* we've set up the interconnect */
			if (estate->es_sliceTable->hasMotions)
				estate->motionlayer_context = createMotionLayerState(queryDesc->plannedstmt->numSlices - 1);

			shouldDispatch = !(eflags & EXEC_FLAG_EXPLAIN_ONLY);
		}
		else
		{
			/* QD-only query, no dispatching required */
			shouldDispatch = false;
		}

		/*
		 * If this is CREATE TABLE AS ... WITH NO DATA, there's no need
		 * need to actually execute the plan.
		 */
		if (queryDesc->plannedstmt->intoClause &&
			queryDesc->plannedstmt->intoClause->skipData)
			shouldDispatch = false;
	}
	else if (Gp_role == GP_ROLE_EXECUTE)
	{
		QueryDispatchDesc *ddesc = queryDesc->ddesc;

		shouldDispatch = false;

		/* qDisp should have sent us a slice table via MPPEXEC */
		if (ddesc && ddesc->sliceTable != NULL)
		{
			SliceTable *sliceTable;
			ExecSlice  *slice;

			sliceTable = ddesc->sliceTable;
			Assert(IsA(sliceTable, SliceTable));
			slice = &sliceTable->slices[sliceTable->localSlice];

			estate->es_sliceTable = sliceTable;
			estate->es_cursorPositions = ddesc->cursorPositions;

			estate->currentSliceId = slice->rootIndex;

			/* set our global sliceid variable for elog. */
			currentSliceId = LocallyExecutingSliceIndex(estate);

			/* Should we collect statistics for EXPLAIN ANALYZE? */
			estate->es_instrument = sliceTable->instrument_options;
			queryDesc->instrument_options = sliceTable->instrument_options;

			/* InitPlan() will acquire locks by walking the entire plan
			 * tree -- we'd like to avoid acquiring the locks until
			 * *after* we've set up the interconnect */
			if (estate->es_sliceTable->hasMotions)
			{
				estate->motionlayer_context = createMotionLayerState(queryDesc->plannedstmt->numSlices - 1);

				PG_TRY();
				{
					/*
					 * Initialize the motion layer for this query.
					 */
					Assert(!estate->interconnect_context);
					CurrentMotionIPCLayer->SetupInterconnect(estate);
					UpdateMotionExpectedReceivers(estate->motionlayer_context, estate->es_sliceTable);

					SIMPLE_FAULT_INJECTOR("qe_got_snapshot_and_interconnect");
					Assert(estate->interconnect_context);
				}
				PG_CATCH();
				{
					mppExecutorCleanup(queryDesc);
					PG_RE_THROW();
				}
				PG_END_TRY();
			}
		}
		else
		{
			/* local query in QE. */
		}
	}
	else
		shouldDispatch = false;

	/*
	 * We don't eliminate aliens if we don't have an MPP plan
	 * or we are executing on master.
	 *
	 * TODO: eliminate aliens even on master, if not EXPLAIN ANALYZE
	 */
	estate->eliminateAliens = execute_pruned_plan && estate->es_sliceTable && estate->es_sliceTable->hasMotions && !IS_QUERY_DISPATCHER();

	/*
	 * Set up an AFTER-trigger statement context, unless told not to, or
	 * unless it's EXPLAIN-only mode (when ExecutorFinish won't be called).
	 */
	if (!(eflags & (EXEC_FLAG_SKIP_TRIGGERS | EXEC_FLAG_EXPLAIN_ONLY)))
		AfterTriggerBeginQuery();

	/*
	 * Initialize the plan state tree
	 *
	 * If the interconnect has been set up; we need to catch any
	 * errors to shut it down -- so we have to wrap InitPlan in a PG_TRY() block.
	 */
	PG_TRY();
	{
		/*
		 * Initialize the plan state tree
		 */
		Assert(CurrentMemoryContext == estate->es_query_cxt);
		InitPlan(queryDesc, eflags);

		Assert(queryDesc->planstate);

#ifdef USE_ASSERT_CHECKING
		AssertSliceTableIsValid(estate->es_sliceTable);
#endif

		if (Debug_print_slice_table && Gp_role == GP_ROLE_DISPATCH)
			elog_node_display(DEBUG3, "slice table", estate->es_sliceTable, true);

		/*
		 * If we're running as a QE and there's a slice table in our queryDesc,
		 * then we need to finish the EState setup we prepared for back in
		 * CdbExecQuery.
		 */
		if (Gp_role == GP_ROLE_EXECUTE && estate->es_sliceTable != NULL)
		{
			MotionState *motionstate = NULL;

			/*
			 * Note that, at this point on a QE, the estate is setup (based on the
			 * slice table transmitted from the QD via MPPEXEC) so that fields
			 * es_sliceTable, cur_root_idx and es_cur_slice_idx are correct for
			 * the QE.
			 *
			 * If responsible for a non-root slice, arrange to enter the plan at the
			 * slice's sending Motion node rather than at the top.
			 */
			if (LocallyExecutingSliceIndex(estate) != RootSliceIndex(estate))
			{
				motionstate = getMotionState(queryDesc->planstate, LocallyExecutingSliceIndex(estate));
				Assert(motionstate != NULL && IsA(motionstate, MotionState));
			}

			if (Debug_print_slice_table)
				elog_node_display(DEBUG3, "slice table", estate->es_sliceTable, true);

			if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
				elog(DEBUG1, "seg%d executing slice%d under root slice%d",
					 GpIdentity.segindex,
					 LocallyExecutingSliceIndex(estate),
					 RootSliceIndex(estate));
		}

		/*
		 * if in dispatch mode, time to serialize plan and query
		 * trees, and fire off cdb_exec command to each of the qexecs
		 */
		if (shouldDispatch)
		{
			/*
			 * MPP-2869: preprocess_initplans() may
			 * dispatch. (interacted with MPP-2859, which caused an
			 * initPlan to do a write which should have happened in
			 * main body of query) We need to call
			 * ExecutorSaysTransactionDoesWrites() before any dispatch
			 * work for this query.
			 */
			needDtx = ExecutorSaysTransactionDoesWrites();
			if (needDtx)
				setupDtxTransaction();

			/*
			 * Aviod dispatching OIDs for InitPlan.
			 *
			 * CTAS will first define relation in QD, and generate the OIDs,
			 * and then dispatch with these OIDs to QEs.
			 * QEs store these OIDs in a static variable and delete the one
			 * used to create table.
			 *
			 * If CTAS's query contains initplan, when we invoke
			 * preprocess_initplan to dispatch initplans, if with
			 * queryDesc->ddesc->oidAssignments be set, these OIDs are
			 * also dispatched to QEs.
			 *
			 * For details please see github issue https://github.com/greenplum-db/gpdb/issues/10760
			 */
			if (queryDesc->ddesc != NULL)
			{
				queryDesc->ddesc->sliceTable = estate->es_sliceTable;
				/*
				 * For CTAS querys that contain initplan, we need to copy a new oid dispatch list,
				 * since the preprocess_initplan will start a subtransaction, and if it's rollbacked,
				 * the memory context of 'Oid dispatch context' will be reset, which will cause invalid
				 * list reference during the serialization of dispatch_oids when dispatching plan.
				 */
				toplevelOidCache = copyObject(GetAssignedOidsForDispatch());
			}

			/*
			 * First, pre-execute any initPlan subplans.
			 */
			if (list_length(queryDesc->plannedstmt->paramExecTypes) > 0) {
				List* prev_ctx = queryDesc->plannedstmt->extensionContext;
				queryDesc->plannedstmt->extensionContext = NULL;
				preprocess_initplans(queryDesc);
				queryDesc->plannedstmt->extensionContext = prev_ctx;
			}

			if (toplevelOidCache != NIL)
			{
				queryDesc->ddesc->oidAssignments = toplevelOidCache;
			}

			/*
			 * This call returns after launching the threads that send the
			 * plan to the appropriate segdbs.  It does not wait for them to
			 * finish unless an error is detected before all slices have been
			 * dispatched.
			 *
			 * Main plan is parallel, send plan to it.
			 */
			if (estate->es_sliceTable->slices[0].gangType != GANGTYPE_UNALLOCATED ||
				estate->es_sliceTable->slices[0].children)
			{
				CdbDispatchPlan(queryDesc,
								estate->es_param_exec_vals,
								needDtx, true);
			}

			if (toplevelOidCache != NIL)
			{
				list_free(toplevelOidCache);
				toplevelOidCache = NIL;
			}
		}

		/*
		 * Get executor identity (who does the executor serve). we can assume
		 * Forward scan direction for now just for retrieving the identity.
		 */
		if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY))
			exec_identity = getGpExecIdentity(queryDesc, ForwardScanDirection, estate);
		else
			exec_identity = GP_IGNORE;

		/*
		 * If we have no slice to execute in this process, mark currentSliceId as
		 * invalid.
		 */
		if (exec_identity == GP_IGNORE)
		{
			estate->currentSliceId = -1;
			currentSliceId = -1;
		}

#ifdef USE_ASSERT_CHECKING
		/* non-root on QE */
		if (exec_identity == GP_NON_ROOT_ON_QE)
		{
			MotionState *motionState = getMotionState(queryDesc->planstate, LocallyExecutingSliceIndex(estate));

			Assert(motionState);

			Assert(IsA(motionState->ps.plan, Motion));
		}
		else
#endif
		if (exec_identity == GP_ROOT_SLICE)
		{
			/* Run a root slice. */
			if (queryDesc->planstate != NULL &&
				estate->es_sliceTable &&
				estate->es_sliceTable->slices[0].gangType == GANGTYPE_UNALLOCATED &&
				estate->es_sliceTable->slices[0].children &&
				!estate->es_interconnect_is_setup)
			{
				Assert(!estate->interconnect_context);
				CurrentMotionIPCLayer->SetupInterconnect(estate);
				Assert(estate->interconnect_context);
				UpdateMotionExpectedReceivers(estate->motionlayer_context, estate->es_sliceTable);
			}
		}
		else if (exec_identity != GP_IGNORE)
		{
			/* should never happen */
			Assert(!"unsupported parallel execution strategy");
		}

		if(estate->es_interconnect_is_setup)
			Assert(estate->interconnect_context != NULL);

	}
	PG_CATCH();
	{
		if (toplevelOidCache != NIL)
		{
			list_free(toplevelOidCache);
			toplevelOidCache = NIL;
		}
		mppExecutorCleanup(queryDesc);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (DEBUG1 >= log_min_messages)
	{
		char		msec_str[32];
		switch (check_log_duration(msec_str, false))
		{
			case 1:
			case 2:
				ereport(LOG, (errmsg("duration to ExecutorStart end: %s ms", msec_str)));
				break;
		}
	}

	MemoryContextSwitchTo(oldcontext);
}

/* ----------------------------------------------------------------
 *		InitPlan
 *
 *		Initializes the query plan: open files, allocate storage
 *		and start up the rule manager
 * ----------------------------------------------------------------
 */
static void
InitPlan(QueryDesc *queryDesc, int eflags)
{
	CmdType		operation = queryDesc->operation;
	PlannedStmt *plannedstmt = queryDesc->plannedstmt;
	Plan	   *plan = plannedstmt->planTree;
	List	   *rangeTable = plannedstmt->rtable;
	EState	   *estate = queryDesc->estate;
	PlanState  *planstate;
	TupleDesc	tupType;
	ListCell   *l;

	Assert(plannedstmt->intoPolicy == NULL ||
		GpPolicyIsPartitioned(plannedstmt->intoPolicy) ||
		GpPolicyIsReplicated(plannedstmt->intoPolicy));

	if (DEBUG1 >= log_min_messages)
	{
		char msec_str[32];
		switch (check_log_duration(msec_str, false))
		{
			case 1:
			case 2:
				ereport(LOG, (errmsg("duration to InitPlan start: %s ms", msec_str)));
				break;
			default:
				/* do nothing */
				break;
		}
	}

	/*
	 * Do permissions checks
	 */
	if (operation != CMD_SELECT || Gp_role != GP_ROLE_EXECUTE)
	{
		ExecCheckRTPerms(rangeTable, true);
	}

	/*
	 * initialize the node's execution state
	 */
	ExecInitRangeTable(estate, rangeTable);

	estate->es_plannedstmt = plannedstmt;

	/*
	 * Next, build the ExecRowMark array from the PlanRowMark(s), if any.
	 */
	if (plannedstmt->rowMarks)
	{
		estate->es_rowmarks = (ExecRowMark **)
			palloc0(estate->es_range_table_size * sizeof(ExecRowMark *));
		foreach(l, plannedstmt->rowMarks)
		{
			PlanRowMark *rc = (PlanRowMark *) lfirst(l);
			Oid			relid;
			Relation	relation;
			ExecRowMark *erm;

			/* ignore "parent" rowmarks; they are irrelevant at runtime */
			if (rc->isParent)
				continue;

			/* get relation's OID (will produce InvalidOid if subquery) */
			relid = exec_rt_fetch(rc->rti, estate)->relid;

			/* open relation, if we need to access it for this mark type */
			switch (rc->markType)
			{
				/*
				 * Cloudberry specific behavior:
				 * The implementation of select statement with locking clause
				 * (for update | no key update | share | key share) in postgres
				 * is to hold RowShareLock on tables during parsing stage, and
				 * generate a LockRows plan node for executor to lock the tuples.
				 * It is not easy to lock tuples in Cloudberry database, since
				 * tuples may be fetched through motion nodes.
				 *
				 * But when Global Deadlock Detector is enabled, and the select
				 * statement with locking clause contains only one table, we are
				 * sure that there are no motions. For such simple cases, we could
				 * make the behavior just the same as Postgres.
				 */
				case ROW_MARK_EXCLUSIVE:
				case ROW_MARK_NOKEYEXCLUSIVE:
				case ROW_MARK_SHARE:
				case ROW_MARK_KEYSHARE:
				case ROW_MARK_REFERENCE:
					relation = ExecGetRangeTableRelation(estate, rc->rti);
					break;
				case ROW_MARK_COPY:
					/* no physical table access is required */
					relation = NULL;
					break;
				default:
					elog(ERROR, "unrecognized markType: %d", rc->markType);
					relation = NULL;	/* keep compiler quiet */
					break;
			}

			/* Check that relation is a legal target for marking */
			if (relation)
				CheckValidRowMarkRel(relation, rc->markType);

			erm = (ExecRowMark *) palloc(sizeof(ExecRowMark));
			erm->relation = relation;
			erm->relid = relid;
			erm->rti = rc->rti;
			erm->prti = rc->prti;
			erm->rowmarkId = rc->rowmarkId;
			erm->markType = rc->markType;
			erm->strength = rc->strength;
			erm->waitPolicy = rc->waitPolicy;
			erm->ermActive = false;
			ItemPointerSetInvalid(&(erm->curCtid));
			erm->ermExtra = NULL;

			Assert(erm->rti > 0 && erm->rti <= estate->es_range_table_size &&
				   estate->es_rowmarks[erm->rti - 1] == NULL);

			estate->es_rowmarks[erm->rti - 1] = erm;
		}
	}

	/*
	 * Initialize the executor's tuple table to empty.
	 */
	estate->es_tupleTable = NIL;

	/* signal that this EState is not used for EPQ */
	estate->es_epq_active = NULL;

	/*
	 * Initialize private state information for each SubPlan.  We must do this
	 * before running VecExecInitNode on the main query tree, since
	 * ExecInitSubPlan expects to be able to find these entries.
	 */
	Assert(estate->es_subplanstates == NIL);
	Plan *start_plan_node = plannedstmt->planTree;

	estate->currentSliceId = 0;

	/*
	 * If eliminateAliens is true then we extract the local Motion node
	 * and subplans for our current slice. This enables us to call VecExecInitNode
	 * for only a subset of the plan tree.
	 */
	if (estate->eliminateAliens)
	{
		Motion *m = findSenderMotion(plannedstmt, LocallyExecutingSliceIndex(estate));

		/*
		 * We may not have any motion in the current slice, e.g., in insert query
		 * the root may not have any motion.
		 */
		if (NULL != m)
		{
			start_plan_node = (Plan *) m;
			ExecSlice *sendSlice = &estate->es_sliceTable->slices[m->motionID];
			estate->currentSliceId = sendSlice->parentIndex;
		}
		/* Compute SubPlans' root plan nodes for SubPlans reachable from this plan root */
		estate->locallyExecutableSubplans = getLocallyExecutableSubplans(plannedstmt, start_plan_node);
	}
	else
		estate->locallyExecutableSubplans = NULL;

	int			subplan_id = 1;
	foreach(l, plannedstmt->subplans)
	{
		PlanState  *subplanstate = NULL;
		int			sp_eflags = 0;

		/*
		 * Initialize only the subplans that are reachable from our local slice.
		 * If alien elimination is not turned on, then all subplans are considered
		 * reachable.
		 */
		if (!estate->eliminateAliens ||
			bms_is_member(subplan_id, estate->locallyExecutableSubplans))
		{
			/*
			 * A subplan will never need to do BACKWARD scan nor MARK/RESTORE.
			 *
			 * GPDB: We always set the REWIND flag, to delay eagerfree.
			 */
			sp_eflags = eflags
				& (EXEC_FLAG_EXPLAIN_ONLY | EXEC_FLAG_WITH_NO_DATA);
			sp_eflags |= EXEC_FLAG_REWIND;

			/* set our global sliceid variable for elog. */
			int			save_currentSliceId = estate->currentSliceId;

			estate->currentSliceId = estate->es_plannedstmt->subplan_sliceIds[subplan_id - 1];

			Plan	   *subplan = (Plan *) lfirst(l);
			subplanstate = ExecInitNode(subplan, estate, sp_eflags);

			estate->currentSliceId = save_currentSliceId;
		}

		estate->es_subplanstates = lappend(estate->es_subplanstates, subplanstate);

		++subplan_id;
	}

	/*
	 * If this is a query that was dispatched from the QE, install precomputed
	 * parameter values from all init plans into our EState.
	 */
	if (Gp_role == GP_ROLE_EXECUTE && queryDesc->ddesc)
		InstallDispatchedExecParams(queryDesc->ddesc, estate);

	/*
	 * Initialize the private state information for all the nodes in the query
	 * tree.  This opens files, allocates storage and leaves us ready to start
	 * processing tuples.
	 */
	planstate = VecExecInitNode(start_plan_node, estate, eflags);

	queryDesc->planstate = planstate;

	Assert(queryDesc->planstate);

	/* GPDB hook for collecting query info */
	if (query_info_collect_hook)
		(*query_info_collect_hook)(METRICS_PLAN_NODE_INITIALIZE, queryDesc);

	if (RootSliceIndex(estate) != LocallyExecutingSliceIndex(estate))
		return;

	/*
	 * Get the tuple descriptor describing the type of tuples to return.
	 */
	tupType = ExecGetResultType(planstate);

	/*
	 * Initialize the junk filter if needed.  SELECT queries need a filter if
	 * there are any junk attrs in the top-level tlist.
	 */
	if (operation == CMD_SELECT)
	{
		bool		junk_filter_needed = false;
		ListCell   *tlist;

		foreach(tlist, plan->targetlist)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(tlist);

			if (tle->resjunk)
			{
				junk_filter_needed = true;
				break;
			}
		}

		if (junk_filter_needed)
		{
			JunkFilter *j;
			TupleTableSlot *slot;

			slot = ExecInitExtraTupleSlot(estate, NULL, &TTSOpsVecTuple);
			j = ExecInitJunkFilter(planstate->plan->targetlist,
								   slot);

			InitSlotSchema(slot);
			TTS_SET_DIRTY(slot);
			estate->es_junkFilter = j;

			/* Want to return the cleaned tuple type */
			tupType = j->jf_cleanTupType;
		}
	}

	queryDesc->tupDesc = tupType;

	/*
	 * GPDB: Hack for CTAS/MatView:
	 *   Need to switch to IntoRelDest for CTAS.
	 *   Also need to create tables in advance.
	 */
	if (queryDesc->plannedstmt->intoClause != NULL)
		intorel_initplan(queryDesc, eflags);
	else if(queryDesc->plannedstmt->copyIntoClause != NULL)
	{
		queryDesc->dest = CreateCopyDestReceiver();
		((DR_copy*)queryDesc->dest)->queryDesc = queryDesc;
	}
	else if (queryDesc->plannedstmt->refreshClause != NULL && Gp_role == GP_ROLE_EXECUTE)
		transientrel_init(queryDesc);
	if (DEBUG1 >= log_min_messages)
			{
				char		msec_str[32];
				switch (check_log_duration(msec_str, false))
				{
					case 1:
					case 2:
						ereport(LOG, (errmsg("duration to InitPlan end: %s ms", msec_str)));
						break;
				}
			}
}

/*
 * Check that a proposed rowmark target relation is a legal target
 *
 * In most cases parser and/or planner should have noticed this already, but
 * they don't cover all cases.
 */
static void
CheckValidRowMarkRel(Relation rel, RowMarkType markType)
{
	FdwRoutine *fdwroutine;

	switch (rel->rd_rel->relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_PARTITIONED_TABLE:
			/* OK */
			break;
		case RELKIND_SEQUENCE:
			/* Must disallow this because we don't vacuum sequences */
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot lock rows in sequence \"%s\"",
							RelationGetRelationName(rel))));
			break;
		case RELKIND_TOASTVALUE:
			/* We could allow this, but there seems no good reason to */
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot lock rows in TOAST relation \"%s\"",
							RelationGetRelationName(rel))));
			break;
		case RELKIND_VIEW:
			/* Should not get here; planner should have expanded the view */
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot lock rows in view \"%s\"",
							RelationGetRelationName(rel))));
			break;
		case RELKIND_MATVIEW:
			/* Allow referencing a matview, but not actual locking clauses */
			if (markType != ROW_MARK_REFERENCE)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("cannot lock rows in materialized view \"%s\"",
								RelationGetRelationName(rel))));
			break;
		case RELKIND_FOREIGN_TABLE:
			/* Okay only if the FDW supports it */
			fdwroutine = GetFdwRoutineForRelation(rel, false);
			if (fdwroutine->RefetchForeignRow == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot lock rows in foreign table \"%s\"",
								RelationGetRelationName(rel))));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot lock rows in relation \"%s\"",
							RelationGetRelationName(rel))));
			break;
	}
}

/* ------------------------------------------------------------------------
 *		VecExecInitNode
 *
 *		Recursively initializes all the nodes in the plan tree rooted
 *		at 'node'.
 *
 *		Inputs:
 *		  'node' is the current node of the plan produced by the query planner
 *		  'estate' is the shared execution state for the plan tree
 *		  'eflags' is a bitwise OR of flag bits described in executor.h
 *
 *		Returns a PlanState node corresponding to the given Plan node.
 * ------------------------------------------------------------------------
 */
PlanState *
VecExecInitNode(Plan *node, EState *estate, int eflags)
{
	PlanState  *result;
	List	   *subps;
	ListCell   *l;
	MemoryContext nodecxt = NULL;
	MemoryContext oldcxt = NULL;

	/*
	 * do nothing when we get to the end of a leaf on tree.
	 */
	if (node == NULL)
		return NULL;

	if (!(eflags & EXEC_FLAG_VECTOR))
	{
		return VecExecInitNode(node, estate, eflags);
	}

	/*
	 * Make sure there's enough stack available. Need to check here, in
	 * addition to ExecProcNode() (via ExecProcNodeFirst()), to ensure the
	 * stack isn't overrun while initializing the node tree.
	 */
	check_stack_depth();

	/*
	 * If per-node memory usage was requested
	 * (explain_memory_verbosity=detail), create a separate memory context
	 * for every node, so that we can attribute memory usage to each node.
	 * Otherwise, everything is allocated in the per-query ExecutorState
	 * context. The extra memory contexts consume some memory on their
	 * own, and prevent reusing memory allocated in one node in another
	 * node, so we only want to do this if the level of detail is needed.
	 */
	if ((estate->es_instrument & INSTRUMENT_MEMORY_DETAIL) != 0)
	{
		nodecxt = AllocSetContextCreate(CurrentMemoryContext,
										"executor node",
										ALLOCSET_SMALL_SIZES);
		MemoryContextDeclareAccountingRoot(nodecxt);
		oldcxt = MemoryContextSwitchTo(nodecxt);
	}

	switch (nodeTag(node))
	{
			/*
			 * control nodes
			 */
		case T_Result:
			result = (PlanState *) ExecInitVecResult((Result *)node,
													 estate, eflags);
			break;

		case T_Append:
			result = (PlanState *)ExecInitVecAppend((Append *)node,
													estate, eflags);
			break;

		case T_Sequence:
			result = (PlanState *) ExecInitVecSequence((Sequence *)node,
													  estate, eflags);
			break;

			/*
			 * scan nodes
			 */
		case T_SeqScan:
			result = (PlanState *)ExecInitVecSeqScan((SeqScan *)node,
													 estate, eflags);
			break;

		case T_SubqueryScan:
			result = (PlanState *)ExecInitVecSubqueryScan((SubqueryScan *)node,
														  estate, eflags);
			break;

		case T_ForeignScan:
			result = (PlanState *)ExecInitVecForeignScan((ForeignScan *)node,
														 estate, eflags);
			break;

			/*
			 * join nodes
			 */
		case T_NestLoop:
			result = (PlanState *) ExecInitVecNestLoop((NestLoop *)node,
													   estate, eflags);
			break;

		case T_HashJoin:
			result = (PlanState *)ExecInitVecHashJoin((HashJoin *)node,
													  estate, eflags);
			break;

			/*
			 * materialization nodes
			 */
		case T_Material:
			result = (PlanState *) ExecInitVecMaterial((Material *) node,
													   estate, eflags);
			break;

		case T_Sort:
			result = (PlanState *)ExecInitVecSort((Sort *)node,
												  estate, eflags);
			break;

		case T_Agg:
			result = (PlanState *)ExecInitVecAgg((Agg *)node,
												 estate, eflags);
			break;

		case T_WindowAgg:
			result = (PlanState *)ExecInitVecWindowAgg((WindowAgg *)node,
													   estate, eflags);
			break;

		case T_Hash:
			result = (PlanState *) ExecInitVecHash((Hash *)node,
												   estate, eflags);
			break;

		case T_Limit:
			result = (PlanState *) ExecInitVecLimit((Limit *) node,
												 estate, eflags);
			break;

		case T_ShareInputScan:
			result = (PlanState *) ExecInitVecShareInputScan((ShareInputScan *) node,
															 estate, eflags);
			break;

		case T_AssertOp:
			result = (PlanState *) ExecInitVecAssertOp((AssertOp *) node,
													   estate, eflags);
			break;

		case T_Motion:
			result = (PlanState *)ExecInitVecMotion((Motion *)node,
													estate, eflags);
			break;

		default:
			elog(ERROR, "unrecognized vector node type: %d", (int) nodeTag(node));
			result = NULL;		/* keep compiler quiet */
			break;
	}

	VecExecSetExecProcNode(result, result->ExecProcNode);

	if ((estate->es_instrument & INSTRUMENT_MEMORY_DETAIL) != 0)
	{
		Assert(CurrentMemoryContext == nodecxt);
		result->node_context = nodecxt;
		MemoryContextSwitchTo(oldcxt);
	}

	/*
	 * Initialize any initPlans present in this node.  The planner put them in
	 * a separate list for us.
	 */
	subps = NIL;
	foreach(l, node->initPlan)
	{
		SubPlan    *subplan = (SubPlan *) lfirst(l);
		SubPlanState *sstate;
		int			origSliceId = estate->currentSliceId;

		Assert(IsA(subplan, SubPlan));

		estate->currentSliceId = estate->es_plannedstmt->subplan_sliceIds[subplan->plan_id - 1];

		sstate = ExecInitSubPlan(subplan, result);
		subps = lappend(subps, sstate);

		estate->currentSliceId = origSliceId;
	}
	if (result != NULL)
		result->initPlan = subps;

	/* Set up instrumentation for this node if requested */
	if (estate->es_instrument && result != NULL)
		result->instrument = GpInstrAlloc(node, estate->es_instrument,
										  result->async_capable);

	return result;
}


void
VecExecutorEnd(QueryDesc *queryDesc)
{
	EState	   *estate;
	MemoryContext oldcontext;
	
	/* GPDB: whether this is a inner query for extension usage */
	bool		isInnerQuery;

	/* sanity checks */
	Assert(queryDesc != NULL);

	estate = queryDesc->estate;

	Assert(estate != NULL);

	/* GPDB: Save SPI flag first in case the memory context of plannedstmt is cleaned up*/
	isInnerQuery = estate->es_plannedstmt->metricsQueryType > TOP_LEVEL_QUERY;

	if (DEBUG1 >= log_min_messages)
	{
		char		msec_str[32];
		switch (check_log_duration(msec_str, false))
		{
			case 1:
			case 2:
				ereport(LOG, (errmsg("duration to ExecutorEnd starting: %s ms", msec_str)));
				break;
		}
	}

	/*
	 * Check that ExecutorFinish was called, unless in EXPLAIN-only mode. This
	 * Assert is needed because ExecutorFinish is new as of 9.1, and callers
	 * might forget to call it.
	 */
	Assert(estate->es_finished ||
		   (estate->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY));

	/*
	 * Switch into per-query memory context to run ExecEndPlan
	 */
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	/*
	 * If EXPLAIN ANALYZE, qExec returns stats to qDisp now.
	 */
	if (estate->es_sliceTable &&
		estate->es_sliceTable->instrument_options &&
		(estate->es_sliceTable->instrument_options & INSTRUMENT_CDB) &&
		Gp_role == GP_ROLE_EXECUTE)
		cdbexplain_sendExecStats(queryDesc);

	/*
	 * if needed, collect mpp dispatch results and tear down
	 * all mpp specific resources (e.g. interconnect).
	 */
	PG_TRY();
	{
		mppExecutorFinishup(queryDesc);
	}
	PG_CATCH();
	{
		/*
		 * we got an error. do all the necessary cleanup.
		 */
		mppExecutorCleanup(queryDesc);

		/*
		 * Remove our own query's motion layer.
		 */
		RemoveMotionLayer(estate->motionlayer_context);

		/*
		 * GPDB specific
		 * Clean the special resources created by INITPLAN.
		 * The resources have long life cycle and are used by the main plan.
		 * It's too early to clean them in preprocess_initplans.
		 */
		if (list_length(queryDesc->plannedstmt->paramExecTypes) > 0)
		{
			postprocess_initplans(queryDesc);
		}

		/*
		 * Release EState and per-query memory context.
		 */
		FreeExecutorState(estate);

		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * GPDB specific
	 * Clean the special resources created by INITPLAN.
	 * The resources have long life cycle and are used by the main plan.
	 * It's too early to clean them in preprocess_initplans.
	 */
	if (list_length(queryDesc->plannedstmt->paramExecTypes) > 0)
	{
		postprocess_initplans(queryDesc);
	}

	/*
	 * If normal termination, let each operator clean itself up.
	 * Otherwise don't risk it... an error might have left some
	 * structures in an inconsistent state.
	 */
	ExecEndPlan(queryDesc->planstate, estate);

	/*
	 * Remove our own query's motion layer.
	 */
	RemoveMotionLayer(estate->motionlayer_context);

	/* do away with our snapshots */
	UnregisterSnapshot(estate->es_snapshot);
	UnregisterSnapshot(estate->es_crosscheck_snapshot);

	/*
	 * Must switch out of context before destroying it
	 */
	MemoryContextSwitchTo(oldcontext);

	queryDesc->es_processed = estate->es_processed;

	/*
	 * Release EState and per-query memory context.  This should release
	 * everything the executor has allocated.
	 */
	FreeExecutorState(estate);
	
	/* GPDB hook for collecting query info */
	if (query_info_collect_hook)
		(*query_info_collect_hook)(isInnerQuery ? METRICS_INNER_QUERY_DONE : METRICS_QUERY_DONE, queryDesc);

	/* Reset queryDesc fields that no longer point to anything */
	queryDesc->tupDesc = NULL;
	queryDesc->estate = NULL;
	queryDesc->planstate = NULL;
	queryDesc->totaltime = NULL;

	if (DEBUG1 >= log_min_messages)
	{
		char		msec_str[32];
		switch (check_log_duration(msec_str, false))
		{
			case 1:
			case 2:
				ereport(LOG, (errmsg("duration to ExecutorEnd end: %s ms", msec_str)));
				break;
		}
	}

	ReportOOMConsumption();
}

/* ----------------------------------------------------------------
 *		ExecEndPlan
 *
 *		Cleans up the query plan -- closes files and frees up storage
 *
 * NOTE: we are no longer very worried about freeing storage per se
 * in this code; FreeExecutorState should be guaranteed to release all
 * memory that needs to be released.  What we are worried about doing
 * is closing relations and dropping buffer pins.  Thus, for example,
 * tuple tables must be cleared or dropped to ensure pins are released.
 * ----------------------------------------------------------------
 */
static void
ExecEndPlan(PlanState *planstate, EState *estate)
{
	ListCell   *l;

	/*
	 * shut down the node-type-specific query processing
	 */
	VecExecEndNode(planstate);

	/*
	 * for subplans too
	 */
	foreach(l, estate->es_subplanstates)
	{
		PlanState  *subplanstate = (PlanState *) lfirst(l);

		ExecEndNode(subplanstate);
	}

	/*
	 * destroy the executor's tuple table.  Actually we only care about
	 * releasing buffer pins and tupdesc refcounts; there's no need to pfree
	 * the TupleTableSlots, since the containing memory context is about to go
	 * away anyway.
	 */
	ExecResetTupleTable(estate->es_tupleTable, false);

	/* Adjust INSERT/UPDATE/DELETE count for replicated table ON QD */
	AdjustReplicatedTableCounts(estate);

	/*
	 * Close any Relations that have been opened for range table entries or
	 * result relations.
	 */
	ExecCloseResultRelations(estate);
	ExecCloseRangeTableRelations(estate);
}


/*
 * Adjust INSERT/UPDATE/DELETE count for replicated table ON QD
 */
static void
AdjustReplicatedTableCounts(EState *estate)
{
	ResultRelInfo *resultRelInfo;
	ListCell   *l;
	bool containReplicatedTable = false;
	int			numsegments =  1;

	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	/* check if result_relations contain replicated table*/
	foreach(l, estate->es_opened_result_relations)
	{
		resultRelInfo = lfirst(l);
		if (!resultRelInfo->ri_RelationDesc->rd_cdbpolicy)
			continue;

		if (GpPolicyIsReplicated(resultRelInfo->ri_RelationDesc->rd_cdbpolicy))
		{
			containReplicatedTable = true;
			numsegments = resultRelInfo->ri_RelationDesc->rd_cdbpolicy->numsegments;
		}
		else if (containReplicatedTable)
		{
			/*
			 * If one is replicated table, error if other tables are not
			 * replicated table.
 			 */
			elog(ERROR, "mix of replicated and non-replicated tables in result_relation is not supported");
		}
	}

	if (containReplicatedTable)
		estate->es_processed = estate->es_processed / numsegments;
}


/* ----------------------------------------------------------------
 *		VecExecEndNode
 *
 *		Recursively cleans up all the nodes in the plan rooted
 *		at 'node'.
 *
 *		After this operation, the query plan will not be able to be
 *		processed any further.  This should be called only after
 *		the query plan has been fully executed.
 * ----------------------------------------------------------------
 */
void
VecExecEndNode(PlanState *node)
{
	/*
	 * do nothing when we get to the end of a leaf on tree.
	 */
	if (node == NULL)
		return;

	/*
	 * Make sure there's enough stack available. Need to check here, in
	 * addition to ExecProcNode() (via ExecProcNodeFirst()), because it's not
	 * guaranteed that ExecProcNode() is reached for all nodes.
	 */
	check_stack_depth();

	if (node->chgParam != NULL)
	{
		bms_free(node->chgParam);
		node->chgParam = NULL;
	}

	/* Free EXPLAIN ANALYZE buffer */
	if (node->cdbexplainbuf)
	{
		if (node->cdbexplainbuf->data)
			pfree(node->cdbexplainbuf->data);
		pfree(node->cdbexplainbuf);
		node->cdbexplainbuf = NULL;
	}

	switch (nodeTag(node))
	{
			/*
			 * control nodes
			 */
		case T_ResultState:
			ExecEndVecResult((ResultState *)node);
			break;

		case T_AppendState:
			ExecEndVecAppend((AppendState *) node);
			break;

		case T_SequenceState:
			ExecEndVecSequence((SequenceState *) node);
			break;

			/*
			 * scan nodes
			 */
		case T_SeqScanState:
			ExecEndVecSeqScan((VecSeqScanState *)node);
			break;

		case T_SubqueryScanState:
			ExecEndVecSubqueryScan((SubqueryScanState *)node);
			break;

		case T_ForeignScanState:
			ExecEndVecForeignScan((VecForeignScanState *) node);
			break;

			/*
			 * join nodes
			 */
		case T_NestLoopState:
			ExecEndVecNestLoop((NestLoopState *) node);
			break;

		case T_HashJoinState:
			ExecEndVecHashJoin((HashJoinState *)node);
			break;

			/*
			 * ShareInput nodes
			 */
		case T_ShareInputScanState:
			ExecEndVecShareInputScan((ShareInputScanState *) node);
			break;

			/*
			 * materialization nodes
			 */
		case T_MaterialState:
			ExecEndVecMaterial((MaterialState *) node);
			break;

		case T_SortState:
			ExecEndVecSort((SortState *) node);
			break;

		case T_AggState:
			ExecEndVecAgg((AggState *)node);
			break;

		case T_WindowAggState:
			ExecEndVecWindowAgg((WindowAggState *)node);
			break;

		case T_HashState:
			ExecEndVecHash((HashState *) node);
			break;

		case T_LimitState:
			ExecEndVecLimit((LimitState *) node);
			break;

		case T_MotionState:
			ExecEndVecMotion((MotionState *) node);
			break;

			/*
			 * DML nodes
			 */
		case T_AssertOpState:
			ExecEndVecAssertOp((AssertOpState *) node);
			break;

		default:
			elog(ERROR, "unrecognized vector node type: %d", (int) nodeTag(node));
			break;
	}
	/* GPDB hook for collecting query info */
	if (query_info_collect_hook)
		(*query_info_collect_hook)(METRICS_PLAN_NODE_FINISHED, node);
}

/*
 * Check that the query does not imply any writes to non-temp tables;
 * unless we're in parallel mode, in which case don't even allow writes
 * to temp tables.
 *
 * This function is used to check if the current statement will perform any writes.
 * It is used to enforce:
 *  (1) read-only mode (both fts and transaction isolation level read only)
 *      as well as
 *  (2) to keep track of when a distributed transaction becomes
 *      "dirty" and will require 2pc.
 *
 * Note: in a Hot Standby this would need to reject writes to temp
 * tables just as we do in parallel mode; but an HS standby can't have created
 * any temp tables in the first place, so no need to check that.
 *
 * In GPDB, an important side-effect of this is to call
 * ExecutorMarkTransactionDoesWrites(), if the query is not read-only. That
 * ensures that we use two-phase commit for this transaction.
 */
static void
ExecCheckXactReadOnly(PlannedStmt *plannedstmt)
{
	ListCell   *l;
	int         rti;

	/*
	 * CREATE TABLE AS or SELECT INTO?
	 *
	 * XXX should we allow this if the destination is temp?  Considering that
	 * it would still require catalog changes, probably not.
	 */
	if (plannedstmt->intoClause != NULL)
	{
		Assert(plannedstmt->intoClause->rel);
		if (plannedstmt->intoClause->rel->relpersistence == RELPERSISTENCE_TEMP)
			ExecutorMarkTransactionDoesWrites();
		else
			PreventCommandIfReadOnly(CreateCommandName((Node *) plannedstmt));
	}

	/*
	 * Refresh matview will write xlog.
	 */
	if (plannedstmt->refreshClause != NULL)
	{
		PreventCommandIfReadOnly(CreateCommandName((Node *) plannedstmt));
	}

    rti = 0;
	/*
	 * Fail if write permissions are requested in parallel mode for table
	 * (temp or non-temp), otherwise fail for any non-temp table.
	 */
	foreach(l, plannedstmt->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(l);

		rti++;

		if (rte->rtekind != RTE_RELATION)
			continue;

		if ((rte->requiredPerms & (~ACL_SELECT)) == 0)
			continue;

		/*
		 * External and foreign tables don't need two phase commit which is for
		 * local mpp tables
		 */
		if (get_rel_relkind(rte->relid) == RELKIND_FOREIGN_TABLE)
			continue;

		if (isTempNamespace(get_rel_namespace(rte->relid)))
		{
			ExecutorMarkTransactionDoesWrites();
			continue;
		}

        /* CDB: Allow SELECT FOR SHARE/UPDATE *
         *
         */
        if ((rte->requiredPerms & ~(ACL_SELECT | ACL_SELECT_FOR_UPDATE)) == 0)
        {
        	ListCell   *cell;
        	bool foundRTI = false;

        	foreach(cell, plannedstmt->rowMarks)
			{
				RowMarkClause *rmc = lfirst(cell);
				if( rmc->rti == rti )
				{
					foundRTI = true;
					break;
				}
			}

			if (foundRTI)
				continue;
        }

		PreventCommandIfReadOnly(CreateCommandName((Node *) plannedstmt));
	}

	if (plannedstmt->commandType != CMD_SELECT || plannedstmt->hasModifyingCTE)
		PreventCommandIfParallelMode(CreateCommandName((Node *) plannedstmt));
}

bool find_extension_context(List *context) 
{
	ListCell *cell;
	foreach(cell, context) 
	{
		ExtensibleNode *e = lfirst(cell);
		if (strcmp(e->extnodename, VECTOR_EXTENSION_CONTEXT) == 0)
			return true;
	}
	return false;
}

static void
copyVectorExtensionContext(struct ExtensibleNode *newnode,
			  const struct ExtensibleNode *oldnode) {
  return;
}

static bool
equalVectorExtensionContext(const struct ExtensibleNode *a,
			   const struct ExtensibleNode *b) {
  return false;
}

static void
outVectorExtensionContext(struct StringInfoData *str,
			 const struct ExtensibleNode *enode) {
  return;
}

static void
readVectorExtensionContext(struct ExtensibleNode *node) {
  return;
}

static const ExtensibleNodeMethods methods[] =
{
    {
      .extnodename = VECTOR_EXTENSION_CONTEXT,
      .node_size = sizeof(VectorExtensionContext),
      .nodeCopy = copyVectorExtensionContext,
      .nodeEqual = equalVectorExtensionContext,
      .nodeOut = outVectorExtensionContext,
      .nodeRead = readVectorExtensionContext,
    }
};

void
RegisterVectorExtensibleNode(void) {
    long unsigned int i;
    for (i = 0; i < lengthof(methods); i++)
        RegisterExtensibleNodeMethods(&methods[i]);
}
