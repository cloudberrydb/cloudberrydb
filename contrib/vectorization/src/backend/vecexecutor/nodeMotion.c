#include "postgres.h"
#include "nodes/plannodes.h"
#include "cdb/cdbhash.h"
#include "cdb/veccdbhash.h"
#include "vecexecutor/execnodes.h"
#include "vecexecutor/executor.h"
#include "vecexecutor/nodeMotion.h"
#include "vecexecutor/execslot.h"
#include "cdb/vecmotion.h"
#include "utils/wrapper.h"
#include "vecnodes/nodes.h"
#include "utils/vecheap.h"
#include "utils/wrapper.h"
#include "catalog/pg_operator_d.h"
#include "cdb/vectupser.h"
#include "optimizer/planner_vec.h"
#include "utils/fmgr_vec.h"
#include "executor/tuptable.h"
#include "vecexecutor/execAmi.h"

/*
 * FUNCTIONS PROTOTYPES
 */
static TupleTableSlot *execVecMotionSender(MotionState *node);
static TupleTableSlot *execVecMotionUnsortedReceiver(MotionState *node);
static TupleTableSlot *execVecMotionSortedReceiver(MotionState *node);
static void execVecMotionSortedReceiverFirstTime(MotionState *node);
static void doSendTupleVec(Motion *motion, MotionState *node, TupleTableSlot *outerTupleSlot);
static void doSendEndOfStreamVec(Motion *motion, MotionState *node);
static inline void setKey(MotionState *node, int route, int offset);
static inline bool fetchBatch(MotionState *node, int route, int offset);
static List* ConvertHasExprsToGandivaNodes(List *hashExpr, GArrowSchema *schema);
static Datum
cdbhashrandomseg_vec(int numsegs, int size, MotionState *node);
static bool IsMatchStrategy(int rows);
static GArrowRecordBatch *ConcatenateBatches(MotionState *node, int route);

static void *
get_segment_recordbatch_take(void *org_recordbatch, Datum filter)
{
	g_autoptr(GArrowRecordBatch)  rrb = NULL;
	g_autoptr(GArrowArray) map_array = garrow_datum_get_array(
			GARROW_ARRAY_DATUM(DatumGetPointer(filter)));
	g_autoptr(GError) error = NULL;

	rrb = garrow_record_batch_take_group(GARROW_RECORD_BATCH(org_recordbatch), map_array, NULL, &error);

	if (error != NULL)
	{
		elog(ERROR, "%s take recordbatch fail caused by %s",
				 __FUNCTION__, error->message);
	}

	 return (void*)garrow_move_ptr(rrb);
}

static void
group_hashkeys_by_seg(Datum hashed_segs, int *groupStart, int *groupSize, int rows, int segnums)
{
	gint64 len = 0;
	int pre_total = 0;
	g_autoptr(GArrowArray) hashed_segs_array = NULL;
	if (!hashed_segs)
		return ;

	hashed_segs_array = garrow_datum_get_array(GARROW_ARRAY_DATUM(DatumGetPointer(hashed_segs)));
	const gint32 *segs_buffer = garrow_int32_array_get_values(GARROW_INT32_ARRAY(hashed_segs_array), &len);
	for (int i = 0; i < len; i++)
	{
		int group = segs_buffer[i];
		groupSize[group]++;
	}
	for (int i = 0; i < segnums; i++)
	{
		int group = i;
		if (groupStart[group] == -1 && groupSize[group] != 0)
		{
			groupStart[group] = pre_total;
			pre_total += groupSize[group];
		}
	}
}


static List*
ConvertHasExprsToGandivaNodes(List *hashExpr, GArrowSchema *schema)
{
	List       *lst_gandiva_nodes = NIL;
	ListCell   *lc = NULL;

	foreach (lc, hashExpr)
	{
		g_autoptr(GArrowDataType) grt = NULL;
		GGandivaNode *node = NULL;

		ExprState  *hash_expr_state = (ExprState *) lfirst(lc);
		node = ExprToNode(hash_expr_state, NULL, schema, &grt);
		lst_gandiva_nodes = lappend(lst_gandiva_nodes, node);
	}

	return lst_gandiva_nodes;
}

static void
init_hash_projector(MotionState *node, GArrowSchema *schema)
{
	VecMotionState *vnode = (VecMotionState *)node;
	ListCell *lc = NULL;
	g_autoptr(GGandivaNode) pre_node = NULL;
	foreach (lc, vnode->hashExprsGandivaNodes)
	{
		GGandivaNode *node_tmp = lfirst(lc);
		if (!pre_node)
		{
			GGandivaNode *node = make_hash32_fnode(node_tmp);
			garrow_store_func(pre_node, node);
		}
		else
		{
			GGandivaNode *node = make_cdbhash32_fnode(node_tmp, pre_node);
			garrow_store_func(pre_node, node);
		}
	}

	if (pre_node)
	{
		GError *error = NULL;

		g_autoptr(GArrowInt32DataType) dt_int32 = garrow_int32_data_type_new();
		g_autoptr(GArrowField) retfield = garrow_field_new("hash_result", GARROW_DATA_TYPE(dt_int32));

		g_autoptr(GGandivaExpression) expr = ggandiva_expression_new(pre_node, retfield);

		/* projector */
		GList *exprs = garrow_list_append_ptr(NULL, expr);

		g_autoptr(GGandivaProjector) projector = ggandiva_projector_new(schema, exprs, &error);

		if (error)
			elog(ERROR, "Failed to initialize hash projector, cause: %s", error->message);

		garrow_store_ptr(vnode->hash_projector, projector);

		garrow_list_free_ptr(&exprs);
	}
}

static GArrowSchema*
motion_rewrite_numeric_type_schema(MotionState *node, TupleTableSlot *outerTupleSlot)
{
	g_autoptr(GArrowSchema) schema = NULL;
	ListCell   *lc = NULL;
	schema = garrow_record_batch_get_schema(GARROW_RECORD_BATCH(VECSLOT(outerTupleSlot)->tts_recordbatch));
	foreach (lc, node->hashExprs)
	{
		ExprState  *hash_expr_state = (ExprState *) lfirst(lc);
		Expr *expr = hash_expr_state->expr;
		switch (nodeTag(expr))
		{
			case T_Var:
			{
				g_autoptr(GArrowField) field = NULL;
				Var *variable = (Var *) expr;
				field = garrow_schema_get_field(schema, variable->varattno - 1);
				if(variable->vartype == NUMERICOID) 
				{
					
					g_autoptr(GError) error = NULL;
					g_autoptr(GArrowDataType) new_type = GARROW_DATA_TYPE(garrow_decimal128_data_type_new(35, 0));
					garrow_store_func(field, garrow_field_new(garrow_field_get_name(field), new_type));
					garrow_store_func(schema, garrow_schema_replace_field(schema, variable->varattno - 1, field, &error));
				}
				break;
			}
			default:
				break;
		}
	}
	return garrow_move_ptr(schema);
}

static void
hashAndSendVec_vechash(Motion *motion, MotionState *node, TupleTableSlot *outerTupleSlot)
{
	VecMotionState *vnode = (VecMotionState *)node;
	VecCdbHash *cdbhash = (VecCdbHash *) node->cdbhash;
	Datum hval = (Datum)0;
	int16 targetRoute = 0;
	int rows = 0;

	ExprContext *econtext = node->ps.ps_ExprContext;

	if (motion->motionType != MOTIONTYPE_HASH)
		elog(ERROR, "invalid motion type, MOTIONTYPE_HASH needed!");

	rows = GetNumRows(outerTupleSlot);

	/* check whether need to redistribute */
	if (outerTupleSlot->tts_tupleDescriptor->natts <= 0
		|| (rows <= 0))
		return;
	econtext->ecxt_outertuple = outerTupleSlot;
	g_autoptr(GArrowSchema) rewrite_schema = motion_rewrite_numeric_type_schema(node, outerTupleSlot);
	g_autoptr(GArrowRecordBatch) rewrite_batch = garrow_record_batch_copy_with_schema(VECSLOT(outerTupleSlot)->tts_recordbatch, rewrite_schema);
	if (!vnode->hash_projector) 
	{
		vnode->hashExprsGandivaNodes = ConvertHasExprsToGandivaNodes(node->hashExprs, rewrite_schema);
		init_hash_projector(node, rewrite_schema);
	}
	if (vnode->hash_projector != NULL)
		hval = evalHashKeyVec(vnode->hash_projector, rewrite_batch, cdbhash, rows);
	else
		hval = cdbhashrandomseg_vec(cdbhash->base.numsegs, rows, node);
	int* groupStart = (int*)palloc(cdbhash->base.numsegs * sizeof(int));
	int* groupSize = (int*)palloc(cdbhash->base.numsegs * sizeof(int));

	memset(groupStart, -1, cdbhash->base.numsegs * sizeof(int));
	memset(groupSize, 0, cdbhash->base.numsegs * sizeof(int));
	group_hashkeys_by_seg(hval, groupStart, groupSize, rows, cdbhash->base.numsegs);

	void  *group_recordbatch = get_segment_recordbatch_take(VECSLOT(outerTupleSlot)->tts_recordbatch, hval);

	for (int seg = 0; seg < cdbhash->base.numsegs; seg++)
	{
		if (groupStart[seg] != -1)
		{
			g_autoptr(GArrowRecordBatch) sub_recordbatch = garrow_record_batch_slice(group_recordbatch, groupStart[seg], groupSize[seg]);
			ExecStoreBatch(vnode->vecslot, sub_recordbatch);
			int rows = GetNumRows(vnode->vecslot);
			if (rows > 0)
			{
				targetRoute = seg;
				Assert(targetRoute != BROADCAST_SEGIDX);

				SendReturnCode sendRC;
				sendRC = SendTupleVec(node->ps.state->motionlayer_context,
								   node->ps.state->interconnect_context,
								   motion->motionID,
								   vnode->vecslot,
								   targetRoute);

				Assert(sendRC == SEND_COMPLETE || sendRC == STOP_SENDING);
				if (sendRC == SEND_COMPLETE)
					node->numTuplesToAMS += rows;
				else
					node->stopRequested = true;
			}
		}
	}

	free_array_datum((void**)&hval);
	free_batch(&group_recordbatch);
	pfree(groupStart);
	pfree(groupSize);
}


static void
init_random_const_template(uint32 segnums, int rows, MotionState *node)
{
	g_autoptr(GArrowUInt32ArrayBuilder) uint32_array_builder = NULL;
	VecMotionState *vmotionstate = (VecMotionState *)node;

	GError *error = NULL;
	uint32 i = 0;
	uint32 val = 0;
	int64  len = 0;
	bool   bCreate = true;

	uint32_array_builder = garrow_uint32_array_builder_new();

	if (vmotionstate->random_const_array_template != NULL)
	{
		len = garrow_array_get_length(GARROW_ARRAY(vmotionstate->random_const_array_template));

		/* max_batch_size has not been changed */
		if (len == rows)
		{
			bCreate = false;
		}
	}

	if (bCreate)
	{
		free_array((void**)&vmotionstate->random_const_array_template);

		for (i = 0; i < rows; i++)
		{
			val = i % segnums;
			garrow_uint32_array_builder_append_value(uint32_array_builder,
													val,
													&error);

			if (error)
			{
				garrow_array_builder_reset(GARROW_ARRAY_BUILDER(uint32_array_builder));
				elog(ERROR, "%s append value for random const template array fail caused by %s",
						__FUNCTION__, error->message);
			}
		}

		vmotionstate->random_const_array_template = GARROW_UINT32_ARRAY(
				garrow_array_builder_finish(
				GARROW_ARRAY_BUILDER(uint32_array_builder), &error));

		if (error)
		{
			garrow_array_builder_reset(GARROW_ARRAY_BUILDER(uint32_array_builder));
			elog(ERROR, "%s finish value for random const template array fail caused by %s",
					__FUNCTION__, error->message);
		}
	}

	return;
}

static uint32
get_seg_andop_value(uint32 segnums)
{
	/* hash logical part max value is 1024 */
	uint32 mask = 0x00000400;
	uint32 most_non_zero_bit = 10;
	uint32 ret_init_val = 0x00000001;
	uint32 ret = 0;

	Assert(segnums > 0);

	/* get the most non-zero bit */
	while((mask & segnums) == 0)
	{
		most_non_zero_bit--;
		mask = mask >> 1;
	}

	ret = (ret_init_val << (most_non_zero_bit + 1)) - 1;

	return ret;
}


/*
 * Return array of random segment number, for randomly distributed policy.
 * 
 * 1. get the array that contains a uniform distribution of segment number
 * 2. get a random value and all elts of the array mentioned by step 1 are added to this random number
 * 3. get the mask value (arrow lib not provide mod algorithm)
 * 4. all elts of the array generated by step 2 bit-and the mask
 * 5. get the offset value, (the mask value may enlarge segment number range)
 * 6. part elts of the array generated by step 4 which greate-equal segment number are subtraced the offset value
 */
Datum
cdbhashrandomseg_vec(int numsegs, int size, MotionState *node)
{
	g_autoptr(GArrowUInt32Scalar) uint32_random_scalar = NULL;
	g_autoptr(GArrowScalarDatum)  uint32_random_scalar_datum = NULL;
	g_autoptr(GArrowUInt32Scalar) uint32_and_val_scalar = NULL;
	g_autoptr(GArrowScalarDatum)  uint32_and_val_scalar_datum = NULL;
	g_autoptr(GArrowUInt32Scalar) uint32_delta_val_scalar = NULL;
	g_autoptr(GArrowScalarDatum)  uint32_delta_val_scalar_datum = NULL;
	g_autoptr(GArrowUInt32Scalar) uint32_seg_val_scalar = NULL;
	g_autoptr(GArrowScalarDatum)  uint32_seg_val_scalar_datum = NULL;
	g_autoptr(GArrowArrayDatum)   uint32_bool_array_datum = NULL;
	g_autoptr(GArrowArrayDatum)   uint32_res_array_datum = NULL;
	g_autoptr(GArrowArrayDatum)   uint32_random_template_array_datum = NULL;
	g_autoptr(GArrowArrayDatum)   uint32_subtract_array_datum = NULL;
	g_autoptr(GArrowArray)        uint32_random_template_array_slice = NULL;
	VecMotionState *vmotionstate = (VecMotionState *)node;

	uint32 and_value = 0;
	uint32 delta = 0;
	Assert(numsegs > 0);

	/* initialize random segment array template */
	init_random_const_template(numsegs, size, node);

	/* acquire random data */
	uint32 r = random();

	/* according to segment number to calculate
 * 	 * the adn mask value */
	and_value = get_seg_andop_value(numsegs);

	/* get the compensate delta value */
	delta = and_value - numsegs + 1;

	uint32_random_template_array_slice =
		GARROW_ARRAY(garrow_copy_ptr(vmotionstate->random_const_array_template));

	/* add random */
	uint32_random_scalar = garrow_uint32_scalar_new(r);

	uint32_random_scalar_datum =
			garrow_scalar_datum_new(GARROW_SCALAR(uint32_random_scalar));

	uint32_random_template_array_datum =
			garrow_array_datum_new(uint32_random_template_array_slice);

	garrow_store_func(uint32_res_array_datum,
			DirectCallVecFunc2ArgsAndU32ArrayRes("add",
					uint32_random_template_array_datum,
					uint32_random_scalar_datum));

	/* bit-and */
	uint32_and_val_scalar = garrow_uint32_scalar_new(and_value);
	uint32_and_val_scalar_datum = garrow_scalar_datum_new(
			GARROW_SCALAR(uint32_and_val_scalar));

	garrow_store_func(uint32_res_array_datum,
			DirectCallVecFunc2ArgsAndU32ArrayRes("bit_wise_and",
					uint32_res_array_datum,
					uint32_and_val_scalar_datum));

	/* get elts which greate-equal segment number */
	uint32_seg_val_scalar = garrow_uint32_scalar_new(numsegs);
	uint32_seg_val_scalar_datum = garrow_scalar_datum_new(
			GARROW_SCALAR(uint32_seg_val_scalar));

	garrow_store_func(uint32_bool_array_datum,
			DirectCallVecFunc2Args("greater_equal",
					uint32_res_array_datum,
					uint32_seg_val_scalar_datum));

	/* get subtracted array */
	uint32_delta_val_scalar = garrow_uint32_scalar_new(delta);
	uint32_delta_val_scalar_datum = garrow_scalar_datum_new(
			GARROW_SCALAR(uint32_delta_val_scalar));

	garrow_store_func(uint32_subtract_array_datum,
			DirectCallVecFunc2ArgsAndU32ArrayRes("subtract",
					uint32_res_array_datum,
					uint32_delta_val_scalar_datum));

	/* elts which greate-equal segment number subtract delta value */
	garrow_store_func(uint32_res_array_datum,
			DirectCallVecFunc3ArgsAndU32ArrayRes("if_else",
					uint32_bool_array_datum,
					uint32_subtract_array_datum,
					uint32_res_array_datum));

	PG_VEC_RETURN_POINTER(uint32_res_array_datum);
}

/* ----------------------------------------------------------------
 * ExecVecMotion
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecVecMotion(PlanState *pstate)
{
	MotionState *node = castNode(MotionState, pstate);

	Motion	   *motion = (Motion *) node->ps.plan;

	/*
	 * Check for interrupts. Without this we've seen the scenario before that
	 * it could be quite slow to cancel a query that selects all the tuples
	 * from a big distributed table because the motion node on QD has no chance
	 * of checking the cancel signal.
	 */
	CHECK_FOR_INTERRUPTS();

	/* sanity check */
 	if (node->stopRequested)
 		ereport(ERROR,
 				(errcode(ERRCODE_INTERNAL_ERROR),
 				 errmsg("unexpected internal error"),
 				 errmsg("Already stopped motion node is executed again, data will lost"),
 				 errhint("Likely motion node is incorrectly squelched earlier")));

	/*
	 * at the top here we basically decide: -- SENDER vs. RECEIVER and --
	 * SORTED vs. UNSORTED
	 */
	if (node->mstype == MOTIONSTATE_RECV)
	{
		TupleTableSlot *tuple;
#ifdef MEASURE_MOTION_TIME
		struct timeval startTime;
		struct timeval stopTime;

		gettimeofday(&startTime, NULL);
#endif

		if (node->ps.state->active_recv_id >= 0)
		{
			if (node->ps.state->active_recv_id != motion->motionID)
			{
				/*
				 * See motion_sanity_walker() for details on how a deadlock
				 * may occur.
				 */
				elog(LOG, "DEADLOCK HAZARD: Updating active_motion_id from %d to %d",
					 node->ps.state->active_recv_id, motion->motionID);
				node->ps.state->active_recv_id = motion->motionID;
			}
		}
		else
			node->ps.state->active_recv_id = motion->motionID;

		if (motion->sendSorted) 
			tuple = execVecMotionSortedReceiver(node);
		else
			tuple = execVecMotionUnsortedReceiver(node);

		/*
		 * We tell the upper node as if this was the end of tuple stream if
		 * query-finish is requested.  Unlike other nodes, we skipped this
		 * check in ExecProc because this node in sender mode should send EoS
		 * to the receiver side, but the receiver side can simply stop
		 * processing the stream.  The sender side of this stream could still
		 * be sending more tuples, but this slice will eventually clean up the
		 * executor and eventually Stop message will be delivered to the
		 * sender side.
		 */
		if (QueryFinishPending)
			tuple = NULL;

		if (tuple == NULL)
			node->ps.state->active_recv_id = -1;
#ifdef MEASURE_MOTION_TIME
		gettimeofday(&stopTime, NULL);

		node->motionTime.tv_sec += stopTime.tv_sec - startTime.tv_sec;
		node->motionTime.tv_usec += stopTime.tv_usec - startTime.tv_usec;

		while (node->motionTime.tv_usec < 0)
		{
			node->motionTime.tv_usec += 1000000;
			node->motionTime.tv_sec--;
		}

		while (node->motionTime.tv_usec >= 1000000)
		{
			node->motionTime.tv_usec -= 1000000;
			node->motionTime.tv_sec++;
		}
#endif
		return tuple;
	}
	else if (node->mstype == MOTIONSTATE_SEND)
	{
		return execVecMotionSender(node);
	}
	else
	{
		elog(ERROR, "cannot execute inactive Motion");
		return NULL;
	}
}

/* ----------------------------------------------------------------
 *		ExecInitMotion
 *
 * NOTE: have to be a bit careful, estate->es_cur_slice_idx is not the
 *		 ultimate correct value that it should be on the QE. this happens
 *		 after this call in mppexec.c.	This is ok since we don't need it,
 *		 but just be aware before you try and use it here.
 * ----------------------------------------------------------------
 */

MotionState *
ExecInitVecMotion(Motion *node, EState *estate, int eflags)
{
	VecMotionState *vmotionstate = NULL;
	MotionState *motionstate = NULL;
	TupleDesc	tupDesc;
	ExecSlice  *sendSlice;
	ExecSlice  *recvSlice;
	SliceTable *sliceTable = estate->es_sliceTable;
	PlanState  *outerPlan;
	int			parentIndex;

	/*
	 * If GDD is enabled, the lock of table may downgrade to RowExclusiveLock,
	 * (see CdbTryOpenRelation function), then EPQ would be triggered, EPQ will
	 * execute the subplan in the executor, so it will create a new EState,
	 * but there are no slice tables in the new EState and we can not AssignGangs
	 * on the QE. In this case, we raise an error.
	 */
	if (estate->es_epq_active)
		ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 errmsg("EvalPlanQual can not handle subPlan with Motion node")));

	Assert(node->motionID > 0);
	Assert(node->motionID < sliceTable->numSlices);
	AssertImply(node->motionType == MOTIONTYPE_HASH, node->numHashSegments > 0);

	parentIndex = estate->currentSliceId;
	estate->currentSliceId = node->motionID;

	/*
	 * create state structure
	 */
	vmotionstate = (VecMotionState*) palloc0(sizeof(VecMotionState));
	vmotionstate->random_const_array_template = NULL;
	motionstate = (MotionState*) vmotionstate;
	NodeSetTag(motionstate, T_MotionState);
	motionstate->ps.plan = (Plan *) node;
	motionstate->ps.state = estate;
	motionstate->ps.ExecProcNode = ExecVecMotion;
	motionstate->mstype = MOTIONSTATE_NONE;
	motionstate->stopRequested = false;
	motionstate->hashExprs = NIL;
	motionstate->cdbhash = NULL;

	/* Look up the sending and receiving gang's slice table entries. */
	sendSlice = &sliceTable->slices[node->motionID];
	Assert(sendSlice->sliceIndex == node->motionID);
	recvSlice = &sliceTable->slices[parentIndex];
	Assert(parentIndex == sendSlice->parentIndex);

	/* QD must fill in the global slice table. */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);

		if (node->motionType == MOTIONTYPE_GATHER ||
			node->motionType == MOTIONTYPE_GATHER_SINGLE)
		{
			/* Sending to a single receiving process on the entry db? */
			/* Is receiving slice a root slice that runs here in the qDisp? */
			if (recvSlice->sliceIndex == recvSlice->rootIndex)
			{
				motionstate->mstype = MOTIONSTATE_RECV;
				/* For parallel retrieve cursor, the motion's gang type could be set as
				 * GANGTYPE_ENTRYDB_READER explicitly*/
				Assert(recvSlice->gangType == GANGTYPE_UNALLOCATED ||
					   recvSlice->gangType == GANGTYPE_ENTRYDB_READER ||
					   recvSlice->gangType == GANGTYPE_PRIMARY_WRITER ||
					   recvSlice->gangType == GANGTYPE_PRIMARY_READER);
			}
			else
			{
				/* sanity checks */
				if (list_length(recvSlice->segments) != 1)
					elog(ERROR, "unexpected gang size: %d", list_length(recvSlice->segments));
			}
		}

		MemoryContextSwitchTo(oldcxt);
	}

	/* QE must fill in map from motionID to MotionState node. */
	else
	{
		Assert(Gp_role == GP_ROLE_EXECUTE);

		if (LocallyExecutingSliceIndex(estate) == recvSlice->sliceIndex)
		{
			/* this is recv */
			motionstate->mstype = MOTIONSTATE_RECV;
		}
		else if (LocallyExecutingSliceIndex(estate) == sendSlice->sliceIndex)
		{
			/* this is send */
			motionstate->mstype = MOTIONSTATE_SEND;
		}
		/* TODO: If neither sending nor receiving, don't bother to initialize. */
	}

	motionstate->tupleheapReady = false;
	motionstate->sentEndOfStream = false;

	motionstate->otherTime.tv_sec = 0;
	motionstate->otherTime.tv_usec = 0;
	motionstate->motionTime.tv_sec = 0;
	motionstate->motionTime.tv_usec = 0;

	motionstate->numTuplesFromChild = 0;
	motionstate->numTuplesToAMS = 0;
	motionstate->numTuplesFromAMS = 0;
	motionstate->numTuplesToParent = 0;

	motionstate->stopRequested = false;
	motionstate->numInputSegs = list_length(sendSlice->segments);

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &motionstate->ps);

	/*
	 * Initializes child nodes. If alien elimination is on, we skip children
	 * of receiver motion.
	 */
	if (!estate->eliminateAliens || motionstate->mstype == MOTIONSTATE_SEND)
	{
		outerPlanState(motionstate) = VecExecInitNode(outerPlan(node), estate, eflags);
	}

	/*
	 * initialize tuple type.  no need to initialize projection info because
	 * this node doesn't do projections.
	 */
	outerPlan = outerPlanState(motionstate);

	/*
	 * Initialize result type and slot
	 */
	ExecInitResultTupleSlotTL(&motionstate->ps, &TTSOpsVecTuple);
	tupDesc = ExecGetResultType(&motionstate->ps);

	motionstate->ps.ps_ProjInfo = NULL;
	motionstate->numHashSegments = node->numHashSegments;

	/* Set up motion send data structures */
	if (motionstate->mstype == MOTIONSTATE_SEND && node->motionType == MOTIONTYPE_HASH)
	{
		int			nkeys;

		Assert(node->numHashSegments > 0);
		Assert(node->numHashSegments <= recvSlice->planNumSegments);

		nkeys = list_length(node->hashExprs);

        
		if (nkeys > 0)
		{

			motionstate->hashExprs = ExecInitExprList(node->hashExprs,
													  (PlanState *) motionstate);

		}

		/*
		 * FIXME:  ispowof2 and ispowof2_vec implementations are the same, but with different names
		 * Nothing needs to be changed at the moment
		 */
		motionstate->cdbhash = (CdbHash *) makeVecCdbHash(motionstate->numHashSegments,
										   nkeys,
										   node->hashFuncs);

	}

	/* FIXME: merge sort motion will be implement in the future */
	/*
	 * Merge Receive: Set up the key comparator and priority queue.
	 *
	 * This is very similar to a Merge Append.
	 */
	if (node->sendSorted && motionstate->mstype == MOTIONSTATE_RECV)
	{
		int			numInputSegs = motionstate->numInputSegs;
		int			lastSortColIdx = 0;

		/* Allocate array to slots for the next tuple from each sender */
		motionstate->slots = palloc0(numInputSegs * sizeof(TupleTableSlot *));

		/* Prepare SortSupport data for each column */
		motionstate->numSortCols = node->numSortCols;
		motionstate->sortKeys = (SortSupport) palloc0(node->numSortCols * sizeof(SortSupportData));

		for (int i = 0; i < node->numSortCols; i++)
		{
			SortSupport sortKey = &motionstate->sortKeys[i];

			AssertArg(node->sortColIdx[i] != 0);
			AssertArg(node->sortOperators[i] != 0);

			sortKey->ssup_cxt = CurrentMemoryContext;
			sortKey->ssup_collation = node->collations[i];
			sortKey->ssup_nulls_first = node->nullsFirst[i];
			sortKey->ssup_attno = node->sortColIdx[i];


			/* get ording asc or desc */
			PrepareSortSupportFromOrderingOp(node->sortOperators[i], sortKey);

			/* check whether exist sys column, citd etc. */
			if (tupDesc->attrs[sortKey->ssup_attno - 1].atttypid == TIDOID)
			{
				Oid op_oid = Int8LessOperator;
				if (sortKey->ssup_reverse)
					op_oid = Int8LessOperator + 1; /* int8gt */
				/* reset comparator and reassign ctid compare function. */
				sortKey->comparator = NULL;
				PrepareSortSupportFromOrderingOp(op_oid, sortKey);
			}

			/* Also make note of the last column used in the sort key */
			if (node->sortColIdx[i] > lastSortColIdx)
				lastSortColIdx = node->sortColIdx[i];
		}
		motionstate->lastSortColIdx = lastSortColIdx;
		vmotionstate->tupleheap =
			vecheap_allocate(motionstate->numInputSegs,
							 motionstate->sortKeys,
							 tupDesc->attrs,
							 node->numSortCols);
	}

	vmotionstate->transTupDesc = CreateTemplateTupleDesc(1);
	TupleDescInitEntry(vmotionstate->transTupDesc, (AttrNumber)1, "vec_batch",
					   BYTEAOID, -1, 0);
	/*
	 * Perform per-node initialization in the motion layer.
	 */
	UpdateMotionLayerNode(motionstate->ps.state->motionlayer_context,
						  node->motionID,
						  node->sendSorted,
						  vmotionstate->transTupDesc);

#ifdef CDB_MOTION_DEBUG
	motionstate->outputFunArray = (Oid *) palloc(tupDesc->natts * sizeof(Oid));
	for (int i = 0; i < tupDesc->natts; i++)
	{
		bool		typisvarlena;

		getTypeOutputInfo(tupDesc->attrs[i].atttypid,
						  &motionstate->outputFunArray[i],
						  &typisvarlena);
	}
#endif

	estate->currentSliceId = parentIndex;

    vmotionstate->offset = (int*)palloc0(sizeof(int)*1024);
	vmotionstate->memtups = (char*)palloc0(1024*1024);

	vmotionstate->rowslot = ExecInitExtraTupleSlot(estate, tupDesc, &TTSOpsMinimalTuple);
	vmotionstate->vecslot = ExecInitExtraTupleSlot(estate, tupDesc, &TTSOpsVecTuple);
	vmotionstate->hashExprsGandivaNodes = NULL;
	vmotionstate->hash_projector = NULL;

	return motionstate;
}

/* ----------------------------------------------------------------
 *		ExecEndVecMotion(node)
 * ----------------------------------------------------------------
 */
void
ExecEndVecMotion(MotionState *node)
{
	VecMotionState* vnode = (VecMotionState *) node;
	Motion	   *motion = (Motion *) node->ps.plan;
#ifdef MEASURE_MOTION_TIME
	double		otherTimeSec;
	double		motionTimeSec;
#endif

	VecTupleTableSlot *slot = (VecTupleTableSlot *) node->ps.ps_ResultTupleSlot;
	if (slot->vec_schema.schema)
		ARROW_FREE(GArrowSchema, &slot->vec_schema.schema);

	pfree(vnode->transTupDesc);

	ExecFreeExprContext(&node->ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ps.ps_ResultTupleSlot);
	/* clear the vectorized aux slot */
	ExecClearTuple(vnode->vecslot);
	ExecClearTuple(vnode->rowslot);
	/*
	 * Set the slice no for the nodes under this motion.
	 */
	Assert(node->ps.state != NULL);

	/*
	 * shut down the subplan
	 */
	VecExecEndNode(outerPlanState(node));
#ifdef MEASURE_MOTION_TIME
	motionTimeSec = (double) node->motionTime.tv_sec + (double) node->motionTime.tv_usec / 1000000.0;

	if (node->mstype == MOTIONSTATE_RECV)
	{
		elog(DEBUG1,
			 "Motion Node %d (RECEIVER) Statistics:\n"
			 "Timing:  \n"
			 "\t Time receiving the tuple: %f sec\n"
			 "Counters: \n"
			 "\tnumTuplesFromChild: %d\n"
			 "\tnumTuplesFromAMS: %d\n"
			 "\tnumTuplesToAMS: %d\n"
			 "\tnumTuplesToParent: %d\n",
			 motNodeID,
			 motionTimeSec,
			 node->numTuplesFromChild,
			 node->numTuplesFromAMS,
			 node->numTuplesToAMS,
			 node->numTuplesToParent
			);
	}
	else if (node->mstype == MOTIONSTATE_SEND)
	{
		otherTimeSec = (double) node->otherTime.tv_sec + (double) node->otherTime.tv_usec / 1000000.0;
		elog(DEBUG1,
			 "Motion Node %d (SENDER) Statistics:\n"
			 "Timing:  \n"
			 "\t Time getting next tuple to send: %f sec \n"
			 "\t Time sending the tuple:          %f  sec\n"
			 "\t Percentage of time sending:      %2.2f%% \n"
			 "Counters: \n"
			 "\tnumTuplesFromChild: %d\n"
			 "\tnumTuplesToAMS: %d\n",
			 motNodeID,
			 otherTimeSec,
			 motionTimeSec,
			 (double) (motionTimeSec / (otherTimeSec + motionTimeSec)) * 100,
			 node->numTuplesFromChild,
			 node->numTuplesToAMS
			);
	}
#endif							/* MEASURE_MOTION_TIME */


	/* Merge Receive: Free the priority queue and associated structures. */
	/* Free the slices and routes */
	if (vnode->tupleheap != NULL)
	{
		vecheap_free(vnode->tupleheap);
		node->tupleheap = NULL;
	}
	if (node->cdbhash != NULL)
	{
		VecCdbHash *vechash = (VecCdbHash *) node->cdbhash;

		for (int i =0; i < node->numHashSegments; i++)
		{
			ARROW_FREE(GArrowScalar, &vechash->segmapping_scalar[i]);
		}
		ARROW_FREE(GArrowArray, &vechash->hasharray);

		pfree(node->cdbhash);
		node->cdbhash = NULL;
	}

	/* free gandiva projector */
	if (vnode->hash_projector != NULL)
	{
		free_prj_project(&vnode->hash_projector);
		vnode->hash_projector = NULL;
	}

	if (vnode->hashExprsGandivaNodes != NULL)
	{
		ListCell *lc = NULL;
		GGandivaNode *gn = NULL;

		foreach(lc, vnode->hashExprsGandivaNodes)
		{
			gn = (GGandivaNode*)lfirst(lc);
			free_gandiva_node((void**)&gn);
		}

		list_free(vnode->hashExprsGandivaNodes);
		vnode->hashExprsGandivaNodes = NIL;
	}

	/*
	 * Free up this motion node's resources in the Motion Layer.
	 *
	 * TODO: For now, we don't flush the comm-layer.  NO ERRORS DURING AMS!!!
	 */
	EndMotionLayerNode(node->ps.state->motionlayer_context, motion->motionID,
					   /* flush-comm-layer */ false);
	if (vnode->random_const_array_template != NULL)
		free_array((void**)&vnode->random_const_array_template);
#ifdef CDB_MOTION_DEBUG
	if (node->outputFunArray)
		pfree(node->outputFunArray);
#endif
}


static TupleTableSlot *
execVecMotionUnsortedReceiver(MotionState *node)
{
	/* RECEIVER LOGIC */
	TupleTableSlot *slot;
	Motion	   *motion = (Motion *) node->ps.plan;
	EState	   *estate = node->ps.state;
	g_autoptr(GArrowRecordBatch) receivedBatch = NULL;

	AssertState(motion->motionType == MOTIONTYPE_GATHER ||
				motion->motionType == MOTIONTYPE_GATHER_SINGLE ||
				motion->motionType == MOTIONTYPE_HASH ||
				motion->motionType == MOTIONTYPE_BROADCAST ||
				(motion->motionType == MOTIONTYPE_EXPLICIT && motion->segidColIdx > 0));

	Assert(node->ps.state->motionlayer_context);

	if (node->stopRequested)
	{
		SendStopMessage(node->ps.state->motionlayer_context,
						node->ps.state->interconnect_context,
						motion->motionID);
		return NULL;
	}

	if (estate->interconnect_context == NULL)
	{
		if (!estate->es_interconnect_is_setup && estate->dispatcherState &&
			!estate->es_got_eos)
		{
			/*
			 * We could only possibly get here in the following scenario:
			 * 1. We are QD gracefully aborting a transaction.
			 * 2. We have torn down the interconnect of the current slice.
			 * 3. Since an error has happened, we no longer need to finish fetching
			 * all the tuples, hence squelching the executor subtree.
			 * 4. We are in the process of ExecSquelchShareInputScan(), and the
			 * Shared Scan has this Motion below it.
			 *
			 * NB: if you need to change this, see also execMotionSortedReceiver()
			 */
			ereport(NOTICE,
					(errmsg("An ERROR must have happened. Stopping a Shared Scan.")));
			return NULL;
		}
		else
			ereport(ERROR, (errmsg("Interconnect is down unexpectedly.")));
	}

	receivedBatch = ConcatenateBatches(node, ANY_ROUTE);

	if (!receivedBatch)
	{
#ifdef CDB_MOTION_DEBUG
		if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
			elog(DEBUG4, "motionID=%d saw end of stream", motion->motionID);
#endif
		Assert(node->numTuplesFromAMS == node->numTuplesToParent);
		Assert(node->numTuplesFromChild == 0);
		Assert(node->numTuplesToAMS == 0);
		return NULL;
	}

	/* store it in our result slot and return this. */
	slot = node->ps.ps_ResultTupleSlot;

	ExecClearTuple(slot);

	ExecStoreBatch(slot, receivedBatch);
	TTS_SET_SHOULDFREE(slot);

	if (slot == NULL)
		return NULL;

	int64 rows = (int64)GetNumRows(slot);
	node->numTuplesFromAMS += rows;
	node->numTuplesToParent += rows;

#ifdef CDB_MOTION_DEBUG
	if (node->numTuplesToParent <= 20)
	{
		StringInfoData buf;

		initStringInfo(&buf);
		appendStringInfo(&buf, "   motion%-3d rcv      %5d.",
						 motion->motionID,
						 node->numTuplesToParent);
		formatTuple(&buf, slot, node->outputFunArray);
		elog(DEBUG3, "%s", buf.data);
		pfree(buf.data);
	}
#endif

	/* Stats */
	if (slot) {
		statRecvTupleVec(node->ps.state->motionlayer_context,
						 motion->motionID,
						 rows);
	}
	return slot;
}

static bool
IsFullConcate(int rows)
{
	return rows >= min_redistribute_handle_rows;
}

static void
ConcatenateDistributeBatches(TupleTableSlot **slot, PlanState  *outerNode, bool *isnull)
{
	int totalRows = 0;
	g_autoptr(GArrowRecordBatch) concatenateBatch = NULL;
	GList *rbs = NULL;
	GError *error = NULL;

	TupleTableSlot * outerTupleSlot =  ExecProcNode(outerNode);
	*slot = outerTupleSlot;
	if (unlikely(TupIsNull(outerTupleSlot)))
		return ;

	totalRows += garrow_record_batch_get_n_rows(VECSLOT(outerTupleSlot)->tts_recordbatch);
	if (totalRows >= min_redistribute_handle_rows)
		return;

	while (true)
	{
		rbs = garrow_list_append_ptr(rbs, VECSLOT(outerTupleSlot)->tts_recordbatch);
		if (IsFullConcate(totalRows))
			break;
		outerTupleSlot =  ExecProcNode(outerNode);
		if (TupIsNull(outerTupleSlot))
		{
			*isnull = true;
			break;
		}
		totalRows += garrow_record_batch_get_n_rows(VECSLOT(outerTupleSlot)->tts_recordbatch);
	}
	concatenateBatch = garrow_record_batch_concatenate(rbs, &error);
	if (error)
		elog(ERROR, "Failed to concatenate the motion node input batches, cause: %s", error->message);

	garrow_list_free_ptr(&rbs);
	ExecStoreBatch(*slot, garrow_move_ptr(concatenateBatch));
}
static TupleTableSlot *
execVecMotionSender(MotionState *node)
{
	/* SENDER LOGIC */
	TupleTableSlot *outerTupleSlot;
	PlanState  *outerNode;
	Motion	   *motion = (Motion *) node->ps.plan;
	bool		done = false;
	bool IsNull = false;

#ifdef MEASURE_MOTION_TIME
	struct timeval time1;
	struct timeval time2;

	gettimeofday(&time1, NULL);
#endif

	AssertState(motion->motionType == MOTIONTYPE_GATHER ||
				motion->motionType == MOTIONTYPE_GATHER_SINGLE ||
				motion->motionType == MOTIONTYPE_HASH ||
				motion->motionType == MOTIONTYPE_BROADCAST ||
				(motion->motionType == MOTIONTYPE_EXPLICIT && motion->segidColIdx > 0));
	Assert(node->ps.state->interconnect_context);
	while (!done)
	{
		/* grab TupleTableSlot from our child. */
		outerNode = outerPlanState(node);
		if (likely(min_redistribute_handle_rows < 1 || motion->motionType != MOTIONTYPE_HASH))
			outerTupleSlot = ExecProcNode(outerNode);
		else
		{
			if (likely(!IsNull))
				ConcatenateDistributeBatches(&outerTupleSlot, outerNode, &IsNull);
			else
				outerTupleSlot = NULL;
		}
#ifdef MEASURE_MOTION_TIME
		gettimeofday(&time2, NULL);

		node->otherTime.tv_sec += time2.tv_sec - time1.tv_sec;
		node->otherTime.tv_usec += time2.tv_usec - time1.tv_usec;

		while (node->otherTime.tv_usec < 0)
		{
			node->otherTime.tv_usec += 1000000;
			node->otherTime.tv_sec--;
		}

		while (node->otherTime.tv_usec >= 1000000)
		{
			node->otherTime.tv_usec -= 1000000;
			node->otherTime.tv_sec++;
		}
#endif
		if (done || TupIsNull(outerTupleSlot))
		{
			doSendEndOfStreamVec(motion, node);
			done = true;
		}
		else if (motion->motionType == MOTIONTYPE_GATHER_SINGLE &&
				 GpIdentity.segindex != (gp_session_id % node->numInputSegs))
		{
			/*
			 * For explicit gather motion, receiver gets data from one
			 * segment only. The others execute the subplan normally, but
			 * throw away the resulting tuples.
			 */
		}
		else
		{
			if (motion->motionType == MOTIONTYPE_HASH)
			{

				hashAndSendVec_vechash(motion, node, outerTupleSlot);

			}
			else
			{
				doSendTupleVec(motion, node, outerTupleSlot);
			}
			/* doSendTuple() may have set node->stopRequested as a side-effect */

			if (node->stopRequested)
			{
				elog(gp_workfile_caching_loglevel, "Motion calling Squelch on child node");
				/* propagate stop notification to our children */
				ExecVecSquelchNode(outerNode);
				done = true;
			}
		}
#ifdef MEASURE_MOTION_TIME
		gettimeofday(&time1, NULL);

		node->motionTime.tv_sec += time1.tv_sec - time2.tv_sec;
		node->motionTime.tv_usec += time1.tv_usec - time2.tv_usec;

		while (node->motionTime.tv_usec < 0)
		{
			node->motionTime.tv_usec += 1000000;
			node->motionTime.tv_sec--;
		}

		while (node->motionTime.tv_usec >= 1000000)
		{
			node->motionTime.tv_usec -= 1000000;
			node->motionTime.tv_sec++;
		}
#endif
	}

	//Assert(node->stopRequested || node->numTuplesFromChild == node->numTuplesToAMS);

	/* nothing else to send out, so we return NULL up the tree. */
	return NULL;
}

static void doSendTupleVec(Motion *motion, MotionState *node, TupleTableSlot *outerTupleSlot) 
{
	int 		rows;
	int16		targetRoute;
	SendReturnCode sendRC;

	/* We got a lots of tuples from the child-plan. */
	Assert(TupHasVectorTuple(outerTupleSlot));

	rows = GetNumRows(outerTupleSlot);

	/* We got a tuple from the child-plan. */
	node->numTuplesFromChild += rows;

	if (motion->motionType == MOTIONTYPE_GATHER ||
		motion->motionType == MOTIONTYPE_GATHER_SINGLE)
	{
		/*
		 * Actually, since we can only send to a single output segment
		 * here, we are guaranteed that we only have a single targetRoute
		 * setup that we could possibly send to.  So we can cheat and just
		 * fix the targetRoute to 0 (the 1st route).
		 */
		targetRoute = 0;

	}
	else if (motion->motionType == MOTIONTYPE_BROADCAST)
	{
		targetRoute = BROADCAST_SEGIDX;
	}
	else if (motion->motionType == MOTIONTYPE_HASH) /* Redistribute */
	{
		elog(ERROR, "should not run here");
	}
	else if (motion->motionType == MOTIONTYPE_EXPLICIT)
	{
		Datum		segidColIdxDatum;

		Assert(motion->segidColIdx > 0 && motion->segidColIdx <= list_length((motion->plan).targetlist));
		bool		is_null = false;

		segidColIdxDatum = slot_getattr(outerTupleSlot, motion->segidColIdx, &is_null);
		targetRoute = Int32GetDatum(segidColIdxDatum);
		Assert(!is_null);
	}
	else
		elog(ERROR, "unknown motion type %d", motion->motionType);

	CheckAndSendRecordCache(node->ps.state->motionlayer_context,
							node->ps.state->interconnect_context,
							motion->motionID,
							targetRoute);

	/* send the tuple out. */
	sendRC = SendTupleVec(node->ps.state->motionlayer_context,
						  node->ps.state->interconnect_context,
						  motion->motionID,
						  outerTupleSlot,
						  targetRoute);

	Assert(sendRC == SEND_COMPLETE || sendRC == STOP_SENDING);
	if (sendRC == SEND_COMPLETE)
		node->numTuplesToAMS += rows;
	else
		node->stopRequested = true;

#ifdef CDB_MOTION_DEBUG
	if (sendRC == SEND_COMPLETE && node->numTuplesToAMS <= 20)
	{
		StringInfoData buf;

		initStringInfo(&buf);
		appendStringInfo(&buf, "   motion%-3d snd->%-3d, %5d.",
						 motion->motionID,
						 targetRoute,
						 node->numTuplesToAMS);
		formatTuple(&buf, outerTupleSlot, node->outputFunArray);
		elog(DEBUG3, "%s", buf.data);
		pfree(buf.data);
	}
#endif
}

static void
doSendEndOfStreamVec(Motion *motion, MotionState *node)
{
	/*
	 * We have no more child tuples, but we have not successfully sent an
	 * End-of-Stream token yet.
	 */
	SendEndOfStream(node->ps.state->motionlayer_context,
					node->ps.state->interconnect_context,
					motion->motionID);
	node->sentEndOfStream = true;
}


/*
 * General background on Sorted Motion:
 * -----------------------------------
 * NOTE: This function is only used for order-preserving motion.  There are
 * only 2 types of motion that order-preserving makes sense for: FIXED and
 * BROADCAST (HASH does not make sense). so we have:
 *
 * CASE 1:	 broadcast order-preserving fixed motion.  This should only be
 *			 called for SENDERs.
 *
 * CASE 2:	 single-destination order-preserving fixed motion.	The SENDER
 *			 side will act like Unsorted motion and won't call this. So only
 *			 the RECEIVER should be called for this case.
 *
 *
 * Sorted Receive Notes:
 * --------------------
 *
 * The 1st time we execute, we need to pull a tuple from each of our source
 * and store them in our tupleheap.  Once that is done, we can pick the lowest
 * (or whatever the criterion is) value from amongst all the sources.  This
 * works since each stream is sorted itself.
 *
 * We keep track of which one was selected, this will be slot we will need
 * to fill during the next call.
 *
 * Subsequent calls to this function (after the 1st time) will start by
 * trying to receive a tuple for the slot that was emptied the previous call.
 * Then we again select the lowest value and return that tuple.
 */

/* Sorted receiver using binary heap */
static TupleTableSlot *
execVecMotionSortedReceiver(MotionState *node)
{
	VecMotionState *vnode = (VecMotionState *) node;

	vecheap *vp = vnode->tupleheap;
	TupleTableSlot *slot = node->ps.ps_ResultTupleSlot;
	Motion	   *motion = (Motion *) node->ps.plan;
	EState	   *estate = node->ps.state;

	g_autoptr(GArrowRecordBatch) batch = NULL;
	g_autoptr(GArrowArray) array = NULL;
	g_autoptr(GArrowTable) table = NULL;
	g_autoptr(GArrowTable) ret_table = NULL;
	g_autoptr(GArrowTable) comb_table = NULL;
	g_autoptr(GArrowInt32ArrayBuilder) builder = NULL;
	g_autoptr(GArrowTableBatchReader) reader = NULL;
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowSchema) schema = NULL;

	int i;
	int cur_index = 0;

	AssertState(motion->motionType == MOTIONTYPE_GATHER &&
				motion->sendSorted &&
				vp != NULL);

	/* Notify senders and return EOS if caller doesn't want any more data. */
	if (node->stopRequested)
	{

		SendStopMessage(node->ps.state->motionlayer_context,
						node->ps.state->interconnect_context,
						motion->motionID);
		return NULL;
	}

	if (estate->interconnect_context == NULL)
	{
		if (!estate->es_interconnect_is_setup && estate->dispatcherState &&
			!estate->es_got_eos)
		{
			/*
			 * We could only possibly get here in the following scenario:
			 * 1. We are QD gracefully aborting a transaction.
			 * 2. We have torn down the interconnect of the current slice.
			 * 3. Since an error has happened, we no longer need to finish fetching
			 * all the tuples, hence squelching the executor subtree.
			 * 4. We are in the process of ExecSquelchShareInputScan(), and the
			 * Shared Scan has this Motion below it.
			 *
			 * NB: if you need to change this, see also execMotionUnsortedReceiver()
			 */
			ereport(NOTICE,
					(errmsg("An ERROR must have happened. Stopping a Shared Scan.")));
			return NULL;
		}
		else
			ereport(ERROR, (errmsg("Interconnect is down unexpectedly.")));
	}

	/*
	 * On first call, fill the priority queue with each sender's first tuple.
	 */
	if (!node->tupleheapReady)
	{
		execVecMotionSortedReceiverFirstTime(node);
	} 

	/* Finished if all senders have returned EOS. */
	if (vecheap_empty(vp))
	{
		Assert(node->numTuplesFromChild == 0);
		Assert(node->numTuplesToAMS == 0);
		return NULL;
	}

	if (!vecheap_get_first(vp))
		return NULL;

	for (int i = 0; i < vp->vh_num_route * 2; i++)
	{
		if (vp->vh_rtbatches[i]) 
		{
			schema = garrow_record_batch_get_schema(vp->vh_rtbatches[i]);
			break;
		}
	}
	/* take batch by indices */
	table = garrow_table_new_record_batches(schema,
											(GArrowRecordBatch **)vp->vh_rtbatches,
											vp->vh_num_route * 2, &error);
	if (error)
		elog(ERROR, "can not form table from batches for merge sort : %s", error->message);

	builder = garrow_int32_array_builder_new();
	garrow_int32_array_builder_append_values(builder,
											 vp->vh_indices, vp->vh_num_indices, NULL, -1, &error);
	if (error)
		elog(ERROR, "new builder error for merge sort : %s", error->message);

	array = garrow_array_builder_finish(
			GARROW_ARRAY_BUILDER(builder), &error);

	ret_table = garrow_table_take(table, array, NULL, &error);
	if (error)
		elog(ERROR, "table take error for merge sort: %s", error->message);

	reader = garrow_table_batch_reader_new(ret_table);
	batch = garrow_record_batch_reader_read_next(GARROW_RECORD_BATCH_READER(reader), &error);

	for (i = 0; i < vp->vh_num_route; i++)
	{
		int j;
		int offset = 0;

		/* fetching second batch, receive new tuple and update */
		if (vp->vh_rtcurrows[i] >= vp->vh_rttotalrows[i * 2])
		{
			offset = vp->vh_rttotalrows[i * 2];

			garrow_store_ptr(vp->vh_rtbatches[i * 2], vp->vh_rtbatches[i * 2 + 1]);
			vp->vh_rttotalrows[i * 2] = vp->vh_rttotalrows[i * 2 + 1];
			for (j = 0; j < vp->vh_nkey; j++)
			{
				if (vp->vh_keysize[j] < 0)
				{
					garrow_store_ptr(vp->vh_sortkeys[i * 2 * vp->vh_nkey + j],
						vp->vh_sortkeys[(i * 2 + 1) * vp->vh_nkey + j]);
				}
				else
				{
					vp->vh_sortkeys[i * 2 * vp->vh_nkey + j] =
						vp->vh_sortkeys[(i * 2 + 1) * vp->vh_nkey + j];
				}

				vp->vh_keybitmaps[i * 2 * vp->vh_nkey + j] =
						vp->vh_keybitmaps[(i * 2 + 1) * vp->vh_nkey + j];
			}

			vp->vh_rtcurrows[i] = vp->vh_rtcurrows[i] - offset;

			fetchBatch(node, i, 1);
		}
		vp->vh_rtstartrows[i] = cur_index;
		cur_index += vp->vh_rttotalrows[i * 2];
		cur_index += vp->vh_rttotalrows[i * 2 + 1];
	}

	/* Update counters. */
	node->numTuplesToParent++;

	/* Store tuple in our result slot. */
	slot = ExecStoreBatch(slot, batch);

	/* Return result slot. */
	return slot;
}								/* execMotionSortedReceiver */

void
execVecMotionSortedReceiverFirstTime(MotionState *node)
{
	int cur_index = 0;
	Motion	   *motion = (Motion *) node->ps.plan;
	int iSegIdx;
	ListCell *lcProcess;
	ExecSlice *sendSlice = &node->ps.state->es_sliceTable->slices[motion->motionID];
	VecMotionState* vnode = (VecMotionState *) node;
	vecheap *vp = vnode->tupleheap;

	Assert(sendSlice->sliceIndex == motion->motionID);

	foreach_with_count(lcProcess, sendSlice->primaryProcesses, iSegIdx)
	{
		vp->vh_noderoutes[vp->vh_size] = iSegIdx;
		vp->vh_num_route++;

		if (lfirst(lcProcess) == NULL)
			continue; /* skip this one: we are not receiving from it */

		if (!fetchBatch(node, iSegIdx, 0))
			continue;
		/* init routes */
		setData(vp, iSegIdx, 0, vp->vh_size);

		vp->vh_has_heap_property = false;

		fetchBatch(node, iSegIdx, 1);
		vp->vh_rtstartrows[iSegIdx] = cur_index;
		cur_index += vp->vh_rttotalrows[iSegIdx * 2];
		cur_index += vp->vh_rttotalrows[iSegIdx * 2 + 1];
		vp->vh_size++;

		node->numTuplesFromAMS++;
		}
		Assert(iSegIdx == node->numInputSegs);

		/*
		 * Done adding the elements, now arrange the heap to satisfy the heap
		 * property. This is quicker than inserting the initial elements one by
		 * one.
		 */
		vecheap_build(vp);
		node->tupleheapReady = true;
}								/* execMotionSortedReceiverFirstTime */

/*
 * batch range [16384, 32768] 
 */
static bool
IsMatchStrategy(int rows)
{
	return rows >= min_concatenate_rows;
}

static GArrowRecordBatch *
ConcatenateBatches(MotionState *node, int route) 
{
	Motion	   *motion = (Motion *) node->ps.plan;
	MinimalTuple inputTuple;
	g_autoptr(GArrowRecordBatch) inputBatch = NULL;
	g_autoptr(GArrowRecordBatch) batch = NULL;
	g_autoptr(GArrowRecordBatch) concatenateBatch = NULL;
	int totalRows = 0;
	GList *rbs = NULL;
	GError *error = NULL;
	List *tuples = NIL;
	inputTuple = RecvTupleFrom(node->ps.state->motionlayer_context,
							   node->ps.state->interconnect_context,
							   motion->motionID,
							   route);
	if (!inputTuple)
		return NULL;
	inputBatch = DeserializeMinimalTuple(inputTuple, true);
	totalRows += garrow_record_batch_get_n_rows(inputBatch);
	if (totalRows >= min_concatenate_rows)
	{
		return garrow_move_ptr(inputBatch);
	}
	
	while (true) {
		rbs = garrow_list_append_ptr(rbs, inputBatch);
		if (IsMatchStrategy(totalRows)) {
			break;
		}
		inputTuple = RecvTupleFrom(node->ps.state->motionlayer_context,
								   node->ps.state->interconnect_context,
								   motion->motionID,
								   route);

		if (!inputTuple)
			break;
		tuples = lappend(tuples, inputTuple);
		inputBatch = DeserializeMinimalTuple(inputTuple, false);
		totalRows += garrow_record_batch_get_n_rows(inputBatch);
	}
	concatenateBatch = garrow_record_batch_concatenate(rbs, &error);
	garrow_list_free_ptr(&rbs);
	if (tuples != NIL)
	{
		list_free_deep(tuples);	
	}	
	if (error)
		elog(ERROR, "Failed to concatenate the motion node input batches, cause: %s", error->message);
	return garrow_move_ptr(concatenateBatch);
}

/*
 * offset 0 for first batch of this route, 1 for second batch.
 */
static inline bool
fetchBatch(MotionState *node, int route, int offset)
{
	VecMotionState *vnode = (VecMotionState *) node;
	TupleTableSlot *slot = node->ps.ps_ResultTupleSlot;
	vecheap *vp = vnode->tupleheap;
	g_autoptr(GArrowRecordBatch) inputBatch = NULL;
	Motion	   *motion = (Motion *) node->ps.plan;

	inputBatch = ConcatenateBatches(node, route);
	if (!inputBatch)
	{
		ARROW_FREE(GArrowRecordBatch, &vp->vh_rtbatches[route * 2 + offset]);
		vp->vh_rttotalrows[route * 2 + offset] = 0;
		return false;
	}

	int64 rows = garrow_record_batch_get_n_rows(inputBatch);

	vp->vh_rttotalrows[route * 2 + offset] = rows;

	garrow_store_ptr(vp->vh_rtbatches[route * 2 + offset], inputBatch);

	setKey(node, route, offset);

	/* stat motion */
	if (slot) {
		statRecvTupleVec(node->ps.state->motionlayer_context,
						 motion->motionID,
						 rows);
	}
	return true;
}

/*
 * set key data from first batch
 */
static inline void
setKey(MotionState *node, int route, int offset)
{
	VecMotionState *vnode = (VecMotionState *) node;
	vecheap *vp = vnode->tupleheap;
	void *inputBatch = NULL;
	Motion	   *motion = (Motion *) node->ps.plan;
	void *schema;
	int i, nkey = vp->vh_nkey;
	struct ArrowArray *c_array;
	g_autoptr(GError) error = NULL;

	inputBatch = vp->vh_rtbatches[route * 2 + offset];
	for (i = 0; i < nkey; i++)
	{
		AttrNumber	key_attno = motion->sortColIdx[i];

		g_autoptr(GArrowArray) array = garrow_record_batch_get_column_data(
				GARROW_RECORD_BATCH(inputBatch), key_attno - 1);
		/* for variable size, sortkeys are arrow array*/
		if (vp->vh_keysize[i] < 0)
		{
			garrow_store_ptr(vp->vh_sortkeys[(route * 2 + offset) * nkey + i], array);
		}
		else
		{
			garrow_array_export(array, (void **) &c_array, &schema, &error);
			vp->vh_sortkeys[(route * 2 + offset) * nkey + i] =
				(void *)c_array->buffers[1];
			vp->vh_keybitmaps[(route * 2 + offset) * nkey + i] =
				(void *)c_array->buffers[0];
		}
	}
}

void
ExecSquelchVecMotion(MotionState *node)
{
	Motion	   *motion;

	AssertArg(node != NULL);

	motion = (Motion *) node->ps.plan;
	node->stopRequested = true;
	node->ps.state->active_recv_id = -1;

	/* pass down */
	SendStopMessage(node->ps.state->motionlayer_context,
					node->ps.state->interconnect_context,
					motion->motionID);
}
