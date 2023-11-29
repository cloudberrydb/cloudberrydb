/*-------------------------------------------------------------------------
 *
 * cdbmotion.c
 *		Access into the motion-layer in order to send and receive tuples
 *		within a motion node ID within a particular process group id.
 *
 * Portions Copyright (c) 2005-2008, Cloudberry inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/vecmotion/cdbmotion.c
 *
 * Reviewers: jzhang, tkordas
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "cdb/cdbconn.h"
#include "cdb/cdbvars.h"
#include "cdb/htupfifo.h"
#include "cdb/ml_ipc.h"
#include "utils/memutils.h"
#include "utils/typcache.h"

#include "cdb/vecmotion.h"
#include "cdb/vectupser.h"
#include "vecexecutor/execslot.h"
#include "vecexecutor/executor.h"

MotionIPCLayer *CurrentMotionIPCLayer = NULL;
/*
 * MOTION NODE INFO DATA STRUCTURES
 */
extern int			Gp_max_tuple_chunk_size;

/*
 * HELPER FUNCTION DECLARATIONS
 */
static MotionNodeEntry *getMotionNodeEntry(MotionLayerState *mlStates, int16 motNodeID);

/* Stats-function declarations. */
static void statSendTupleVec(MotionLayerState *mlStates,
							 MotionNodeEntry *pMNEntry,
							 TupleChunkList tcList,
							 TupleTableSlot *slot);

/*
 * Function:  SendTuple - Sends a portion or whole tuple to the AMS layer.
 */
SendReturnCode
SendTupleVec(MotionLayerState *mlStates,
			 ChunkTransportState *transportStates,
			 int16 motNodeID,
			 TupleTableSlot *slot,
			 int16 targetRoute)
{
	MotionNodeEntry *pMNEntry;
	TupleChunkListData tcList;
	MemoryContext oldCtxt;
	SendReturnCode rc;

	AssertArg(!TupIsNull(slot));

	/* no need to send empty vector batch*/
	if (GetNumRows(slot) == 0)
		return SEND_COMPLETE;

	/*
	 * Analyze tools.  Do not send any thing if this slice is in the bit mask
	 */
	if (gp_motion_slice_noop != 0 && (gp_motion_slice_noop & (1 << currentSliceId)) != 0)
		return SEND_COMPLETE;

	/*
	 * Pull up the motion node entry with the node's details.  This includes
	 * details that affect sending, such as whether the motion node needs to
	 * include backup segment-dbs.
	 */
	pMNEntry = getMotionNodeEntry(mlStates, motNodeID);

#ifdef AMS_VERBOSE_LOGGING
	elog(DEBUG5, "Serializing HeapTuple for sending.");
#endif

	struct directTransportBuffer b;
	if (targetRoute != BROADCAST_SEGIDX)
		CurrentMotionIPCLayer->GetTransportDirectBuffer(transportStates, motNodeID, targetRoute, &b);

	int			sent = 0;

	/* Create and store the serialized form, and some stats about it. */
	oldCtxt = MemoryContextSwitchTo(mlStates->motion_layer_mctx);

	sent = SerializeTupleVec(slot, &pMNEntry->ser_tup_info, &b, &tcList, targetRoute);	

	MemoryContextSwitchTo(oldCtxt);
	if (sent > 0)
	{
		CurrentMotionIPCLayer->PutTransportDirectBuffer(transportStates, motNodeID, targetRoute, sent);

		/* fill-in tcList fields to update stats */
		tcList.num_chunks = 1;
		tcList.serialized_data_length = sent;

		/* update stats */
		statSendTupleVec(mlStates, pMNEntry, &tcList, slot);

		return SEND_COMPLETE;
	}
	/* Otherwise fall-through */

#ifdef AMS_VERBOSE_LOGGING
	elog(DEBUG5, "Serialized HeapTuple for sending:\n"
		 "\ttarget-route %d \n"
		 "\t%d bytes in serial form\n"
		 "\tbroken into %d chunks",
		 targetRoute,
		 tcList.serialized_data_length,
		 tcList.num_chunks);
#endif

	/* do the send. */
	if (!CurrentMotionIPCLayer->SendTupleChunkToAMS(transportStates, motNodeID, targetRoute, tcList.p_first))
	{
		pMNEntry->stopped = true;
		rc = STOP_SENDING;
	}
	else
	{
		/* update stats */
		statSendTupleVec(mlStates, pMNEntry, &tcList, slot);

		rc = SEND_COMPLETE;
	}

	/* cleanup */
	clearTCList(&pMNEntry->ser_tup_info.chunkCache, &tcList);

	return rc;
}

/*
 * Helper function to get the motion node entry for a given ID.  NULL
 * is returned if the ID is unrecognized.
 */
static MotionNodeEntry *
getMotionNodeEntry(MotionLayerState *mlStates, int16 motNodeID)
{
	MotionNodeEntry *pMNEntry = NULL;

	if (motNodeID > mlStates->mneCount ||
		!mlStates->mnEntries[motNodeID - 1].valid)
	{
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("interconnect error: Unexpected Motion Node Id: %d",
						motNodeID),
				 errdetail("This means a motion node that wasn't setup is requesting interconnect resources.")));
	}
	else
		pMNEntry = &mlStates->mnEntries[motNodeID - 1];

	if (pMNEntry != NULL)
		Assert(pMNEntry->motion_node_id == motNodeID);

	return pMNEntry;
}

/*
 * STATISTICS HELPER-FUNCTIONS
 *
 * NOTE: the only fields that are required to be valid are
 * tcList->num_chunks and tcList->serialized_data_length, and
 * SerializeTupleDirect() only fills those fields out.
 */
static void
statSendTupleVec(MotionLayerState *mlStates,
				 MotionNodeEntry *pMNEntry,
				 TupleChunkList tcList,
				 TupleTableSlot *slot)
{
	int			headerOverhead;

	AssertArg(pMNEntry != NULL);

	headerOverhead = TUPLE_CHUNK_HEADER_SIZE * tcList->num_chunks;

	/* per motion-node stats. */
	pMNEntry->stat_total_sends += GetNumRows(slot);
	pMNEntry->stat_total_chunks_sent += tcList->num_chunks;
	pMNEntry->stat_total_bytes_sent += tcList->serialized_data_length + headerOverhead;
	pMNEntry->stat_tuple_bytes_sent += tcList->serialized_data_length;

	/* Update global motion-layer statistics. */
	mlStates->stat_total_chunks_sent += tcList->num_chunks;

	mlStates->stat_total_bytes_sent +=
		tcList->serialized_data_length + headerOverhead;

	mlStates->stat_tuple_bytes_sent += tcList->serialized_data_length;

}


void
statRecvTupleVec(MotionLayerState *mlStates,
		         int  motionId,
				 int64 tuple_cnt)
{
	MotionNodeEntry *pMNEntry = NULL;

	AssertArg(mlStates != NULL);

	pMNEntry = getMotionNodeEntry(mlStates, motionId);

	Assert(pMNEntry != NULL);

	/* Count tuples received. */
	pMNEntry->stat_total_recvs += tuple_cnt;
	/* Update "tuples available" counts for high watermark stats. */
	pMNEntry->stat_tuples_available -= tuple_cnt;
}

/* end of file */
