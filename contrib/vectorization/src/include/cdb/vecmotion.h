/*-------------------------------------------------------------------------
 *
 * cdbmotion.c
 *		Access into the motion-layer in order to send and receive tuples
 *		within a motion node ID within a particular process group id.
 *
 * Portions Copyright (c) 2016-2020, HashData
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/vemotion.h
 *
 * Reviewers: jzhang, tkordas
 *-------------------------------------------------------------------------
 */
#ifndef VECMOTION_H 
#define VECMOTION_H

#include "cdb/cdbinterconnect.h"
#include "cdb/cdbmotion.h"

/* non-blocking operation that may perform only part (or none) of the
 * send before returning.  The TupleSendContext is used to help keep track
 * of the send operations status.  The caller of SendTuple() is responsible
 * for continuing to call SendTuple() until the entire tuple has been sent.
 *
 * Failing to do so and calling SendTuple() with a new Tuple before the old
 * one has been sent is a VERY BAD THING to do and will surely cause bad
 * failures.
 *
 * PARAMETERS:
 *	 - mn_info:  Motion node this tuple is being sent from.
 *
 *	 - tupCtxt: tuple data to send, and state of send operation.
 *
 * RETURN:	return codes to indicate result of send. Possible values are:
 *
 *		SEND_COMPLETE - The entire tuple was accepted by the AMS.
 *				Note that the tuple data may still be in the
 *				AMS send-buffers, but as far as the motion node
 *				is concerned the send is done.
 *
 *		STOP_SENDING - Receiver no longer wants to receive from us.
 */
extern SendReturnCode SendTupleVec(MotionLayerState *mlStates,
                                   ChunkTransportState *transportStates,
                                   int16 motNodeID,
                                   TupleTableSlot *slot,
                                   int16 targetRoute);

extern void statRecvTupleVec(MotionLayerState *mlStates,
                             int motionId,
                             int64 tuple_cnt);
#endif							/* VECMOTION_H */