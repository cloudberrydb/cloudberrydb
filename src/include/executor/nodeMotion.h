/*-------------------------------------------------------------------------
 *
 * nodeMotion.h
 *
 *
 *
 * Portions Copyright (c) 1996-2004, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEMOTION_H
#define NODEMOTION_H

#include "nodes/execnodes.h"

extern int	ExecCountSlotsMotion(Motion *node);
extern MotionState *ExecInitMotion(Motion *node, EState *estate, int eflags);
extern TupleTableSlot *ExecMotion(MotionState *node);
extern void ExecEndMotion(MotionState *node);
extern void ExecReScanMotion(MotionState *node, ExprContext *exprCtxt);

extern void ExecStopMotion(MotionState *node);

extern bool isMotionGather(const Motion *m);


enum 
{
	GPMON_MOTION_BYTES_SENT = GPMON_QEXEC_M_NODE_START,
	GPMON_MOTION_TOTAL_ACK_TIME,
	GPMON_MOTION_AVG_ACK_TIME,
	GPMON_MOTION_MAX_ACK_TIME,
	GPMON_MOTION_MIN_ACK_TIME,
	GPMON_MOTION_COUNT_RESENT,
	GPMON_MOTION_MAX_RESENT,
	GPMON_MOTION_BYTES_RECEIVED,
	GPMON_MOTION_COUNT_DROPPED,
	GPMON_MOTION_TOTAL
};

static inline gpmon_packet_t * GpmonPktFromMotionState(MotionState *node)
{
	return &node->ps.gpmon_pkt;
}

#endif   /* NODEMOTION_H */
