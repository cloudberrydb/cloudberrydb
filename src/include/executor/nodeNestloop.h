/*-------------------------------------------------------------------------
 *
 * nodeNestloop.h
 *
 *
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/executor/nodeNestloop.h,v 1.25 2006/03/05 15:58:56 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODENESTLOOP_H
#define NODENESTLOOP_H

#include "nodes/execnodes.h"

extern int	ExecCountSlotsNestLoop(NestLoop *node);
extern NestLoopState *ExecInitNestLoop(NestLoop *node, EState *estate, int eflags);
extern TupleTableSlot *ExecNestLoop(NestLoopState *node);
extern void ExecEndNestLoop(NestLoopState *node);
extern void ExecReScanNestLoop(NestLoopState *node, ExprContext *exprCtxt);

enum {
	GPMON_NLJ_INNERTUPLE = GPMON_QEXEC_M_NODE_START, 
	GPMON_NLJ_OUTERTUPLE,
	GPMON_NLJ_TOTAL
};

static inline gpmon_packet_t * GpmonPktFromNLJState(NestLoopState *node)
{
	return &node->js.ps.gpmon_pkt;
}

#endif   /* NODENESTLOOP_H */
