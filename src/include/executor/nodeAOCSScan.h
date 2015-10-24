/*
 * nodeAOCSScan.h
 *
 *      Copyright (c) 2009, Greenplum Inc.
 */

#ifndef NODE_AOCS_SCAN_H
#define NODE_AOCS_SCAN_H

#include "nodes/execnodes.h"

extern int	ExecCountSlotsAOCSScan(AOCSScan *node);
extern AOCSScanState *ExecInitAOCSScan(AOCSScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecAOCSScan(AOCSScanState *node);
extern void ExecEndAOCSScan(AOCSScanState *node);
extern void ExecAOCSReScan(AOCSScanState *node, ExprContext *exprCtxt);
extern void ExecEagerFreeAOCSScan(AOCSScanState *node);

enum 
{
	GPMON_AOCSSCAN_RESCAN = GPMON_QEXEC_M_NODE_START,
	GPMON_AOCSSCAN_TOTAL,
};

static inline gpmon_packet_t * GpmonPktFromAOCSScanState (AOCSScanState *node)
{
	return &node->ss.ps.gpmon_pkt;
}

#endif
