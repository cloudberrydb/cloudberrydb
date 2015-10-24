/*
 * nodeDynamicIndexScan.h
 *
 * Copyright (c) 2013 - present, EMC/Greenplum
 */
#ifndef NODEDYNAMICINDEXSCAN_H
#define NODEDYNAMICINDEXSCAN_H

#include "nodes/execnodes.h"

extern int ExecCountSlotsDynamicIndexScan(DynamicIndexScan *node);
extern DynamicIndexScanState *ExecInitDynamicIndexScan(DynamicIndexScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecDynamicIndexScan(DynamicIndexScanState *node);
extern void ExecEndDynamicIndexScan(DynamicIndexScanState *node);
extern void ExecDynamicIndexReScan(DynamicIndexScanState *node, ExprContext *exprCtxt);

enum 
{
	GPMON_DYNAMICINDEXSCAN_RESCAN = GPMON_QEXEC_M_NODE_START,
	GPMON_DYNAMICINDEXSCAN_TOTAL,
};

static inline gpmon_packet_t * GpmonPktFromDynamicIndexScanState(DynamicIndexScanState *node)
{
	return &node->indexScanState.ss.ps.gpmon_pkt;
}
#endif

