/*-------------------------------------------------------------------------
 *
 * nodeSeqScan.h
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *		src/include/vecexecutor/nodeSeqscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_NODE_SEQ_SCAN_H
#define VEC_NODE_SEQ_SCAN_H

#include "executor/tuptable.h"
#include "nodes/execnodes.h"

#include "vecexecutor/execnodes.h"

extern VecSeqScanState *ExecInitVecSeqScan(SeqScan *node, EState *estate, int eflags);
extern void ExecEndVecSeqScan(VecSeqScanState *node);
extern void ExecReScanVecSeqScan(VecSeqScanState *node);
extern VecSeqScanState *ExecInitVecSeqScanForPartition(SeqScan *node, EState *estate,
													Relation currentRelation);
extern void ExecSquelchVecSeqScan(SeqScanState *node);
#endif /* VEC_NODE_SEQ_SCAN_H */
