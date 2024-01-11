/*-------------------------------------------------------------------------
 *
 * nodeShareInputScan.h
 *
 * Portions Copyright (c) 2007-2008, Cloudberry inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	    src/include/executor/nodeShareInputScan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_NODESHAREINPUTSCAN_H
#define VEC_NODESHAREINPUTSCAN_H

#include "nodes/execnodes.h"
#include "storage/sharedfileset.h"

extern ShareInputScanState *ExecInitVecShareInputScan(ShareInputScan *node, EState *estate, int eflags);
extern void ExecEndVecShareInputScan(ShareInputScanState *node);
extern void ExecReScanVecShareInputScan(ShareInputScanState *node);
extern void ExecSquelchVecShareInputScan(ShareInputScanState *node);

extern Size ShareInputShmemSizeVec(void);

extern SharedFileSet *get_shareinput_fileset_vec(void);

#endif   /* VEC_NODESHAREINPUTSCAN_H */
