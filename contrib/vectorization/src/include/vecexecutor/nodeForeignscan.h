/*-------------------------------------------------------------------------
 *
 * nodeForeignscan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeForeignscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_NODEFOREIGNSCAN_H
#define VEC_NODEFOREIGNSCAN_H

#include "access/parallel.h"
#include "nodes/execnodes.h"

extern VecForeignScanState *ExecInitVecForeignScan(ForeignScan *node, EState *estate, int eflags);
extern void ExecEndVecForeignScan(VecForeignScanState *node);
extern void ExecReScanVecForeignScan(VecForeignScanState *node);
extern void ExecSquelchVecForeignScan(ForeignScanState *node);
extern void ExecShutdownForeignScan(ForeignScanState *node);
#endif							/* NODEFOREIGNSCAN_H */
