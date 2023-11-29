/*-------------------------------------------------------------------------
 *
 * nodeSubqueryscan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/vecexecutor/nodeSubqueryscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_NODE_SUBQUERYSCAN_H
#define VEC_NODE_SUBQUERYSCAN_H

#include "nodes/execnodes.h"

extern SubqueryScanState *ExecInitVecSubqueryScan(SubqueryScan *node, EState *estate, int eflags);
extern void ExecEndVecSubqueryScan(SubqueryScanState *node);
extern void ExecReScanSubqueryScan(SubqueryScanState *node);
extern void ExecSquelchVecSubqueryScan(SubqueryScanState *node);

#endif							/* VEC_NODE_SUBQUERYSCAN_H */
