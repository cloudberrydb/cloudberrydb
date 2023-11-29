/*-------------------------------------------------------------------------
 *
 * nodeSequence.h
 *    header file for nodeSequence.c.
 *
 * Portions Copyright (c) 2012 - present, EMC/Cloudberry
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/vecexecutor/nodeSequence.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_NODE_SEQUENCE_H
#define VEC_NODE_SEQUENCE_H

#include "executor/tuptable.h"
#include "nodes/execnodes.h"

extern SequenceState *ExecInitVecSequence(Sequence *node, EState *estate, int eflags);
extern TupleTableSlot *ExecVecSequence(PlanState *pstate);
extern void ExecReScanSequence(SequenceState *node);
extern void ExecEndVecSequence(SequenceState *node);
extern void ExecSquelchVecSequence(SequenceState *node);

#endif /* VEC_NODE_SEQUENCE_H */
