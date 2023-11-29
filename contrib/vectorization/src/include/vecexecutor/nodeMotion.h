/*-------------------------------------------------------------------------
 *
 * nodeMotion.h
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *		src/include/vecexecutor/nodeMotion.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_MOTION_H
#define VEC_MOTION_H

#include "nodes/plannodes.h"
#include "vecexecutor/execnodes.h"

/* tts_flags */
#define         TTS_VIRTUAL     8


struct GenericTupleData;
typedef struct GenericTupleData *GenericTuple;
/*
 * CdbTupleHeapInfo
 * A priority queue element holding the next tuple of the
 * sorted tuple stream received from a particular sender.
 * Used by sorted receiver (Merge Receive).
 */
typedef struct CdbTupleHeapInfo
{
	/* Next tuple from this sender */
	GenericTuple tuple;
	Datum		datum1;			/* value of first key column */
	bool		isnull1;		/* is first key column NULL? */
}			CdbTupleHeapInfo;

typedef struct CdbMergeComparatorContext
{
	int			numSortCols;
	SortSupport sortKeys;
	TupleDesc	tupDesc;
	MemTupleBinding *mt_bind;

	CdbTupleHeapInfo *tupleheap_entries;
} CdbMergeComparatorContext;

typedef struct hash_motion_target
{
	int seg;
	void *recordbatch;
}hash_motion_target;

extern MotionState *ExecInitVecMotion(Motion *node, EState *estate, int eflags);
extern void ExecEndVecMotion(MotionState *node);
extern void ExecSquelchVecMotion(MotionState *node);

#endif /* VEC_MOTION_H */
