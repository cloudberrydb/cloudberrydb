/*-------------------------------------------------------------------------
 *
 * nodeHash.h
 *	  prototypes for nodeHash.c
 * 
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *		src/include/vecexecutor/nodeHash.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_NODEHASH_H
#define VEC_NODEHASH_H

extern HashState *ExecInitVecHash(Hash *node, EState *estate, int eflags);
extern void ExecEndVecHash(HashState *node);
extern TupleTableSlot *ExecVecHash(PlanState *pstate);
#endif   /* VEC_NODEHASH_H */