/*-------------------------------------------------------------------------
 *
 * nodeSort.h
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *		src/include/vecexecutor/nodeSort.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_NODE_SORT_H
#define VEC_NODE_SORT_H

#include "executor/tuptable.h"
#include "nodes/execnodes.h"

#include "vecexecutor/execnodes.h"

extern SortState *ExecInitVecSort(Sort *node, EState *estate, int eflags);
extern void ExecEndVecSort(SortState *node);
extern void ExecVecSortMarkPos(SortState *node);
extern void ExecVecSortRestrPos(SortState *node);
extern void ExecReScanVecSort(SortState *node);
extern void ExecSquelchVecSort(SortState *node);

#endif /* VEC_NODE_SORT_H */
