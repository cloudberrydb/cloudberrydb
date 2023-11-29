/*-------------------------------------------------------------------------
 *
 * nodeWindowAgg.h
 *
 * Portions Copyright (c) 2016-Present, HashData
 *
 * src/include/vecexecutor/nodeWindowAgg.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_NODEWINDOWAGG_H
#define VEC_NODEWINDOWAGG_H

#include "nodes/execnodes.h"

#include "vecnodes/plannodes.h"
#include "vecexecutor/execnodes.h"

extern WindowAggState *ExecInitVecWindowAgg(WindowAgg *node, EState *estate, int eflags);
extern void ExecEndVecWindowAgg(WindowAggState *node);
extern void ExecSquelchVecWindowAgg(WindowAggState *node);

#endif   /* VEC_NODEWINDOWAGG_H*/

