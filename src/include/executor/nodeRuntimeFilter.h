/*-------------------------------------------------------------------------
 *
 * nodeRuntimeFilter.h
 *
 * Portions Copyright (c) 2023, HashData Technology Limited.
 *
 * src/include/executor/nodeRuntimeFilter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODERUNTIMEFILTER_H
#define NODERUNTIMEFILTER_H

#include "access/parallel.h"
#include "nodes/execnodes.h"

extern RuntimeFilterState *ExecInitRuntimeFilter(RuntimeFilter *node,
                                                 EState *estate, int eflags);
extern void ExecEndRuntimeFilter(RuntimeFilterState *node);
extern void ExecReScanRuntimeFilter(RuntimeFilterState *node);
extern void RFBuildFinishCallback(RuntimeFilterState *rfstate, bool parallel);
extern void RFAddTupleValues(RuntimeFilterState *rfstate, List *vals);

extern void ExecInitRuntimeFilterFinish(RuntimeFilterState *node,
                                        double inner_rows);

#endif							/* NODERUNTIMEFILTER_H */
