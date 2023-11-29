/*-------------------------------------------------------------------------
 *
 * nodeMaterial.h
 *
 *
 * Portions Copyright (c) 2023, Hashdata Inc.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeMaterial.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_NODEMATERIAL_H
#define VEC_NODEMATERIAL_H

#include "nodes/execnodes.h"
#include "vecexecutor/execnodes.h"

struct NTupleStoreLobRef;

extern MaterialState *ExecInitVecMaterial(Material *node, EState *estate, int eflags);
extern void ExecEndVecMaterial(MaterialState *node);
extern void ExecVecMaterialMarkPos(MaterialState *node);
extern void ExecVecMaterialRestrPos(MaterialState *node);
extern void ExecReScanVecMaterial(MaterialState *node);
extern void ExecSquelchVecMaterial(MaterialState *node);


#endif							/* VEC_NODEMATERIAL_H */
