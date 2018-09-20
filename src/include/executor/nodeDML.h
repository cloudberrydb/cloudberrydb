/*-------------------------------------------------------------------------
 *
 * nodeDML.h
 *	  Prototypes for nodeDML.
 *
 * Portions Copyright (c) 2012, EMC Corp.
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/executor/nodeDML.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef NODEDML_H
#define NODEDML_H

extern void ExecDMLExplainEnd(PlanState *planstate, struct StringInfoData *buf);
extern TupleTableSlot* ExecDML(DMLState *node);
extern DMLState* ExecInitDML(DML *node, EState *estate, int eflags);
extern void ExecEndDML(DMLState *node);

#endif   /* NODEDML_H */

