/*-------------------------------------------------------------------------
 *
 * nodeDML.h
 *	  Prototypes for nodeDML.
 *
 * Copyright (c) 2012, EMC Corp.
 *
 *-------------------------------------------------------------------------
 */

#ifndef NODEDML_H
#define NODEDML_H

extern void ExecDMLExplainEnd(PlanState *planstate, struct StringInfoData *buf);
extern TupleTableSlot* ExecDML(DMLState *node);
extern DMLState* ExecInitDML(DML *node, EState *estate, int eflags);
extern void ExecEndDML(DMLState *node);
extern int ExecCountSlotsDML(DML *node);

extern void initGpmonPktForDML(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate);

#endif   /* NODEDML_H */

