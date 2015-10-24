/*-------------------------------------------------------------------------
 *
 * nodeSplitUpdate.h
 *        Prototypes for nodeSplitUpdate.
 *
 * Copyright (c) 2012, EMC Corp.
 *
 *-------------------------------------------------------------------------
 */

#ifndef NODESplitUpdate_H
#define NODESplitUpdate_H

extern void ExecSplitUpdateExplainEnd(PlanState *planstate, struct StringInfoData *buf);
extern TupleTableSlot* ExecSplitUpdate(SplitUpdateState *node);
extern SplitUpdateState* ExecInitSplitUpdate(SplitUpdate *node, EState *estate, int eflags);
extern void ExecEndSplitUpdate(SplitUpdateState *node);
extern int ExecCountSlotsSplitUpdate(SplitUpdate *node);

extern void initGpmonPktForSplitUpdate(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate);

#endif   /* NODESplitUpdate_H */

