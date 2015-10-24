/*-------------------------------------------------------------------------
 *
 * nodeAssertOp.h
 *	  Prototypes for nodeAssertOp.
 *
 * Copyright (c) 2012, EMC Corp.
 *
 *-------------------------------------------------------------------------
 */

#ifndef NODEASSERTOP_H
#define NODEASSERTOP_H

extern void ExecAssertOpExplainEnd(PlanState *planstate, struct StringInfoData *buf);
extern TupleTableSlot* ExecAssertOp(AssertOpState *node);
extern AssertOpState* ExecInitAssertOp(AssertOp *node, EState *estate, int eflags);
extern void ExecEndAssertOp(AssertOpState *node);
extern int ExecCountSlotsAssertOp(AssertOp *node);
extern void ExecReScanAssertOp(AssertOpState *node, ExprContext *exprCtxt);
extern void initGpmonPktForAssertOp(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate);

#endif   /* NODEASSERTOP_H */


