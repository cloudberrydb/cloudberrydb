/*-------------------------------------------------------------------------
 * executor.h
 *	  support for the POSTGRES executor module
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 *
 * IDENTIFICATION
 *		src/include/vecexecutor/executor.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_EXECUTOR_H
#define VEC_EXECUTOR_H

#include "commands/explain.h"

#include "executor/tuptable.h"
#include "executor/executor.h"
#include "utils/tuptable_vec.h"
#include "vecexecutor/execnodes.h"

#define EXEC_FLAG_VECTOR 0x8000 /* Vector execute plan */

typedef TupleTableSlot *(*ExecScanLMComboTupMtd) (ScanState *node, TupleTableSlot *slot);

extern TupleTableSlot *ExecVecScan(ScanState *node, VecExecuteState *vestate,
		TupleTableSlot *vscanslot, ExecScanAccessMtd accessMtd, ExecScanRecheckMtd recheckMtd);
extern TupleTableSlot * ExecVecScanFetch(ScanState *node, ExecScanAccessMtd accessMtd,
		ExecScanRecheckMtd recheckMtd, TupleTableSlot *vscanslot);
extern TupleTableSlot *VecSeqNext(VecSeqScanState *node);
extern bool VecSeqRecheck(VecSeqScanState *node, TupleTableSlot *slot);
extern TupleTableSlot *ForeignNext(ForeignScanState *node);
extern bool ForeignRecheck(ForeignScanState *node, TupleTableSlot *slot);
extern TupleDesc BuildDescForScanExtract(TupleDesc tupdesc, List *targetlist, List *qual, int *map);
extern void ExecInitScanTupleSlotVec(EState *estate, ScanState *scanstate, TupleTableSlot **vscanslot,
		TupleDesc tupledesc, int *columnmap);
extern bool has_ctid(Expr *expr, void *context);

/*
 * prototypes from functions in execVecUtils.c
 */
extern void ExecAssignScanProjectionInfoVec(ScanState *node);
extern void ExecAssignProjectionInfoVec(PlanState *planstate,
									TupleDesc inputDesc);
/* FIXME: will be removed in the future. */
extern PlanState *VecExecInitNode(Plan *node, EState *estate, int eflags);
extern void VecExecEndNode(PlanState *node);

extern void ExecVecSlotToLog(TupleTableSlot *slot, const char *label);

/*
 *  Arrow expression
 */

/* proj expr unique, such as _ for add(i, j)_1, _ is one char */
#define UNDERLINE_JOIN_CHAR 1
#define OID_TEXT_NOT_LIKE_OP 1210
#define OID_INT2_MOD 529
#define OID_INT4_MOD 530
#define OID_INT8_MOD 439

typedef struct ArrowExprContext
{
	GArrowSchema *schema;
} ArrowExprContext;

/*
 * input and output for arrow proj node 
 */
typedef struct ProjExpression
{
	/* input expressions */
	GList *expressions;
	/* output column names */
	gchar **names;
	/* the number of names. */
	gsize n_names;
} ProjExpression;

extern GArrowScalar *const_expression_scalar(Const *node);

extern const char *get_op_name(Oid opno);
extern const char *get_function_name(Oid opno);
extern GArrowExpression *arrow_expression_tree_mutator(Expr *node, ArrowExprContext *context);

/*
 *  Arrow plan
 */
extern void BuildVecPlan(PlanState *planstate, VecExecuteState *estate);
extern TupleTableSlot *ExecuteVecPlan(VecExecuteState *estate);
extern void FreeVecExecuteState(VecExecuteState *estate);


extern ExplainOneQuery_hook_type   vec_explain_prev;
extern ExecutorStart_hook_type     vec_exec_start_prev;
extern ExecutorRun_hook_type       vec_exec_run_prev;
extern ExecutorEnd_hook_type       vec_exec_end_prev;

extern void ExecutorStartWrapper(QueryDesc *queryDesc, int eflags);
extern void ExecutorRunWrapper(QueryDesc *queryDesc,
							   ScanDirection direction,
							   uint64 count,
							   bool execute_once);
extern void ExecutorEndWrapper(QueryDesc *queryDesc);
#endif							/* VEC_EXECUTOR_H */
