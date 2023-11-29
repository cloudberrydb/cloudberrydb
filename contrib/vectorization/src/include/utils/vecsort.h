/*--------------------------------------------------------------------
 * vecsort.h
 *	  Generalized tuple sorting routines.
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/utils/vecsort.h
 *
 *--------------------------------------------------------------------
 */
#ifndef VEC_SORT_H
#define VEC_SORT_H

#include "utils/arrow.h"
#include "utils/tuplesort.h"

typedef enum
{
	VEC_TSS_LOADING_FROM_SUBPLAN = 0, /* loading tuples from sub plan */
	VEC_TSS_FINISHED_FROM_SUBPLAN,
	VEC_TSS_LOADING_FROM_WORKFILE,    /* loading tuples from workfile */
	VEC_TSS_FINISHED_FROM_WORKFILE,
	VEC_TSS_FINISHED

} VecTupSortStatus;

typedef struct VecTuplesortstate VecTuplesortstate;

typedef void *(*SortGetNextBatch) (PlanState *outerPlan);

extern VecTuplesortstate *
vecsort_begin(TupleDesc tupDesc,
			  int nkeys, AttrNumber *attNums,
			  Oid *sortOperators, Oid *sortCollations,
			  bool *nullsFirstFlags,
			  int workMem, SortCoordinate coordinate, bool randomAccess,
			  TupleTableSlot *slot, SortGetNextBatch getnext, SortState *sortState, 
			  bool bounded, int64 bound);

extern bool vecsort_gettupleslot(VecTuplesortstate *state, bool forward, TupleTableSlot *slot);
extern void vecsort_end(VecTuplesortstate *state);
extern void vecsort_performsort(VecTuplesortstate *state);
extern void vecsort_get_stats(VecTuplesortstate *state, TuplesortInstrumentation *stats);
extern void vecsort_set_bound(VecTuplesortstate *state, int64 bound);
extern void vecsort_rescan(VecTuplesortstate *state);
extern GList* create_sort_keys(SortSupport ssup, int nkeys, GArrowSchema *schema);
#endif   /* VEC_SORT_H */
