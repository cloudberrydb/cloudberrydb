/*-------------------------------------------------------------------------
 *
 * planner_vec.h
 *	  prototypes for vectorized planner.
 *
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 *
 * IDENTIFICATION
 *		src/include/optimizer/planner_vec.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLANNER_VEC_H
#define PLANNER_VEC_H

#include "optimizer/planner.h"

extern planner_hook_type           planner_prev;

extern PlannedStmt *planner_hook_wrapper(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams);
extern bool try_vectorize_plan(PlannedStmt *result);
extern bool has_column_store_relation(Plan* top_plan, List *rtable);

#endif /* PLANNER_VEC_H */
