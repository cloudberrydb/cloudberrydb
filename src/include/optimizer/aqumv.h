/*-------------------------------------------------------------------------
 *
 * aqumv.h
 *	  prototypes for optimizer/plan/aqumv.c.
 *
 * Portions Copyright (c) 2024, HashData Technology Limited.
 * 
 * Author: Zhang Mingli <avamingli@gmail.com>
 *
 * src/include/optimizer/aqumv.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AQUMV_H
#define AQUMV_H

#include "nodes/parsenodes.h"

/*
 * Adjust parse tree storaged in view's actions.
 * Query should be a simple query, ex:
 * select from a single table.
 */
extern void aqumv_adjust_simple_query(Query *viewQuery);

#endif   /* AQUMV_H */
