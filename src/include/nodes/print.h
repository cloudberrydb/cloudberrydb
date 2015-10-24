/*-------------------------------------------------------------------------
 *
 * print.h
 *	  definitions for nodes/print.c
 *
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/nodes/print.h,v 1.25 2006/03/05 15:58:57 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef PRINT_H
#define PRINT_H

#include "nodes/parsenodes.h"

struct Plan;                            /* #include "nodes/plannodes.h" */
struct Query;                           /* #include "nodes/plannodes.h" */
struct TupleTableSlot;                  /* #include "executor/tuptable.h" */

#define nodeDisplay(x)		pprint(x)

extern char * plannode_type(struct Plan *p);
extern void print(void *obj);
extern void pprint(void *obj);
extern void elog_node_display(int lev, const char *title,
				  void *obj, bool pretty);
extern char *format_node_dump(const char *dump);
extern char *pretty_format_node_dump(const char *dump);
extern void print_rt(List *rtable);
extern void print_expr(Node *expr, List *rtable);
extern void print_pathkeys(List *pathkeys, List *rtable);
extern void print_tl(List *tlist, List *rtable);
extern void print_slot(struct TupleTableSlot *slot);
extern void print_plan_recursive(struct Plan *p, struct Query *parsetree,
					 int indentLevel, char *label);
extern void print_plan(struct Plan *p, struct Query *parsetree);

#endif   /* PRINT_H */
