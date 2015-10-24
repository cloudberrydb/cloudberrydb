/*-------------------------------------------------------------------------
 *
 * parse_utilcmd.h
 *		parse analysis for utility commands
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/parser/parse_utilcmd.h,v 1.3 2008/01/01 19:45:58 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_UTILCMD_H
#define PARSE_UTILCMD_H

#include "nodes/parsenodes.h"
#include "parser/analyze.h"
#include "parser/parse_node.h"

extern void transformIndexConstraints(ParseState *pstate, CreateStmtContext *cxt, bool mayDefer);
extern void transformInhRelation(ParseState *pstate, CreateStmtContext *cxt,
								 InhRelation *inhRelation, bool forceBareCol);

#endif   /* PARSE_UTILCMD_H */
