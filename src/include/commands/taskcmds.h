/*-------------------------------------------------------------------------
 *
 * taskcmds.h
 *      prototypes for taskcmds.c.
 *
 * Portions Copyright (c) 2023, HashData Technology Limited.
 *
 * IDENTIFICATION
 *		src/include/commands/taskcmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef TASKCMDS_H
#define TASKCMDS_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"

extern ObjectAddress DefineTask(ParseState *pstate, CreateTaskStmt *stmt);

extern ObjectAddress AlterTask(ParseState *pstate, AlterTaskStmt *stmt);

extern ObjectAddress DropTask(ParseState *pstate, DropTaskStmt *stmt);

extern void RemoveTaskById(Oid jobid);

#endif                  /* TASKCMDS_H */
