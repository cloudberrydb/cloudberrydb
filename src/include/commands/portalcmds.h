/*-------------------------------------------------------------------------
 *
 * portalcmds.h
 *	  prototypes for portalcmds.c.
 *
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/commands/portalcmds.h,v 1.19 2006/03/05 15:58:55 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef PORTALCMDS_H
#define PORTALCMDS_H

#include "nodes/parsenodes.h"
#include "utils/portal.h"


extern void PerformCursorOpen(PlannedStmt *stmt, ParamListInfo params,
				  const char *queryString, bool isTopLevel);

extern void PerformPortalFetch(FetchStmt *stmt, DestReceiver *dest,
				   char *completionTag);

extern void PerformPortalClose(const char *name);

extern void PortalCleanup(Portal portal);

extern void PersistHoldablePortal(Portal portal);

#endif   /* PORTALCMDS_H */
