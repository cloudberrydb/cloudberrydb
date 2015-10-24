/*-------------------------------------------------------------------------
 *
 * pquery.h
 *	  prototypes for pquery.c.
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/tcop/pquery.h,v 1.39 2006/09/03 03:19:45 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef PQUERY_H
#define PQUERY_H

#include "utils/portal.h"


extern PGDLLIMPORT Portal ActivePortal;


extern PortalStrategy ChoosePortalStrategy(List *stmts);

extern List *FetchPortalTargetList(Portal portal);

extern List *FetchStatementTargetList(Node *stmt);

extern void PortalStart(Portal portal, ParamListInfo params,
						Snapshot snapshot,
						const char *seqServerHost, int seqServerPort);

extern void PortalSetResultFormat(Portal portal, int nFormats,
					  int16 *formats);

extern bool PortalRun(Portal portal, int64 count, bool isTopLevel,
		  DestReceiver *dest, DestReceiver *altdest,
		  char *completionTag);

extern int64 PortalRunFetch(Portal portal,
			   FetchDirection fdirection,
			   int64 count,
			   DestReceiver *dest);

#endif   /* PQUERY_H */
