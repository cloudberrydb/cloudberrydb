/*-------------------------------------------------------------------------
 *
 * cluster.h
 *	  header file for postgres cluster command stuff
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/commands/cluster.h,v 1.30 2006/03/05 15:58:55 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_H
#define CLUSTER_H

#include "nodes/parsenodes.h"
#include "utils/rel.h"


extern void cluster(ClusterStmt *stmt);

extern void check_index_is_clusterable(Relation OldHeap, Oid indexOid,
						   bool recheck);
extern void mark_index_clustered(Relation rel, Oid indexOid);
extern Oid make_new_heap(Oid OIDOldHeap, const char *NewName, Oid NewTableSpace, 
		TableOidInfo * oidInfo,
        bool createAoBlockDirectory);
extern void swap_relation_files(Oid r1, Oid r2, bool swap_stats);

extern void populate_oidInfo(TableOidInfo *oidInfo, Oid TableSpace, 
							 bool relisshared, bool withTypes);

#endif   /* CLUSTER_H */
