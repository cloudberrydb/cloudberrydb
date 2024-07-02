/*-------------------------------------------------------------------------
 *
 * gp_matview_dependency.h
 *
 * Portions Copyright (c) 2024, HashData Technology Limited.
 *
 * gp_matview_dependency.h
 *     Definition about catalog of matviews dependency
 *
 * IDENTIFICATION
 *          src/include/catalog/gp_matview_dependency.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GP_MATVIEW_DEPENDENCY_H
#define GP_MATVIEW_DEPENDENCY_H

#include "catalog/genbki.h"
#include "catalog/gp_matview_dependency_d.h"

#include "nodes/pathnodes.h"

#define timestamptz Datum

CATALOG(gp_matview_dependency,8757,MatviewDependencyId) BKI_SHARED_RELATION
{
	Oid 			matviewid BKI_FORCE_NOT_NULL;
	int8    		trans_version;
	bool			defer;
	bool			partial;
	bool			isvaild;
	timestamptz		refresh_time;
	int8			combine_version;
	oidvector		relids;
} FormData_gp_matview_dependency;


typedef FormData_gp_matview_dependency *Form_gp_matview_dependency;


extern void create_matview_dependency_tuple(Oid matviewOid, Relids relids, bool defer);
extern Datum get_matview_dependency_relids(Oid matviewOid);
extern void mark_matview_dependency_valid(Oid matviewOid, bool isvaild);
extern void remove_matview_dependency_byoid(Oid matviewOid);
extern void record_restart_trans_version(Oid matviewOid, uint64 version, TimestampTz ftime);
extern void record_restart_combine_version(Oid matviewOid, uint64 version, TimestampTz ftime);
extern uint64 get_restart_trans_version(Oid matviewOid, Snapshot snapshot);

#undef timestamptz

#endif   /* GP_MATVIEW_DEPENDENCY_H */
