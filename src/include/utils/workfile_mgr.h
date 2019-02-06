/*-------------------------------------------------------------------------
 *
 * workfile_mgr.h
 *	  Interface for workfile manager
 *
 *
 * Portions Copyright (c) 2011, EMC Corp.
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/utils/workfile_mgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __WORKFILE_MGR_H__
#define __WORKFILE_MGR_H__

#include "fmgr.h"
#include "storage/fd.h"

typedef struct workfile_set workfile_set;

/* Workfile Set operations */

extern Size WorkFileShmemSize(void);
extern void WorkFileShmemInit(void);

extern void RegisterFileWithSet(File file, struct workfile_set *work_set);
extern void UpdateWorkFileSize(File file, uint64 newsize);
extern void WorkFileDeleted(File file);

extern workfile_set *workfile_mgr_create_set(const char *operator_name, const char *prefix);
extern void workfile_mgr_close_set(workfile_set *work_set);
extern char *workfile_mgr_get_prefix(workfile_set *work_set);

extern Datum gp_workfile_mgr_cache_entries_internal(PG_FUNCTION_ARGS);
extern uint64 WorkfileSegspace_GetSize(void);

#endif /* __WORKFILE_MGR_H__ */
