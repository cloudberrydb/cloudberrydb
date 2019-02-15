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
#include "lib/ilist.h"

#define WORKFILE_PREFIX_LEN		64

typedef struct WorkFileUsagePerQuery WorkFileUsagePerQuery;

typedef struct workfile_set
{
	/* Session id for the query creating the workfile set */
	int			session_id;

	/* Command count for the query creating the workfile set */
	int			command_count;

	/* Number of files in set */
	uint32		num_files;

	/* Size in bytes of the files in this workfile set */
	int64		total_bytes;

	/* Prefix of files in the workfile set */
	char		prefix[WORKFILE_PREFIX_LEN];

	/* Type of operator creating the workfile set */
	char		operator[NAMEDATALEN];

	/* Slice in which the spilling operator was */
	int			slice_id;

	WorkFileUsagePerQuery *perquery;

	dlist_node	node;

	bool		active;
} workfile_set;

/* Workfile Set operations */

extern Size WorkFileShmemSize(void);
extern void WorkFileShmemInit(void);

extern void RegisterFileWithSet(File file, struct workfile_set *work_set);
extern void UpdateWorkFileSize(File file, uint64 newsize);
extern void WorkFileDeleted(File file);

extern workfile_set *workfile_mgr_create_set(const char *operator_name, const char *prefix);
extern void workfile_mgr_close_set(workfile_set *work_set);

extern Datum gp_workfile_mgr_cache_entries_internal(PG_FUNCTION_ARGS);
extern workfile_set *workfile_mgr_cache_entries_get_copy(int* num_actives);
extern uint64 WorkfileSegspace_GetSize(void);

#endif /* __WORKFILE_MGR_H__ */
