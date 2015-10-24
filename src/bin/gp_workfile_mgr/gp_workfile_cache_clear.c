/*
 * Copyright (c) 2012 EMC Corporation All Rights Reserved
 *
 * This software is protected, without limitation, by copyright law
 * and international treaties. Use of this software and the intellectual
 * property contained therein is expressly limited to the terms and
 * conditions of the License Agreement under which it is provided by
 * or on behalf of EMC.
 *
 * ---------------------------------------------------------------------
 *
 * Interface to gp_workfile_manager_clear_cache function.
 *
 * The gp_workfile_manager_clear_cache_wrapper function is a wrapper around the
 * gp_workfile_manager_clear_cache
 * function located in the postgres backend executable. Since we can not call the
 * gp_workfile_manager_clear_cache function directly without a change to the
 * catalog tables, this
 * wrapper will allow a user to create a function using this wrapper and indirectly
 * call gp_workfile_manager_clear_cache.
 *
 * The dynamically linked library created from this source can be reference by
 * creating a function in psql that references it. For example,
 *
 * CREATE OR REPLACE FUNCTION gp_workfile_cache_clear()
 *   RETURNS bool
 *   AS '$libdir/gp_workfile_mgr.so', 'gp_workfile_manager_clear_cache_wrapper' LANGUAGE C STRICT;
 *
 */

#include "postgres.h"
#include "utils/workfile_mgr.h"

Datum gp_workfile_mgr_clear_cache(PG_FUNCTION_ARGS);
Datum gp_workfile_mgr_reset_segspace(PG_FUNCTION_ARGS);

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(gp_workfile_mgr_clear_cache);

/*
 * gp_workfile_mgr_clear_cache
 *    The wrapper for a user-defined C function to clear the workfile cache
 */
Datum
gp_workfile_mgr_clear_cache(PG_FUNCTION_ARGS)
{
	int seg_id = PG_GETARG_INT32(0);
	int32 n_removed = workfile_mgr_clear_cache(seg_id);
	return Int32GetDatum(n_removed);
}

PG_FUNCTION_INFO_V1(gp_workfile_mgr_reset_segspace);

/*
 * gp_workfile_mgr_reset_segspace
 *    Function to reset the used segspace on a segment
 *    This directly manipulates the segspace counter and
 *    should be used for testing purposes only
 *  Returns the size before the reset
 */
Datum
gp_workfile_mgr_reset_segspace(PG_FUNCTION_ARGS)
{
	int64 size = WorkfileSegspace_GetSize();
	WorkfileSegspace_Commit(0, size);
	return Int64GetDatum(size);
}
