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
 * The dynamically linked library created from this source can be reference by
 * creating a function in psql that references it. For example,
 *
 * CREATE OR REPLACE FUNCTION gp_workfile_cache_test()
 *   RETURNS bool
 *   AS '$libdir/gp_workfile_mgr.so', 'gp_workfile_manager_test_harness_wrapper' LANGUAGE C STRICT;
 *
 */

#include "postgres.h"
#include <string.h>
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"

Datum gp_workfile_mgr_test_harness(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(gp_workfile_mgr_test_harness_wrapper);

extern Datum gp_workfile_mgr_test_harness_wrapper(PG_FUNCTION_ARGS);

/*
 * gp_workfile_manager_clear_cache_wrapper
 *    The wrapper for creating a user-defined C function for gp_workfile_manager_clear_cache.
 */
Datum
gp_workfile_mgr_test_harness_wrapper(PG_FUNCTION_ARGS)
{
	return gp_workfile_mgr_test_harness(fcinfo);
}

