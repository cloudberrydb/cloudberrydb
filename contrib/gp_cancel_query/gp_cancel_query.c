/*
 * Copyright (c) 2011 EMC Corporation All Rights Reserved
 *
 * This software is protected, without limitation, by copyright law
 * and international treaties. Use of this software and the intellectual
 * property contained therein is expressly limited to the terms and
 * conditions of the License Agreement under which it is provided by
 * or on behalf of EMC.
 *
 * ---------------------------------------------------------------------
 *
 * Interface to gp_cancel_query_wrapper function.
 *
 * The gp_cancel_query_wrapper function is a wrapper around the gp_cancel_query
 * function located in the postgres backend executable. Since we can not call the
 * gp_cancel_query function directly without a change to the catalog tables, this
 * wrapper will allow a user to create a function using this wrapper and indirectly
 * call gp_cancel_query.
 *
 * The dynamicly linked library created from this source can be reference by
 * creating a function in psql that references it. For example,
 *
 * CREATE OR REPLACE FUNCTION gp_cancel_query(int, int)
 *   RETURNS bool
 *   AS '$libdir/gp_cancel_query.so', 'gp_cancel_query_wrapper' LANGUAGE C STRICT;
 *
 */

#include "postgres.h"
#include <string.h>
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern Datum gp_cancel_query(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(gp_cancel_query_wrapper);

extern Datum gp_cancel_query_wrapper(PG_FUNCTION_ARGS);

/*
 * gp_cancel_query_wrapper
 *    The wrapper for creating a user-defined C function for gp_cancel_query.
 */
Datum
gp_cancel_query_wrapper(PG_FUNCTION_ARGS)
{
	return gp_cancel_query(fcinfo);
}

