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
 * Interface to gp_aoseg_history_wrapper function.
 *
 * The gp_aoseg_history_wrapper function is a wrapper around the gp_aoseg_history
 * function located in the postgres backend executable. Since we can not call the
 * gp_aoseg_history function directly without a change to the catalog tables, this
 * wrapper will allow a user to create a function using this wrapper and indirectly
 * call gp_aoseg_history.
 *
 * The dynamicly linked library created from this source can be reference by
 * creating a faction in psql that references it. For example,
 *
 * CREATE FUNCTION get_gp_aoseg_history(oid)
 *   RETURNS TABLE( gp_tid tid
 *                , gp_xmin integer
 *                , gp_xmin_status text
 *                , gp_xmin_commit_distrib_id text
 *                , gp_xmax integer
 *                , gp_xmax_status text
 *                , gp_xmax_commit_distrib_id text
 *                , gp_command_id integer
 *                , gp_infomask text
 *                , gp_update_tid tid
 *                , gp_visibility text
 *                , segno integer
 *                , tupcount bigint
 *                , eof bigint
 *                , eof_uncompressed bigint
 *                )
 *   AS '$GPHOME/lib/aoseghistory', 'gp_aoseg_history_wrapper' LANGUAGE C STRICT;
 *
 */

#include "postgres.h"
#include <string.h>
#include "fmgr.h"
#include "funcapi.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern Datum
gp_aoseg_history(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(gp_aoseg_history_wrapper);

extern Datum
gp_aoseg_history_wrapper(PG_FUNCTION_ARGS);

Datum
gp_aoseg_history_wrapper(PG_FUNCTION_ARGS)
{
  Datum returnValue = gp_aoseg_history(fcinfo);

  PG_RETURN_DATUM(returnValue);
}

