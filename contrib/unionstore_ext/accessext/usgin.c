/*-------------------------------------------------------------------------
 *
 * ginutil.c
 *	  Utility routines for the Postgres inverted index access method.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/gin/ginutil.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/gin_private.h"
#include "access/ginxlog.h"
#include "access/reloptions.h"
#include "access/xloginsert.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "utils/builtins.h"
#include "utils/index_selfuncs.h"
#include "utils/typcache.h"

PG_FUNCTION_INFO_V1(usginhandler);
/*
 * GIN handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
Datum
usginhandler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

    amroutine->amstrategies = 0;
    amroutine->amsupport = GINNProcs;
    amroutine->amoptsprocnum = GIN_OPTIONS_PROC;
    amroutine->amcanorder = false;
    amroutine->amcanorderbyop = false;
    amroutine->amcanbackward = false;
    amroutine->amcanunique = false;
    amroutine->amcanmulticol = true;
    amroutine->amoptionalkey = true;
    amroutine->amsearcharray = false;
    amroutine->amsearchnulls = false;
    amroutine->amstorage = true;
    amroutine->amclusterable = false;
    amroutine->ampredlocks = true;
    amroutine->amcanparallel = false;
    amroutine->amcaninclude = false;
    amroutine->amusemaintenanceworkmem = true;
    amroutine->amparallelvacuumoptions =
            VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_CLEANUP;
    amroutine->amkeytype = InvalidOid;

    amroutine->ambuild = ginbuild;
    amroutine->ambuildempty = ginbuildempty;
    amroutine->aminsert = gininsert;
    amroutine->ambulkdelete = ginbulkdelete;
    amroutine->amvacuumcleanup = ginvacuumcleanup;
    amroutine->amcanreturn = NULL;
    amroutine->amcostestimate = gincostestimate;
    amroutine->amoptions = ginoptions;
    amroutine->amproperty = NULL;
    amroutine->ambuildphasename = NULL;
    amroutine->amvalidate = ginvalidate;
    amroutine->amadjustmembers = ginadjustmembers;
    amroutine->ambeginscan = ginbeginscan;
    amroutine->amrescan = ginrescan;
    amroutine->amgettuple = NULL;
    amroutine->amgetbitmap = gingetbitmap;
    amroutine->amendscan = ginendscan;
    amroutine->ammarkpos = NULL;
    amroutine->amrestrpos = NULL;
    amroutine->amestimateparallelscan = NULL;
    amroutine->aminitparallelscan = NULL;
    amroutine->amparallelrescan = NULL;

    PG_RETURN_POINTER(amroutine);
}
