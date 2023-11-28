/*-------------------------------------------------------------------------
 *
 * gist.c
 *	  interface routines for the postgres GiST index access method.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/gist/gist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/gist_private.h"
#include "access/gistscan.h"
#include "catalog/pg_collation.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "utils/builtins.h"
#include "utils/index_selfuncs.h"
#include "utils/memutils.h"
#include "utils/rel.h"

PG_FUNCTION_INFO_V1(usgisthandler);
/*
 * GiST handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
Datum
usgisthandler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

    amroutine->amstrategies = 0;
    amroutine->amsupport = GISTNProcs;
    amroutine->amoptsprocnum = GIST_OPTIONS_PROC;
    amroutine->amcanorder = false;
    amroutine->amcanorderbyop = true;
    amroutine->amcanbackward = false;
    amroutine->amcanunique = false;
    amroutine->amcanmulticol = true;
    amroutine->amoptionalkey = true;
    amroutine->amsearcharray = false;
    amroutine->amsearchnulls = true;
    amroutine->amstorage = true;
    amroutine->amclusterable = true;
    amroutine->ampredlocks = true;
    amroutine->amcanparallel = false;
    amroutine->amcaninclude = true;
    amroutine->amusemaintenanceworkmem = false;
    amroutine->amparallelvacuumoptions =
            VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP;
    amroutine->amkeytype = InvalidOid;

    amroutine->ambuild = gistbuild;
    amroutine->ambuildempty = gistbuildempty;
    amroutine->aminsert = gistinsert;
    amroutine->ambulkdelete = gistbulkdelete;
    amroutine->amvacuumcleanup = gistvacuumcleanup;
    amroutine->amcanreturn = gistcanreturn;
    amroutine->amcostestimate = gistcostestimate;
    amroutine->amoptions = gistoptions;
    amroutine->amproperty = gistproperty;
    amroutine->ambuildphasename = NULL;
    amroutine->amvalidate = gistvalidate;
    amroutine->amadjustmembers = gistadjustmembers;
    amroutine->ambeginscan = gistbeginscan;
    amroutine->amrescan = gistrescan;
    amroutine->amgettuple = gistgettuple;
    amroutine->amgetbitmap = gistgetbitmap;
    amroutine->amendscan = gistendscan;
    amroutine->ammarkpos = NULL;
    amroutine->amrestrpos = NULL;
    amroutine->amestimateparallelscan = NULL;
    amroutine->aminitparallelscan = NULL;
    amroutine->amparallelrescan = NULL;

    PG_RETURN_POINTER(amroutine);
}