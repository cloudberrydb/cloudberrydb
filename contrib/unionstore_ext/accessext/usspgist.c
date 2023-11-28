/*-------------------------------------------------------------------------
 *
 * spgutils.c
 *	  various support functions for SP-GiST
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/spgist/spgutils.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/amvalidate.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/spgist_private.h"
#include "access/toast_compression.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_amop.h"
#include "commands/vacuum.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_coerce.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/index_selfuncs.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

PG_FUNCTION_INFO_V1(usspghandler);
/*
 * SP-GiST handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
Datum
usspghandler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

    amroutine->amstrategies = 0;
    amroutine->amsupport = SPGISTNProc;
    amroutine->amoptsprocnum = SPGIST_OPTIONS_PROC;
    amroutine->amcanorder = false;
    amroutine->amcanorderbyop = true;
    amroutine->amcanbackward = false;
    amroutine->amcanunique = false;
    amroutine->amcanmulticol = false;
    amroutine->amoptionalkey = true;
    amroutine->amsearcharray = false;
    amroutine->amsearchnulls = true;
    amroutine->amstorage = true;
    amroutine->amclusterable = false;
    amroutine->ampredlocks = false;
    amroutine->amcanparallel = false;
    amroutine->amcaninclude = true;
    amroutine->amusemaintenanceworkmem = false;
    amroutine->amparallelvacuumoptions =
            VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP;
    amroutine->amkeytype = InvalidOid;

    amroutine->ambuild = spgbuild;
    amroutine->ambuildempty = spgbuildempty;
    amroutine->aminsert = spginsert;
    amroutine->ambulkdelete = spgbulkdelete;
    amroutine->amvacuumcleanup = spgvacuumcleanup;
    amroutine->amcanreturn = spgcanreturn;
    amroutine->amcostestimate = spgcostestimate;
    amroutine->amoptions = spgoptions;
    amroutine->amproperty = spgproperty;
    amroutine->ambuildphasename = NULL;
    amroutine->amvalidate = spgvalidate;
    amroutine->amadjustmembers = spgadjustmembers;
    amroutine->ambeginscan = spgbeginscan;
    amroutine->amrescan = spgrescan;
    amroutine->amgettuple = spggettuple;
    amroutine->amgetbitmap = spggetbitmap;
    amroutine->amendscan = spgendscan;
    amroutine->ammarkpos = NULL;
    amroutine->amrestrpos = NULL;
    amroutine->amestimateparallelscan = NULL;
    amroutine->aminitparallelscan = NULL;
    amroutine->amparallelrescan = NULL;

    PG_RETURN_POINTER(amroutine);
}