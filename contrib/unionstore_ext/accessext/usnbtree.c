/*-------------------------------------------------------------------------
 *
 * nbtree.c
 *	  Implementation of Lehman and Yao's btree management algorithm for
 *	  Postgres.
 *
 * NOTES
 *	  This file contains only the public interface routines.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtree.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/nbtree.h"
#include "access/nbtxlog.h"
#include "access/relscan.h"
#include "access/xlog.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "storage/condition_variable.h"
#include "storage/indexfsm.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/index_selfuncs.h"
#include "utils/memutils.h"
#include "utils/guc.h"

#include "catalog/indexing.h"
#include "catalog/pg_namespace.h"

PG_FUNCTION_INFO_V1(usbthandler);
/*
 * Btree handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
Datum
usbthandler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

    amroutine->amstrategies = BTMaxStrategyNumber;
    amroutine->amsupport = BTNProcs;
    amroutine->amoptsprocnum = BTOPTIONS_PROC;
    amroutine->amcanorder = true;
    amroutine->amcanorderbyop = false;
    amroutine->amcanbackward = true;
    amroutine->amcanunique = true;
    amroutine->amcanmulticol = true;
    amroutine->amoptionalkey = true;
    amroutine->amsearcharray = true;
    amroutine->amsearchnulls = true;
    amroutine->amstorage = false;
    amroutine->amclusterable = true;
    amroutine->ampredlocks = true;
    amroutine->amcanparallel = true;
    amroutine->amcaninclude = true;
    amroutine->amusemaintenanceworkmem = false;
    amroutine->amparallelvacuumoptions =
            VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP;
    amroutine->amkeytype = InvalidOid;

    amroutine->ambuild = btbuild;
    amroutine->ambuildempty = btbuildempty;
    amroutine->aminsert = btinsert;
    amroutine->ambulkdelete = btbulkdelete;
    amroutine->amvacuumcleanup = btvacuumcleanup;
    amroutine->amcanreturn = btcanreturn;
    amroutine->amcostestimate = btcostestimate;
    amroutine->amoptions = btoptions;
    amroutine->amproperty = btproperty;
    amroutine->ambuildphasename = btbuildphasename;
    amroutine->amvalidate = btvalidate;
    amroutine->amadjustmembers = btadjustmembers;
    amroutine->ambeginscan = btbeginscan;
    amroutine->amrescan = btrescan;
    amroutine->amgettuple = btgettuple;
    amroutine->amgetbitmap = btgetbitmap;
    amroutine->amendscan = btendscan;
    amroutine->ammarkpos = btmarkpos;
    amroutine->amrestrpos = btrestrpos;
    amroutine->amestimateparallelscan = btestimateparallelscan;
    amroutine->aminitparallelscan = btinitparallelscan;
    amroutine->amparallelrescan = btparallelrescan;

    PG_RETURN_POINTER(amroutine);
}
