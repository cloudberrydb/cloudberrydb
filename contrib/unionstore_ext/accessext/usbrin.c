/*
 * brin.c
 *		Implementation of BRIN indexes for Postgres
 *
 * See src/backend/access/brin/README for details.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/brin/brin.c
 *
 * TODO
 *		* ScalarArrayOpExpr (amsearcharray -> SK_SEARCHARRAY)
 */
#include "postgres.h"

#include "access/aosegfiles.h"
#include "access/aocssegfiles.h"
#include "access/brin.h"
#include "access/brin_page.h"
#include "access/brin_pageops.h"
#include "access/brin_xlog.h"
#include "access/relation.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xloginsert.h"
#include "catalog/index.h"
#include "catalog/gp_fastsequence.h"
#include "catalog/pg_am.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgrprotos.h"
#include "utils/index_selfuncs.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/* GPDB includes */
#include "catalog/pg_appendonly.h"
#include "executor/executor.h"
#include "storage/procarray.h"
#include "utils/snapshot.h"

PG_FUNCTION_INFO_V1(usbrinhandler);
/*
 * BRIN handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
Datum
usbrinhandler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

    amroutine->amstrategies = 0;
    amroutine->amsupport = BRIN_LAST_OPTIONAL_PROCNUM;
    amroutine->amoptsprocnum = BRIN_PROCNUM_OPTIONS;
    amroutine->amcanorder = false;
    amroutine->amcanorderbyop = false;
    amroutine->amcanbackward = false;
    amroutine->amcanunique = false;
    amroutine->amcanmulticol = true;
    amroutine->amoptionalkey = true;
    amroutine->amsearcharray = false;
    amroutine->amsearchnulls = true;
    amroutine->amstorage = true;
    amroutine->amclusterable = false;
    amroutine->ampredlocks = false;
    amroutine->amcanparallel = false;
    amroutine->amcaninclude = false;
    amroutine->amusemaintenanceworkmem = false;
    amroutine->amparallelvacuumoptions =
            VACUUM_OPTION_PARALLEL_CLEANUP;
    amroutine->amkeytype = InvalidOid;

    amroutine->ambuild = brinbuild;
    amroutine->ambuildempty = brinbuildempty;
    amroutine->aminsert = brininsert;
    amroutine->ambulkdelete = brinbulkdelete;
    amroutine->amvacuumcleanup = brinvacuumcleanup;
    amroutine->amcanreturn = NULL;
    amroutine->amcostestimate = brincostestimate;
    amroutine->amoptions = brinoptions;
    amroutine->amproperty = NULL;
    amroutine->ambuildphasename = NULL;
    amroutine->amvalidate = brinvalidate;
    amroutine->amadjustmembers = NULL;
    amroutine->ambeginscan = brinbeginscan;
    amroutine->amrescan = brinrescan;
    amroutine->amgettuple = NULL;
    amroutine->amgetbitmap = bringetbitmap;
    amroutine->amendscan = brinendscan;
    amroutine->ammarkpos = NULL;
    amroutine->amrestrpos = NULL;
    amroutine->amestimateparallelscan = NULL;
    amroutine->aminitparallelscan = NULL;
    amroutine->amparallelrescan = NULL;

    PG_RETURN_POINTER(amroutine);
}
