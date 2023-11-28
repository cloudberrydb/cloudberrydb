/*-------------------------------------------------------------------------
*
* bitmap.c
        *        Implementation of the Hybrid Run-Length (HRL) on-disk bitmap index.
*
* Portions Copyright (c) 2007-2010 Cloudberry Inc
* Portions Copyright (c) 2010-2012 EMC Corporation
* Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
* Portions Copyright (c) 2006-2008, PostgreSQL Global Development Group
*
*
* IDENTIFICATION
        *        src/backend/access/bitmap/bitmap.c
        *
        * NOTES
*      This file contains only the public interface routines.
*
*-------------------------------------------------------------------------
*/

#include "postgres.h"

#include "access/bitmap.h"
#include "access/amapi.h"
#include "access/amvalidate.h"
#include "access/genam.h"
#include "access/bitmap.h"
#include "access/bitmap_private.h"
#include "access/reloptions.h"
#include "access/nbtree.h"              /* for btree_or_bitmap_validate() */
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_opclass.h"
#include "miscadmin.h"
#include "nodes/tidbitmap.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "parser/parse_oper.h"
#include "utils/memutils.h"
#include "utils/index_selfuncs.h"
#include "utils/syscache.h"

#include "nodes/execnodes.h"

PG_FUNCTION_INFO_V1(usbmhandler);
Datum
usbmhandler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

    /* these are mostly the same as B-tree */
    amroutine->amstrategies = BTMaxStrategyNumber;
    amroutine->amsupport = BTNProcs;
    amroutine->amcanorder = false;
    amroutine->amcanorderbyop = false;
    amroutine->amcanbackward = false;
    amroutine->amcanunique = true;
    amroutine->amcanmulticol = true;
    amroutine->amcanparallel = false;
    amroutine->amoptionalkey = true;
    amroutine->amsearcharray = false;
    amroutine->amsearchnulls = false;
    amroutine->amstorage = false;
    amroutine->amclusterable = false;
    amroutine->ampredlocks = false;
    amroutine->amkeytype = InvalidOid;

    amroutine->ambuild = bmbuild;
    amroutine->ambuildempty = bmbuildempty;
    amroutine->aminsert = bminsert;
    amroutine->ambulkdelete = bmbulkdelete;
    amroutine->amvacuumcleanup = bmvacuumcleanup;
    amroutine->amcanreturn = NULL;
    amroutine->amcostestimate = bmcostestimate;
    amroutine->amoptions = bmoptions;
    amroutine->amproperty = NULL;
    amroutine->amvalidate = bmvalidate;
	amroutine->ambeginscan = bmbeginscan;
	amroutine->amrescan = bmrescan;
	amroutine->amgettuple = bmgettuple;
	amroutine->amgetbitmap = bmgetbitmap;
	amroutine->amendscan = bmendscan;
	amroutine->ammarkpos = bmmarkpos;
	amroutine->amrestrpos = bmrestrpos;

    PG_RETURN_POINTER(amroutine);
}
