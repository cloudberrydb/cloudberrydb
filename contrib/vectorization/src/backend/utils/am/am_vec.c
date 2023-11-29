/*-------------------------------------------------------------------------
 *
 * am_vec.c
 *   implement some methods related to AM
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/backend/utils/am/am_vec.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "utils/am_vec.h"
#include "utils/aocs_vec.h"
#include "utils/vecfuncs.h"

scan_begin scan_begin_prev = NULL;                        
scan_getnextslot scan_getnextslot_prev = NULL;    
scan_end scan_end_prev = NULL;

void
InitAOCSVecHandler() 
{
	TableAmRoutine *aocs_routine = NULL;

	aocs_routine = GetVecTableAmRoutine(F_AO_COLUMN_TABLEAM_HANDLER);
    scan_begin_prev = aocs_routine->scan_begin_extractcolumns;
	aocs_routine->scan_begin_extractcolumns = aoco_scan_begin_wrapper;
	scan_getnextslot_prev = aocs_routine->scan_getnextslot;
    aocs_routine->scan_getnextslot = aoco_getnextslot_wrapper;
}

TableAmRoutine*
GetVecTableAmRoutine(Oid amhandler)
{
	Datum		datum;
	TableAmRoutine *routine;

	datum = OidFunctionCall0(amhandler);
	routine = (TableAmRoutine *) DatumGetPointer(datum);

	if (routine == NULL || !IsA(routine, TableAmRoutine))
		elog(ERROR, "table access method handler %u did not return a TableAmRoutine struct",
			 amhandler);

	/*
	 * Assert that all required callbacks are present. That makes it a bit
	 * easier to keep AMs up to date, e.g. when forward porting them to a new
	 * major version.
	 */
	Assert(routine->scan_begin != NULL);
	Assert(routine->scan_end != NULL);
	Assert(routine->scan_rescan != NULL);
	Assert(routine->scan_getnextslot != NULL);

	Assert(routine->parallelscan_estimate != NULL);
	Assert(routine->parallelscan_initialize != NULL);
	Assert(routine->parallelscan_reinitialize != NULL);

	Assert(routine->index_fetch_begin != NULL);
	Assert(routine->index_fetch_reset != NULL);
	Assert(routine->index_fetch_end != NULL);
	Assert(routine->index_fetch_tuple != NULL);

	Assert(routine->tuple_fetch_row_version != NULL);
	Assert(routine->tuple_tid_valid != NULL);
	Assert(routine->tuple_get_latest_tid != NULL);
	Assert(routine->tuple_satisfies_snapshot != NULL);
	Assert(routine->index_delete_tuples != NULL);

	Assert(routine->tuple_insert != NULL);

	/*
	 * Could be made optional, but would require throwing error during
	 * parse-analysis.
	 */
	Assert(routine->tuple_insert_speculative != NULL);
	Assert(routine->tuple_complete_speculative != NULL);

	Assert(routine->multi_insert != NULL);
	Assert(routine->tuple_delete != NULL);
	Assert(routine->tuple_update != NULL);
	Assert(routine->tuple_lock != NULL);

	Assert(routine->relation_set_new_filenode != NULL);
	Assert(routine->relation_nontransactional_truncate != NULL);
	Assert(routine->relation_copy_data != NULL);
	Assert(routine->relation_copy_for_cluster != NULL);
	Assert(routine->relation_vacuum != NULL);
	Assert(routine->scan_analyze_next_block != NULL);
	Assert(routine->scan_analyze_next_tuple != NULL);
	Assert(routine->index_build_range_scan != NULL);
	Assert(routine->index_validate_scan != NULL);

	Assert(routine->relation_size != NULL);
	Assert(routine->relation_needs_toast_table != NULL);

	Assert(routine->relation_estimate_size != NULL);

	/* optional, but one callback implies presence of the other */
	Assert((routine->scan_bitmap_next_block == NULL) ==
		   (routine->scan_bitmap_next_tuple == NULL));
	Assert(routine->scan_sample_next_block != NULL);
	Assert(routine->scan_sample_next_tuple != NULL);

	return routine;
}

/*
 * GPDB: Like table_beginscan_es(), but vectorization-specific flags can be set
 */
TableScanDesc
table_beginscan_es_vec(Relation rel, Snapshot snapshot,
					   List *targetList, List *qual, uint32 flags)
{
	flags |= SO_TYPE_SEQSCAN |
	SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE;

	if (rel->rd_tableam->scan_begin_extractcolumns)
		return rel->rd_tableam->scan_begin_extractcolumns(rel, snapshot, NULL,
														  targetList, qual,
														  flags);

	return rel->rd_tableam->scan_begin(rel, snapshot,
									   0, NULL,
									   NULL, flags);
}

TableScanDesc
aoco_scan_begin_wrapper(Relation rel,
						Snapshot snapshot,
					    ParallelTableScanDesc parallel_scan,
						List *targetlist,
						List *qual,
						uint32 flags)
{

	/* Vectorization aocs scan */
	if (flags & SO_TYPE_VECTOR)
	{
		return aoco_beginscan_vec(rel, snapshot, targetlist, qual, flags);
	}
	else
	{
		return scan_begin_prev(rel, snapshot, parallel_scan, targetlist, qual, flags);
	}
}

bool
aoco_getnextslot_wrapper(TableScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{
	/* Vectorization aocs scan */
	if (scan->rs_flags & SO_TYPE_VECTOR) 
	{
		return aoco_getnextslot_vec(scan, direction, slot);
	} 
	else 
	{
		return scan_getnextslot_prev(scan, direction, slot);
	}
}

void
aoco_endscan_wrapper(TableScanDesc scan)
{
	/* Vectorization aocs scan */
	if (scan->rs_flags & SO_TYPE_VECTOR) 
	{
		aoco_endscan_vec(scan);
	} 
	else 
	{
		scan_end_prev(scan);
	}
}
