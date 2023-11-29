/*-------------------------------------------------------------------------
 * aocs_vec.h
 *	  declare some vectorization-related functions
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 *
 * IDENTIFICATION
 *		src/include/utils/aocs_vec.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AOCS_VEC_H
#define AOCS_VEC_H

#include "cdb/cdbaocsam.h"
#include "arrow.h"
#include "access/relscan.h"


typedef struct VecAOCSScanDescData {
	AOCSScanDescData base_scan;

	/* for aocs_getnext_vec */
    VecDesc vecdes;
    bool islast;
	bool has_ctid;

	/* for late materialize */
	AttrNumber *proj_atts_lm;
	int	  num_proj_atts_lm;
	int   cur_seg_lm;

	AttrNumber *proj_atts_filter;
	int num_proj_atts_filter;
	int   cur_seg_filter;

	AttrNumber *proj_atts_full;
	int	  num_proj_atts_full;
	int64 base_row;
} VecAOCSScanDescData;

#define VECAOCSSCANDESCDATA(aocsscan) ((VecAOCSScanDescData*)aocsscan)

extern TableScanDesc aoco_beginscan_vec(Relation rel, Snapshot snapshot, List *targetlist, List *qual, uint32 flags);
extern AOCSScanDesc aocs_beginscan_vec(Relation relation, Snapshot snapshot, bool *proj, uint32 flags, bool has_ctid);
extern bool aoco_getnextslot_vec(TableScanDesc scan, ScanDirection direction, TupleTableSlot *slot);
extern bool aocs_getnext_vec(AOCSScanDesc scan, ScanDirection direction, TupleTableSlot *slot);
extern void aoco_endscan_vec(TableScanDesc scan);
extern void aocs_endscan_vec(AOCSScanDesc scan);

#endif   /* AOCS_VEC_H */
