/*-------------------------------------------------------------------------
 * am_vec.h
 *	  declare some methods related to AM
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 *
 * IDENTIFICATION
 *		src/include/utils/am_vec.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_AM_H
#define VEC_AM_H

#include "access/tableam.h"

typedef enum VecScanOptions {
    SO_TYPE_VECTOR = 1 << 12,
    SO_TYPE_HAS_CTID = 1 << 13
} VecScanOptions;

extern TableAmRoutine *GetVecTableAmRoutine(Oid amhandler);
extern void InitAOCSVecHandler(void);
typedef TableScanDesc (*scan_begin) (Relation rel,
									 Snapshot snapshot,
									 ParallelTableScanDesc parallel_scan,
									 List *targetlist,
									 List *qual,
									 uint32 flags);

typedef bool (*scan_getnextslot)(TableScanDesc scan,
                                 ScanDirection direction,
                                 TupleTableSlot *slot);
typedef void (*scan_end)(TableScanDesc scan);
extern scan_begin scan_begin_prev;                        
extern scan_getnextslot scan_getnextslot_prev;                        
extern scan_end scan_end_prev;

extern TableScanDesc aoco_scan_begin_wrapper(Relation rel,
                                             Snapshot snapshot,
											 ParallelTableScanDesc parallel_scan,
                                             List *targetlist,
                                             List *qual,
                                             uint32 flags);
extern bool aoco_getnextslot_wrapper(TableScanDesc scan, ScanDirection direction, TupleTableSlot *slot);
extern void aoco_endscan_wrapper(TableScanDesc scan);

extern TableScanDesc
table_beginscan_es_vec(Relation rel, Snapshot snapshot, List *targetList, List *qual, uint32 flags);

#endif   /* VEC_AM_H */
