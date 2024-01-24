/*-------------------------------------------------------------------------
 *
 * vecfuncs.h
 *	  arrow functions tables.
 *
 * Portions Copyright (c) 2016-2020, HashData
 *
 * src/include/utils/vecfuncs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VECFUNCS_H
#define VECFUNCS_H

#include "postgres.h"
#include "catalog/pg_attribute.h"
#include "executor/tuptable.h"
#include "utils/arrow.h"

extern void *ArrowArrayNew(Oid type, int64 length, uint8 *data, uint8 *null_bitmap);
extern GArrowScalar *ArrowScalarNew(GArrowType type, Datum datum, Oid pg_type);
extern Datum ArrowArrayGetValueDatum(void *array, int i, Form_pg_attribute att);
extern Datum ArrowScalarGetDatum(void *scalar);
extern GArrowArray *ArrowScalarToArray(void *arrow_scalar_datum, int length);

extern int64 GetArrowArraySize(void *ptr_array);
extern void* SerializeRecordBatch(void *record_batch);
extern const void* GetSerializedRecordBatchDataBufDataAndSize(void* buffer, int64 *size);

extern void GandivaInitVecFilter(void *tup_des, void *qual, void **filter);

extern void *get_arrow_table_from_slot(TupleTableSlot* slot);
extern void *get_arrow_table_from_slot_slice(TupleTableSlot* slot, int offset, int num);
void ArrowArraysToBatchRecord(void *array,
                              void *schema,
                              int row_cnt,
                              int col_cnt,
                              void **record_batch);
#endif   /* VECFUNCS_H */
