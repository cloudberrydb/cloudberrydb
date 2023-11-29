/*-------------------------------------------------------------------------
 * execslot.h
 *	  support for the vectorization executor slot
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 *
 * IDENTIFICATION
 *		src/include/vecexecutor/execslot.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_EXEC_SLOT_H
#define VEC_EXEC_SLOT_H
#include "nodes/execnodes.h"
#include "executor/executor.h"
#include "executor/tuptable.h"

#include "utils/tuptable_vec.h"
#include "utils/arrow.h"

extern TupleTableSlot *ExecMaterializeSlotVec(TupleTableSlot *slot);
extern TupleTableSlot *ExecStoreAttrArray(TupleTableSlot *slot, GArrowArray **parray,
                                          bool isnull, bool shouldfree, int colindex);
extern TupleTableSlot *GenerateVecValue(VecDesc vecdesc, TupleTableSlot *out);
TupleTableSlot *ExecStoreBatch(TupleTableSlot *slot, void *batch);
extern void *DeserializeRecordBatch(GArrowBuffer *buffer, int64 buf_len, bool need_copy);
extern TupleTableSlot *ExecGetVirtualSlotFromVec(TupleTableSlot *vec_slot, int index,
		TupleTableSlot *out, ItemPointer itid);
extern void slot_getallattrs_vec(TupleTableSlot *slot);
extern GArrowRecordBatch *GetBatch(TupleTableSlot *slot);
extern TupleTableSlot *StoreEmptyTuples(TupleTableSlot *slot, int numtuples);
extern void *ExecGetBatchSlice(TupleTableSlot *slot, int64 start, int64 count);
extern TupleTableSlot *ExecImportABISlot(TupleTableSlot *virtualslot,
		TupleTableSlot *vecslot);

#endif							/* VEC_EXEC_SLOT_H */
