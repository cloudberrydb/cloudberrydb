/*-------------------------------------------------------------------------
 *
 * execSlot.c
 *	  Implement slot interacts with arrow.
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/backend/vecexecutor/execSlot.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "executor/tuptable.h"
#include "utils/builtins.h"

#include "utils/tuptable_vec.h"
#include "utils/wrapper.h"
#include "utils/vecfuncs.h"
#include "vecexecutor/execslot.h"
#include "vecexecutor/executor.h"
#include "arrow/c/abi.h"

static inline void slot_getsomeattrs_vec(TupleTableSlot *slot, int attnum);

/*
 * Form record batch from arrow arrays.
 *
 * Force release the previous batch in slot.
 */
TupleTableSlot *
ExecMaterializeSlotVec(TupleTableSlot *slot)
{
	g_autoptr(GArrowRecordBatch) batch = NULL;
	/* No need g_autoptr(GArrowNullArray) here, special case for NULLArray reuse. */
	GArrowNullArray *single_nullarray = NULL;
	int ncolumns, i, nrows = 0;
	GList *columns = NULL;
	GList *node = NULL;
	GError *error = NULL;

	if (TupIsNull(slot))
		return NULL;

	Assert(TTS_IS_VECTOR(slot));

	if(!TTS_IS_DIRTY(slot))
		return slot;

	ncolumns = slot->tts_tupleDescriptor->natts;

	for (i = 0; i < ncolumns; i++)
	{
		g_autoptr(GArrowArray) array = NULL;
		gint64 length;

		if (slot->tts_values[i])
		{
			array = GARROW_ARRAY(garrow_copy_ptr((void *)slot->tts_values[i]));
			length = garrow_array_get_length(array);

			if (nrows <= 0)
				nrows = length;
			else if(nrows != length)
				elog(ERROR, "array length dismatch for batch, column id: %d", i);
		}

		columns = garrow_list_append_ptr(columns, array);
	}

	if (nrows == 0)
	{
		ExecClearTuple(slot);
		garrow_list_free_ptr(&columns);

		return NULL;
	}

	/* form GArrayNullArray for empty columns */
	for (node = columns; node; node = node->next)
	{
		if (node->data == NULL)
		{
			if (single_nullarray == NULL)
				single_nullarray = garrow_null_array_new(nrows);

			node->data = single_nullarray;
			g_object_ref(single_nullarray);
		}
	}

	batch = garrow_record_batch_new(GetSchemaFromSlot(slot),
									nrows, columns, &error);
	if (error)
		elog(ERROR, "Error when forming recored batch from vector slot: %s",
				error->message);

	slot->tts_nvalid = ncolumns;
	garrow_store_ptr(VECSLOT(slot)->tts_recordbatch, batch);
	TTS_CLEAR_EMPTY(slot);
	TTS_CLEAR_DIRTY(slot);

	garrow_list_free_ptr(&columns);

	return slot;
}


/*
 * Store a single array into vector slot.
 *
 * Store a GArrowArray, the array will be moved.
 *
 * colindex: index of column, start from 0.
 * shouldFree: Whether this slot should free this array? Means that this slot owns the array,
 *             and should free array when cleared or released.
 * 	           If the slot is copy from other vector slot shouldfree should be false,
 * 	           others, should be true.
 *
 * Notice that storing array into vector slot will make slot dirty.
 */
TupleTableSlot *
ExecStoreAttrArray(TupleTableSlot *slot, GArrowArray **parray,
				   bool isnull, bool shouldfree, int colindex)
{

	Assert(TTS_IS_VECTOR(slot));
	Assert(slot->tts_values != NULL);
	Assert(colindex < slot->tts_tupleDescriptor->natts);

	/* storing NULL array to emptytuples slot is meaningless */
	if (*parray == NULL && (IsContainEmptyTuples(slot) ||
						  slot->tts_values[colindex] == (Datum)0))
		return slot;

	Assert(*parray == NULL || GARROW_IS_ARRAY(*parray));

	free_array((void **)&slot->tts_values[colindex]);

	slot->tts_values[colindex] = (Datum) g_steal_pointer(parray);
	slot->tts_isnull[colindex] = isnull;

	if (colindex >= slot->tts_nvalid)
		slot->tts_nvalid = colindex + 1;

	TTS_SET_DIRTY(slot);
	TTS_CLEAR_EMPTY(slot);

	return slot;
}

/*
 * Generate data to virtual slot from VecDesc.
 */
TupleTableSlot *
GenerateVecValue(VecDesc vecdesc, TupleTableSlot *out)
{
	int i = 0;
	gint64 length = 0;
	GList *arrays = NULL;
	VirtualTupleTableSlot *vslot = (VirtualTupleTableSlot *) out;
	bool ret;
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowRecordBatch) batch = NULL;

	Assert(TTS_IS_VIRTUAL(out));
	ExecClearTuple(out);

	for (i = 0; i < vecdesc->natts; i++)
	{
		ColDesc *coldesc = vecdesc->coldescs[i];
		if (coldesc)
		{
			if (!coldesc->is_lm)
			{
				g_autoptr(GArrowArray) aw_array = NULL;

				aw_array = GARROW_ARRAY(FinishColDesc(coldesc));
				length = garrow_array_get_length(GARROW_ARRAY(aw_array));
				arrays = garrow_list_append_ptr(arrays, aw_array);

				if (coldesc->is_pos_recorder)
				{
					g_autoptr(GArrowArray) tmp_array = NULL;
					tmp_array = FinishPosColDesc(coldesc);
					garrow_store_ptr(vecdesc->selective, tmp_array);
				}
			}
		}
	}
	batch = garrow_record_batch_new(vecdesc->schema, length, arrays, &error);
	garrow_list_free_ptr(&arrays);
	if (error)
		elog(ERROR, "Record batch new for GenerateVecValue failed: %s", error->message);
	vslot->data = (char *)&vecdesc->abi_batch;
	ret = garrow_abi_record_batch_export(batch, (void**) &vslot->data, &error);
	if (ret == false || error)
		elog(ERROR, "Record batch export for GenerateVecValue failed: %s", error->message);
	/* Mark slot as valid.*/
	TTS_CLEAR_EMPTY(out);
	TTS_CLEAR_DIRTY(out);

	/* At least, we have read visbitmapoffset.
	 * Release until visbitmapoffset falls in first visimap. */
	while (vecdesc->visbitmapoffset - 1> APPENDONLY_VISIMAP_MAX_RANGE)
	{
		vecdesc->visbitmaps = list_delete_first(vecdesc->visbitmaps);
		vecdesc->visbitmapoffset -= APPENDONLY_VISIMAP_MAX_RANGE;
		vecdesc->firstrow += APPENDONLY_VISIMAP_MAX_RANGE;
	}

	return out;
}

/*
 * Get a non-vector virtual tuple slot from a vectorial slot by index.
 *
 * out: The non-vector slot to store the result.
 */
TupleTableSlot *
ExecGetVirtualSlotFromVec(TupleTableSlot *vec_slot, int index, TupleTableSlot *out, ItemPointer itid)
{
	int i;

	Assert(TTS_IS_VECTOR(vec_slot));
	Assert(!TTS_IS_VECTOR(out));

	/* invalid tts_values[], fetch them first */
	if (vec_slot->tts_nvalid == 0)
		slot_getallattrs_vec(vec_slot);

	/* no more tuples */
	if (index >= GetNumRows(vec_slot))
		return NULL;

	ExecClearTuple(out);
	out->tts_nvalid = vec_slot->tts_nvalid;
	for (i = 0; i < out->tts_nvalid; i++)
	{
		GArrowArray *array;

		array = (GArrowArray *)vec_slot->tts_values[i];
		out->tts_isnull[i] = garrow_array_is_null(array, index);
		out->tts_values[i] = ArrowArrayGetValueDatum(array, index);
		/* print ctid */
		if (vec_slot->tts_tupleDescriptor->attrs[i].atttypid == TIDOID)
		{
			int64 data = DatumGetInt64(out->tts_values[i]);
			BlockNumber bk = data >> 16;
			/**
			 * hi   lo | posid
			 *    32   | 16
			 * 16 | 16 | 16
			 */
    		itid->ip_blkid.bi_hi = (uint16) ((bk >> 16) & 0xFFFF);  
			itid->ip_blkid.bi_lo = (uint16) (bk & 0xFFFF);       
			itid->ip_posid = (OffsetNumber) (data & 0xFFFF);
			out->tts_values[i] = ItemPointerGetDatum(itid);
		}

	}
	return ExecStoreVirtualTuple(out);
}

/*
 * This function forces the entries of the slot's Datum/isnull arrays to be
 * valid at least up through the attnum'th entry.
 */
static inline void
slot_getsomeattrs_vec(TupleTableSlot *slot, int attnum)
{
	if (slot->tts_nvalid < attnum)
		slot->tts_ops->getsomeattrs(slot, attnum);
}


/*
 * slot_getallattrs_vec
 *  Extract all arrays from record batch to tts_values.
 */
void
slot_getallattrs_vec(TupleTableSlot *slot)
{
	slot_getsomeattrs_vec(slot, slot->tts_tupleDescriptor->natts);
}

void *
ExecGetBatchSlice(TupleTableSlot *slot, int64 start, int64 count)
{
	if (TupIsNull(slot))
		return NULL;

	int rows = GetNumRows(slot);
	if (start < 0 || count < 1 || (start + count) > rows)
		elog(ERROR, "out of range for the array, start: %lld, count: %lld, "
			"row in slot: %d", (long long int)start, (long long int)count, rows);

	return (void *)garrow_record_batch_slice((GArrowRecordBatch *)VECSLOT(slot)->tts_recordbatch, start, count);
}


/*
 * ExecStoreBatch
 *     Store a vector record batch to vector slot.
 *
 *	   batch: GArrowRecordBatch type.
 *
 *     The tts_values will be set at the same time, pointing
 *     to each arrays in the record batch. A vector slot
 *     must free the memory when itself is cleared up.
 */
TupleTableSlot*
ExecStoreBatch(TupleTableSlot *slot, void *batch)
{
	VecTupleTableSlot* vslot = (VecTupleTableSlot*) slot;
	if (slot == NULL)
	{
		elog(ERROR, "Invalid slot when storing a vectorial tuple");
		return NULL;
	}
	if (batch == NULL)
	{
		return ExecClearTuple(slot);
	}

	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowRecordBatch) recordbatch = GARROW_RECORD_BATCH(garrow_copy_ptr(batch));

	/* avoid same value repeatly setting */
	if (batch == vslot->tts_recordbatch)
		return slot;

	slot = ExecClearTuple(slot);

	/* need not rebuild schema for null recordbatch */
	garrow_store_ptr(vslot->tts_recordbatch, recordbatch);

	slot_getallattrs_vec(slot);

	TTS_CLEAR_EMPTY(slot);
	TTS_CLEAR_DIRTY(slot);

	RebuildRuntimeSchema(slot);
	return slot;
}

/* Store an batch which contains no data but only count of rows.*/
TupleTableSlot *
StoreEmptyTuples(TupleTableSlot *slot, int numtuples)
{
	VecTupleTableSlot* vslot = (VecTupleTableSlot*) slot;
	g_autoptr(GArrowRecordBatch) batch = NULL;
	g_autoptr(GArrowSchema) schema = NULL;
	g_autoptr(GArrowNullArray) nullarray = NULL;
	GList *columns = NULL;
	GError *error = NULL;

	ExecClearTuple(slot);

	if (numtuples == 0)
		return slot;

	nullarray = garrow_null_array_new(numtuples);
	columns = garrow_list_append_ptr(columns, nullarray);
	schema = garrow_copy_ptr(GetDummySchema());
	batch = garrow_record_batch_new(schema,
									numtuples, columns, &error);
	garrow_list_free_ptr(&columns);
	if (error)
		elog(ERROR, "Error when store empty batch from vector slot: %s",
				error->message);
	garrow_store_ptr(vslot->tts_recordbatch, batch);

	TTS_CLEAR_EMPTY(slot);

	return slot;
}

/*
 * Get GArrowRecordBatch from vector slot.
 */
GArrowRecordBatch *
GetBatch(TupleTableSlot *slot)
{
	if (TupIsNull(slot))
		return NULL;

	Assert(TTS_IS_VECTOR(slot));
	Assert(!TTS_IS_DIRTY(slot));
	return (GArrowRecordBatch *)VECSLOT(slot)->tts_recordbatch;
}

void *
DeserializeRecordBatch(GArrowBuffer *buffer, int64 buf_len, bool need_copy)
{
	g_autoptr(GArrowBufferInputStream) buffer_stream = NULL;
	g_autoptr(GArrowRecordBatchReader) reader = NULL;

	g_autoptr(GArrowRecordBatch) batch = NULL;
	GError *error = NULL;

	if (need_copy)
	{
		g_autoptr(GArrowBuffer) copy_buffer;
		g_autoptr(GBytes) old_gbytes;
		g_autoptr(GBytes) new_gbytes;
		void *old_data;
		void *new_data;
		gsize data_size;
		buf_len = garrow_buffer_get_size(buffer);
		if (error)
		{
			elog(ERROR, "Cann't get buffer length caused by : %s",
					error->message);
		}
		copy_buffer = garrow_buffer_new_default_mem(buf_len, &error);
		old_gbytes = garrow_buffer_get_data(buffer);
		old_data = (void *) g_bytes_get_data(old_gbytes, &data_size);
		new_gbytes = garrow_buffer_get_data(copy_buffer);
		new_data = (void *) g_bytes_get_data(new_gbytes, &data_size);
		memcpy(new_data, old_data, data_size);
		buffer_stream = garrow_buffer_input_stream_new(copy_buffer);
	}
	else
	{
		buffer_stream = garrow_buffer_input_stream_new(buffer);
	}
	reader = (GArrowRecordBatchReader *)garrow_record_batch_stream_reader_new(
		GARROW_INPUT_STREAM(buffer_stream), &error);
	if (error)
	{
		elog(ERROR, "Cann't get reader from buffer stream: %s",
				error->message);
	}

	batch = garrow_record_batch_reader_read_next(reader, &error);
	if (error)
	{
		elog(ERROR, "Cann't read record batch from minimal tuple: %s",
				error->message);
	}	
	return (void*)garrow_move_ptr(batch);
}

/* Import a VirtualTupleTableSlot which contains ABI ArrowRecordBatch as data
 * to another VecTupleTableSlot.
 * Notice that we can never convert a VirtualTupleTableSlot to VecTupleTableSlot.
 * Return the imported VecTupleTableSlot. */
TupleTableSlot *
ExecImportABISlot(TupleTableSlot *virtualslot, TupleTableSlot *vecslot)
{
	VirtualTupleTableSlot *slot = (VirtualTupleTableSlot *)virtualslot;
	struct ArrowRecordBatch *abibatch;
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowRecordBatch) batch = NULL;

	if (TupIsNull(virtualslot))
		return ExecClearTuple(vecslot);
	abibatch = (struct ArrowRecordBatch *) slot->data;
	if (!abibatch)
		elog(ERROR, "Got empty data from non-null slot!");
	batch = garrow_abi_record_batch_import(abibatch, &error);
	if (error)
		elog(ERROR, "Import abi batch failed: %s", error->message);
	return ExecStoreBatch(vecslot, batch);
}
