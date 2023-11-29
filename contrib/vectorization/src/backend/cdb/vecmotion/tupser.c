/*-------------------------------------------------------------------------
 * vectupser.c
 *	   Functions for serializing and deserializing a heap tuple.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/motion/vectupser.c
 *
 *      Reviewers: jzhang, ftian, tkordas
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/memtup.h"
#include "access/htup.h"
#include "catalog/pg_type.h"
#include "cdb/cdbmotion.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbvars.h"
#include "cdb/tupser.h"
#include "libpq/pqformat.h"
#include "storage/smgr.h"
#include "utils/acl.h"
#include "utils/date.h"
#include "utils/numeric.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#include "utils/arrow.h"
#include "utils/wrapper.h"
#include "cdb/vectupser.h"
#include "utils/vecfuncs.h"
#include "utils/tuptable_vec.h"
#include "vecexecutor/execslot.h"
#include "vecexecutor/executor.h"

int SerializeTupleVec(TupleTableSlot *slot,
                      SerTupInfo *pSerInfo,
                      struct directTransportBuffer *b,
                      TupleChunkList tcList,
                      int16 targetRoute)
{
	int64 			buf_size = 0;
	void 			*buffer = NULL;
	TupleTableSlot 	*tmpslot;
	int 			sent;
	Datum		 	*values;
	bool 			*isnull;
	const void 		*data_pos;
	VarBuffer	   	*input;
	int 			sz;
	/* convert array to record batch */
	Assert(VECSLOT(slot)->tts_recordbatch);

	/* serielized the record batch */
	buffer = SerializeRecordBatch(GetBatch(slot));
	Assert(buffer);

	data_pos = GetSerializedRecordBatchDataBufDataAndSize(buffer, &buf_size);

	tmpslot = MakeTupleTableSlot(pSerInfo->tupdesc, &TTSOpsMinimalTuple);;
	ExecClearTuple(tmpslot);
	values = tmpslot->tts_values;
	isnull = tmpslot->tts_isnull;

	sz =  VARHDRSZ + PADSIZE + buf_size;
	input = (VarBuffer *) palloc(sz);
	/* pad + data */
	SET_VARSIZE(input, PADSIZE + buf_size);
	memcpy((void *) VARBUFFER(input), data_pos, buf_size);

	values[0] = PointerGetDatum(input);
	isnull[0] = false;
	ExecStoreVirtualTuple(tmpslot);

	/* do serialize */
	sent = SerializeTuple(tmpslot, pSerInfo, b, tcList, targetRoute);

	free_array_buffer(&buffer);
	pfree(input);
	pfree(tmpslot);

	return sent;
}

/*
 * Store a VecTuple into vector slot.
 *
 */
TupleTableSlot *
ExecStoreMinimalTupleTupleVec(MinimalTuple tup, TupleTableSlot *slot, bool shouldFree)
{
	g_autoptr(GArrowRecordBatch) batch = NULL;
	batch = DeserializeMinimalTuple(tup, false);
	ExecStoreBatch(slot, batch);

	/* release memory */
	if (shouldFree)
	{
		TTS_SET_SHOULDFREE(slot);
	}
	else
	{
		TTS_CLEAR_SHOULDFREE(slot);
	}

	return slot;
}

 MinimalTuple
ExecCopySlotMinimalTupleVec(TupleTableSlot *slot)
{
	int64 			buf_size = 0;
	void 			*buffer = NULL;
	TupleTableSlot 	*tmpslot;
	Datum		 	*values;
	bool 			*isnull;
	const void 		*data_pos;
	VarBuffer	   	*input;
	int 			sz;
	MinimalTuple   copied_mtup;
	TupleDesc 	   tmpTupDesc;

	/* convert array to record batch */
	Assert(VECSLOT(slot)->tts_recordbatch);

	/* serielized the record batch */
	buffer = SerializeRecordBatch(GetBatch(slot));
	Assert(buffer);

	data_pos = GetSerializedRecordBatchDataBufDataAndSize(buffer, &buf_size);

	tmpTupDesc = CreateTemplateTupleDesc(1);
	TupleDescInitEntry(tmpTupDesc, (AttrNumber)1, "vec_batch",
						   BYTEAOID, -1, 0);

	tmpslot = MakeTupleTableSlot(tmpTupDesc, &TTSOpsMinimalTuple);;
	ExecClearTuple(tmpslot);
	values = tmpslot->tts_values;
	isnull = tmpslot->tts_isnull;

	sz =  VARHDRSZ + PADSIZE + buf_size;
	input = (VarBuffer *) palloc(sz);
	/* pad + data */
	SET_VARSIZE(input, PADSIZE + buf_size);
	memcpy((void *) VARBUFFER(input), data_pos, buf_size);

	values[0] = PointerGetDatum(input);
	isnull[0] = false;
	ExecStoreVirtualTuple(tmpslot);

	copied_mtup = tmpslot->tts_ops->copy_minimal_tuple(tmpslot);

	free_array_buffer(&buffer);
	pfree(input);
	pfree(tmpslot);
	pfree(tmpTupDesc);

	return copied_mtup;
}

/*
 * Get a batch from MinimalTuple
 */
void *
DeserializeMinimalTuple(MinimalTuple tup, bool need_copy)
{
	bool isnull[1];
	TupleDesc	tupdesc;
	TupleTableSlot *tmpslot;
	Datum data;
	VarBuffer* vardata;
	void* buffer;
	int64 buf_len;
	g_autoptr(GArrowRecordBatch) batch = NULL;
	g_autoptr(GArrowBuffer) arrow_buffer = NULL;

	tupdesc = CreateTemplateTupleDesc(1);
	TupleDescInitEntry(tupdesc, (AttrNumber)1, "vec_batch",
					   BYTEAOID, -1, 0);

	tmpslot = MakeTupleTableSlot(tupdesc, &TTSOpsMinimalTuple);
	ExecStoreMinimalTuple(tup, tmpslot, true);

	data = slot_getattr(tmpslot, 1, isnull);
	vardata = DatumGetVarBufferP(data);
	buffer = (void *) VARBUFFERFROMDATUM(vardata);
	buf_len = VARBUFFERFDATASIZE(vardata);

	arrow_buffer = garrow_buffer_new(buffer, buf_len);
	batch = DeserializeRecordBatch(arrow_buffer, buf_len, need_copy);
	pfree(tupdesc);
	pfree(tmpslot);
	if (need_copy) 
	{
		pfree(tup);
	}
	return garrow_move_ptr(batch);
}

