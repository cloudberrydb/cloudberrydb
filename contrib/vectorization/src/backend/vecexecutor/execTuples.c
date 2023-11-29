/*-------------------------------------------------------------------------
 *
 * execTuples.c
 *    Routines dealing with VecTupleTableSlot. 
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/backend/vecexecutor/execTuples.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cdb/cdbvars.h"
#include "cdb/tupser.h"
#include "cdb/vectupser.h"
#include "utils/tuptable_vec.h"
#include "utils/wrapper.h"
#include "vecexecutor/execslot.h"
#include "vecexecutor/executor.h"

static void
tts_vec_init(TupleTableSlot *slot)
{
    VecTupleTableSlot *vslot = (VecTupleTableSlot *) slot;
    vslot->tts_recordbatch = NULL;
	vslot->vec_schema.schema = NULL;
	vslot->vec_schema.schema_rebuilt = false;
	InitSlotSchema(slot);
}

static void
tts_vec_release(TupleTableSlot *slot)
{
	VecTupleTableSlot *vslot = (VecTupleTableSlot *) slot;
	if (TupHasVectorTuple(slot))
		free_batch(&vslot->tts_recordbatch);
	FreeVecSchema(slot);
}

static void
tts_vec_clear(TupleTableSlot *slot)
{
	VecTupleTableSlot *vslot = (VecTupleTableSlot *) slot;

	/* free all arrays and batch */
	for (int i = 0; i < slot->tts_nvalid; i++)
	{
		free_array((void **)&slot->tts_values[i]);
	}

	if (TupHasVectorTuple(slot))
		free_batch(&vslot->tts_recordbatch);

	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);
}

static void
tts_vec_getsomeattrs(TupleTableSlot *slot, int natts)
{
	VecTupleTableSlot *vslot = (VecTupleTableSlot *) slot;

	int attnum, i;
	GArrowRecordBatch	*batch;

	/* Quick out for dirty slot or empty batch */
	if(TupIsNull(slot) || TTS_IS_DIRTY(slot) || IsContainEmptyTuples(slot))
		return;

	batch = (GArrowRecordBatch *)vslot->tts_recordbatch;

	Assert(batch != NULL);

	/* release all arrow arrays before. */
	for (i = 0; i < slot->tts_nvalid; i++)
	{
		ARROW_FREE(GArrowArray, &slot->tts_values[i]);
	}

	/*
	 * load up any slots available from physical tuple
	 */
	attnum = garrow_record_batch_get_n_columns(batch);
	attnum = Min(attnum, natts);

	/*
	 * extract all valid arrays
	 */
	for (i = 0; i < attnum; i++)
	{
		slot->tts_values[i]
		    = (Datum)garrow_record_batch_get_column_data(batch ,i);
		slot->tts_isnull[i] = false;
	}

	/*
	 * If tuple doesn't have all the atts indicated by tupleDesc, read the
	 * rest as null
	 */
	for (; attnum < natts; attnum++)
	{
		slot->tts_values[attnum] = (Datum) 0;
		slot->tts_isnull[attnum] = true;
	}
	slot->tts_nvalid = natts;
}

static bool
slot_getsysattr_vec(TupleTableSlot *slot, int attnum, Datum *result, bool *isnull)
{
	Assert(attnum < 0);         /* else caller error */
	if (TupIsNull(slot))
		return false;

	*result = 0;

	/* Currently, no sys attribute ever reads as NULL. */
	if (isnull)
		*isnull = false;

	switch(attnum)
	{
		case SelfItemPointerAttributeNumber:
			*isnull = false;
			*result =  PointerGetDatum(&slot->tts_tid);
			break;
		case GpSegmentIdAttributeNumber:
			*result = Int32GetDatum(GpIdentity.segindex);
			break;
		default:
			elog(ERROR, "Invalid attnum: %d", attnum);
	}

	return true;
}

static Datum
tts_vec_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull) 
{
	int tdesc_natts = slot->tts_tupleDescriptor->natts;

	if(TupIsNull(slot))
		return (Datum) 0;

	Assert(attnum <= tdesc_natts);
	Assert((attnum > 0)
			|| (attnum == SelfItemPointerAttributeNumber)
			|| (attnum == GpSegmentIdAttributeNumber));


	if (TTS_IS_DIRTY(slot)){
		/* needn't get tts_values*/
	}
	else if (attnum <= 0)
	{
		Datum value;

		if (!slot_getsysattr_vec(slot, attnum, &value, isnull))
			elog(ERROR, "Fail to get system attribute with attnum: %d", attnum);
		return value;

	}
	else if (IsContainEmptyTuples(slot))
	{
		ARROW_FREE(GArrowArray, &slot->tts_values[attnum - 1]);
		slot->tts_values[attnum - 1] = (Datum) 0;
	}

	 /* Assume that not-null values[] is always valid. */
	else if (slot->tts_values[attnum - 1] == (Datum) 0)
	{

		ARROW_FREE(GArrowArray, &slot->tts_values[attnum - 1]);
		slot->tts_values[attnum - 1] =
			(Datum) garrow_record_batch_get_column_data(GetBatch(slot), attnum - 1);

		slot->tts_isnull[attnum - 1] = false;
	}

	slot->tts_nvalid = tdesc_natts;

	return slot->tts_values[attnum-1];
}

static void tts_vec_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	ExecStoreBatch(dstslot, GetBatch(srcslot));
}

static MinimalTuple tts_vec_copy_minimal_tuple(TupleTableSlot *slot)
{
	return ExecCopySlotMinimalTupleVec(slot);
}
const TupleTableSlotOps TTSOpsVecTuple = {
	.base_slot_size = sizeof(VecTupleTableSlot),
	.init = tts_vec_init,
	.release = tts_vec_release,
	.clear = tts_vec_clear,
	.getsomeattrs = tts_vec_getsomeattrs,
	.getsysattr = tts_vec_getsysattr,
	.materialize = NULL,
	.copyslot = tts_vec_copyslot,
	.get_heap_tuple = NULL,
	.get_minimal_tuple = NULL,
	.copy_heap_tuple = NULL,
	.copy_minimal_tuple = tts_vec_copy_minimal_tuple
};

/*
 * PG name to unique name
 */
const char *
GetUniqueAttrName(char *attr_name, AttrNumber attr)
{
	char *name;
	int len = NAMEDATALEN + 20;

	name = (char *) palloc(len);
	/* GP may set empty name to unnamed_attr_%d for motion targetlist.
	 * So we hack in the same way to keep schema match.
	 */
	if (!attr_name || *attr_name == '\0')
		snprintf(name, len, "unnamed_attr_%d(att%d)", attr, attr);
	else
		snprintf(name, len, "%s(att%d)", attr_name, attr);
	return name;
}

void
InitSlotSchema(TupleTableSlot *slot)
{
	VecTupleTableSlot *vslot = (VecTupleTableSlot *) slot;
	TupleDesc tupdesc = slot->tts_tupleDescriptor;
	g_autoptr(GArrowSchema) schema = NULL;

	/*
     * ExecInitExtraTupleSlot(xxx, NULL, xx) will set a null tupdesc.
	 */
	if (!tupdesc)
		return;

	schema = TupDescToSchema(tupdesc);
	garrow_store_ptr(vslot->vec_schema.schema, schema);
	vslot->vec_schema.schema_rebuilt = false;
}

/*
 * Construct a Arrow-type schema from PG's TupleDesc and set vslot->arrow_schema.schema.
 */
GArrowSchema *
TupDescToSchema(TupleDesc tupdesc)
{
    int i;
	g_autoptr(GArrowSchema) schema = NULL;
	GList *fields = NULL;

	/* construct field for each attributes and add it to the schema*/
	for (i = 0; i < tupdesc->natts; i++)
	{
		const char *name;
		FormData_pg_attribute *attr = &tupdesc->attrs[i];
		g_autoptr(GArrowDataType) datatype = NULL;
		g_autoptr(GArrowField) field = NULL;

		datatype = PGTypeToArrow(attr->atttypid);
		name = GetUniqueAttrName(attr->attname.data, i + 1);
		field = garrow_field_new(name, datatype);
		fields = garrow_list_append_ptr(fields, field);
		pfree((void *)name);
	}

	if (!fields)
	{
		g_autoptr(GArrowField) field = NULL;
		g_autoptr(GArrowDataType) null_type = NULL;

		null_type = GARROW_DATA_TYPE(garrow_null_data_type_new());
		field = garrow_field_new("dummy", null_type);
		fields = garrow_list_append_ptr(fields, field);
	}

	schema = garrow_schema_new(fields);

	garrow_list_free_ptr(&fields);
	return garrow_move_ptr(schema);
}

void
RebuildRuntimeSchema(TupleTableSlot *slot)
{
	VecTupleTableSlot *vslot = (VecTupleTableSlot *) slot;
	void *recordbatch = vslot->tts_recordbatch;
	int ncolumns, i;
	g_autoptr(GArrowSchema) schema = NULL;

	if (TupIsNull(slot) || vslot->vec_schema.schema_rebuilt)
		return;

	/* Rebuild schema from recordbatch first.*/
	if(!TTS_IS_DIRTY(slot))
	{
		GArrowRecordBatch *batch = GARROW_RECORD_BATCH(recordbatch);
		g_autoptr(GError) error = NULL;

		if (!garrow_record_batch_is_null(batch))
		{
			if(!garrow_record_batch_rebuild_schema(batch, &error))
				elog(ERROR, "Rebuild schema Error: %s", error->message);
		}
		schema = garrow_record_batch_get_schema(batch);
	}
	else
	{
		GList *fields = NULL;

		/* Dirty slot, rebuild from arrays*/
		ncolumns = slot->tts_tupleDescriptor->natts;
		for (i = 0; i < ncolumns; i++)
		{
			GArrowArray *array;
			FormData_pg_attribute* attr = &slot->tts_tupleDescriptor->attrs[i];
			g_autoptr(GArrowField) field = NULL;
			g_autoptr(GArrowDataType) datatype = NULL;
			const char *name;
			name = GetUniqueAttrName(attr->attname.data, i + 1);
			if (slot->tts_values[i])
			{
				array = GARROW_ARRAY((void *)slot->tts_values[i]);
				garrow_store_func(datatype, garrow_array_get_value_data_type(array));
			}
			else
			{
				array = NULL;
				garrow_store_func(datatype, PGTypeToArrow(attr->atttypid));
			}
			garrow_store_func(field, garrow_field_new(name, datatype));
			fields = garrow_list_append_ptr(fields, field);
			pfree((void *)name);
		}

		garrow_store_func(schema, garrow_schema_new(fields));

		garrow_list_free_ptr(&fields);
	}

	garrow_store_ptr(vslot->vec_schema.schema, schema);
	vslot->vec_schema.schema_rebuilt = true;
}

/*
 * Get Arrow schema after Node initialization. If not set yet, set it.
 */
GArrowSchema *
GetSchemaFromSlot(TupleTableSlot *slot)
{
	VecTupleTableSlot* vslot = (VecTupleTableSlot *) slot;

	Assert(TTS_IS_VECTOR(slot));
	if (vslot->vec_schema.schema == NULL)
		InitSlotSchema(slot);

	if (!vslot->vec_schema.schema_rebuilt)
		RebuildRuntimeSchema(slot);

	return GARROW_SCHEMA(vslot->vec_schema.schema);
}

int
GetNumRows(TupleTableSlot *slot)
{
	int nrows = 0;
	VecTupleTableSlot *vslot = (VecTupleTableSlot *) slot;

	if(TupIsNull(slot))
		return 0;

	if (!TTS_IS_DIRTY(slot))
	{
		nrows = (int)garrow_record_batch_get_n_rows(vslot->tts_recordbatch);
		return nrows;
	}
	else {
		int i;

		for (i = 0; i < slot->tts_nvalid; i++)
		{
			if (slot->tts_values[i])
			{
				nrows = garrow_array_get_length(
					GARROW_ARRAY((void *)slot->tts_values[i]));
				break;
			}
		}
		return nrows;
	}
}


TupleDesc
CreateCtidTupleDesc(TupleDesc tupdesc)
{
	TupleDesc	desc;
	int			i;

	desc = CreateTemplateTupleDesc(tupdesc->natts + 1);

	/* Flat-copy the attribute array */
	memcpy(TupleDescAttr(desc, 0),
		   TupleDescAttr(tupdesc, 0),
		   desc->natts * sizeof(FormData_pg_attribute));

	/*
	 * Since we're not copying constraints and defaults, clear fields
	 * associated with them.
	 */
	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);

		if (i == desc->natts - 1)
			TupleDescInitEntry(desc, (AttrNumber) i + 1, CTID_FIELD,
							   INT8OID, -1, 0);

		att->attnotnull = false;
		att->atthasdef = false;
		att->atthasmissing = false;
		att->attidentity = '\0';
		att->attgenerated = '\0';
	}

	/* We can copy the tuple type identification, too */
	desc->tdtypeid = tupdesc->tdtypeid;
	desc->tdtypmod = tupdesc->tdtypmod;

	return desc;
}
