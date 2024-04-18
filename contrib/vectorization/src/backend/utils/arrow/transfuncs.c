/*-------------------------------------------------------------------------
 *
 * transfuncs.c
 *	  Implement conversion from pg type to arrow type
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/backend/utils/arrow/transfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dlfcn.h>

#include "catalog/pg_type_d.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "utils/arrow.h"
#include "utils/tuptable_vec.h"
#include "utils/guc_vec.h"
#include "utils/wrapper.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "catalog/objectaccess.h"
#include "funcapi.h"
#include "vecexecutor/execnodes.h"
#include "utils/fmgr_vec.h"

Oid AvgIntByteAOid = InvalidOid;
Oid STDDEVOID = InvalidOid;

static GGandivaNode *TextlikeToGandiva(Expr *expr, GArrowSchema *schema, GArrowDataType **rettype);
static GGandivaNode *AddCastType(Expr *expr, GArrowSchema *schema, GArrowDataType **rettype);

void
init_vector_types(void)
{
	Oid oid;
	Oid namespace;

	namespace = PG_EXTAUX_NAMESPACE;

	if (AvgIntByteAOid == InvalidOid)
	{
		oid = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
								PointerGetDatum("arrow_avg_int_bytea"),
								ObjectIdGetDatum(namespace));
		if (!OidIsValid(oid))
			elog(ERROR, "Lookup arrow_avg_int_bytea type failed!");
		AvgIntByteAOid = oid;
	}
	if(STDDEVOID == InvalidOid)
	{
                oid = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
                                                                PointerGetDatum("stddev"),
                                                                ObjectIdGetDatum(namespace));
                if (!OidIsValid(oid))
                        elog(ERROR, "Lookup stddev type failed!");
                STDDEVOID = oid;
	}
}

static bool
CastTypeNeeded(Oid funcid)
{
	if (funcid == 159 || funcid == 165 || funcid == 161 ||
	    funcid == 163 || funcid == 167 || funcid == 169)
	{
		/* int42eq, int42ne, ..., add castINT() for 2nd arg */
		return true;
	}
	else if (funcid == 158 || funcid == 164 || funcid == 160 ||
	         funcid == 162 || funcid == 166 || funcid == 168 || funcid == 178)
	{
		/* int24eq, int24ne, ..., add castINT() for 1st arg */
		return true;
	}
	else if (funcid == 474  || funcid == 475  || funcid == 476  ||
	         funcid == 477  || funcid == 478  || funcid == 479  ||
	         funcid == 1856 || funcid == 1857 || funcid == 1858 ||
	         funcid == 1859 || funcid == 1860 || funcid == 1861 ||
	         funcid == 1274 || funcid == 1275 || funcid == 1276 || funcid == 1277)
	{
		/* int84xx, int82xx, ..., add castBIGINT() for 2nd arg */
		return true;
	}
	else if (funcid == 852  || funcid == 853  || funcid == 854  ||
	         funcid == 855  || funcid == 856  || funcid == 857  ||
	         funcid == 1850 || funcid == 1851 || funcid == 1852 ||
	         funcid == 1853 || funcid == 1854 || funcid == 1855 ||
	         funcid == 1278 || funcid == 1279 || funcid == 1280 || funcid == 1281)
	{
		/* int48xx, int28xx, ..., add castBIGINT() for 1st arg */
		return true;
	}
	else
		return false;
}

static GArrowDataType *
BuildAvgStructType(GArrowDataType *sum_dt)
{
	g_autoptr(GArrowField) count, sum;
	g_autoptr(GArrowDataType) int64_dt;
	g_autoptr(GArrowDataType) dt = NULL;
	GList *fields = NULL;
	int64_dt = GARROW_DATA_TYPE(garrow_int64_data_type_new());
	count = garrow_field_new("count", int64_dt);
	sum = garrow_field_new("sum", sum_dt);
	fields = garrow_list_append_ptr(fields, sum);
	fields = garrow_list_append_ptr(fields, count);
	dt = GARROW_DATA_TYPE(garrow_struct_data_type_new(fields));
	garrow_list_free_ptr(&fields);
	return garrow_move_ptr(dt);
}

static GArrowDataType *
BuildAvgStructStddevType(GArrowDataType *sum_dt)
{
	g_autoptr(GArrowField) count, sum, stddev;
	g_autoptr(GArrowDataType) int64_dt;
	g_autoptr(GArrowDataType) dt = NULL;
	g_autoptr(GArrowDataType) stddev_dt = NULL;
	GList *fields = NULL;
	int64_dt = GARROW_DATA_TYPE(garrow_int64_data_type_new());
	stddev_dt = GARROW_DATA_TYPE(garrow_int64_data_type_new());
	count = garrow_field_new("count", int64_dt);
	sum = garrow_field_new("sum", sum_dt);
	stddev = garrow_field_new("square", stddev_dt);
	fields = garrow_list_append_ptr(fields, sum);
	fields = garrow_list_append_ptr(fields, count);
	fields = garrow_list_append_ptr(fields, stddev);
	dt = GARROW_DATA_TYPE(garrow_struct_data_type_new(fields));
	garrow_list_free_ptr(&fields);
	return garrow_move_ptr(dt);
}


/* Form an Arrow type by pg Oid
 *
 * Caller need to free the returned GArrowDataType
 * by ARROW_FREE(GArrowDataType, &ret_type);
 */
GArrowDataType *
PGTypeToArrow(Oid pg_type)
{
 	if (pg_type == AvgIntByteAOid)
 	{
 		g_autoptr(GArrowDataType) numeric128_dt;
 		numeric128_dt = GARROW_DATA_TYPE(garrow_numeric128_data_type_new());
 		return BuildAvgStructType(numeric128_dt);
 	}
	else if (pg_type == STDDEVOID)
	{
                g_autoptr(GArrowDataType) int64_dt;
                int64_dt = GARROW_DATA_TYPE(garrow_int64_data_type_new());
                return BuildAvgStructStddevType(int64_dt);
	}
	/* Fixme: Numeric is not support now. */
	switch (pg_type)
	{
		case BOOLOID:
			return (GArrowDataType *)garrow_boolean_data_type_new();
		case INT8ARRAYOID:
		{
			g_autoptr(GArrowDataType) int64_dt;
			int64_dt = GARROW_DATA_TYPE(garrow_int64_data_type_new());
			return BuildAvgStructType(int64_dt);
		}
		case FLOAT8ARRAYOID: /* FIXME: just use in avg(float8) */
		{
			g_autoptr(GArrowDataType) double_dt;
			double_dt = GARROW_DATA_TYPE(garrow_double_data_type_new());
			return BuildAvgStructType(double_dt);
		}
		case BYTEAOID:
		case INTERNALOID:
		{
			g_autoptr(GArrowField) count, sum;
			GArrowDataType *struct_dt;

			g_autolist(GObject) dts;
			g_autoptr(GArrowDataType) int64_dt;
			g_autoptr(GArrowDataType) decimal_dt;

			int64_dt = GARROW_DATA_TYPE(garrow_int64_data_type_new());
			decimal_dt = GARROW_DATA_TYPE(garrow_numeric128_data_type_new());

			count = garrow_field_new("count", int64_dt);
			sum   = garrow_field_new("sum",   decimal_dt);

			/* make new data type */
			dts = NULL;
			dts = garrow_list_append_ptr(dts, sum);
			dts = garrow_list_append_ptr(dts, count);

			struct_dt = GARROW_DATA_TYPE(garrow_struct_data_type_new(dts));

			return struct_dt;
		}
		case NUMERICOID:
			return (GArrowDataType *)garrow_numeric128_data_type_new();
		case CHAROID:
			return (GArrowDataType *)garrow_int8_data_type_new();
		case NAMEOID:
			return (GArrowDataType *)garrow_fixed_size_binary_data_type_new(63);
		case TIMEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return (GArrowDataType *)garrow_timestamp_data_type_new(GARROW_TIME_UNIT_MICRO);		
		case INT8OID:
			return (GArrowDataType *)garrow_int64_data_type_new();
		case INT2OID:
			return (GArrowDataType *)garrow_int16_data_type_new();
		case DATEOID:
			return (GArrowDataType *)garrow_date32_data_type_new();
		case INT4OID:
			return (GArrowDataType *)garrow_int32_data_type_new();
		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
			return (GArrowDataType *)garrow_string_data_type_new();
		case FLOAT4OID:
			return (GArrowDataType *)garrow_float_data_type_new();
		case FLOAT8OID:
			return (GArrowDataType *)garrow_double_data_type_new();
		case TIDOID:
			return (GArrowDataType *)garrow_int64_data_type_new();
		case OIDOID:
		case XIDOID:
		case CIDOID:
		case REGPROCOID: /* refer to gp_acquire_sample_rows_col_type func */
			return (GArrowDataType *)garrow_int32_data_type_new();
		case PG_NODE_TREEOID: /* refer to gp_acquire_sample_rows_col_type func */
			return (GArrowDataType *)garrow_string_data_type_new();
		default:
			elog(ERROR, "cannot convert PG type %d to arrow data type", pg_type);
			return NULL;
	}
}

GArrowType
PGTypeToArrowID(Oid pg_type)
{
 	if (pg_type == AvgIntByteAOid)
 		return GARROW_TYPE_STRUCT;
	else if (pg_type == STDDEVOID)
		return GARROW_TYPE_STRUCT;
	switch (pg_type)
	{
		case BOOLOID:
			return GARROW_TYPE_BOOLEAN;
		case BYTEAOID:
		case NUMERICOID:
			return GARROW_TYPE_NUMERIC128;
		case CHAROID:
			return  GARROW_TYPE_INT8;
		case TIMEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return GARROW_TYPE_TIMESTAMP;
		case INT8OID:
			return  GARROW_TYPE_INT64;
		case INT2OID:
			return  GARROW_TYPE_INT16;
		case DATEOID:
			return GARROW_TYPE_DATE32;
		case INT4OID:
			return  GARROW_TYPE_INT32;
		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
			return  GARROW_TYPE_STRING;
		case TIDOID:
			return  GARROW_TYPE_INT64;
		case FLOAT4OID:
			return GARROW_TYPE_FLOAT;
		case FLOAT8OID:
			return GARROW_TYPE_DOUBLE;
		/* Fixme: Other ARRAY OID needed to support, such as:
		 * BOOLARRAYOID, CHARARRAYOID and so on. */
		case INT2ARRAYOID:
		case INT4ARRAYOID:
		case INT8ARRAYOID:
		case FLOAT4ARRAYOID:
		case FLOAT8ARRAYOID:
		case TEXTARRAYOID:
		case VARCHARARRAYOID:
		case BPCHARARRAYOID:
		case DATEARRAYOID:
			return  GARROW_TYPE_LIST;
		case NAMEOID:
		case XIDOID:
		case CIDOID:
		case OIDVECTOROID:
		case JSONOID:
		case OIDOID:
		case REGPROCOID:
		case INT2VECTOROID:
		default:
			return GARROW_TYPE_NA;
	}
}

/*
 * Memory-savely convert Gbytes to PG Datum.
 *
 * The Datum is newly palloced and copied, so the Bytes could be released.
 *
 * Used for the Bytes got from GArrow functions.
 */
Datum
GBytesToDatum(GBytes *bytes)
{
	gsize size;
	const void *data;
	void *pgdata;

	data = g_bytes_get_data(bytes, &size);
	pgdata = palloc(size);
	memcpy(pgdata, data, size);
	return PointerGetDatum(pgdata);
}

/* Call this before read. */
void
ResetColDesc(ColDesc *coldesc)
{
	g_autoptr(GArrowBuffer) valuebuffer = NULL;
	g_autoptr(GArrowBuffer) posbuffer = NULL;
	g_autoptr(GBytes) valuebytes = NULL;
	g_autoptr(GBytes) posbytes = NULL;
	g_autoptr(GArrowBuffer) bitmapbuffer = NULL;
	g_autoptr(GBytes) bitmapbytes = NULL;
	g_autoptr(GArrowBuffer) offsetbuffer = NULL;
	g_autoptr(GBytes) offsetbytes = NULL;
	gsize size;
	int rowToRead;
	g_autoptr(GError) error = NULL;

	if (!coldesc)
		return;
	coldesc->cur_pos_index = 0;
	coldesc->lm_row_index = 0;
	coldesc->rowstoread = max_batch_size;
	coldesc->currows = 0;
	//coldesc->pos_value_buffer = NULL;
	coldesc->pos_values = NULL;
	coldesc->values_len = 0;
	coldesc->block_row_count = 0;

	rowToRead = coldesc->rowstoread;

	if (coldesc->datalen >= 0)
	{
		valuebuffer = garrow_buffer_new_default_mem(coldesc->datalen * rowToRead, &error);
		if (!valuebuffer)
			elog(ERROR, "Can't new value buffer from arrow:%s ", error->message);
		valuebytes = garrow_buffer_get_data(valuebuffer);
		coldesc->values = (void *)g_bytes_get_data(valuebytes, &size);
		garrow_store_ptr(coldesc->value_buffer, valuebuffer);
	}
	/* Boolean in Arrow is stored as bitmap */
	else if (coldesc->type == GARROW_TYPE_BOOLEAN)
	{
		valuebuffer = garrow_buffer_new_default_mem((rowToRead + 7) / 8, &error);
		if (!valuebuffer)
			elog(ERROR, "Can't new value buffer from arrow:%s ", error->message);
		valuebytes = garrow_buffer_get_data(valuebuffer);
		coldesc->values = (void *)g_bytes_get_data(valuebytes, &size);
		memset(coldesc->values, 0, (rowToRead + 7) / 8);
		garrow_store_ptr(coldesc->value_buffer, valuebuffer);
	}
	else
	{

		valuebuffer = garrow_buffer_new_default_mem(BASIC_COL_DATA_BUFF_SIZE, &error);

		if ((valuebuffer == NULL) && (error != NULL))
		{
			elog(ERROR, "Can't new value buffer from arrow:%s ", error->message);
		}

		valuebytes = garrow_buffer_get_data(valuebuffer);
		coldesc->values = (void *)g_bytes_get_data(valuebytes, &size);
		coldesc->values_len = size;
		garrow_store_ptr(coldesc->value_buffer, valuebuffer);

		offsetbuffer = garrow_buffer_new_default_mem(BASIC_COL_OFFSET_BUFF_SIZE, &error);

		if ((offsetbuffer == NULL) && (error != NULL))
			elog(ERROR, "Can't new value buffer from arrow:%s ", error->message);

		offsetbytes = garrow_buffer_get_data(offsetbuffer);
		coldesc->offsets = (void *)g_bytes_get_data(offsetbytes, &size);
		memset(coldesc->offsets, 0, size);
		garrow_store_ptr(coldesc->offset_buffer, offsetbuffer);

	}

	bitmapbuffer = garrow_buffer_new_default_mem((rowToRead + 7) / 8, &error);

	if ((bitmapbuffer == NULL) && (error != NULL))
		elog(ERROR, "Can't new bitmap buffer from arrow:%s ", error->message);

	bitmapbytes = garrow_buffer_get_data(bitmapbuffer);
	coldesc->bitmap = (uint8 *)g_bytes_get_data(bitmapbytes, &size);
	memset(coldesc->bitmap, 0xff, size);
	garrow_store_ptr(coldesc->bitmap_buffer, bitmapbuffer);

	/* This column scan is responsible for collecting row numbers */
	if (coldesc->is_pos_recorder)
	{
		posbuffer = garrow_buffer_new_default_mem(sizeof(int64) * rowToRead, &error);
		if (!posbuffer)
			elog(ERROR, "Can't new position buffer from arrow:%s ", error->message);
		posbytes = garrow_buffer_get_data(posbuffer);
		coldesc->pos_values = (void *)g_bytes_get_data(posbytes, &size);
		garrow_store_ptr(coldesc->pos_value_buffer, posbuffer);
	}
}

void*
FinishPosColDesc(ColDesc *coldesc)
{
	g_autoptr(GArrowInt64Array) rs = NULL;

	rs =  garrow_int64_array_new(coldesc->currows,
								coldesc->pos_value_buffer,
								NULL,
								0);

	return garrow_move_ptr(rs);
}

void*
FinishColDesc(ColDesc *coldesc)
{
	GArrowType type = coldesc->type;
	gint64 n_nulls;
	void *rs = NULL;
	GError *error = NULL;

	if (coldesc->hasnull)
	{
		n_nulls = -1;
	} else {
		n_nulls = 0;
	}

	switch (type)
	{
	case GARROW_TYPE_BOOLEAN:
		rs = (void *)garrow_boolean_array_new(coldesc->currows, coldesc->value_buffer,
				coldesc->bitmap_buffer, n_nulls);
		break;
	case GARROW_TYPE_INT8:
		rs = (void *)garrow_int8_array_new(coldesc->currows, coldesc->value_buffer,
				coldesc->bitmap_buffer, n_nulls);
		break;
	case GARROW_TYPE_INT16:
		rs = (void *)garrow_int16_array_new(coldesc->currows, coldesc->value_buffer,
				coldesc->bitmap_buffer, n_nulls);
		break;
	case GARROW_TYPE_INT32:
		rs = (void *)garrow_int32_array_new(coldesc->currows, coldesc->value_buffer,
				coldesc->bitmap_buffer, n_nulls);
		break;
	case GARROW_TYPE_INT64:
	{
		rs = (void *)garrow_int64_array_new(coldesc->currows, coldesc->value_buffer,
				coldesc->bitmap_buffer, n_nulls);
		break;
	}
	case GARROW_TYPE_UINT64:
	{
		rs = (void *)garrow_uint64_array_new(coldesc->currows, coldesc->value_buffer,
				coldesc->bitmap_buffer, n_nulls);
		break;
	}
	case GARROW_TYPE_FLOAT:
		rs = (void *)garrow_float_array_new(coldesc->currows, coldesc->value_buffer,
				coldesc->bitmap_buffer, n_nulls);
		break;
	case GARROW_TYPE_DOUBLE:
		rs = (void *)garrow_double_array_new(coldesc->currows, coldesc->value_buffer,
				coldesc->bitmap_buffer, n_nulls);
		break;
	case GARROW_TYPE_DATE32:
		rs = (void *)garrow_date32_array_new(coldesc->currows, coldesc->value_buffer,
				coldesc->bitmap_buffer, n_nulls);
		break;
	case GARROW_TYPE_TIME64:
	{
		g_autoptr(GArrowTime64DataType) arrowtype;

		arrowtype = garrow_time64_data_type_new(GARROW_TIME_UNIT_MICRO, &error);
		if (!rs)
			elog(ERROR, "Failed to make GArrowDataType for TIMEOID, cause: %s", error->message);

		rs = (void *)garrow_time64_array_new(arrowtype, coldesc->currows,
				coldesc->value_buffer, coldesc->bitmap_buffer, n_nulls);
		break;
	}
	case GARROW_TYPE_NUMERIC128:
	{
		g_autoptr(GArrowNumeric128DataType) dt = NULL;
		dt = garrow_numeric128_data_type_new();
		rs = garrow_fixed_size_binary_array_new(
			GARROW_FIXED_SIZE_BINARY_DATA_TYPE(dt), coldesc->currows,
			coldesc->value_buffer, coldesc->bitmap_buffer, n_nulls);
		break;
    }
	case GARROW_TYPE_STRING:
	{
		rs = garrow_string_array_new(coldesc->currows,
									coldesc->offset_buffer,
									coldesc->value_buffer,
									coldesc->bitmap_buffer,
									n_nulls);

		break;
	}
	case GARROW_TYPE_BINARY:
	{
		g_autoptr(GError) error = NULL;
		g_autoptr(GArrowBinaryArrayBuilder) builder = GARROW_BINARY_ARRAY_BUILDER(coldesc->values);

		rs = (void *)garrow_array_builder_finish((GArrowArrayBuilder *)builder, &error);
		if (!rs)
			elog(ERROR, "finish string builer error: %s", error->message);
		break;
	}
	case GARROW_TYPE_TIMESTAMP:
	{
		g_autoptr(GArrowTimestampDataType) arrowtype = garrow_timestamp_data_type_new(GARROW_TIME_UNIT_MICRO);
		rs = (void *) garrow_timestamp_array_new(arrowtype, coldesc->currows, coldesc->value_buffer, coldesc->bitmap_buffer, n_nulls);
		break;
	}
	default:
		elog(ERROR, "cannot new arrow array of PG type %d", type);
		break;
	}

	return rs;
}

void
FreeResDesc(ResDesc resdesc)
{
	int i = 0;

	for (i = 0; i < resdesc->natts; i++)
	{
		if (resdesc->cols_array[i] != NULL)
		{
			free_array((void**)&resdesc->cols_array[i]);
		}

		if (resdesc->lst_col[i] != NULL)
		{
			garrow_list_free_ptr(&resdesc->lst_col[i]);
		}

	}

	if (resdesc->selective_pos != NULL)
	{
		free_array((void**)&resdesc->selective_pos);
	}

	if (resdesc->lst_pos != NULL)
	{
		garrow_list_free_ptr(&resdesc->lst_pos);
	}

	pfree(resdesc->cols_array);
	pfree(resdesc->lst_col);

	pfree(resdesc);
}

/*
 * Free the arrow builders and arrow values inside VecDesc.
 */
void
FreeVecDesc(VecDesc vecdesc)
{
	int i;

	for (i = 0; i < vecdesc->natts; i++)
	{
		ColDesc *coldesc = vecdesc->coldescs[i];

		if (coldesc)
		{
			if (coldesc->datalen == -1)
			{
				ARROW_FREE(GArrowBuffer, &coldesc->value_buffer);
				ARROW_FREE(GArrowBuffer, &coldesc->bitmap_buffer);
				ARROW_FREE(GArrowBuffer, &coldesc->offset_buffer);
			}
			else
			{
				if (coldesc->value_buffer)
				{
					ARROW_FREE(GArrowBuffer, &coldesc->value_buffer);
				}
				if (coldesc->bitmap_buffer)
				{
					ARROW_FREE(GArrowBuffer, &coldesc->bitmap_buffer);
				}
			}

			if (coldesc->is_pos_recorder)
			{
				ARROW_FREE(GArrowBuffer, &coldesc->pos_value_buffer);
			}
		}

		vecdesc->coldescs[i] = NULL;
	}

	if (vecdesc->selective != NULL)
		free_array((void**)&vecdesc->selective);

	ARROW_FREE(GArrowSchema, &vecdesc->schema);

	pfree(vecdesc);

	return;
}

static void
InitColDesc(ColDesc *coldesc, GArrowType arrow_type)
{
	coldesc->type = arrow_type;
	coldesc->currows = 0;
	coldesc->hasnull = false;
	coldesc->value_buffer = NULL;
	coldesc->bitmap_buffer = NULL;
	coldesc->is_lm = false;
	coldesc->is_pos_recorder = false;
	coldesc->pos_value_buffer = NULL;
	coldesc->pos_values = NULL;
	coldesc->rowstoread = max_batch_size;
	coldesc->block_row_count = 0;
	coldesc->isbpchar = false;

	switch (coldesc->type)
	{
		case GARROW_TYPE_BOOLEAN:
			coldesc->datalen = -1;
			break;
		case GARROW_TYPE_INT8:
			coldesc->datalen = 1;
			break;
		case GARROW_TYPE_INT16:
			coldesc->datalen = 2;
			break;
		case GARROW_TYPE_INT32:
			coldesc->datalen = 4;
			break;
		case GARROW_TYPE_UINT64:
		case GARROW_TYPE_INT64:
			coldesc->datalen = 8;
			break;
		case GARROW_TYPE_FLOAT:
			coldesc->datalen = 4;
			break;
		case GARROW_TYPE_DOUBLE:
			coldesc->datalen = 8;
			break;
		case GARROW_TYPE_DATE32:
			coldesc->datalen = 4;
			break;
		case GARROW_TYPE_TIME64:
			coldesc->datalen = 8;
			break;
		case GARROW_TYPE_TIMESTAMP:
			coldesc->datalen = 8;
			break;
		case GARROW_TYPE_NUMERIC128:
			coldesc->datalen = 16;
			break;
		/* Use builder for variable-size data.*/
		case GARROW_TYPE_STRING:
			coldesc->datalen = -1;
			coldesc->values = NULL;
			coldesc->offsets = NULL;
			coldesc->bitmap = NULL;
			coldesc->bitmap_buffer = NULL;
			coldesc->offset_buffer = NULL;
			coldesc->value_buffer = NULL;
			break;
		default:
			elog(ERROR, "arrow cannot supported type_id (%d)", coldesc->type);
		break;
	}
}

/*
 * Construct a VecDesc from tuple descriptor.
 *
 * valid_attrs: valid attrs indices, start from 0. If Null, all attrs is valid.
 * nvalid: number of valid attrs. If <= 0, all attrs is valid.
 */
VecDesc
TupleDescToVecDesc(TupleDesc tupdesc)
{
	int i, attroffset, natts = tupdesc->natts;
	char *stg;
	VecDesc result = NULL;
	ColDesc *coldesc = NULL;
	g_autoptr(GArrowSchema) schema = NULL;

	/*
	 * sanity checks
	 */
	AssertArg(natts >= 0);

	attroffset = sizeof(struct vecDesc) + tupdesc->natts * sizeof(ColDesc *)
										+ natts * sizeof(ColDesc);
	stg = palloc0(attroffset);
	result = (VecDesc) stg;
	result->selective = NULL;
	result->natts = tupdesc->natts;
	result->visbitmaps = NULL;
	result->visbitmapoffset = 0;
	result->firstrow = -1;
	result->boundrow = -1;
	result->scan_ctid = false;
	result->coldescs = (ColDesc **)(stg + sizeof(struct vecDesc));
	/* The oid of uint64 was not found. */

	schema = TupDescToSchema(tupdesc);
	garrow_store_ptr(result->schema, schema);
	for (i = 0; i < natts; i++)
	{
		int index = i;

		Assert(index < tupdesc->natts);

		result->coldescs[index] = (ColDesc *)(stg + sizeof(struct vecDesc)
				+ tupdesc->natts * sizeof(ColDesc *) + i * sizeof(ColDesc));
		coldesc = result->coldescs[index];
		InitColDesc(coldesc, PGTypeToArrowID(tupdesc->attrs[index].atttypid));	
		if (tupdesc->attrs[index].atttypid == BPCHAROID)
			coldesc->isbpchar = true;
	}
	return result;
}

ResDesc
TupleDescToResDescLM(TupleDesc tupdesc)
{
	int natts = 0;

	Assert(tupdesc);

	natts = tupdesc->natts;

	ResDesc resdesc = palloc0(sizeof(struct resDesc));
	resdesc->cols_array = palloc0(sizeof(GArrowArray*) * natts);
	resdesc->lst_col = palloc0(sizeof(void*) * natts);

	resdesc->natts = natts;
	resdesc->is_send = false;
	resdesc->rows = 0;

	return resdesc;
}


/*
 * Construct a VecDesc from tuple descriptor.
 *
 * valid_attrs: valid attrs indices, start from 0. If Null, all attrs is valid.
 * nvalid: number of valid attrs. If <= 0, all attrs is valid.
 */
VecDesc
TupleDescToVecDescLM(TupleDesc tupdesc,
					 AttrNumber *valid_attrs,
					 int nvalid,
					 AttrNumber *lm_attrs,
					 int nlm)
{
	int i, natts, attroffset;
	char *stg;
	VecDesc result = NULL;
	ColDesc *coldesc = NULL;

	if (nvalid <= 0)
		natts = tupdesc->natts;
	else
		natts = nvalid;

	/*
	 * sanity checks
	 */
	AssertArg(natts >= 0);

	attroffset = sizeof(struct vecDesc) + tupdesc->natts * sizeof(ColDesc *)
										+ natts * sizeof(ColDesc);
	stg = palloc0(attroffset);
	result = (VecDesc) stg;
	result->natts = tupdesc->natts;
	result->coldescs = (ColDesc **)(stg + sizeof(struct vecDesc));
	result->selective = NULL;

	for (i = 0; i < natts; i++)
	{
		int index;

		if (valid_attrs)
			index = valid_attrs[i];
		else
			index = i;

		Assert(index < tupdesc->natts);

		result->coldescs[index] = (ColDesc *)(stg + sizeof(struct vecDesc)
				+ tupdesc->natts * sizeof(ColDesc *) + i * sizeof(ColDesc));
		coldesc = result->coldescs[index];

		coldesc->type = PGTypeToArrowID(tupdesc->attrs[index].atttypid);

		coldesc->currows = 0;
		coldesc->cur_pos_index = 0;
		coldesc->lm_row_index = 0;
		coldesc->hasnull = false;
		coldesc->value_buffer = NULL;
		coldesc->bitmap_buffer = NULL;
		coldesc->is_lm = false;
		coldesc->is_pos_recorder = false;
		coldesc->pos_value_buffer = NULL;
		coldesc->pos_values = NULL;
		coldesc->rowstoread = max_batch_size;
		coldesc->block_row_count = 0;
		switch (coldesc->type)
		{
		case GARROW_TYPE_BOOLEAN:
			coldesc->datalen = sizeof(gboolean);
			break;
		case GARROW_TYPE_INT8:
			coldesc->datalen = sizeof(gint8);
			break;
		case GARROW_TYPE_INT16:
			coldesc->datalen = 2;
			break;
		case GARROW_TYPE_INT32:
			coldesc->datalen = 4;
			break;
		case GARROW_TYPE_INT64:
			coldesc->datalen = 8;
			break;
		case GARROW_TYPE_FLOAT:
			coldesc->datalen = 4;
			break;
		case GARROW_TYPE_DOUBLE:
			coldesc->datalen = 8;
			break;
		case GARROW_TYPE_DATE32:
			coldesc->datalen = 4;
			break;
		case GARROW_TYPE_TIME64:
			coldesc->datalen = 8;
			break;
		case GARROW_TYPE_TIMESTAMP:
			coldesc->datalen = 8;
			break;
		case GARROW_TYPE_NUMERIC128:
			coldesc->datalen = 16;
			break;
		/* Use builder for variable-size data.*/
		case GARROW_TYPE_STRING:
			coldesc->datalen = -1;
			coldesc->values = NULL;
			coldesc->offsets = NULL;
			coldesc->bitmap = NULL;
			coldesc->bitmap_buffer = NULL;
			coldesc->offset_buffer = NULL;
			coldesc->value_buffer = NULL;
			break;
		default:
			elog(ERROR, "arrow cannot supported type_id (%d, %d)", coldesc->type, tupdesc->attrs[i].atttypid);
			break;
		}
	}

	/* mark late materialize flag */
	for (i = 0; i < nlm; i++)
	{
		int idx = lm_attrs[i];
		result->coldescs[idx]->is_lm = true;
	}

	for (i = 0; i < nvalid; i++)
	{
		int idx = valid_attrs[i];

		if ((result->coldescs[idx])
			&&(!result->coldescs[idx]->is_lm))
		{
			result->coldescs[idx]->is_pos_recorder = true;
			break;
		}
	}

	/* check whether mark the position recorder */
	if (i == nvalid)
	{
		elog(ERROR, "can not get the position recorder column");
	}

	return result;
}

void resetVecDescBitmap(VecDesc vecdes)
{
	vecdes->firstrow = -1;
	vecdes->boundrow = -1;
	vecdes->visbitmapoffset = 0;

	if (vecdes->visbitmaps != NIL)
	{
		ListCell *lc = NULL;

		foreach (lc, vecdes->visbitmaps)
		{
			void* p = lfirst(lc);

			if (p != NULL)
				pfree(p);

		}
	}

	list_free(vecdes->visbitmaps);

	vecdes->visbitmaps = NIL;

	return;
}

void
FreeVecSchema(TupleTableSlot* slot)
{
	VecTupleTableSlot *vslot = (VecTupleTableSlot *) slot;
	if (vslot->vec_schema.schema != NULL)
	{
		glib_autoptr_cleanup_GArrowSchema((GArrowSchema **)&vslot->vec_schema.schema);
	}
	vslot->vec_schema.schema = NULL;

	return;
}



void
PrintArrowDatum(void *datum, const char *label)
{
	if (garrow_datum_is_array(datum))
		PrintArrowArray(garrow_datum_get_array(GARROW_ARRAY_DATUM(datum)), label);
	else if (garrow_datum_is_scalar(datum))
	{
		elog(INFO, "----------- %s start: %p -------------", label, datum);
		elog(INFO, "Scalar: %s\n", garrow_scalar_to_string(
				garrow_datum_get_scalar(GARROW_SCALAR_DATUM(datum))));
		elog(INFO, "----------- %s end: %p -------------", label, datum);
	}
	else
		elog(INFO, "Invalid Arrow Datum");
}

void
PrintArrowArray(void *array, const char *label)
{
	int64 nrows;
	GError *error = NULL;
	GArrowType type;

	elog(INFO, "----------- %s start: %p -------------", label, array);
	if (array)
	{
		nrows = garrow_array_get_length((GArrowArray *)array);
		type = garrow_array_get_value_type((GArrowArray *)array);
		elog(INFO, "Array of row %ld, type %d, : %s\n", nrows, type,
				garrow_array_to_string((GArrowArray *)array, &error));
	}
	else
		elog(INFO, "Array is NULL");

	elog(INFO, "----------- %s end: %p-------------", label, array);
}


static void*
PGTypeToGandivaNode(Oid type, Datum datum)
{
	switch (type)
	{
		case BOOLOID:
			return ggandiva_boolean_literal_node_new(DatumGetBool(datum));
		case CHAROID:
			return ggandiva_int8_literal_node_new(DatumGetInt32(datum));
		case INT2OID:
			return ggandiva_int16_literal_node_new(DatumGetInt32(datum));
		case DATEOID:
		case INT4OID:
			return ggandiva_int32_literal_node_new(DatumGetInt32(datum));
		case TIMEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case INT8OID:
			return ggandiva_int64_literal_node_new(DatumGetInt64(datum));
		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
			{
				const gchar *value = text_to_cstring(DatumGetTextP(datum));
				return ggandiva_string_literal_node_new(value);
			}
		default:
			elog(ERROR, "invalid pg_type Oid for T_Const Expr: %u", type);
			break;
	}

	return NULL;
}
/*
 * init_fcache - initialize a FuncExprState node during first use
 */
static void
init_fcache(Oid foid, Oid input_collation, FuncExprState *fcache,
			MemoryContext fcacheCxt, bool needDescForSets)
{
	AclResult	aclresult;

	/* Check permission to call function */
	aclresult = pg_proc_aclcheck(foid, GetUserId(), ACL_EXECUTE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(foid));
	InvokeFunctionExecuteHook(foid);

	/*
 	 * Safety check on nargs.  Under normal circumstances this should never
  	 * fail, as parser should check sooner.  But possibly it might fail if
	 * server has been compiled with FUNC_MAX_ARGS smaller than some functions
 	 * declared in pg_proc?
	 */
	if (list_length(fcache->args) > FUNC_MAX_ARGS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
			 errmsg_plural("cannot pass more than %d argument to a function",
						   "cannot pass more than %d arguments to a function",
						   FUNC_MAX_ARGS,
						   FUNC_MAX_ARGS)));

	/* Set up the primary fmgr lookup information */
	fmgr_info_cxt(foid, &(fcache->func), fcacheCxt);
	fmgr_info_set_expr((Node *) fcache->xprstate.expr, &(fcache->func));

	/* Initialize the function call parameter struct as well */
	InitFunctionCallInfoData(fcache->fcinfo_data, &(fcache->func),
							 list_length(fcache->args),
							 input_collation, NULL, NULL);

	/* If function returns set, prepare expected tuple descriptor */
	if (fcache->func.fn_retset && needDescForSets)
	{
		TypeFuncClass functypclass;
		Oid			funcrettype;
		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		functypclass = get_expr_result_type(fcache->func.fn_expr,
											&funcrettype,
											&tupdesc);

		/* Must save tupdesc in fcache's context */
		oldcontext = MemoryContextSwitchTo(fcacheCxt);

		if (functypclass == TYPEFUNC_COMPOSITE)
		{
			/* Composite data type, e.g. a table's row type */
			Assert(tupdesc);
			/* Must copy it out of typcache for safety */
			fcache->funcResultDesc = CreateTupleDescCopy(tupdesc);
			fcache->funcReturnsTuple = true;
		}
		else if (functypclass == TYPEFUNC_SCALAR)
		{
			/* FIXME : not sure CreateTemplateTupleDesc(1, false)- >Base data type, i.e. scalar */
			tupdesc = CreateTemplateTupleDesc(1);
			TupleDescInitEntry(tupdesc,
							   (AttrNumber) 1,
							   NULL,
							   funcrettype,
							   -1,
							   0);
			fcache->funcResultDesc = tupdesc;
			fcache->funcReturnsTuple = false;
		}
		else if (functypclass == TYPEFUNC_RECORD)
		{
			/* This will work if function doesn't need an expectedDesc */
			fcache->funcResultDesc = NULL;
			fcache->funcReturnsTuple = true;
		}
		else
		{
			/* Else, we will fail if function needs an expectedDesc */
			fcache->funcResultDesc = NULL;
		}

		MemoryContextSwitchTo(oldcontext);
	}
	else
		fcache->funcResultDesc = NULL;

	/* Initialize additional state */
	fcache->funcResultStore = NULL;
	fcache->funcResultSlot = NULL;
	fcache->setArgsValid = false;
	fcache->shutdown_reg = false;
}

static GGandivaNode *
ScalarArrayToGandiva(ExprState *exprstate, GArrowSchema *schema, GArrowDataType **rettype)
{
	ScalarArrayOpExprState *sstate = (ScalarArrayOpExprState *)exprstate;
	ScalarArrayOpExpr *opexpr = (ScalarArrayOpExpr *) sstate->fxprstate.xprstate.expr;
	ArrayType  *arr;
	int			nitems;
	int			i;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	char	   *s;
	bits8	   *bitmap;
	int			bitmask;

	/*
	 * Initialize function cache if first time through
	 */
	if (sstate->fxprstate.func.fn_oid == InvalidOid)
	{
		init_fcache(opexpr->opfuncid, opexpr->inputcollid, &sstate->fxprstate,
					CurrentMemoryContext, true);
		Assert(!sstate->fxprstate.func.fn_retset);
	}

	/*
	 * Evaluate arguments
 	 */
	Assert(list_length(sstate->fxprstate.args) == 2);

	/* only if the second args are const */
	ExprState *argstate = (ExprState *) lsecond(sstate->fxprstate.args);
	if (!IsA(argstate->expr, Const))
		elog(ERROR, "Invalid expr type for ScalarArrayOp");

	Const *argconst = (Const *) argstate->expr;

	/* We do not handle null */
	if (argconst->constisnull)
		return GGANDIVA_NODE(ggandiva_boolean_literal_node_new(false));

	arr = DatumGetArrayTypeP(argconst->constvalue);
	nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));

	/*
 	 * We arrange to look up info about the element type only once per series
	 * of calls, assuming the element type doesn't change underneath us.
	 */
	if (sstate->element_type != ARR_ELEMTYPE(arr))
	{
		get_typlenbyvalalign(ARR_ELEMTYPE(arr),
							 &sstate->typlen,
							 &sstate->typbyval,
							 &sstate->typalign);
		sstate->element_type = ARR_ELEMTYPE(arr);
	}
	typlen = sstate->typlen;
	typbyval = sstate->typbyval;
	typalign = sstate->typalign;

	if (sstate->element_type != INT4OID)
		elog(ERROR, "Just INT4OID supported in ScalarArrayOpExpr");

	g_autolist(GObject) orexprs = NULL;
	g_autoptr(GGandivaNode) varnode = NULL;
	g_autoptr(GArrowDataType) int4type = PGTypeToArrow(INT4OID);
	g_autoptr(GArrowDataType) int2type = PGTypeToArrow(INT2OID);

	g_autoptr(GGandivaNode) arg1st = ExprToNode((ExprState *)linitial(sstate->fxprstate.args), NULL, schema, rettype);
	if (garrow_data_type_equal(int4type, *rettype))
	{
		varnode = arg1st;
	}
	else if (garrow_data_type_equal(int2type, *rettype))
	{
		g_autolist(GObject) cast_args = garrow_list_append_ptr(NULL, arg1st);
		GGandivaNode *cast_node = GGANDIVA_NODE(ggandiva_function_node_new("castINT", cast_args, int4type));
		varnode = cast_node;
	}
	else
		elog(ERROR, "Invalid data type for var in ScalarArrayOp");

	/* Loop over the array elements */
	s = (char *) ARR_DATA_PTR(arr);
	bitmap = ARR_NULLBITMAP(arr);
	bitmask = 1;

	for (i = 0; i < nitems; i++)
	{
		Datum		elt;

		/* Get array element, checking for NULL */
		if (bitmap && (*bitmap & bitmask) == 0)
		{
			g_autoptr(GGandivaNode) node = GGANDIVA_NODE(ggandiva_boolean_literal_node_new(false));
			orexprs = garrow_list_append_ptr(orexprs, node);
		}
		else
		{
			elt = fetch_att(s, typbyval, typlen);
			s = att_addlength_pointer(s, typlen, s);
			s = (char *) att_align_nominal(s, typalign);
			g_autoptr(GGandivaNode) arg2nd = PGTypeToGandivaNode(INT4OID, elt);

			g_autolist(GObject) args = NULL;
			args = garrow_list_append_ptr(args, varnode);
			args = garrow_list_append_ptr(args, arg2nd);
			g_autoptr(GArrowDataType) rstype = PGTypeToArrow(BOOLOID);
			g_autoptr(GGandivaNode) equal_node = GGANDIVA_NODE(ggandiva_function_node_new("equal", args, rstype));
			orexprs = garrow_list_append_ptr(orexprs, equal_node);
		}

		/* advance bitmap pointer if any */
		if (bitmap)
		{
			bitmask <<= 1;
			if (bitmask == 0x100 /* 1<<8 */)
			{
				bitmap++;
				bitmask = 1;
			}
		}
	}

	*rettype = PGTypeToArrow(BOOLOID);
	GGandivaNode *node = GGANDIVA_NODE(ggandiva_or_node_new(orexprs));

	return node;
}
/*
 * text_to_cstring
 *
 * Create a palloc'd, null-terminated C string from a text value.
 *
 * We support being passed a compressed or toasted text value.
 * This is a bit bogus since such values shouldn't really be referred to as
 * "text *", but it seems useful for robustness.  If we didn't handle that
 * case here, we'd need another routine that did, anyway.
 */
char *
text_to_cstring(const text *t)
{
	/* must cast away the const, unfortunately */
	text	   *tunpacked = pg_detoast_datum_packed((struct varlena *) t);
	int			len = VARSIZE_ANY_EXHDR(tunpacked);
	char	   *result;

	result = (char *) palloc(len + 1);
	memcpy(result, VARDATA_ANY(tunpacked), len);
	result[len] = '\0';

	if (tunpacked != t)
		pfree(tunpacked);

	return result;
}

/*
 *Convert a PG expr node to Gandiva node.
 *
 * attrs: the attributes corresponding to expressions. Note that AttrNumber
 *        in the expressions is for the outer/inner slot, not intermediate
 *        slot, such as projslot of hashAgg.
 * rettype: the result field type.
 *
 * Caller need to free the returned GGandivaNode*
 * by ARROW_FREE(GGandivaNode, &ret_type);
 */
GGandivaNode *
ExprToNode(ExprState *exprstate, Expr *expr, GArrowSchema *schema, GArrowDataType **rettype)
{
	g_autoptr(GArrowDataType) lrettype = NULL;
	g_autoptr(GGandivaNode) node = NULL;

	if (!expr && exprstate)
		expr = exprstate->expr;

	switch (nodeTag(expr))
	{
		case T_Var:
		{
			g_autoptr(GArrowField) field = NULL;
			Var *variable = (Var *) expr;

			field = garrow_schema_get_field(schema, variable->varattno - 1);
			garrow_store_func(lrettype, garrow_field_get_data_type(field));
			garrow_store_func(node, ggandiva_field_node_new(field));
			break;
		}

		case T_Const:
		{
			Const  *con = (Const *) expr;

			switch (con->consttype)
			{
				case INT4OID:
				case DATEOID:
					garrow_store_func(lrettype, PGTypeToArrow(INT4OID));
					garrow_store_func(node, ggandiva_int32_literal_node_new(DatumGetInt32(con->constvalue)));
					break;
				case INT8OID:
					garrow_store_func(lrettype, PGTypeToArrow(INT8OID));
					garrow_store_func(node, ggandiva_int64_literal_node_new(DatumGetInt64(con->constvalue)));
					break;
				case TEXTOID:
				{
					garrow_store_func(lrettype, PGTypeToArrow(TEXTOID));
					text *t = DatumGetTextP(con->constvalue);
					const char *s = text_to_cstring(t);
					garrow_store_func(node, ggandiva_string_literal_node_new(s));
					break;
				}
				default:
				elog(ERROR, "invalid pg_type Oid for T_Const Expr: %d",
					 (int)con->consttype);
			}
			break;
		}
		case T_OpExpr:
		{
			OpExpr *funcexpr;
			OpExpr *opexpr = (OpExpr *) expr;
			const char *opname;
			GList *args = NULL;
			const FmgrVecBuiltin *fbp;
			ListCell   *l;

			/* get function information*/
			fbp = fmgr_isbuiltin_vec(opexpr->opfuncid);
			if (!fbp)
				elog(ERROR, "converting invalid opexpr to gandiva node, type id : %d",
					 (int)opexpr->opfuncid);

			/* specical case */
			if (opexpr->opfuncid == 850 || opexpr->opfuncid == 851)
			{
				garrow_store_func(node, TextlikeToGandiva(expr, schema, &lrettype));
			}
			else if (CastTypeNeeded(opexpr->opfuncid))
			{
				garrow_store_func(node, AddCastType(expr, schema, &lrettype));
			}
			else
			{
				opname = fbp->opname;

				funcexpr = (OpExpr *)expr;
				/* get args*/
				foreach(l, funcexpr->args)
				{
					g_autoptr(GGandivaNode) node_tmp = NULL;
					garrow_store_func(node_tmp, ExprToNode(NULL, lfirst(l), schema, &lrettype));
					args = garrow_list_append_ptr(args, node_tmp);
				}

				/* get return type*/
				garrow_store_func(lrettype, PGTypeToArrow(opexpr->opresulttype));

				garrow_store_func(node, ggandiva_function_node_new(opname, args, lrettype));

				garrow_list_free_ptr(&args);
			}
			break;
		}
		case T_FuncExpr:
		{
			ListCell   *l;
			FuncExpr	  *funcexpr = (FuncExpr *)expr;

			switch (funcexpr->funcid)
			{
				case F_SUBSTR_TEXT_INT4_INT4:
					{
						GList *args = NULL;

						/* get args */
						foreach (l, funcexpr->args)
						{
							g_autoptr(GGandivaNode) node_tmp = NULL;
							Expr *expr = lfirst(l);

							if (IsA(expr, Const))
							{
								Const *con = (Const *)expr;
								Oid orioid = con->consttype;
								con->consttype = INT8OID;
								garrow_store_func(node_tmp, ExprToNode(NULL, expr, schema, &lrettype));
								con->consttype = orioid;
							}
							else
							{
								garrow_store_func(node_tmp, ExprToNode(NULL, expr, schema, &lrettype));
							}
							args = garrow_list_append_ptr(args, node_tmp);
						}

						/* get return type */
						garrow_store_func(lrettype, PGTypeToArrow(funcexpr->funcresulttype));
						garrow_store_func(node, ggandiva_function_node_new("substr", args, lrettype));
						garrow_list_free_ptr(&args);
						break;
					}
				case F_UPPER_TEXT:
					{
						GList *args = NULL;
						foreach (l, funcexpr->args)
						{
							g_autoptr(GGandivaNode) node_tmp = NULL;
							Expr *expr = lfirst(l);
							garrow_store_func(node_tmp, ExprToNode(NULL, expr, schema, &lrettype));
							args = garrow_list_append_ptr(args, node_tmp);
						}
						/* get return type */
						garrow_store_func(lrettype, PGTypeToArrow(funcexpr->funcresulttype));
						garrow_store_func(node, ggandiva_function_node_new("upper", args, lrettype));
						garrow_list_free_ptr(&args);
						break;
					}
				default:
					{
						elog(ERROR, "doesn't process unsupported gandiva node, func id: %d", (int) funcexpr->funcid);
						break;
					}
			}
			break;
		}
		case T_TargetEntry:
		{
			GenericExprState *gstate = (GenericExprState *)exprstate;

			garrow_store_func(node, ExprToNode(gstate->arg, NULL, schema, &lrettype));
			break;
		}
		/*
         *	FIXME:this type in nodes.h file  not defined.
         */
		case T_Aggref:
		{
			Aggref	   *aggref = (Aggref *) expr;
			VecAggrefExprState *aggrefstate = (VecAggrefExprState *)exprstate;
			g_autoptr(GArrowField) field = NULL;
			const gchar *name;

			name = aggrefstate->name;
			garrow_store_func(lrettype, PGTypeToArrow(aggref->aggtype));
			garrow_store_func(field, garrow_field_new(name, lrettype));
			garrow_store_func(node, ggandiva_field_node_new(field));

			break;
		}

		case T_BoolExpr:
		{
			BoolExpr *bexpr = (BoolExpr *) expr;
			garrow_store_func(lrettype, (GArrowDataType *)garrow_boolean_data_type_new());

			if (bexpr->boolop == AND_EXPR || bexpr->boolop == OR_EXPR)
			{
				ListCell *l;
				GList *nodes_list = NULL;
				foreach(l, bexpr->args)
				{
					g_autoptr(GGandivaNode) node_tmp = NULL;
					garrow_store_func(node_tmp, ExprToNode(NULL, (Expr *) lfirst(l), schema, &lrettype));
					nodes_list = garrow_list_append_ptr(nodes_list, node_tmp);
				}

				if (bexpr->boolop == AND_EXPR)
				{
					garrow_store_func(node, ggandiva_and_node_new(nodes_list));
				}
				else
				{
					garrow_store_func(node, ggandiva_or_node_new(nodes_list));
				}

				garrow_list_free_ptr(&nodes_list);
			}
			break;
		}

		case T_ScalarArrayOpExpr:
		{
			garrow_store_func(node, ScalarArrayToGandiva(exprstate, schema, &lrettype));
			break;
		}

		case T_RelabelType:
		{
			/*
			 * Just valid with typecast from varchar to text.
			 */
			RelabelType *relabel = (RelabelType *) expr;
			if (relabel->resulttype == TEXTOID && relabel->relabelformat == COERCE_IMPLICIT_CAST)
			{
				if (IsA(relabel->arg, Var))
				{
					Var *var = (Var *)relabel->arg;
					if (var->vartype == VARCHAROID)
					{
						ExprState *es = makeNode(ExprState);
						es->expr = relabel->arg;
						garrow_store_func(node, ExprToNode(es, NULL, schema, &lrettype));
						break;
					}
				}
			}
			elog(ERROR, "invalid RelabelType node, just typecast from varchar to text allowed");

			break;
		}
		default:
			elog(ERROR, "converting unrecognized node type to Gandiva node: %d",
				 (int) nodeTag(expr));
	}

	garrow_store_ptr(*rettype, lrettype);

	return garrow_move_ptr(node);
}


static GGandivaNode *
TextlikeToGandiva(Expr *expr, GArrowSchema *schema, GArrowDataType **rettype)
{
	const char *opname = "";
	g_autolist(GObject) args = NULL;
	g_autoptr(GArrowDataType) lrettype = NULL;

	OpExpr *opexpr = (OpExpr *) expr;

	/* 1st arg to gandiva node */
	g_autoptr(GGandivaNode) node_tmp = NULL;
	node_tmp = ExprToNode(NULL, linitial(opexpr->args), schema, &lrettype);
	args = garrow_list_append_ptr(args, node_tmp);

	/* 2nd arg to gandiva node */
	if (!IsA(lsecond(opexpr->args), Const))
		elog(ERROR, "like operator with non-Const expr");

	Const *con = (Const *)lsecond(opexpr->args);

	char *value = text_to_cstring(DatumGetTextP(con->constvalue));
	int len = strlen(value);

	bool prefix_match = false;
	bool suffix_match = false;

	if (value[0] == '%')
		prefix_match = true;
	if (value[len-1] == '%')
		suffix_match = true;

	if (prefix_match && suffix_match)       /* like '%abc%' */
		opname = "is_substr";
	else if (prefix_match && !suffix_match) /* like '%abc'  */
		opname = "ends_with";
	else if (!prefix_match && suffix_match) /* like 'abc%'  */
		opname = "starts_with";
	else                                    /* like 'abc'   */
		opname = "equal";

	char *newval = pstrdup(value);
	if (suffix_match)
		newval[len-1] = '\0';
	if (prefix_match)
		++newval;

	g_autoptr(GGandivaNode) const_node = GGANDIVA_NODE(ggandiva_string_literal_node_new(newval));

	args = garrow_list_append_ptr(args, const_node);

	/* get return type*/
	garrow_store_func(lrettype, PGTypeToArrow(BOOLOID));

	g_autoptr(GGandivaNode) funcnode = GGANDIVA_NODE(ggandiva_function_node_new(opname, args, lrettype));

	if (opexpr->opfuncid == 850)
	{
		garrow_store_ptr(*rettype, lrettype);
		return garrow_move_ptr(funcnode);
	}
	/*
  	 * append "not" op for textlike node
  	 * No "not" op in gandiva, so use "if then else"
  	 */
	Assert(opexpr->opfuncid == 851);

	g_autoptr(GError) error = NULL;
	g_autoptr(GGandivaNode) then_node = NULL;
	g_autoptr(GGandivaNode) else_node = NULL;
	g_autoptr(GGandivaNode) node = NULL;

	then_node = GGANDIVA_NODE(ggandiva_boolean_literal_node_new(false));
	else_node = GGANDIVA_NODE(ggandiva_boolean_literal_node_new(true));
	node      = GGANDIVA_NODE(ggandiva_if_node_new(funcnode, then_node,
	                                               else_node, lrettype, &error));
	if (error)
		elog(ERROR, "Failed to make gandiva node for NOT, msg: %s", error->message);

	garrow_store_ptr(*rettype, lrettype);

	return node;
}


static GGandivaNode *
AddCastType(Expr *expr, GArrowSchema *schema, GArrowDataType **rettype)
{
	const char *opname = "";
	g_autolist(GObject) args = NULL;

	OpExpr *opexpr = (OpExpr *) expr;

	/* get function information*/
	const FmgrVecBuiltin *fbp = fmgr_isbuiltin_vec(opexpr->opfuncid);
	if (!fbp)
		elog(ERROR, "converting invalid opexpr to gandiva node, type id : %d",
			 (int)opexpr->opfuncid);
	opname = fbp->opname;

	Oid funcid = opexpr->opfuncid;
	if (funcid == 159 || funcid == 165 || funcid == 161 ||
	    funcid == 163 || funcid == 167 || funcid == 169)
	{
		/* int42eq, int42ne, ..., add castINT() for 2nd arg */
		g_autoptr(GGandivaNode) arg1st = ExprToNode(NULL, linitial(opexpr->args), schema, rettype);
		g_autoptr(GGandivaNode) arg2nd = ExprToNode(NULL, lsecond(opexpr->args), schema, rettype);

		/* add cast node for int2 to int4 */
		g_autolist(GObject) cast_args = NULL;
		g_autoptr(GArrowDataType) casttype = NULL;

		cast_args = garrow_list_append_ptr(NULL, arg2nd);
		casttype  = PGTypeToArrow(INT4OID);
		g_autoptr(GGandivaNode) cast_node = GGANDIVA_NODE(ggandiva_function_node_new("castINT", cast_args, casttype));

		/* add all args to list */
		args = garrow_list_append_ptr(args, arg1st);
		args = garrow_list_append_ptr(args, cast_node);
	}
	else if (funcid == 158 || funcid == 164 || funcid == 160 ||
	         funcid == 162 || funcid == 166 || funcid == 168 || funcid == 178)
	{
		/* int24eq, int24ne, ..., add castINT() for 1st arg */
		g_autoptr(GGandivaNode)arg1st = ExprToNode(NULL, linitial(opexpr->args), schema, rettype);
		g_autoptr(GGandivaNode)arg2nd = ExprToNode(NULL, lsecond(opexpr->args), schema, rettype);

		/* add cast node for int2 to int4 */
		g_autolist(GObject) cast_args = NULL;
		g_autoptr(GArrowDataType) casttype = NULL;

		cast_args = garrow_list_append_ptr(NULL, arg1st);
		casttype  = PGTypeToArrow(INT4OID);
		g_autoptr(GGandivaNode)cast_node = GGANDIVA_NODE(ggandiva_function_node_new("castINT", cast_args, casttype));

		/* add all args to list */
		args = garrow_list_append_ptr(args, cast_node);
		args = garrow_list_append_ptr(args, arg2nd);
	}
	else if (funcid == 474  || funcid == 475  || funcid == 476  ||
	         funcid == 477  || funcid == 478  || funcid == 479  ||
	         funcid == 1856 || funcid == 1857 || funcid == 1858 ||
	         funcid == 1859 || funcid == 1860 || funcid == 1861 ||
	         funcid == 1274 || funcid == 1275 || funcid == 1276 || funcid == 1277)
	{
		/* int84xx, int82xx, ..., add castBIGINT() for 2nd arg */
		g_autoptr(GGandivaNode)arg1st = ExprToNode(NULL, linitial(opexpr->args), schema, rettype);
		g_autoptr(GGandivaNode)arg2nd = ExprToNode(NULL, lsecond(opexpr->args), schema, rettype);

		/* add cast node for int4 to int8 */
		g_autolist(GObject) cast_args = NULL;
		g_autoptr(GArrowDataType) casttype = NULL;

		cast_args = garrow_list_append_ptr(NULL, arg2nd);
		casttype = PGTypeToArrow(INT8OID);
		g_autoptr(GGandivaNode)cast_node = GGANDIVA_NODE(ggandiva_function_node_new("castBIGINT", cast_args, casttype));

		/* add all args to list */
		args = garrow_list_append_ptr(args, arg1st);
		args = garrow_list_append_ptr(args, cast_node);
	}
	else if (funcid == 852  || funcid == 853  || funcid == 854  ||
	         funcid == 855  || funcid == 856  || funcid == 857  ||
	         funcid == 1850 || funcid == 1851 || funcid == 1852 ||
	         funcid == 1853 || funcid == 1854 || funcid == 1855 ||
	         funcid == 1278 || funcid == 1279 || funcid == 1280 || funcid == 1281)
	{
		/* int48xx, int28xx, ..., add castBIGINT() for 1st arg */
		g_autoptr(GGandivaNode)arg1st = ExprToNode(NULL, linitial(opexpr->args), schema, rettype);
		g_autoptr(GGandivaNode)arg2nd = ExprToNode(NULL, lsecond(opexpr->args), schema, rettype);

		/* add cast node for int4 to int8 */
		g_autolist(GObject) cast_args = NULL;
		g_autoptr(GArrowDataType) casttype = NULL;

		cast_args = garrow_list_append_ptr(NULL, arg1st);
		casttype = PGTypeToArrow(INT8OID);
		g_autoptr(GGandivaNode)cast_node = GGANDIVA_NODE(ggandiva_function_node_new("castBIGINT", cast_args, casttype));

		/* add all args to list */
		args = garrow_list_append_ptr(args, cast_node);
		args = garrow_list_append_ptr(args, arg2nd);
	}
	else
	{
		Assert(false);
		elog(ERROR, "should not run here");
	}

	*rettype = PGTypeToArrow(opexpr->opresulttype);
	GGandivaNode *node = GGANDIVA_NODE(ggandiva_function_node_new(fbp->opname, args, *rettype));

	return node;
}


GGandivaNode*
make_hash32_fnode(GGandivaNode *node)
{
	GList* args = NULL;
	g_autoptr(GGandivaNode) arg = garrow_copy_ptr(node);
	g_autoptr(GArrowInt32DataType) dt_int32 = NULL;

	dt_int32 = garrow_int32_data_type_new();

	args = garrow_list_append_ptr(args, arg);

	/* function node */
	GGandivaNode *funcnode = GGANDIVA_NODE(ggandiva_function_node_new("cohash32", args, GARROW_DATA_TYPE(dt_int32)));

	garrow_list_free_ptr(&args);
	return funcnode;
}

GGandivaNode*
make_cdbhash32_fnode(GGandivaNode *node_first, GGandivaNode *node_second)
{
	GList* args = NULL;
	g_autoptr(GGandivaNode) arg1 = garrow_copy_ptr(node_first);
	g_autoptr(GGandivaNode) arg2 = garrow_copy_ptr(node_second);
	g_autoptr(GArrowInt32DataType) dt_int32 = NULL;

	dt_int32 = garrow_int32_data_type_new();

	args = garrow_list_append_ptr(args, arg1);
	args = garrow_list_append_ptr(args, arg2);

	/* function node */
	GGandivaNode *funcnode = GGANDIVA_NODE(ggandiva_function_node_new("cdbhash32", args, GARROW_DATA_TYPE(dt_int32)));

	garrow_list_free_ptr(&args);
	return funcnode;
}

GGandivaNode*
make_result_cols_hash_fnode(GArrowSchema *schema, List *hash_keys)
{
	GGandivaNode *pre_node = NULL;
	g_autoptr(GError) error = NULL;
	ListCell *lc;
	foreach(lc, hash_keys)
	{
		g_autoptr(GArrowField) field = NULL;
		g_autoptr(GGandivaNode) cur_node = NULL;
		int key = lfirst_int(lc);
		field = garrow_schema_get_field(schema, key);
		garrow_store_func(cur_node, ggandiva_field_node_new(field));
		if (!pre_node)
		{
			garrow_store_func(pre_node, make_hash32_fnode(cur_node));
		}
		else
		{
			garrow_store_func(pre_node, make_cdbhash32_fnode(cur_node, pre_node));
		}
	}
	return pre_node;
}

GGandivaNode*
make_xor_fnode(GGandivaNode *node1, GGandivaNode *node2)
{
	GList* args = NULL;
	g_autoptr(GGandivaNode) arg1 = garrow_copy_ptr(node1);
	g_autoptr(GGandivaNode) arg2 = garrow_copy_ptr(node2);
	g_autoptr(GArrowInt32DataType) dt_int32 = NULL;

	dt_int32 = garrow_int32_data_type_new();

	/* argument node */
	args = garrow_list_append_ptr(args, arg1);
	args = garrow_list_append_ptr(args, arg2);

	/* function node */
	GGandivaNode *funcnode = GGANDIVA_NODE(ggandiva_function_node_new("bitwise_xor", args, GARROW_DATA_TYPE(dt_int32)));

	garrow_list_free_ptr(&args);
	return funcnode;
}

GGandivaNode*
make_int32_literal_fnode(int32 val)
{
	GGandivaNode *funcnode =  GGANDIVA_NODE(ggandiva_int32_literal_node_new(val));

	return funcnode;
}


GGandivaNode*
make_left_shift_fnode(GGandivaNode *node,  GGandivaNode *lnode)
{
	GList* args = NULL;
	g_autoptr(GArrowInt32DataType) dt_int32 = NULL;
	g_autoptr(GGandivaNode) arg1 = garrow_copy_ptr(node);
	g_autoptr(GGandivaNode) arg2 = garrow_copy_ptr(lnode);

	dt_int32 = garrow_int32_data_type_new();

	/* argument node */
	args = garrow_list_append_ptr(args, arg1);
	args = garrow_list_append_ptr(args, arg2);

	/* function node */
	GGandivaNode *funcnode = GGANDIVA_NODE(ggandiva_function_node_new("bitwise_shift_left", args, GARROW_DATA_TYPE(dt_int32)));

	garrow_list_free_ptr(&args);
	return funcnode;
}

GGandivaNode*
make_right_shift_fnode(GGandivaNode *node,  GGandivaNode *rnode)
{
	GList* args = NULL;
	g_autoptr(GArrowInt32DataType) dt_int32 = NULL;
	g_autoptr(GGandivaNode) arg1 = garrow_copy_ptr(node);
	g_autoptr(GGandivaNode) arg2 = garrow_copy_ptr(rnode);

	dt_int32 = garrow_int32_data_type_new();

	/* argument node */
	args = garrow_list_append_ptr(args, arg1);
	args = garrow_list_append_ptr(args, arg2);

	/* function node */
	GGandivaNode *funcnode = GGANDIVA_NODE(ggandiva_function_node_new("bitwise_shift_right", args, GARROW_DATA_TYPE(dt_int32)));

	garrow_list_free_ptr(&args);
	return funcnode;
}

GGandivaNode*
make_bitwise_or_fnode(GGandivaNode *node1,  GGandivaNode *node2)
{
	g_autoptr(GArrowInt32DataType) dt_int32 = NULL;
	GList* args = NULL;
	g_autoptr(GGandivaNode) arg1 = garrow_copy_ptr(node1);
	g_autoptr(GGandivaNode) arg2 = garrow_copy_ptr(node2);

	dt_int32 = garrow_int32_data_type_new();

	/* argument node */
	args = garrow_list_append_ptr(args, arg1);
	args = garrow_list_append_ptr(args, arg2);

	/* function node */
	GGandivaNode *funcnode = GGANDIVA_NODE(ggandiva_function_node_new("bitwise_or", args, GARROW_DATA_TYPE(dt_int32)));

	garrow_list_free_ptr(&args);
	return funcnode;
}
