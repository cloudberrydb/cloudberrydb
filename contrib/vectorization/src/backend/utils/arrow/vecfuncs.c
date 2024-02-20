/*-------------------------------------------------------------------------
 *
 * vecfuncs.c
 *
 * arrow functions tables.
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 *
 * IDENTIFICATION
 *	    src/include/utils/arrow/vecfuntab.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/arrow.h"
#include "utils/vecfuncs.h"
#include "utils/wrapper.h"
#include "utils/decimal.h"
#include "utils/array.h"
#include "vecexecutor/execslot.h"

const char *LEFT_PREFIX = "left_";
const char *RIGHT_PREFIX = "right_";

#define ARROW_FUNC(prefix, type, func, ...) prefix##_##type##_##func(__VA_ARGS__)

/*
 * new an arrow array.
 */
void *
ArrowArrayNew(Oid type, int64 length, uint8 *data, uint8 *null_bitmap)
{
	GError *error = NULL;
	g_autoptr(GArrowBuffer) data_buffer = NULL;
	g_autoptr(GArrowBuffer) bitmap_buffer;
	gint64 data_size, bitmap_size, n_nulls;

	bitmap_size = (length % 8 == 0) ? length / 8 : (length + 7) / 8;

	if (null_bitmap == NULL)
	{
		n_nulls = 0;
		bitmap_buffer = NULL;
	} else {
		n_nulls = -1;
		bitmap_buffer = garrow_buffer_new(null_bitmap, bitmap_size);
	}

	switch (type)
	{
	case BOOLOID:
		data_size = length * sizeof(bool);
		data_buffer = garrow_buffer_new(data, data_size);

		return (void *)garrow_boolean_array_new(length, data_buffer,
				bitmap_buffer, n_nulls);
	case INT2OID:
		data_size = length * sizeof(gint16);
		data_buffer = garrow_buffer_new(data, data_size);

		return (void *)garrow_int16_array_new(length, data_buffer,
				bitmap_buffer, n_nulls);
		break;
	break;
	case INT4OID:
		data_size = length * sizeof(gint32);
		data_buffer = garrow_buffer_new(data, data_size);

		return (void *)garrow_int32_array_new(length, data_buffer,
				bitmap_buffer, n_nulls);
		break;
	case INT8OID:
		data_size = length * sizeof(gint64);
		data_buffer = garrow_buffer_new(data, data_size);

		return (void *)garrow_int64_array_new(length, data_buffer,
				bitmap_buffer, n_nulls);
		break;
	case DATEOID:
		data_size = length * sizeof(gint32);
		data_buffer = garrow_buffer_new(data, data_size);

		return (void *)garrow_date32_array_new(length, data_buffer,
				bitmap_buffer, n_nulls);
		break;
	case TIMEOID:
	{
		g_autoptr(GArrowTime64DataType) arrowtype;

		data_size = length * sizeof(gint64);
		data_buffer = garrow_buffer_new(data, data_size);

		arrowtype = garrow_time64_data_type_new(GARROW_TIME_UNIT_MICRO, &error);
		if (error)
			elog(ERROR, "Failed to make GArrowDataType for TIMEOID, cause: %s", error->message);

		return (void *)garrow_time64_array_new(arrowtype, length, data_buffer,
				bitmap_buffer, n_nulls);
		break;
	}
	case TEXTOID:
	{
		GError *error = NULL;
		g_autoptr(GArrowStringArrayBuilder) builder = garrow_string_array_builder_new();
		char **strs = (char**)data;

		for (int i = 0; i < length; ++i)
		{
			uint8 mask = (char)0x01 << (i & 0x7);
			bool  isnull = !(null_bitmap[i/8] & mask);
			if (isnull)
				garrow_string_array_builder_append_string(builder, "", &error);
			else
			{
				Assert(strs[i]);
				garrow_string_array_builder_append_string(builder, strs[i], &error);
			}
			if (error)
				elog(ERROR, "make string array error: %s", error->message);
		}

		error = NULL;
		g_autoptr(GArrowArray) rs = garrow_array_builder_finish((GArrowArrayBuilder *)builder, &error);
		if (error)
			elog(ERROR, "finish string builer error: %s", error->message);
		return garrow_move_ptr(rs);
		break;
	}
	case BYTEAOID:
	{
		GError *error = NULL;
		g_autoptr(GArrowBinaryArrayBuilder) builder = garrow_binary_array_builder_new();
		char **vals = (char**)data;

		for (int i = 0; i < length; ++i)
		{
			uint8 mask = (char)0x01 << (i & 0x7);
			bool  isnull = !(null_bitmap[i/8] & mask);
			if (isnull)
				garrow_array_builder_append_null((GArrowArrayBuilder *)builder, &error);
			else
			{
				garrow_binary_array_builder_append_value(builder, (const guint8*)vals[i], VARSIZE_ANY(vals[i]), &error);
			}
			if (error)
				elog(ERROR, "make string array error: %s", error->message);
		}

		error = NULL;
		g_autoptr(GArrowArray) rs = garrow_array_builder_finish((GArrowArrayBuilder *)builder, &error);
		if (error)
			elog(ERROR, "finish string builer error: %s", error->message);
		return garrow_move_ptr(rs);
		break;
	}
	default:
		elog(ERROR, "cannot new arrow array of PG type %d", type);
		break;
	}
	return NULL;
}

GArrowScalar *
ArrowScalarNew(GArrowType type, Datum datum, Oid pg_type)
{
	g_autoptr(GArrowScalar) ret = NULL;

	switch (type)
	{
		case GARROW_TYPE_NA:
			ret = (GArrowScalar*)garrow_null_scalar_new(PGTypeToArrow(pg_type));
			break;
		case GARROW_TYPE_BOOLEAN:
			ret = (GArrowScalar*)garrow_boolean_scalar_new(DatumGetBool(datum));
			break;
		case GARROW_TYPE_INT8:
			ret = (GArrowScalar*)garrow_int8_scalar_new(DatumGetInt8(datum));
			break;
		case GARROW_TYPE_INT16:
			ret = (GArrowScalar*)garrow_int16_scalar_new(DatumGetInt16(datum));
			break;
		case GARROW_TYPE_INT32:
			ret = (GArrowScalar*)garrow_int32_scalar_new(DatumGetInt32(datum));
			break;
		case GARROW_TYPE_INT64:
			ret = (GArrowScalar*)garrow_int64_scalar_new(DatumGetInt64(datum));
			break;
		case GARROW_TYPE_FLOAT:
			ret = (GArrowScalar*)garrow_float_scalar_new(DatumGetFloat4(datum));
			break;
		case GARROW_TYPE_DOUBLE:
			ret = (GArrowScalar*)garrow_double_scalar_new(DatumGetFloat8(datum));
			break;
		case GARROW_TYPE_DATE32:
			ret = (GArrowScalar*)garrow_date32_scalar_new(DatumGetInt32(datum));
			break;
		case GARROW_TYPE_DATE64:
			ret = (GArrowScalar *)garrow_date64_scalar_new(DatumGetInt64(datum));
			break;
		case GARROW_TYPE_TIMESTAMP:
		{
			g_autoptr(GArrowTimestampDataType) timestamp_data_type = garrow_timestamp_data_type_new(GARROW_TIME_UNIT_MICRO);
			ret = (GArrowScalar*)garrow_timestamp_scalar_new(timestamp_data_type,DatumGetInt64(datum));
			break;
		}

		case GARROW_TYPE_STRING:
		{
			struct varlena *s = (struct varlena *) datum;
			g_autoptr(GArrowBuffer) buffer = NULL;
			char *str = NULL;

			str = text_to_cstring(s);
			buffer = garrow_buffer_new((guint8 *)str, VARSIZE_ANY_EXHDR(s));
			ret = (GArrowScalar*)garrow_string_scalar_new(buffer);
			break;
		}
		case GARROW_TYPE_LIST:
		{
			ArrayType  *arr;
			ArrayIterator array_iterator;
			Datum value;
			bool isnull;
			GError *error = NULL;
			GArrowType arrtype;
			g_autoptr(GArrowArray) array;

			arr = DatumGetArrayTypeP(datum);
			arrtype = PGTypeToArrowID(arr->elemtype);
			array_iterator = array_create_iterator(arr, 0, NULL);

			switch (arrtype)
			{
				case GARROW_TYPE_INT8:
				{
					g_autoptr(GArrowInt8ArrayBuilder) builder =
							garrow_int8_array_builder_new();

					while (array_iterate(array_iterator, &value, &isnull))
					{
						if (isnull)
							garrow_array_builder_append_null(GARROW_ARRAY_BUILDER(builder), &error);
						else
							garrow_int8_array_builder_append_value(builder,
																   DatumGetInt8(value), &error);
					}
					array = garrow_array_builder_finish(
							GARROW_ARRAY_BUILDER(builder), &error);
					break;
				}
				case GARROW_TYPE_INT16:
				{
					g_autoptr(GArrowInt16ArrayBuilder) builder =
							garrow_int16_array_builder_new();

					while (array_iterate(array_iterator, &value, &isnull))
					{
						if (isnull)
							garrow_array_builder_append_null(GARROW_ARRAY_BUILDER(builder), &error);
						else
							garrow_int16_array_builder_append_value(builder,
																	DatumGetInt16(value), &error);
					}
					array = garrow_array_builder_finish(
							GARROW_ARRAY_BUILDER(builder), &error);
					break;
				}
				case GARROW_TYPE_INT32:
				{
					g_autoptr(GArrowInt32ArrayBuilder) builder =
							garrow_int32_array_builder_new();

					while (array_iterate(array_iterator, &value, &isnull))
					{
						if (isnull)
							garrow_array_builder_append_null(GARROW_ARRAY_BUILDER(builder), &error);
						else
							garrow_int32_array_builder_append_value(builder,
																	DatumGetInt32(value), &error);
					}
					array = garrow_array_builder_finish(
							GARROW_ARRAY_BUILDER(builder), &error);
					break;
				}
				case GARROW_TYPE_INT64:
				{
					g_autoptr(GArrowInt64ArrayBuilder) builder =
							garrow_int64_array_builder_new();

					while (array_iterate(array_iterator, &value, &isnull))
					{
						if (isnull)
							garrow_array_builder_append_null(GARROW_ARRAY_BUILDER(builder), &error);
						else
							garrow_int64_array_builder_append_value(builder,
																	DatumGetInt64(value), &error);
					}
					array = garrow_array_builder_finish(
							GARROW_ARRAY_BUILDER(builder), &error);
					break;
				}
				case GARROW_TYPE_FLOAT:
				{
					g_autoptr(GArrowFloatArrayBuilder) builder =
							garrow_float_array_builder_new();

					while (array_iterate(array_iterator, &value, &isnull))
					{
						if (isnull)
							garrow_array_builder_append_null(GARROW_ARRAY_BUILDER(builder), &error);
						else
							garrow_float_array_builder_append_value(builder,
																	DatumGetFloat4(value), &error);
					}
					array = garrow_array_builder_finish(
							GARROW_ARRAY_BUILDER(builder), &error);
					break;
				}
				case GARROW_TYPE_DOUBLE:
				{
					g_autoptr(GArrowDoubleArrayBuilder) builder =
							garrow_double_array_builder_new();

					while (array_iterate(array_iterator, &value, &isnull))
					{
						if (isnull)
							garrow_array_builder_append_null(GARROW_ARRAY_BUILDER(builder), &error);
						else
							garrow_double_array_builder_append_value(builder,
																	 DatumGetFloat8(value), &error);
					}
					array = garrow_array_builder_finish(
							GARROW_ARRAY_BUILDER(builder), &error);
					break;
				}
				case GARROW_TYPE_STRING:
				{
					g_autoptr(GArrowStringArrayBuilder) builder =
						garrow_string_array_builder_new();

					while (array_iterate(array_iterator, &value, &isnull))
					{
						if (isnull)
							garrow_array_builder_append_null(GARROW_ARRAY_BUILDER(builder), &error);
						else
						{
							struct varlena *s = (struct varlena *) value;
							char *str = text_to_cstring(s);
							garrow_string_array_builder_append_string(builder, str, &error);
						}
					}
					array = garrow_array_builder_finish(
							GARROW_ARRAY_BUILDER(builder), &error);
					break;
				}
				case GARROW_TYPE_DATE32:
				{
				    g_autoptr(GArrowDate32ArrayBuilder) builder =
						garrow_date32_array_builder_new();
					while (array_iterate(array_iterator, &value, &isnull))
					{
						if (isnull)
							garrow_array_builder_append_null(GARROW_ARRAY_BUILDER(builder), &error);
						else
							garrow_date32_array_builder_append_value(builder,
																	 DatumGetInt32(value), &error);
					}
					array = garrow_array_builder_finish(
							GARROW_ARRAY_BUILDER(builder), &error);
					break;
				}
				default:
					elog(ERROR, "unsupported arrow list data type: %d", arrtype);
			}
			ret = (void *)garrow_list_scalar_new(array);
			array_free_iterator(array_iterator);
			break;
		}
		default:
			elog(ERROR, "unsupported arrow data type: %d", type);
	}

	return garrow_move_ptr(ret);
}

/*
 * Get PG Datum by garrow_xxx_array_get_value
 */
Datum
ArrowArrayGetValueDatum(void *array, int i)
{
	g_autoptr(GArrowArray) arrowarray;
	GArrowType type;

	arrowarray = GARROW_ARRAY(garrow_get_argument(array));
	type = garrow_array_get_value_type(arrowarray);

	switch (type)
	{
		case GARROW_TYPE_BOOLEAN:
			return BoolGetDatum(garrow_boolean_array_get_value(
					GARROW_BOOLEAN_ARRAY(arrowarray), i));
			break;
		case GARROW_TYPE_INT8:
			return Int8GetDatum(garrow_int8_array_get_value(
					GARROW_INT8_ARRAY(arrowarray), i));
			break;
		case GARROW_TYPE_INT16:
			return Int16GetDatum(garrow_int16_array_get_value(
					GARROW_INT16_ARRAY(arrowarray), i));
			break;
		case GARROW_TYPE_INT32:
			return Int32GetDatum(garrow_int32_array_get_value(
					GARROW_INT32_ARRAY(arrowarray), i));
			break;
		case GARROW_TYPE_INT64:
			return Int64GetDatum(garrow_int64_array_get_value(
					GARROW_INT64_ARRAY(arrowarray), i));
			break;
		case GARROW_TYPE_FLOAT:
			return Float4GetDatum(garrow_float_array_get_value(
					GARROW_FLOAT_ARRAY(arrowarray), i));
			break;
		case GARROW_TYPE_DOUBLE:
			return Float8GetDatum(garrow_double_array_get_value(
					GARROW_DOUBLE_ARRAY(arrowarray), i));
			break;
		case GARROW_TYPE_DATE32:
			return Int32GetDatum(garrow_date32_array_get_value(
					GARROW_DATE32_ARRAY(arrowarray), i));
			break;
		case GARROW_TYPE_DATE64:
			return Int64GetDatum(garrow_date64_array_get_value(
					GARROW_DATE64_ARRAY(arrowarray), i));
			break;
		case GARROW_TYPE_BINARY:
		{
			gsize size;
			g_autoptr(GBytes) data;

			data = garrow_binary_array_get_value(
					GARROW_BINARY_ARRAY(arrowarray), i);
			return PointerGetDatum(g_bytes_get_data(data, &size));
			break;
		}
		case GARROW_TYPE_STRING:
		{
			g_autofree gchar *data;

			data = garrow_string_array_get_string(GARROW_STRING_ARRAY(arrowarray), i);
			return (Datum) cstring_to_text(data);
			break;
		}
		case GARROW_TYPE_DECIMAL128:
		{
			return Decimal128ArrayGetDatum(GARROW_DECIMAL128_ARRAY(arrowarray), i);
			break;
		}
		case GARROW_TYPE_DECIMAL256:
		{
			return Decimal256ArrayGetDatum(GARROW_DECIMAL256_ARRAY(arrowarray), i);
			break;
		}
		case GARROW_TYPE_INT128:
		{
			return Int128ArrayGetDatum(GARROW_INT128_ARRAY(arrowarray), i);
		}
		case GARROW_TYPE_TIMESTAMP:
		{
			return Int64GetDatum(garrow_timestamp_array_get_value(GARROW_TIMESTAMP_ARRAY(arrowarray), i));
			break;
		}
		default:
			elog(ERROR, "unsupported arrow data type: %d", type);
	}
	return (Datum) 0;
}

/*
 * Get PG Datum from GArrowScalar
 */
Datum
ArrowScalarGetDatum(void *scalar)
{
	g_autoptr(GArrowScalar) arrowscalar;
	g_autoptr(GArrowDataType) datatype;
	GArrowType type;

	arrowscalar = GARROW_SCALAR(garrow_get_argument(scalar));
	datatype = garrow_scalar_get_data_type(arrowscalar);
	type = garrow_data_type_get_id(datatype);

	switch (type)
	{
		case GARROW_TYPE_BOOLEAN:
			return BoolGetDatum(garrow_boolean_scalar_get_value(
					GARROW_BOOLEAN_SCALAR(arrowscalar)));
			break;
		case GARROW_TYPE_INT8:
			return Int8GetDatum(garrow_int8_scalar_get_value(
					GARROW_INT8_SCALAR(arrowscalar)));
			break;
		case GARROW_TYPE_INT16:
			return Int16GetDatum(garrow_int16_scalar_get_value(
					GARROW_INT16_SCALAR(arrowscalar)));
			break;
		case GARROW_TYPE_INT32:
			return Int32GetDatum(garrow_int32_scalar_get_value(
					GARROW_INT32_SCALAR(arrowscalar)));
			break;
		case GARROW_TYPE_INT64:
			return Int64GetDatum(garrow_int64_scalar_get_value(
					GARROW_INT64_SCALAR(arrowscalar)));
			break;
		case GARROW_TYPE_FLOAT:
			return Float4GetDatum(garrow_float_scalar_get_value(
					GARROW_FLOAT_SCALAR(arrowscalar)));
			break;
		case GARROW_TYPE_DOUBLE:
			return Float8GetDatum(garrow_double_scalar_get_value(
					GARROW_DOUBLE_SCALAR(arrowscalar)));
			break;
		case GARROW_TYPE_DATE32:
			return Int32GetDatum(garrow_date32_scalar_get_value(
					GARROW_DATE32_SCALAR(arrowscalar)));
			break;
		case GARROW_TYPE_BINARY:
		{
			g_autoptr(GArrowBuffer) buffer;
			g_autoptr(GBytes) data;

			buffer = garrow_base_binary_scalar_get_value(
					GARROW_BASE_BINARY_SCALAR(arrowscalar));
			data = garrow_buffer_get_data(buffer);
			return GBytesToDatum(data);
			break;
		}
		case GARROW_TYPE_STRING:
		{
			gsize size;
			g_autoptr(GArrowBuffer) buffer;
			g_autoptr(GBytes) data;

			buffer = garrow_base_binary_scalar_get_value(
					GARROW_BASE_BINARY_SCALAR(arrowscalar));
			data = garrow_buffer_get_data(buffer);
			return (Datum) cstring_to_text(g_bytes_get_data(data, &size));
			break;
		}
		default:
			elog(ERROR, "unsupported arrow data type: %d", type);
	}
	return (Datum) 0;
}

/*
 * Convert a scalar to arrow constant array.
 */
GArrowArray *
ArrowScalarToArray(void *arrow_scalar_datum, int length)
{
	int i;
	GArrowType type;
	g_autoptr(GArrowScalar) scalar = NULL;
	g_autoptr(GArrowDataType) datatype = NULL;
	g_autoptr(GArrowBuffer) data_buffer = NULL;

	scalar = garrow_datum_get_scalar(GARROW_SCALAR_DATUM(arrow_scalar_datum));
	datatype = garrow_scalar_get_data_type(scalar);
	type = garrow_data_type_get_id(datatype);

	switch (type)
	{
		case GARROW_TYPE_BOOLEAN:
		{
			gboolean value;
			gboolean *array_data;

			value = garrow_boolean_scalar_get_value(
					GARROW_BOOLEAN_SCALAR(scalar));
			array_data = palloc(sizeof(gboolean) * length);
			for (i = 0; i < length; i++)
				array_data[i] = value;
			data_buffer = garrow_buffer_new((guint8 *)array_data,
					length * sizeof(gboolean));
			return GARROW_ARRAY(garrow_boolean_array_new(length, data_buffer,
					NULL, -1));
		}
			break;
		case GARROW_TYPE_INT8:
		{
			gint8 value;
			gint8 *array_data;

			value = garrow_int8_scalar_get_value(
					GARROW_INT8_SCALAR(scalar));
			array_data = palloc(sizeof(gint8) * length);
			for (i = 0; i < length; i++)
				array_data[i] = value;
			data_buffer = garrow_buffer_new((guint8 *)array_data,
					length * sizeof(gint8));
			return GARROW_ARRAY(garrow_int8_array_new(length, data_buffer,
					NULL, -1));
		}
			break;
		case GARROW_TYPE_INT16:
		{
			gint16 value;
			gint16 *array_data;

			value = garrow_int16_scalar_get_value(
					GARROW_INT16_SCALAR(scalar));
			array_data = palloc(sizeof(gint16) * length);
			for (i = 0; i < length; i++)
				array_data[i] = value;
			data_buffer = garrow_buffer_new((guint8 *)array_data,
					length * sizeof(gint16));
			return GARROW_ARRAY(garrow_int16_array_new(length, data_buffer,
					NULL, -1));
		}
			break;
		case GARROW_TYPE_INT32:
		{
			gint32 value;
			gint32 *array_data;

			value = garrow_int32_scalar_get_value(
					GARROW_INT32_SCALAR(scalar));
			array_data = palloc(sizeof(gint32) * length);
			for (i = 0; i < length; i++)
				array_data[i] = value;
			data_buffer = garrow_buffer_new((guint8 *)array_data,
					length * sizeof(gint32));
			return GARROW_ARRAY(garrow_int32_array_new(length, data_buffer,
					NULL, -1));
		}
			break;
		case GARROW_TYPE_UINT32:
		{
			guint32 value;
			guint32 *array_data;

			value = garrow_uint32_scalar_get_value(
					GARROW_UINT32_SCALAR(scalar));
			array_data = palloc(sizeof(guint32) * length);
			for (i = 0; i < length; i++)
				array_data[i] = value;
			data_buffer = garrow_buffer_new((guint8 *)array_data,
					length * sizeof(guint32));
			return GARROW_ARRAY(garrow_uint32_array_new(length, garrow_copy_ptr(data_buffer),
					NULL, -1));
		}
			break;
		case GARROW_TYPE_INT64:
		{
			gint64 value;
			gint64 *array_data;

			value = garrow_int64_scalar_get_value(
					GARROW_INT64_SCALAR(scalar));
			array_data = palloc(sizeof(gint64) * length);
			for (i = 0; i < length; i++)
				array_data[i] = value;
			data_buffer = garrow_buffer_new((guint8 *)array_data,
					length * sizeof(gint64));
			return GARROW_ARRAY(garrow_int64_array_new(length, data_buffer,
					NULL, -1));
		}
			break;
		case GARROW_TYPE_FLOAT:
		{
			gfloat value;
			gfloat *array_data;

			value = garrow_float_scalar_get_value(
					GARROW_FLOAT_SCALAR(scalar));
			array_data = palloc(sizeof(gfloat) * length);
			for (i = 0; i < length; i++)
				array_data[i] = value;
			data_buffer = garrow_buffer_new((guint8 *)array_data,
					length * sizeof(gfloat));
			return GARROW_ARRAY(garrow_float_array_new(length, data_buffer,
					NULL, -1));
		}
			break;
		case GARROW_TYPE_DOUBLE:
		{
			gdouble value;
			gdouble *array_data;

			value = garrow_double_scalar_get_value(
					GARROW_DOUBLE_SCALAR(scalar));
			array_data = palloc(sizeof(gdouble) * length);
			for (i = 0; i < length; i++)
				array_data[i] = value;
			data_buffer = garrow_buffer_new((guint8 *)array_data,
					length * sizeof(gdouble));
			return GARROW_ARRAY(garrow_double_array_new(length, data_buffer,
					NULL, -1));
		}
			break;
		default:
			elog(ERROR, "unsupported arrow data type: %d", type);\
	}
	return NULL;
}

int64 GetArrowArraySize(void *ptr_array)
{
	Assert(ptr_array);
	return garrow_array_get_length((GArrowArray*) ptr_array);
}

void* SerializeRecordBatch(void *record_batch)
{
	g_autoptr(GArrowResizableBuffer) buffer = NULL;
	g_autoptr(GArrowOutputStream) stream = NULL;
	GArrowRecordBatchWriter *writer = NULL;
	g_autoptr(GArrowSchema) schema = NULL;
	GError *error = NULL;
	g_autoptr(GArrowWriteOptions) options = NULL;
	GArrowRecordBatch *batch = GARROW_RECORD_BATCH(record_batch);

	Assert(record_batch != NULL);

	buffer = garrow_resizable_buffer_new(0, &error);
	if (error)
		elog(ERROR, "Cann't new resizable buffer: %s", error->message);
	stream = (GArrowOutputStream *)garrow_buffer_output_stream_new(buffer);

	schema = garrow_record_batch_get_schema(batch);
	writer = (GArrowRecordBatchWriter *)garrow_record_batch_stream_writer_new(
			stream, schema, &error);
	if (error)
		elog(ERROR, "Cann't write batch: %s", error->message);
	garrow_record_batch_writer_write_record_batch(writer, batch, &error);
	if (error)
		elog(ERROR, "Cann't serialize a record batch to buffer: %s",
				error->message);

	garrow_record_batch_writer_close(writer, &error);
	if (error)
		elog(ERROR, "Cann't close record batch writer for serialization: %s",
				error->message);

	g_object_unref(&writer->parent_instance);

	return (void*)garrow_move_ptr(buffer);
}

const void* GetSerializedRecordBatchDataBufDataAndSize(void* buffer, int64 *size)
{
	const void *data_pos = NULL;
	GBytes *bytes = NULL;

	bytes = garrow_buffer_get_data(buffer);
	data_pos = g_bytes_get_data(bytes, (gsize *)size);

	free_glib_obj_misc((void**)&bytes);

	return data_pos;
}

void DebugArrowPlan(GArrowExecutePlan *plan, const char *label)
{
	gchar *s;
	s = garrow_execute_plan_to_string(plan);
	elog(INFO, "%s : %d: %s, %s", __FILE__, __LINE__, label, s);
	g_free(s);
}

/* end of file */
