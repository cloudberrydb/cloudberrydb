#include <memory>
#include <stdexcept>
#include "common.h"

void *
gpdbPalloc(Size size)
{
	void *result;
	bool error = false;

	PG_TRY();
	{
		result = palloc(size);
	}
	PG_CATCH();
	{
		error = true;
	}
	PG_END_TRY();

	if (error)
		throw std::runtime_error("out of memory");

	return result;
}

Datum
gpdbDirectFunctionCall3(PGFunction func, Datum arg1, Datum arg2, Datum arg3)
{
	Datum result;
	char errStr[ERROR_STR_LEN];
	bool error = false;
	MemoryContext mcxt = CurrentMemoryContext;

	PG_TRY();
	{
		result = DirectFunctionCall3(func, arg1, arg2, arg3);
	}
	PG_CATCH();
	{
		ErrorData *errdata;

		error = true;

		MemoryContextSwitchTo(mcxt);
		errdata = CopyErrorData();
		FlushErrorState();
		strncpy(errStr, errdata->message, ERROR_STR_LEN - 1);
		FreeErrorData(errdata);
	}
	PG_END_TRY();

	if (error)
		throw std::runtime_error(errStr);

	return result;
}

Oid
mapParquetDataType(parquet::Type::type type)
{
	switch (type)
	{
		case parquet::Type::BOOLEAN:
			return BOOLOID;
		case parquet::Type::INT32:
			return INT4OID;
		case parquet::Type::INT64:
			return INT8OID;
		case parquet::Type::INT96:
			return NUMERICOID;
		case parquet::Type::FLOAT:
			return FLOAT8OID;
		case parquet::Type::DOUBLE:
			return FLOAT8OID;
		case parquet::Type::BYTE_ARRAY:
			return TEXTOID;
		case parquet::Type::FIXED_LEN_BYTE_ARRAY:
			return NUMERICOID;
		default:
			return InvalidOid;
	}
}

Oid
mapAvroDataType(avro_type_t type)
{
	switch (type)
	{
		case AVRO_STRING:
			return TEXTOID;
		case AVRO_BYTES:
			return BYTEAOID;
		case AVRO_INT32:
			return INT4OID;
		case AVRO_INT64:
			return INT8OID;
		case AVRO_FLOAT:
			return FLOAT4OID;
		case AVRO_DOUBLE:
			return FLOAT8OID;
		case AVRO_BOOLEAN:
			return BOOLOID;
		case AVRO_FIXED:
			return NUMERICOID;
		default:
			return InvalidOid;
	}
}

std::string
convertToGopherPath(std::string &filePath)
{
	const char *pathPos;
	const char *hostPos = filePath.c_str() + 7; /* remove "hdfs://" */

	pathPos = strchr(hostPos, '/'); /* remove host */
	return std::string(pathPos);
}
