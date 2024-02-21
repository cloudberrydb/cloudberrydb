#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/memutils.h"
#include "utils/memdebug.h"
#include "utils/timestamp.h"
#include "utils/hsearch.h"
#include "utils/int8.h"
#include "gopher/gopher.h"
#include "utils.h"
#include <curl/curl.h>
#include "common/hashfn.h"

/*
 * hdfs://hashdata:9000/user/hive/warehouse/iceberg_db.db/table6/data/00000-0-adce9cbd-f6e5-4a9d-8ea1-2a09b6ca06ed-00001.parquet
 */
char *
splitPath(char *uri)
{
	char *pathPos;
	char *hostPos = uri + 7; /* Remove "hdfs://" */

	pathPos = strchr(hostPos, '/');
	return pstrdup(pathPos);
}

List *
createFieldDescription(TupleDesc tupleDesc)
{
	int i;
	List *result = NIL;

    for (i = 0; i < tupleDesc->natts; i++)
    {
		FieldDescription *fieldDesc = (FieldDescription *) palloc0(sizeof(FieldDescription));
		Oid typeOid = TupleDescAttr(tupleDesc, i)->atttypid;
		int typeMod = TupleDescAttr(tupleDesc, i)->atttypmod;
        const char *attname = NameStr(TupleDescAttr(tupleDesc, i)->attname);

		strcpy(fieldDesc->name, attname);
		fieldDesc->typeOid = typeOid;
		fieldDesc->typeMod = typeMod;

		result = lappend(result, fieldDesc);
	}

	return result;
}

List *
createPositionDeletesDescription(void)
{
	FieldDescription *filePath = palloc0(sizeof(FieldDescription));
	FieldDescription *pos = palloc0(sizeof(FieldDescription));

	strcpy(filePath->name, "file_path");
	filePath->typeOid = TEXTOID;
	filePath->typeMod = -1;

	strcpy(pos->name, "pos");
	pos->typeOid = INT8OID;
	pos->typeMod = -1; 

	return list_make2(filePath, pos);
}

int64
getFileRecordCount(List *deletes)
{
	ListCell *lc;
	int64 result = 0;

	foreach(lc, deletes)
	{
		FileFragment *deleteFile = (FileFragment *) lfirst(lc);
		result += deleteFile->recordCount;
	}

	return result;
}

bool
charSeqEquals(char *s1, int s1Len, char *s2, int s2Len)
{
	int i;

	if (s1Len != s2Len)
		return false;

	/*
	 * File paths inside a delete file normally have more identical chars at the beginning. For example,
	 * a typical path is like "s3:/bucket/db/table/data/partition/00000-0-[uuid]-00001.parquet".
	 * The uuid is where the difference starts. So it's faster to find the first diff backward.
	 */
	for (i = s1Len - 1; i >= 0; i--)
	{
		if (s1[i] != s2[i])
			return false;
	}

	return true;
}

int
charSeqIndexOf(char *array, int arrayLength, char *target, int targetLength)
{
	int  i;
	int  j;
	bool match;

	if (targetLength == 0)
		return 0;

	for(i = 0; i < arrayLength - targetLength + 1; ++i)
	{
		match = true;

		for(j = 0; j < targetLength; ++j)
		{
			if (array[i + j] != target[j])
			{
				match = false;
				break;
			}
		}

		if (!match)
			continue;

		return i;
	}

	return -1;
}

int *
createRecordKeyIndexes(List *recordKeys, List *columnDesc)
{
	ListCell *lco;
	ListCell *lci;
	List *indexes = NIL;
	int i;
	int *result;
	bool found;

	foreach(lco, recordKeys)
	{
		char *keyName = strVal(lfirst(lco));

		found = false;
		foreach_with_count(lci, columnDesc, i)
		{
			FieldDescription *entry = (FieldDescription *) lfirst(lci);
			if (pg_strcasecmp(entry->name, keyName) == 0)
			{
				found = true;
				indexes = lappend_int(indexes, i);
				break;
			}
		}

		if (!found)
			elog(ERROR, "could not find record key \"%s\" in table's column list", keyName);
	}

	result = palloc(sizeof(int ) * list_length(indexes));
	foreach_with_count(lco, indexes, i)
	{
		result[i] = lfirst_int(lco);
	}

	list_free(indexes);

	return result;
}

bool *
createDeleteProjectionColumns(List *recordKeys, List *columnDesc)
{
	int i;
	ListCell *lco;
	ListCell *lci;
	bool *attrUsed = palloc0(sizeof(Oid) * list_length(columnDesc));

	foreach(lco, recordKeys)
	{
		char *keyName = strVal(lfirst(lco));

		foreach_with_count(lci, columnDesc, i)
		{
			FieldDescription *entry = (FieldDescription *) lfirst(lci);
			if (pg_strcasecmp(entry->name, keyName) == 0)
			{
				attrUsed[i] = true;
				break;
			}
		}

		pfree(keyName);
	}

	return attrUsed;
}

uint32
fieldHash(Datum datum, Oid type)
{
	void  *key;
	Size   keySize;

	switch (type)
	{
		case BOOLOID:
		case INT4OID:
		case TIMEOID:
		case INT8OID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case DATEOID:
		{
			key = &datum;
			keySize = sizeof(Datum);
			break;
		}
		case TEXTOID:
		{
			key = VARDATA_ANY(datum);
			keySize = VARSIZE_ANY_EXHDR(datum);
			break;
		}
		case UUIDOID:
		{
			key = DatumGetPointer(datum);
			keySize = 16;
			break;
		}
		case NUMERICOID:
		{
			key = DatumGetPointer(datum);
			keySize = VARSIZE(datum);
			break;
		}
		default:
			elog(ERROR, "unsupported column type oid: \"%u\"", type);
	}

	return tag_hash(key, keySize);
}

bool
fieldCompare(Datum datum1, Datum datum2, Oid type)
{
	void  *key1;
	void  *key2;
	Size   key1Size;
	Size   key2Size;

	switch (type)
	{
		case BOOLOID:
		case INT4OID:
		case TIMEOID:
		case INT8OID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case DATEOID:
		{
			key1 = &datum1;
			key2 = &datum2;
			key1Size = key2Size = sizeof(Datum);
			break;
		}
		case TEXTOID:
		{
			key1 = VARDATA_ANY(datum1);
			key2 = VARDATA_ANY(datum2);
			key1Size = VARSIZE_ANY_EXHDR(datum1);
			key2Size = VARSIZE_ANY_EXHDR(datum2);
			break;
		}
		case UUIDOID:
		{
			key1 = DatumGetPointer(datum1);
			key2 = DatumGetPointer(datum2);
			key1Size = key2Size = 16;
			break;
		}
		case NUMERICOID:
		{
			key1 = DatumGetPointer(datum1);
			key2 = DatumGetPointer(datum2);
			key1Size = VARSIZE(datum1);
			key2Size = VARSIZE(datum2);
			break;
		}
		default:
			elog(ERROR, "unsupported column type oid: \"%u\"", type);
	}

	if (key1Size != key2Size)
		return false;

	return !memcmp(key1, key2, key1Size);
}

void
deepCopyField(Datum *datum, Oid type, int index)
{
	Datum tempDatum;

	switch (type)
	{
		case BOOLOID:
		case INT4OID:
		case TIMEOID:
		case INT8OID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case DATEOID:
		case FLOAT4OID:
		case FLOAT8OID:
			break;
		case NUMERICOID:
		case TEXTOID:
		{
			tempDatum = datum[index];
			datum[index] = PointerGetDatum(palloc(VARSIZE(tempDatum)));
			memcpy(DatumGetPointer(datum[index]), DatumGetPointer(tempDatum), VARSIZE(tempDatum));
			break;
		}
		case UUIDOID:
		{
			tempDatum = datum[index];
			datum[index] = PointerGetDatum(palloc(16));
			memcpy(DatumGetPointer(datum[index]), DatumGetPointer(tempDatum), 16);
			break;
		}
		default:
			elog(ERROR, "unsupported column type oid: \"%u\"", type);
	}
}

InternalRecordWrapper *
createInternalRecordWrapper(void *recordDesc, int nColumns)
{
	InternalRecordWrapper *recordWrapper = palloc(sizeof(InternalRecordWrapper));

	recordWrapper->record.values = (Datum *) palloc(nColumns * sizeof(Datum));
	recordWrapper->record.nulls = (bool *) palloc(nColumns * sizeof(bool));
	recordWrapper->recordDesc = recordDesc;

	return recordWrapper;
}

void
destroyInternalRecordWrapper(InternalRecordWrapper *recordWrapper)
{
	pfree(recordWrapper->record.values);
	pfree(recordWrapper->record.nulls);
	pfree(recordWrapper);
}

uint32
recordHash(const void *key, Size keysize)
{
	int i;
	int colIndex;
	uint32 hashValue = 0;
	uint32 hashKey;
	InternalRecordWrapper *recordWrapper = *((InternalRecordWrapper **) key);
	InternalRecordDesc *recordDesc = recordWrapper->recordDesc;

	for (i = 0; i < recordDesc->nKeys; i++)
	{
		colIndex = recordDesc->keyIndexes[i];

		hashKey = hashValue;

		/* rotate hashkey left 1 bit at each step */
		hashKey = (hashKey << 1) | ((hashKey & 0x80000000) ? 1 : 0);
		if (!recordWrapper->record.nulls[colIndex])
		{
			uint32 hkey = fieldHash(recordWrapper->record.values[colIndex], recordDesc->attrType[colIndex]);
			hashKey ^= hkey;
		}

		hashValue = hashKey;
	}

	return hashValue;
}

int
recordMatch(const void *key1, const void *key2, Size keysize)
{
	int i;
	int colIndex;
	InternalRecordWrapper *recordWrapper1 = *((InternalRecordWrapper **) key1);
	InternalRecordWrapper *recordWrapper2 = *((InternalRecordWrapper **) key2);
	InternalRecordDesc *recordDesc = recordWrapper1->recordDesc;

	for (i = 0; i < recordDesc->nKeys; i++)
	{
		colIndex = recordDesc->keyIndexes[i];

		if (recordWrapper1->record.nulls[colIndex] != recordWrapper2->record.nulls[colIndex])
			return 1;

		if (recordWrapper1->record.nulls[colIndex])
			continue;

		if (!fieldCompare(recordWrapper1->record.values[colIndex],
						  recordWrapper2->record.values[colIndex],
						  recordDesc->attrType[colIndex]))
			return 1;
	}

	return 0;
}

void
deepCopyRecord(InternalRecordWrapper *recordWrapper)
{
	int i;
	InternalRecordDesc *recordDesc = recordWrapper->recordDesc;

	for (i = 0; i < recordDesc->nColumns; i++)
	{
		if (recordWrapper->record.nulls[i])
			continue;

		deepCopyField(recordWrapper->record.values, recordDesc->attrType[i], i);
	}
}

int
extractScalFromTypeMod(int32 typmod)
{
	int			precision;
	int			scale;

	if (typmod < (int32) (VARHDRSZ))
		return 0;

	typmod -= VARHDRSZ;
	precision = (typmod >> 16) & 0xffff;
	scale = typmod & 0xffff;

	return scale;
}

static List *
parseKeyValue(List *result, char *pair, char *recordKey)
{
	int       keyLen;
	int       valueLen;
	char     *sep = strchr(pair, ':');
	KeyValue *data = palloc0(sizeof(KeyValue));

	if (sep == NULL)
		elog(ERROR, "invalid record key %s: '%s' missing delimiter ':'", recordKey, pair);

	keyLen = sep - pair;

	if (keyLen == 0)
		elog(ERROR, "invalid record key %s: '%s' missing key before ':'", recordKey, pair);

	valueLen = strlen(pair) - keyLen + 1;
	if (valueLen == 2)
		elog(ERROR, "invalid record key %s: '%s' missing value after ':'", recordKey, pair);

	data->key = pnstrdup(pair, keyLen);
	data->value = pnstrdup(sep + 1, valueLen);

	return lappend(result, data);
}

/* id:1,name:1 */
List *
extractRecordKeyValues(char *strRecordKey, int strSize, int keySize)
{
	char *pair;
	char *ctx;
	char *dupStr;
	char *start;
	char *sep;
	List *result = NIL;

	dupStr = pnstrdup(strRecordKey, strSize);
	sep = strchr(dupStr, ':');

	if (keySize == 1 && sep == NULL)
	{
		KeyValue *data = palloc0(sizeof(KeyValue));

		data->value = pnstrdup(strRecordKey, strSize);
		result = lappend(result, data);

		pfree(dupStr);
		return result;
	}

	start = dupStr;
	for (pair = strtok_r(start, ",", &ctx); pair; pair = strtok_r(NULL, ",", &ctx))
		result = parseKeyValue(result, pair, strRecordKey);

	pfree(dupStr);
	return result;
}

void
freeKeyValueList(List *kvs)
{
	ListCell *lc;

	foreach(lc, kvs)
	{
		KeyValue *kv = (KeyValue *) lfirst(lc);

		if (kv->key)
			pfree(kv->key);

		if (kv->value)
			pfree(kv->value);
	}

	list_free_deep(kvs);
}

Datum
createDatumByText(Oid attType, const char *value)
{
	Datum  datum;

	switch (attType)
	{
		case BOOLOID:
			datum = DirectFunctionCall1(boolin, CStringGetDatum(value));
			break;
		case INT4OID:
			datum = DirectFunctionCall1(int4in, CStringGetDatum(value));
			break;
		case INT8OID:
			datum = DirectFunctionCall1(int8in, CStringGetDatum(value));
			break;
		case FLOAT4OID:
			datum = DirectFunctionCall1(float4in, CStringGetDatum(value));
			break;
		case FLOAT8OID:
			datum = DirectFunctionCall1(float8in, CStringGetDatum(value));
			break;
		case NUMERICOID:
			datum = DirectFunctionCall3(numeric_in,
										 CStringGetDatum(value),
										 ObjectIdGetDatum(InvalidOid),
										 Int32GetDatum(-1));
			break;
		case TIMESTAMPOID:
			datum = DirectFunctionCall3(timestamp_in,
										 CStringGetDatum(value),
										 ObjectIdGetDatum(InvalidOid),
										 Int32GetDatum(-1));
			break;
		case TIMESTAMPTZOID:
			datum = DirectFunctionCall3(timestamptz_in,
										 CStringGetDatum(value),
										 ObjectIdGetDatum(InvalidOid),
										 Int32GetDatum(-1));
			break;
		case DATEOID:
			datum = DirectFunctionCall1(date_in, CStringGetDatum(value));
			break;
		case TEXTOID:
			datum = DirectFunctionCall1(textin, CStringGetDatum(value));
			break;
		default:
			elog(ERROR, "does not support type:\"%u\" as part of record key", attType);
	}

	return datum;
}

static char **
splitString(const char *value, char delim, int *size)
{
	int    i;
	int    pos = 0;
	int    len = strlen(value);
	List  *elements = NIL;
	char **result;
	ListCell *lc;

	for (i = 0; i < len; i++)
	{
		if (value[i] == delim)
		{
			elements = lappend(elements, pnstrdup(value + pos, i - pos));
			pos = i + 1;
		}
	}

	if ((len - pos) > 0)
		elements = lappend(elements, pnstrdup(value + pos, len - pos));

	result = palloc(sizeof(char *) * list_length(elements));
	foreach_with_count(lc, elements, i)
	{
		result[i] = lfirst(lc);
	}

	*size = list_length(elements);

	list_free(elements);

	return result;
}

static void
freeSplitString(char **elements, int size)
{
	int i;

	for (i = 0; i < size; i++)
		pfree(elements[i]);

	pfree(elements);
}

static char *
unescapePathName(char *value, int length)
{
	char *result;
	char *decodedStr = curl_unescape(value, length);

	result = pstrdup(decodedStr);
	curl_free(decodedStr);

	return result;
}

List *
extractPartitionKeyValues(char *filePath, List *partitionKeys, bool isHiveStyle)
{
	int i;
	int size;
	char *pair;
	int curDepth = 0;
	List *result = NIL;
	KeyValue *keyvalue;
	Value *partitionKey;
	char **elements = splitString(filePath, '/', &size);

	for (i = size - 1; i >= 0; i--)
	{
		pair = elements[i];
		if (curDepth++ == 0)
			continue;

		keyvalue = palloc0(sizeof(KeyValue));
		if (isHiveStyle)
		{
			int   keyLen;
			int   valueLen;
			char *sep = strchr(pair, '=');

			if (sep == NULL)
				elog(ERROR, "invalid partition path %s: '%s' missing delimiter '='", filePath, pair);

			keyLen = sep - pair;
			if (keyLen == 0)
				elog(ERROR, "invalid partition path %s: '%s' missing key before '='", filePath, pair);

			valueLen = strlen(pair) - keyLen + 1;
			if (valueLen == 2)
				elog(ERROR, "invalid partition path %s: '%s' missing value after '='", filePath, pair);

			keyvalue->key = unescapePathName(pair, keyLen);
			keyvalue->value = unescapePathName(sep + 1, valueLen);
		}
		else
		{
			partitionKey = list_nth(partitionKeys, list_length(partitionKeys)  - curDepth + 1);
			keyvalue->key = pstrdup(strVal(partitionKey));
			keyvalue->value = unescapePathName(pair, strlen(pair));
		}

		result = lappend(result, keyvalue);

		if ((curDepth - 1) == list_length(partitionKeys))
			break;
	}

	freeSplitString(elements, size);

	return result;
}
