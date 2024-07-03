#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_type.h"
#include "catalog/pg_collation.h"
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

static void
deepCopyField(Datum *newDatum, Datum *datum, Oid type, int index)
{
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
			newDatum[index] = datum[index];
			break;
		case NUMERICOID:
		case TEXTOID:
		{
			newDatum[index] = PointerGetDatum(palloc(VARSIZE(datum[index])));
			memcpy(DatumGetPointer(newDatum[index]), DatumGetPointer(datum[index]), VARSIZE(datum[index]));
			break;
		}
		case UUIDOID:
		{
			newDatum[index] = PointerGetDatum(palloc(16));
			memcpy(DatumGetPointer(newDatum[index]), DatumGetPointer(datum[index]), 16);
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

InternalRecordWrapper *
deepCopyRecord(InternalRecordWrapper *recordWrapper)
{
	int i;
	InternalRecordWrapper *result;
	InternalRecordDesc *recordDesc = recordWrapper->recordDesc;

	result = palloc(sizeof(InternalRecordWrapper));
	result->record.values = (Datum *) palloc(recordDesc->nColumns * sizeof(Datum));
	result->record.nulls = (bool *) palloc(recordDesc->nColumns * sizeof(bool));
	result->recordDesc = recordDesc;

	for (i = 0; i < recordDesc->nColumns; i++)
	{
		result->record.nulls[i] = recordWrapper->record.nulls[i];
		if (recordWrapper->record.nulls[i])
			continue;

		deepCopyField(result->record.values,
					  recordWrapper->record.values,
					  recordDesc->attrType[i], i);
	}

	result->record.position = recordWrapper->record.position;

	return result;
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

bool
hudiGreaterThan(Oid type, Datum datum1, Datum datum2)
{
	switch (type)
	{
		case BOOLOID:
		case INT4OID:
		case TIMEOID:
		case INT8OID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case DATEOID:
			return DatumGetInt64(datum1) > DatumGetInt64(datum2);
		case FLOAT4OID:
			return DatumGetFloat4(datum1) > DatumGetFloat4(datum2);
		case FLOAT8OID:
			return DatumGetFloat8(datum1) > DatumGetFloat8(datum2);
		case NUMERICOID:
			return DirectFunctionCall2(numeric_gt,
									   PointerGetDatum(datum1),
									   PointerGetDatum(datum2));
		case TEXTOID:
		{
			return DatumGetBool(DirectFunctionCall2Coll(text_gt,
												DEFAULT_COLLATION_OID,
												PointerGetDatum(datum1),
												PointerGetDatum(datum2)));
		}
		case UUIDOID:
		{
			return DirectFunctionCall2(uuid_gt,
									   PointerGetDatum(datum1),
									   PointerGetDatum(datum2));
		}
		default:
		{
			elog(ERROR, "unsupported column type oid: \"%u\"", type);
			return 0;
		}
	}
}

void
initRecord(InternalRecordWrapper *record, void *recordDesc, int nColumns)
{
	record->record.values = (Datum *) palloc(nColumns * sizeof(Datum));
	record->record.nulls = (bool *) palloc(nColumns * sizeof(bool));
	record->recordDesc = recordDesc;
}

void
cleanupRecord(InternalRecordWrapper *record)
{
	pfree(record->record.values);
	pfree(record->record.nulls);
}

InternalRecordDesc *
createInternalRecordDesc(MemoryContext mcxt,
						 List *columnDesc,
						 List *recordKeyFields,
						 char *preCombineField,
						 int *preCombineFieldIndex,
						 Oid *preCombineFieldType)
{
	int i;
	ListCell *lc;
	HASHCTL  hashCtl;
	bool found = false;

	InternalRecordDesc *result = palloc0(sizeof(InternalRecordDesc));

	result->nColumns = list_length(columnDesc);
	result->nKeys = list_length(recordKeyFields);
	result->keyIndexes = createRecordKeyIndexes(recordKeyFields, columnDesc);
	result->attrType = palloc(sizeof(Oid) * list_length(columnDesc));

	foreach_with_count(lc, columnDesc, i)
	{
		FieldDescription *entry = (FieldDescription *) lfirst(lc);
		result->attrType[i] = entry->typeOid;

		if (preCombineField && !found && pg_strcasecmp(preCombineField, entry->name) == 0)
		{
			*preCombineFieldIndex = i;
			*preCombineFieldType = entry->typeOid;
			found = true;
		}
	}

	if (mcxt != NULL)
	{
		MemSet(&hashCtl, 0, sizeof(hashCtl));
		hashCtl.keysize = sizeof(InternalRecordWrapper *);
		hashCtl.entrysize = sizeof(LogRecordHashEntry);
		hashCtl.hash = recordHash;
		hashCtl.match = recordMatch;
		hashCtl.hcxt = mcxt;
		result->hashTab = hash_create("HudiMergeLogTable", 10000 * 30, &hashCtl,
								   HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);
	}

	return result;
}

void
destroyInternalRecordDesc(InternalRecordDesc *recordDesc)
{
	if (recordDesc->keyIndexes)
		pfree(recordDesc->keyIndexes);

	if (recordDesc->attrType)
		pfree(recordDesc->attrType);

	if (recordDesc->hashTab)
		hash_destroy(recordDesc->hashTab);

	pfree(recordDesc);
}
