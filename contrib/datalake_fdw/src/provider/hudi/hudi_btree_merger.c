#include "postgres.h"
#include "executor/tuptable.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "storage/fd.h"
// #include "datalake_extension.h"
#include "gopher/gopher.h"
#include "hudi_btree_merger.h"
// #include "kryo.h"
// #include "kryo_input.h"
#include "src/provider/common/kryo.h"
#include "src/provider/common/kryo_input.h"

extern double hudiLogSizeScaleFactor;

#define SET_RECORD_DELETED(record, len) ((record)[(len)] = 1)
#define SET_RECORD_SKIPPED(record, len) ((record)[(len)] = 2)
#define SET_RECORD_UPDATED(record, len) ((record)[(len)] = 3)
#define RECORD_IS_DELETED(record, len) ((record)[(len)] == 1)
#define RECORD_IS_SKIPPED(record, len) ((record)[(len)] == 2)
#define RECORD_IS_UPDATED(record, len) ((record)[(len)] == 3)

static void
destroyHudiBtreeMerger(MergeProvider *provider);
static void
combineAndUpdateValue(MergeProvider *provider, InternalRecordWrapper *recordWrapper);
static void
updateOnDelete(MergeProvider *provider, InternalRecordWrapper *recordWrapper);
static bool
next(MergeProvider *provider, InternalRecord *record);
static bool
contains(MergeProvider *provider, InternalRecord *record, InternalRecord **newRecord, bool *isDeleted);

#define RESERVE_MAPFILE_SIZE 1024 * 1024 * 64

static int64
estimateFileSize(int64 fileSize)
{
	return fileSize * hudiLogSizeScaleFactor + RESERVE_MAPFILE_SIZE;
}

static void
formatNamePrefix(char *prefix, int size)
{
	snprintf(prefix, size, "hudi_s%u_", gp_session_id);
}

static void
initLmdb(HudiBtreeMerger *merger, int64 fileSize)
{
	int   result;
	int64 mapSize;
	char prefix[MAXPGPATH];

	result = mdb_env_create(&merger->dbEnv);
	if (result != 0)
		elog(ERROR, "failed to create merger environment: %s", mdb_strerror(result));

	mapSize = estimateFileSize(fileSize);
	result = mdb_env_set_mapsize(merger->dbEnv, mapSize);
	if (result != 0)
		elog(ERROR, "failed to set map size to " INT64_FORMAT " for merger: %s",
					 mapSize, mdb_strerror(result));

	formatNamePrefix(prefix, sizeof(prefix));

	elog(DEBUG1, "fileNamePrefix %s, total filesize " INT64_FORMAT ", map fileSize " INT64_FORMAT " ",
				 prefix, fileSize, mapSize);

	PrepareTempTablespaces();
	merger->file = OpenTemporaryFile(false, prefix);
	result = mdb_env_open_with_fd(merger->dbEnv,
								  prefix,
								  MDB_NOSUBDIR | MDB_WRITEMAP | MDB_NOMETASYNC |
								  MDB_NOSYNC | MDB_NOMEMINIT | MDB_NOTLS | MDB_NOLOCK,
								  FileGetRawDesc(merger->file));
	if (result != 0)
		elog(ERROR, "failed to create merger handle: %s", mdb_strerror(result));

	result = mdb_txn_begin(merger->dbEnv, NULL, 0, &merger->dbTxn);
	if (result != 0)
		elog(ERROR, "failed to prepare merger: %s", mdb_strerror(result));
	
	result = mdb_dbi_open(merger->dbTxn, NULL, 0, &merger->dbi);
	if (result != 0)
		elog(ERROR, "failed to open merger: %s", mdb_strerror(result));
}

static void
cleanupLmdb(HudiBtreeMerger *merger)
{
	if (merger->isRegistered)
		mdb_cursor_close(merger->cursor);

	if (merger->dbTxn)
		mdb_txn_commit(merger->dbTxn);

	if (merger->dbEnv)
		mdb_env_close_with_fd(merger->dbEnv);
}

HudiBtreeMerger *
createHudiBtreeMerger(List *columnDesc, List *recordKeyFields, char *preCombineField, int64 fileSize, TupleDesc tupDesc)
{
	HudiBtreeMerger *merger = palloc0(sizeof(HudiBtreeMerger));

	elog(DEBUG1, "creating btree merger...");

	merger->base.Close = destroyHudiBtreeMerger;
	merger->base.CombineAndUpdate = combineAndUpdateValue;
	merger->base.UpdateOnDelete = updateOnDelete;
	merger->base.Next = next;
	merger->base.Contains = contains;

	merger->base.preCombineFieldType = InvalidOid;
	merger->base.preCombineFieldIndex = -1;

	merger->base.recordDesc = createInternalRecordDesc(NULL,
											columnDesc,
											recordKeyFields,
											preCombineField,
											&merger->base.preCombineFieldIndex,
											&merger->base.preCombineFieldType);

	merger->mcxt = AllocSetContextCreate(CurrentMemoryContext,
									     "MergedLogBtreeContext",
										 ALLOCSET_DEFAULT_MINSIZE,
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);
	merger->isRegistered = false;

	merger->mtBind = create_memtuple_binding(tupDesc);

	initLmdb(merger, fileSize);

	return merger;
}

static void
destroyHudiBtreeMerger(MergeProvider *provider)
{
	HudiBtreeMerger *merger = (HudiBtreeMerger *) provider;

	elog(DEBUG1, "destroying btree merger...");

	if (merger->mtBind)
		destroy_memtuple_binding(merger->mtBind);

	cleanupLmdb(merger);

	if (merger->base.recordDesc != NULL)
		destroyInternalRecordDesc(merger->base.recordDesc);

	if (merger->mcxt)
		MemoryContextDelete(merger->mcxt);

	pfree(merger);
}

static void
encodeRecordKey(MergeProvider *provider, InternalRecordWrapper *recordWrapper, StringInfo key)
{
	int i;
	int colIndex;
	char *value;
	Oid   typOutput;
	bool  typIsVarlena;
	HudiBtreeMerger *merger = (HudiBtreeMerger *) provider;
	InternalRecordDesc *recordDesc = merger->base.recordDesc;

	for (i = 0; i < recordDesc->nKeys; i++)
	{
		colIndex = recordDesc->keyIndexes[i];

		if (recordWrapper->record.nulls[colIndex])
			continue;

		getTypeOutputInfo(recordDesc->attrType[colIndex], &typOutput, &typIsVarlena);
		value = OidOutputFunctionCall(typOutput, recordWrapper->record.values[colIndex]);

		appendStringInfo(key, "%s|", value != NULL ? value : "");
	}

	key->len--;
	key->data[key->len] = '\0';
}

static MemTuple
encodeRecordValue(MergeProvider *provider, InternalRecordWrapper *recordWrapper)
{
	MemTuple tuple;
	// uint32 tupLen = 0;
	HudiBtreeMerger *merger = (HudiBtreeMerger *) provider;

	// memtuple_form_to(merger->mtBind,
	// 				 recordWrapper->record.values,
	// 				 recordWrapper->record.nulls,
	// 				 NULL,
	// 				 &tupLen,
	// 				 false);

	// tuple = palloc0(tupLen + 1);
	// tuple = memtuple_form_to(merger->mtBind,
	// 						 recordWrapper->record.values,
	// 						 recordWrapper->record.nulls,
	// 						 tuple,
	// 						 &tupLen,
	// 						 false);

	tuple = memtuple_form(merger->mtBind,
						  recordWrapper->record.values,
						  recordWrapper->record.nulls);

	return tuple;
}

static void
combineAndUpdateValue(MergeProvider *provider, InternalRecordWrapper *recordWrapper)
{
	int  result;
	MDB_val key;
	MDB_val data;
	MemoryContext oldMcxt;
	StringInfoData strKey;
	MemTuple tuple;
	HudiBtreeMerger *merger = (HudiBtreeMerger *) provider;

	MemoryContextReset(merger->mcxt);
	oldMcxt = MemoryContextSwitchTo(merger->mcxt);

	initStringInfo(&strKey);
	encodeRecordKey(provider, recordWrapper, &strKey);

	key.mv_size = strKey.len;
	key.mv_data = (void *) strKey.data;

	result = mdb_get(merger->dbTxn, merger->dbi, &key, &data);
	if (result != MDB_SUCCESS && result != MDB_NOTFOUND)
		elog(ERROR, "failed to get record: %s", mdb_strerror(result));

	if (result == MDB_SUCCESS)
	{
		result = mdb_del(merger->dbTxn, merger->dbi, &key, NULL);
		if (result == MDB_NOTFOUND)
			elog(ERROR, "failed to delete record: not found");

		if (result != MDB_SUCCESS)
			elog(ERROR, "failed to delete record: %s", mdb_strerror(result));
	}

	tuple = encodeRecordValue(provider, recordWrapper);

	SET_RECORD_UPDATED((char *) tuple, memtuple_get_size(tuple));
	data.mv_size = memtuple_get_size(tuple) + 1;
	data.mv_data = (void *) tuple;

	result = mdb_put(merger->dbTxn, merger->dbi, &key, &data, 0);
	if (result != MDB_SUCCESS)
		elog(ERROR, "failed to put record: %s", mdb_strerror(result));

	MemoryContextSwitchTo(oldMcxt);
}

static void
updateOnDelete(MergeProvider *provider, InternalRecordWrapper *recordWrapper)
{
	int  result;
	MDB_val key;
	MDB_val data;
	MemTuple tuple;
	Datum deleteOrderingVal;
	Datum curOrderingVal;
	MemoryContext oldMcxt;
	StringInfoData strKey;
	HudiBtreeMerger *merger = (HudiBtreeMerger *) provider;
	InternalRecordDesc *recordDesc = merger->base.recordDesc;

	MemoryContextReset(merger->mcxt);
	oldMcxt = MemoryContextSwitchTo(merger->mcxt);

	initStringInfo(&strKey);
	encodeRecordKey(provider, recordWrapper, &strKey);

	key.mv_size = strKey.len;
	key.mv_data = (void *) strKey.data;

	result = mdb_get(merger->dbTxn, merger->dbi, &key, &data);
	if (result != MDB_SUCCESS && result != MDB_NOTFOUND)
		elog(ERROR, "failed to get record: %s", mdb_strerror(result));

	if (result == MDB_SUCCESS)
	{
		Datum  *values;
		bool   *isnull;
		bool    choosePrev;

		if (merger->base.preCombineFieldIndex >= 0)
		{
			values = (Datum *) palloc(recordDesc->nColumns * sizeof(Datum));
			isnull = (bool *) palloc(recordDesc->nColumns * sizeof(bool));

			memtuple_deform((MemTuple) data.mv_data, merger->mtBind, values, isnull);

			deleteOrderingVal = recordWrapper->record.values[merger->base.preCombineFieldIndex];
			curOrderingVal = values[merger->base.preCombineFieldIndex];
		}

		choosePrev = merger->base.preCombineFieldIndex >= 0 && 
					 DatumGetInt64(deleteOrderingVal) != 0 &&
					 hudiGreaterThan(merger->base.preCombineFieldType, curOrderingVal, deleteOrderingVal);

		if (choosePrev)
		{
			/* the DELETE message is obsolete if the old message has greater orderingVal */
			MemoryContextSwitchTo(oldMcxt);
			return;
		}

		result = mdb_del(merger->dbTxn, merger->dbi, &key, NULL);
		if (result == MDB_NOTFOUND)
			elog(ERROR, "failed to delete record: not found");

		if (result != MDB_SUCCESS)
			elog(ERROR, "failed to delete record: %s", mdb_strerror(result));
	}

	tuple = encodeRecordValue(provider, recordWrapper);
	SET_RECORD_DELETED((char *) tuple, memtuple_get_size(tuple));
	data.mv_size = memtuple_get_size(tuple) + 1;
	data.mv_data = (void *) tuple;

	result = mdb_put(merger->dbTxn, merger->dbi, &key, &data, 0);
	if (result != MDB_SUCCESS)
		elog(ERROR, "failed to put record: %s", mdb_strerror(result));

	MemoryContextSwitchTo(oldMcxt);
}

static bool
next(MergeProvider *provider, InternalRecord *record)
{
	int result;
	MDB_val key;
	MDB_val data;
	Datum  *values;
	bool   *isnull;
	MemoryContext oldMcxt;
	HudiBtreeMerger *merger = (HudiBtreeMerger *) provider;
	InternalRecordDesc *recordDesc = merger->base.recordDesc;

	if (!merger->isRegistered)
	{
		result = mdb_cursor_open(merger->dbTxn, merger->dbi, &merger->cursor);
		if (result != 0)
			elog(ERROR, "failed to open merger: %s", mdb_strerror(result));

		merger->isRegistered = true;
	}

	MemoryContextReset(merger->mcxt);
	oldMcxt = MemoryContextSwitchTo(merger->mcxt);

again:
	result = mdb_cursor_get(merger->cursor, &key, &data, MDB_NEXT);
	if (result == MDB_SUCCESS)
	{
		int i;
		
		if (RECORD_IS_DELETED((char *) data.mv_data, data.mv_size - 1))
			goto again;

		if (RECORD_IS_SKIPPED((char *) data.mv_data, data.mv_size -1))
			goto again;

		values = (Datum *) palloc(recordDesc->nColumns * sizeof(Datum));
		isnull = (bool *) palloc(recordDesc->nColumns * sizeof(bool));
		memtuple_deform((MemTuple) data.mv_data, merger->mtBind, values, isnull);

		for (i = 0; i < recordDesc->nColumns; i++)
		{
			record->values[i] = values[i];
			record->nulls[i] = isnull[i];
		}

		MemoryContextSwitchTo(oldMcxt);
		return true;
	}

	if (result != MDB_NOTFOUND)
		elog(ERROR, "failed to get next record from merger: %s", mdb_strerror(result));

	Assert(merger->isRegistered);
	mdb_cursor_close(merger->cursor);

	merger->isRegistered = false;
	FileClose(merger->file);

	MemoryContextSwitchTo(oldMcxt);
	return false;
}

static bool
contains(MergeProvider *provider, InternalRecord *record, InternalRecord **newRecord, bool *isDeleted)
{
	int  result;
	MDB_val key;
	MDB_val data;
	MemoryContext oldMcxt;
	StringInfoData strKey;
	InternalRecordWrapper recordWrapper;
	HudiBtreeMerger *merger = (HudiBtreeMerger *) provider;
	InternalRecordDesc *recordDesc = merger->base.recordDesc;

	*isDeleted = false;

	recordWrapper.record = *record;
	recordWrapper.recordDesc = merger->base.recordDesc;

	MemoryContextReset(merger->mcxt);
	oldMcxt = MemoryContextSwitchTo(merger->mcxt);

	initStringInfo(&strKey);
	encodeRecordKey(provider, &recordWrapper, &strKey);

	key.mv_size = strKey.len;
	key.mv_data = (void *) strKey.data;

	result = mdb_get(merger->dbTxn, merger->dbi, &key, &data);
	if (result != MDB_SUCCESS && result != MDB_NOTFOUND)
		elog(ERROR, "failed to get record: %s", mdb_strerror(result));

	if (result == MDB_NOTFOUND)
	{
		MemoryContextSwitchTo(oldMcxt);
		return false;
	}

	if (RECORD_IS_DELETED((char *) data.mv_data, data.mv_size - 1))
	{
		*isDeleted = true;
		MemoryContextSwitchTo(oldMcxt);
		return true;
	}

	{
		InternalRecord *deformRecord = palloc0(sizeof(InternalRecord));

		deformRecord->values = (Datum *) palloc(recordDesc->nColumns * sizeof(Datum));
		deformRecord->nulls = (bool *) palloc(recordDesc->nColumns * sizeof(bool));

		memtuple_deform((MemTuple) data.mv_data,
						merger->mtBind,
						deformRecord->values,
						deformRecord->nulls);
		*newRecord = deformRecord;
	}

	if (!RECORD_IS_SKIPPED((char *) data.mv_data, data.mv_size - 1))
	{
		MDB_val value;

		value.mv_data = palloc(data.mv_size);
		value.mv_size = data.mv_size;

		memcpy(value.mv_data, data.mv_data, data.mv_size);
		SET_RECORD_SKIPPED((char *) value.mv_data, value.mv_size - 1);

		result = mdb_put(merger->dbTxn, merger->dbi, &key, &value, 0);
		if (result != MDB_SUCCESS)
			elog(ERROR, "failed to put record: %s", mdb_strerror(result));
	}

	MemoryContextSwitchTo(oldMcxt);

	return true;
}
