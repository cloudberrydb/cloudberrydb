#include "postgres.h"
// #include "datalake_extension.h"
#include "gopher/gopher.h"
#include "hudi_hashtab_merger.h"
#include "src/provider/common/kryo.h"
#include "src/provider/common/kryo_input.h"

#define SET_RECORD_DELETED(recordWrapper) ((recordWrapper)->record.position = -1)
#define SET_RECORD_SKIPPED(recordWrapper) ((recordWrapper)->record.position = -2)
#define RECORD_IS_DELETED(recordWrapper) ((recordWrapper)->record.position == -1)
#define RECORD_IS_SKIPPED(recordWrapper) ((recordWrapper)->record.position == -2)

static void
destroyHudiHashTableMerger(MergeProvider *provider);
static void
combineAndUpdateValue(MergeProvider *provider, InternalRecordWrapper *recordWrapper);
static void
updateOnDelete(MergeProvider *provider, InternalRecordWrapper *recordWrapper);
static bool
next(MergeProvider *provider, InternalRecord *record);
static bool
contains(MergeProvider *provider, InternalRecord *record, InternalRecord **newRecord, bool *isDeleted);

HudiHashTableMerger *
createHudiHashTableMerger(MemoryContext mcxt,
						  List *columnDesc,
						  List *recordKeyFields,
						  char *preCombineField)
{
	HudiHashTableMerger *merger = palloc0(sizeof(HudiHashTableMerger));

	elog(DEBUG1, "creating in-memory merger...");

	merger->base.Close = destroyHudiHashTableMerger;
	merger->base.CombineAndUpdate = combineAndUpdateValue;
	merger->base.UpdateOnDelete = updateOnDelete;
	merger->base.Next = next;
	merger->base.Contains = contains;

	merger->base.preCombineFieldType = InvalidOid;
	merger->base.preCombineFieldIndex = -1;

	merger->base.recordDesc = createInternalRecordDesc(mcxt,
											columnDesc,
											recordKeyFields,
											preCombineField,
											&merger->base.preCombineFieldIndex,
											&merger->base.preCombineFieldType);

	merger->mcxt = mcxt;
	merger->isRegistered = false;

	return merger;
}

static void
destroyHudiHashTableMerger(MergeProvider *provider)
{
	HudiHashTableMerger *merger = (HudiHashTableMerger *) provider;

	elog(DEBUG1, "destroying in-memory merger...");

	if (merger->isRegistered)
		hash_seq_term(&merger->hashTabSeqStatus);

	if (merger->base.recordDesc != NULL)
		destroyInternalRecordDesc(merger->base.recordDesc);

	pfree(merger);
}

static void
combineAndUpdateValue(MergeProvider *provider, InternalRecordWrapper *recordWrapper)
{
	bool found;
	MemoryContext oldMcxt;
	LogRecordHashEntry *entry;
	InternalRecordWrapper *newRecord;
	HudiHashTableMerger *merger = (HudiHashTableMerger *) provider;

	entry = (LogRecordHashEntry *) hash_search(merger->base.recordDesc->hashTab, &recordWrapper, HASH_FIND, NULL);
	if (entry)
	{
		InternalRecordWrapper *oldRecord = entry->recordValue;
		hash_search(merger->base.recordDesc->hashTab, &recordWrapper, HASH_REMOVE, &found);

		destroyInternalRecordWrapper(oldRecord);
		Assert(found);
	}

	oldMcxt = MemoryContextSwitchTo(merger->mcxt);
	newRecord = deepCopyRecord(recordWrapper);
	MemoryContextSwitchTo(oldMcxt);

	entry = (LogRecordHashEntry *) hash_search(merger->base.recordDesc->hashTab, &newRecord, HASH_ENTER, &found);
	Assert(!found);

	entry->recordValue = newRecord;
}

static void
updateOnDelete(MergeProvider *provider, InternalRecordWrapper *recordWrapper)
{
	bool found;
	Datum deleteOrderingVal;
	Datum curOrderingVal;
	MemoryContext oldMcxt;
	LogRecordHashEntry *entry;
	InternalRecordWrapper *newRecord;
	HudiHashTableMerger *merger = (HudiHashTableMerger *) provider;

	entry = hash_search(merger->base.recordDesc->hashTab, &recordWrapper, HASH_FIND, NULL);
	if (entry != NULL)
	{
		bool choosePrev;
		InternalRecordWrapper *oldRecord = entry->recordValue;

		if (merger->base.preCombineFieldIndex >= 0)
		{
			deleteOrderingVal = recordWrapper->record.values[merger->base.preCombineFieldIndex];
			curOrderingVal = entry->recordValue->record.values[merger->base.preCombineFieldIndex];
		}

		choosePrev = merger->base.preCombineFieldIndex >= 0 && 
					 DatumGetInt64(deleteOrderingVal) != 0 &&
					 hudiGreaterThan(merger->base.preCombineFieldType, curOrderingVal, deleteOrderingVal);

		if (choosePrev)
		{
			/* the DELETE message is obsolete if the old message has greater orderingVal */
			return;
		}

		hash_search(merger->base.recordDesc->hashTab, &recordWrapper, HASH_REMOVE, &found);
		destroyInternalRecordWrapper(oldRecord);
		Assert(found);
	}

	oldMcxt = MemoryContextSwitchTo(merger->mcxt);
	newRecord = deepCopyRecord(recordWrapper);
	SET_RECORD_DELETED(newRecord);
	MemoryContextSwitchTo(oldMcxt);

	entry = hash_search(merger->base.recordDesc->hashTab, &newRecord, HASH_ENTER, &found);
	Assert(!found);

	entry->recordValue = newRecord;
}

static bool
next(MergeProvider *provider, InternalRecord *record)
{
	LogRecordHashEntry *entry;
	HudiHashTableMerger *merger = (HudiHashTableMerger *) provider;
	InternalRecordDesc *recordDesc = merger->base.recordDesc;

	if (!merger->isRegistered)
	{
		hash_seq_init(&merger->hashTabSeqStatus, merger->base.recordDesc->hashTab);
		merger->isRegistered = true;
	}

again:
	entry = (LogRecordHashEntry *) hash_seq_search(&merger->hashTabSeqStatus);
	if (entry)
	{
		int i;

		if (RECORD_IS_DELETED(entry->recordValue))
			goto again;

		if (RECORD_IS_SKIPPED(entry->recordValue))
			goto again;

		for (i = 0; i < recordDesc->nColumns; i++)
		{
			record->values[i] = entry->recordValue->record.values[i];
			record->nulls[i] = entry->recordValue->record.nulls[i];
		}

		return true;
	}

	Assert(merger->isRegistered);
	merger->isRegistered = false;

	return false;
}

static bool
contains(MergeProvider *provider, InternalRecord *record, InternalRecord **newRecord, bool *isDeleted)
{
	LogRecordHashEntry *entry;
	HudiHashTableMerger *merger = (HudiHashTableMerger *) provider;
	InternalRecordWrapper  recordWrapper;
	InternalRecordWrapper *pRecordWrapper = &recordWrapper;

	*isDeleted = false;
	recordWrapper.record = *record;
	recordWrapper.recordDesc = merger->base.recordDesc;
	entry = (LogRecordHashEntry *) hash_search(merger->base.recordDesc->hashTab, &pRecordWrapper, HASH_FIND, NULL);
	if (entry == NULL)
		return false;

	if (RECORD_IS_DELETED(entry->recordValue))
	{
		*isDeleted = true;
		return true;
	}

	*newRecord = &entry->recordValue->record;
	SET_RECORD_SKIPPED(entry->recordValue);
	return true;
}
