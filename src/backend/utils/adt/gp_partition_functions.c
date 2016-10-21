/*
 * Copyright(c) 2012 - present, EMC/Greenplum
 */

#include "postgres.h"

#include "access/heapam.h"
#include "cdb/cdbpartition.h"
#include "funcapi.h"
#include "nodes/execnodes.h"
#include "utils/array.h"
#include "utils/hsearch.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/elog.h"
#include "utils/guc.h"

#define PARTITION_INVERSE_RECORD_NUM_ATTRS 5
#define PARTITION_INVERSE_RECORD_PARCHILDRELID_ATTNO 1
#define PARTITION_INVERSE_RECORD_MINKEY_ATTNO 2
#define PARTITION_INVERSE_RECORD_MININCLUDED_ATTNO 3
#define PARTITION_INVERSE_RECORD_MAXKEY_ATTNO 4
#define PARTITION_INVERSE_RECORD_MAXINCLUDED_ATTNO 5

/*
 * increaseScanArraySize
 *   Increase the array size for dynamic table scans.
 *
 * The final array size is the maximum of the following two values:
 *   (1) (dynamicTableScanInfo->numScans + NUM_PID_INDEXES_ADDED)
 *   (2) newMaxPartIndex + 1.
 */
static void
increaseScanArraySize(int newMaxPartIndex)
{
	int oldNumScans = dynamicTableScanInfo->numScans;
	int newNumScans = oldNumScans + NUM_PID_INDEXES_ADDED;
	if (newNumScans < newMaxPartIndex)
	{
		newNumScans = newMaxPartIndex;
	}

	dynamicTableScanInfo->numScans = newNumScans;

	if (dynamicTableScanInfo->pidIndexes == NULL)
	{
		dynamicTableScanInfo->pidIndexes = (HTAB **)
			palloc0(dynamicTableScanInfo->numScans * sizeof(HTAB*));

		Assert(dynamicTableScanInfo->iterators == NULL);
		dynamicTableScanInfo->iterators = palloc0(dynamicTableScanInfo->numScans * sizeof(DynamicPartitionIterator*));
	}
	else
	{
		dynamicTableScanInfo->pidIndexes = (HTAB **)
			repalloc(dynamicTableScanInfo->pidIndexes,
					 dynamicTableScanInfo->numScans * sizeof(HTAB*));

		dynamicTableScanInfo->iterators = repalloc(dynamicTableScanInfo->iterators,
				dynamicTableScanInfo->numScans * sizeof(DynamicPartitionIterator*));

		for (int scanNo = oldNumScans; scanNo < dynamicTableScanInfo->numScans; scanNo++)
		{
			dynamicTableScanInfo->pidIndexes[scanNo] = NULL;
			dynamicTableScanInfo->iterators[scanNo] = NULL;
		}
	}
}

/*
 * createPidIndex
 *   Create the pid index for a given dynamic table scan.
 */
static HTAB *
createPidIndex(int index)
{
	Assert((dynamicTableScanInfo->pidIndexes)[index - 1] == NULL);

	HASHCTL hashCtl;
	MemSet(&hashCtl, 0, sizeof(HASHCTL));
	hashCtl.keysize = sizeof(Oid);
	hashCtl.entrysize = sizeof(PartOidEntry);
	hashCtl.hash = oid_hash;
	hashCtl.hcxt = dynamicTableScanInfo->memoryContext;

	return hash_create("Dynamic Table Scan Pid Index",
					   INITIAL_NUM_PIDS,
					   &hashCtl,
					   HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);
}

/*
 * InsertPidIntoDynamicTableScanInfo
 * 		Inserts a partition oid into the dynamicTableScanInfo's
 * 		pidIndexes at the provided index. If partOid is an invalid
 * 		oid, it doesn't insert that, but ensures that the dynahash
 * 		exists at the index position in dynamicTableScanInfo.
 */
void
InsertPidIntoDynamicTableScanInfo(int32 index, Oid partOid, int32 selectorId)
{
	Assert(dynamicTableScanInfo != NULL &&
		   dynamicTableScanInfo->memoryContext != NULL);

	/* It's 1 based indexing */
	Assert(index > 0);

	MemoryContext oldCxt = MemoryContextSwitchTo(dynamicTableScanInfo->memoryContext);

	if (index > dynamicTableScanInfo->numScans)
	{
		increaseScanArraySize(index);
	}
	
	Assert(index <= dynamicTableScanInfo->numScans);
	if ((dynamicTableScanInfo->pidIndexes)[index - 1] == NULL)
	{
		dynamicTableScanInfo->pidIndexes[index - 1] = createPidIndex(index);
	}

	Assert(dynamicTableScanInfo->pidIndexes[index - 1] != NULL);
	
	if (partOid != InvalidOid)
	{
		bool found = false;
		PartOidEntry *hashEntry =
			hash_search(dynamicTableScanInfo->pidIndexes[index - 1],
						&partOid, HASH_ENTER, &found);

		if (found)
		{
			Assert(hashEntry->partOid == partOid);
			Assert(NIL != hashEntry->selectorList);
			hashEntry->selectorList = list_append_unique_int(hashEntry->selectorList, selectorId);
		}
		else
		{
			hashEntry->partOid = partOid;
			hashEntry->selectorList = list_make1_int(selectorId);
		}
	}

	MemoryContextSwitchTo(oldCxt);
}

/*
 * PartitionIterator
 *   Contains the state that are necessary to iterate through all
 * child partitions, one at a time.
 *
 * This is used by set-returning partition functions.
 */
typedef struct PartitionIterator
{
	PartitionNode *partsAndRules;

	/*
	 * The cell to the next PartitionRule.
	 */
	ListCell *nextRuleCell;

	/*
	 * The current child partition that is being processed.
	 */
	PartitionRule *currentRule;

	/*
	 * Indicate whether the information about the default partition
	 * has been returned.
	 */
	bool defaultPartReturned;
	
} PartitionIterator;

/*
 * createPartitionIterator
 *    create a new PartitionIterator object for a given parent oid.
 *
 * The metadata information for the given parent oid is found in
 * dynamicTableScanInfo.
 */
static PartitionIterator *
createPartitionIterator(Oid parentOid)
{
	PartitionIterator *partitionIterator = palloc(sizeof(PartitionIterator));
	PartitionAccessMethods *accessMethods = NULL;

	findPartitionMetadataEntry(dynamicTableScanInfo->partsMetadata,
							   parentOid,
							   &(partitionIterator->partsAndRules),
							   &accessMethods);

	partitionIterator->currentRule = NULL;
	partitionIterator->nextRuleCell = NULL;
	
	Assert(NULL != partitionIterator->partsAndRules);
	partitionIterator->nextRuleCell = list_head(partitionIterator->partsAndRules->rules);
	partitionIterator->defaultPartReturned = true;
	if (NULL != partitionIterator->partsAndRules->default_part)
	{
		partitionIterator->defaultPartReturned = false;
	}

	return partitionIterator;
}

/*
 * setInverseRecordForRange
 *	Set the record value array for the inverse function on a range partition, based
 *	on the given partition rule.
 *
 *	This function does not handle the default partition.
 * 
 *	Range partitions can be of the form:
 *
 *	(-inf ,e], (-inf, e), (s, e), [s, e], (s,e], [s,e), (s,inf),
 *	and [s, inf).
 */
static void
setInverseRecordForRange(PartitionRule *rule,
						 Datum *values,
						 bool *nulls,
						 int numAttrs)
{
	Assert(numAttrs == PARTITION_INVERSE_RECORD_NUM_ATTRS);
	Assert(rule != NULL);

	/* Default partitions should not be handled here. */
	Assert(!rule->parisdefault);

	MemSet(nulls, true, sizeof(bool) * PARTITION_INVERSE_RECORD_NUM_ATTRS);
	MemSet(values, 0, sizeof(Datum) * PARTITION_INVERSE_RECORD_NUM_ATTRS);

	if (NULL != rule->parrangestart)
	{
		Assert(IsA(rule->parrangestart, List) &&
			list_length((List *)rule->parrangestart) == 1);

		Node *rangeStart = (Node *)linitial((List *)rule->parrangestart);
		Assert(IsA(rangeStart, Const));
		Const *rangeStartConst = (Const *)rangeStart;

		values[PARTITION_INVERSE_RECORD_MINKEY_ATTNO - 1] = rangeStartConst->constvalue;
		nulls[PARTITION_INVERSE_RECORD_MINKEY_ATTNO - 1] = rangeStartConst->constisnull;

		values[PARTITION_INVERSE_RECORD_MININCLUDED_ATTNO - 1] = BoolGetDatum(rule->parrangestartincl);
		nulls[PARTITION_INVERSE_RECORD_MININCLUDED_ATTNO - 1] = false;
	}

	if (NULL != rule->parrangeend)
	{
		Assert(IsA(rule->parrangeend, List) && 
			list_length((List *)rule->parrangeend) == 1);

		Node *rangeEnd = (Node *)linitial((List *)rule->parrangeend);
		Assert(IsA(rangeEnd, Const));
		Const *rangeEndConst = (Const *)rangeEnd;

		values[PARTITION_INVERSE_RECORD_MAXKEY_ATTNO - 1] = rangeEndConst->constvalue;
		nulls[PARTITION_INVERSE_RECORD_MAXKEY_ATTNO - 1] = rangeEndConst->constisnull;

		values[PARTITION_INVERSE_RECORD_MAXINCLUDED_ATTNO - 1] = BoolGetDatum(rule->parrangeendincl);
		nulls[PARTITION_INVERSE_RECORD_MAXKEY_ATTNO - 1] = false;
	}

	values[PARTITION_INVERSE_RECORD_PARCHILDRELID_ATTNO - 1] = ObjectIdGetDatum(rule->parchildrelid);
	nulls[PARTITION_INVERSE_RECORD_PARCHILDRELID_ATTNO - 1] = false;
}

/*
 * setInverseRecordForList
 *    Set the record value array for the inverse function on a list partition, based
 * on the given partition rule.
 *
 * This function only supports single-column partition key in the partition level.
 */
static void
setInverseRecordForList(PartitionRule *rule,
						ListCell *listValueCell,
						Datum *values,
						bool *nulls,
						int numAttrs)
{
	Assert(numAttrs == PARTITION_INVERSE_RECORD_NUM_ATTRS);
	Assert(rule != NULL &&
		   rule->parlistvalues != NULL &&
		   listValueCell != NULL);

	/*
	 * Note that in partition rule, list values are stored in a list of lists to support
	 * multi-column partitions.
	 */
	List *listValue = (List *)lfirst(listValueCell);
		
	/* This function only supports single-column partition key. */
	Assert(list_length(listValue) == 1);
	
	Const *listValueConst = (Const *)linitial(listValue);
	Assert(IsA(listValueConst, Const));

	values[PARTITION_INVERSE_RECORD_PARCHILDRELID_ATTNO - 1] = ObjectIdGetDatum(rule->parchildrelid);
	nulls[PARTITION_INVERSE_RECORD_PARCHILDRELID_ATTNO - 1] = false;

	values[PARTITION_INVERSE_RECORD_MINKEY_ATTNO - 1] = listValueConst->constvalue;
	nulls[PARTITION_INVERSE_RECORD_MINKEY_ATTNO - 1] = listValueConst->constisnull;

	values[PARTITION_INVERSE_RECORD_MININCLUDED_ATTNO - 1] = BoolGetDatum(true);
	nulls[PARTITION_INVERSE_RECORD_MININCLUDED_ATTNO - 1] = false;

	values[PARTITION_INVERSE_RECORD_MAXKEY_ATTNO - 1] = listValueConst->constvalue;
	nulls[PARTITION_INVERSE_RECORD_MAXKEY_ATTNO - 1] = false;

	values[PARTITION_INVERSE_RECORD_MAXINCLUDED_ATTNO - 1] = BoolGetDatum(true);
	nulls[PARTITION_INVERSE_RECORD_MAXINCLUDED_ATTNO - 1] = false;
}

/*
 * setInverseRecordForDefaultPart
 *    Set the record value array for the inverse function on both range and list default partitions.
 *
 * The default partition does not contain any constraint information,
 * this function simple returns the default partition oid with null values on other
 * columns in the return record.
 */
static void
setInverseRecordForDefaultPart(PartitionRule *rule,
							   Datum *values,
							   bool *nulls,
							   int numAttrs)
{
	Assert(numAttrs == PARTITION_INVERSE_RECORD_NUM_ATTRS);
	Assert(rule != NULL &&
		   ((rule->parrangestart == NULL &&
			 rule->parrangeend == NULL) ||
			(rule->parlistvalues == NULL)));

	MemSet(nulls, true, sizeof(bool) * PARTITION_INVERSE_RECORD_NUM_ATTRS);
	MemSet(values, 0, sizeof(Datum) * PARTITION_INVERSE_RECORD_NUM_ATTRS);
	values[PARTITION_INVERSE_RECORD_PARCHILDRELID_ATTNO - 1] = ObjectIdGetDatum(rule->parchildrelid);
	nulls[PARTITION_INVERSE_RECORD_PARCHILDRELID_ATTNO - 1] = false;
}

		
/*
 * findPartitionKeyType
 *   Find the type oid and typeMod for the given partition key.
 */
static void
findPartitionKeyType(Oid parentOid,
					 int keyAttNo,
					 Oid *typeOid,
					 int32 *typeMod)
{
	Relation rel = relation_open(parentOid, NoLock);
	TupleDesc tupDesc = RelationGetDescr(rel);

	Assert(tupDesc->natts >= keyAttNo);

	*typeOid = tupDesc->attrs[keyAttNo - 1]->atttypid;
	*typeMod = tupDesc->attrs[keyAttNo - 1]->atttypmod;
	
	relation_close(rel, NoLock);
}

/*
 * dumpDynamicTableScanPidIndex
 *   Write out pids for a given dynamic table scan.
 */
void
dumpDynamicTableScanPidIndex(int index)
{
	if (index < 0 ||
		dynamicTableScanInfo == NULL ||
		index > dynamicTableScanInfo->numScans ||
		dynamicTableScanInfo->pidIndexes[index] == NULL)
	{
		return;
	}
	
	Assert(dynamicTableScanInfo != NULL &&
		   index < dynamicTableScanInfo->numScans &&
		   dynamicTableScanInfo->pidIndexes[index] != NULL);
	
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, dynamicTableScanInfo->pidIndexes[index]);

	StringInfoData pids;
	initStringInfo(&pids);

	Oid *partOid = NULL;
	while ((partOid = (Oid *)hash_seq_search(&status)) != NULL)
	{
		appendStringInfo(&pids, "%d ", *partOid);
	}

	elog(LOG, "Dynamic Table Scan %d pids: %s", index, pids.data);
	pfree(pids.data);
}
