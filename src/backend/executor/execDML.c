/*-------------------------------------------------------------------------
 *
 * execDML.c
 *        Implementation of execDML.
 *        This file performs INSERT, DELETE and UPDATE DML operations.
 *
 * Portions Copyright (c) 2012, EMC Corp.
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/executor/execDML.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_attribute.h"
#include "executor/executor.h"
#include "executor/execDML.h"

/*
 * Are two TupleDescs physically compatible?
 *
 * They are assumed to be "logically" the same. That means that columns
 * are in the same order, and there might be dropped columns.
 */
static bool
physicalEqualTupleDescs(TupleDesc tupdesc1, TupleDesc tupdesc2)
{
	if (tupdesc1->natts != tupdesc2->natts)
		return false;
	if (tupdesc1->tdhasoid != tupdesc2->tdhasoid)
		return false;

	for (int i = 0; i < tupdesc1->natts; i++)
	{
		Form_pg_attribute attr1 = tupdesc1->attrs[i];
		Form_pg_attribute attr2 = tupdesc2->attrs[i];

		if (attr1->attisdropped != attr2->attisdropped)
			return false;

		/* ignore dropped columns */
		if (attr1->attisdropped)
			continue;

		if (attr1->atttypid != attr2->atttypid)
			return false;

		/* these should all be equal, if the type OID is equal */
		Assert(attr1->attlen == attr2->attlen);
		Assert(attr1->attbyval == attr2->attbyval);
		Assert(attr1->attalign == attr2->attalign);
	}

	return true;
}

/*
 * Given a tuple in the partitioned table's parent physical format, map it to
 * the physical format of a particular partition.
 *
 * parentSlot -- slot containing the input tuple
 * childInfo  -- info pertaining to the target partition
 *
 * If no restructuring is required, 'parentSlot' is returned unmodified.
 * Otherwise, the restructured tuple is stored in childInfo->ri_resultSlot.
 */
TupleTableSlot *
reconstructPartitionTupleSlot(TupleTableSlot *parentSlot,
							  ResultRelInfo *childInfo)
{
	AttrMap	   *map = childInfo->ri_partInsertMap;
	TupleDesc	childDesc = RelationGetDescr(childInfo->ri_RelationDesc);

	/* No map and matching tuple descriptor means no restructuring needed. */
	if (map == NULL && physicalEqualTupleDescs(parentSlot->tts_tupleDescriptor,
											   childDesc))
		return parentSlot;

	/*
	 * Mapping is required. Get the target slot ready.
	 */
	if (!childInfo->ri_resultSlot)
		childInfo->ri_resultSlot = MakeSingleTupleTableSlot(childDesc);

	reconstructMatchingTupleSlot(parentSlot, childInfo->ri_resultSlot, map);

	return childInfo->ri_resultSlot;
}

/*
 * Use the supplied attribute map to restructure a tuple from the
 * input slot to target slot.
 */
void
reconstructMatchingTupleSlot(TupleTableSlot *inputSlot,
							 TupleTableSlot *targetSlot,
							 AttrMap *map)
{
	int			input_natts;
	Datum	   *input_values;
	bool	   *input_isnull;
	Datum	   *target_values;
	bool	   *target_isnull;

	/* Put the given tuple into attribute arrays. */
	input_natts = inputSlot->tts_tupleDescriptor->natts;
	slot_getallattrs(inputSlot);
	input_values = slot_get_values(inputSlot);
	input_isnull = slot_get_isnull(inputSlot);

	targetSlot = ExecStoreAllNullTuple(targetSlot);
	target_values = slot_get_values(targetSlot);
	target_isnull = slot_get_isnull(targetSlot);

	/*
	 * Restructure the input tuple.  Non-zero map entries are attribute
	 * numbers in the target tuple, however, not every attribute
	 * number of the input tuple need be present.  In particular,
	 * attribute numbers corresponding to dropped attributes will be
	 * missing.
	 */
	for (int input_attno = 1; input_attno <= input_natts; input_attno++)
	{
		int			target_attno;

		target_attno = attrMap(map, input_attno);
		if (target_attno == 0)
			continue;

		Assert(target_attno <= targetSlot->tts_tupleDescriptor->natts);
		target_values[target_attno - 1] = input_values[input_attno - 1];
		target_isnull[target_attno - 1] = input_isnull[input_attno - 1];
	}
	(void) ExecStoreVirtualTuple(targetSlot);
}
