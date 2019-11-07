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

#include "access/fileam.h"
#include "cdb/cdbaocsam.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbpartition.h"
#include "commands/trigger.h"
#include "executor/execdebug.h"
#include "executor/execDML.h"
#include "utils/lsyscache.h"
#include "parser/parsetree.h"
#include "cdb/cdbvars.h"

/*
 * reconstructTupleValues
 *   Re-construct tuple values based on the pre-defined mapping.
 */
void
reconstructTupleValues(AttrMap *map,
					Datum *oldValues, bool *oldIsnull, int oldNumAttrs,
					Datum *newValues, bool *newIsnull, int newNumAttrs)
{
	for (int oldAttNo = 1; oldAttNo <= oldNumAttrs; oldAttNo++)
	{
		int newAttNo = attrMap(map, oldAttNo);
		if (newAttNo == 0)
		{
			continue;
		}

		Assert(newAttNo <= newNumAttrs);
		newValues[newAttNo - 1] = oldValues[oldAttNo - 1];
		newIsnull[newAttNo - 1] = oldIsnull[oldAttNo - 1];
	}
}

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
 * Use the supplied ResultRelInfo to create an appropriately restructured
 * version of the tuple in the supplied slot, if necessary.
 *
 * slot -- slot containing the input tuple
 * resultRelInfo -- info pertaining to the target part of an insert
 *
 * If no restructuring is required, the result is the argument slot, else
 * it is the slot from the argument result info updated to hold the
 * restructured tuple.
 */
TupleTableSlot *
reconstructMatchingTupleSlot(TupleTableSlot *slot, ResultRelInfo *resultRelInfo)
{
	int natts;
	Datum *values;
	bool *isnull;
	AttrMap *map;
	TupleTableSlot *partslot;
	Datum *partvalues;
	bool *partisnull;

	map = resultRelInfo->ri_partInsertMap;

	TupleDesc inputTupDesc = slot->tts_tupleDescriptor;
	TupleDesc resultTupDesc = resultRelInfo->ri_RelationDesc->rd_att;
	bool tupleDescMatch = (resultRelInfo->tupdesc_match == 1);
	if (resultRelInfo->tupdesc_match == 0)
	{
		tupleDescMatch = physicalEqualTupleDescs(inputTupDesc, resultTupDesc);

		if (tupleDescMatch)
		{
			resultRelInfo->tupdesc_match = 1;
		}
		else
		{
			resultRelInfo->tupdesc_match = -1;
		}
	}

	/* No map and matching tuple descriptor means no restructuring needed. */
	if (map == NULL && tupleDescMatch)
		return slot;

	/* Put the given tuple into attribute arrays. */
	natts = slot->tts_tupleDescriptor->natts;
	slot_getallattrs(slot);
	values = slot_get_values(slot);
	isnull = slot_get_isnull(slot);

	/*
	 * Get the target slot ready.
	 */
	if (resultRelInfo->ri_resultSlot == NULL)
	{
		resultRelInfo->ri_resultSlot = MakeSingleTupleTableSlot(resultTupDesc);
	}
	partslot = resultRelInfo->ri_resultSlot;

	partslot = ExecStoreAllNullTuple(partslot);
	partvalues = slot_get_values(partslot);
	partisnull = slot_get_isnull(partslot);

	/* Restructure the input tuple.  Non-zero map entries are attribute
	 * numbers in the target tuple, however, not every attribute
	 * number of the input tuple need be present.  In particular,
	 * attribute numbers corresponding to dropped attributes will be
	 * missing.
	 */
	reconstructTupleValues(map, values, isnull, natts,
						partvalues, partisnull, partslot->tts_tupleDescriptor->natts);
	partslot = ExecStoreVirtualTuple(partslot);

	return partslot;
}
