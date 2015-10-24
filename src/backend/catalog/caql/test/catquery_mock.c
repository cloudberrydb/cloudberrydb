#include <stdio.h>
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "../catquery.c"

static const FormData_pg_attribute Desc_pg_class[Natts_pg_class] = {Schema_pg_class};
static const FormData_pg_attribute Desc_pg_attribute[Natts_pg_attribute] = {Schema_pg_attribute};
static const FormData_pg_attribute Desc_pg_proc[Natts_pg_proc] = {Schema_pg_proc};
static const FormData_pg_attribute Desc_pg_type[Natts_pg_type] = {Schema_pg_type};
static const FormData_pg_attribute Desc_pg_index[Natts_pg_index] = {Schema_pg_index};

static TupleDesc 
catcore_get_tuple_desc(Oid relid)
{
	TupleDesc tuple_desc = palloc0(sizeof(struct tupleDesc));
	if (relid == RelationRelationId)
	{
		tuple_desc->natts = Natts_pg_class;
		tuple_desc->attrs = Desc_pg_class;
		tuple_desc->tdhasoid = true;
	}
	else if (relid == AttributeRelationId)
	{
		int i;
		tuple_desc->natts = Natts_pg_attribute;
		tuple_desc->attrs = palloc0(sizeof(Form_pg_attribute) * Natts_pg_attribute);

		for (i = 0; i < tuple_desc->natts; i++)
		{
			tuple_desc->attrs[i] = palloc(ATTRIBUTE_FIXED_PART_SIZE);
			memcpy(tuple_desc->attrs[i], &Desc_pg_attribute[i],
					ATTRIBUTE_FIXED_PART_SIZE);
			tuple_desc->attrs[i]->attcacheoff = -1;
		}
		
		tuple_desc->tdhasoid = false;
	}
	else if (relid == TypeRelationId)
	{
		tuple_desc->natts = Natts_pg_type;
		tuple_desc->attrs = Desc_pg_type;
		tuple_desc->tdhasoid = true;
	}
	tuple_desc->constr = NULL;
	tuple_desc->tdtypeid = InvalidOid;
	tuple_desc->tdtypmod = -1;
	tuple_desc->tdrefcount = -1;

	return tuple_desc;
}

static HeapTuple
build_pg_class_tuple()
{
	Datum values[Natts_pg_class];
	bool nulls[Natts_pg_class];

	TupleDesc tuple_desc = catcore_get_tuple_desc(RelationRelationId);
	memset(nulls, 1, sizeof(nulls));
	return heap_form_tuple(tuple_desc, values, nulls); 
}

static HeapTuple
build_pg_attribute_tuple(Oid attrelid)
{
	Datum values[Natts_pg_attribute];
	bool nulls[Natts_pg_attribute];

	TupleDesc tuple_desc = catcore_get_tuple_desc(AttributeRelationId);
	memset(nulls, 1, sizeof(nulls));

	nulls[0] = false;
	values[0] = ObjectIdGetDatum(attrelid);
	return heap_form_tuple(tuple_desc, values, nulls); 
}

static HeapTuple
build_pg_type_tuple()
{
	Datum values[Natts_pg_type];
	bool nulls[Natts_pg_type];

	TupleDesc tuple_desc = catcore_get_tuple_desc(TypeRelationId);
	memset(nulls, 1, sizeof(nulls));
	return heap_form_tuple(tuple_desc, values, nulls); 
}
