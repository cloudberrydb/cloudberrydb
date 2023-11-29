/*-------------------------------------------------------------------------
 *
 * wrapper.c
 *	  Wrap the underlying arrow library
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/backend/utils/arrow/wrapper.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/wrapper.h"
#include "vecexecutor/execslot.h"

void
free_array(void **ptr_array)
{
	Assert(ptr_array);

	/* ARROW_FREE will check whether ptr_array is NULL,
	 * and will set *ptr_array to NULL
	 */
	ARROW_FREE(GArrowArray, ptr_array);
}

void
free_batch(void **ptr_batch)
{
	Assert(ptr_batch);

	ARROW_FREE(GArrowRecordBatch, ptr_batch);
}

void free_array_buffer(void **buffer)
{
	Assert(buffer);

	ARROW_FREE(GArrowBuffer, buffer);
}


void free_glib_obj_misc(void **obj)
{
	Assert(obj);

	if (*obj)
	{
		g_bytes_unref(*obj);
		*obj = NULL;
	}
}

void free_prj_project(void **project)
{
	Assert(project);

	ARROW_FREE(GGandivaProjector, project);
}

void free_gandiva_node(void **gn)
{
	Assert(gn);
	ARROW_FREE(GGandivaNode, gn);
}

void *get_and_reset_auto_ptr(void **ptr)
{
	void *res = *ptr;
	*ptr = NULL;

	return res;
}

void free_array_builder(void **builder)
{
	Assert(builder);

	ARROW_FREE(GArrowArrayBuilder, builder);
}
void free_scalar_datum(void **scalar_datum)
{
	Assert(scalar_datum);

	ARROW_FREE(GArrowScalarDatum, scalar_datum);
}
void free_array_datum(void **array_datum)
{
	Assert(array_datum);

	ARROW_FREE(GArrowArrayDatum, array_datum);
}
