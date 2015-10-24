/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include <postgres.h>
#include <executor/spi.h>
#include <executor/tuptable.h>

#include "pljava/type/Type_priv.h"
#include "pljava/type/TupleTable.h"
#include "pljava/type/Tuple.h"
#include "pljava/type/TupleDesc.h"

static jclass    s_TupleTable_class;
static jmethodID s_TupleTable_init;

jobject TupleTable_createFromSlot(TupleTableSlot* tts)
{
	HeapTuple tuple;
	jobject tupdesc;
	jobjectArray tuples;
	MemoryContext curr;

	if(tts == 0)
		return 0;

	curr = MemoryContextSwitchTo(JavaMemoryContext);

	/* Requires Greenplum MemTuple accessors */
	tupdesc = TupleDesc_internalCreate(tts->tts_tupleDescriptor);
	tuple   = ExecCopySlotHeapTuple(tts);
	tuples  = Tuple_createArray(&tuple, 1, false);

	MemoryContextSwitchTo(curr);

	return JNI_newObject(s_TupleTable_class, s_TupleTable_init, tupdesc, tuples);
}

jobject TupleTable_create(SPITupleTable* tts, jobject knownTD)
{
	jobjectArray tuples;
	MemoryContext curr;

	if(tts == 0)
		return 0;

	curr = MemoryContextSwitchTo(JavaMemoryContext);

	if(knownTD == 0)
		knownTD = TupleDesc_internalCreate(tts->tupdesc);

	tuples = Tuple_createArray(tts->vals, (jint)(tts->alloced - tts->free), true);
	MemoryContextSwitchTo(curr);

	return JNI_newObject(s_TupleTable_class, s_TupleTable_init, knownTD, tuples);
}

/* Make this datatype available to the postgres system.
 */
extern void TupleTable_initialize(void);
void TupleTable_initialize(void)
{
	s_TupleTable_class = JNI_newGlobalRef(PgObject_getJavaClass("org/postgresql/pljava/internal/TupleTable"));
	s_TupleTable_init = PgObject_getJavaMethod(
				s_TupleTable_class, "<init>",
				"(Lorg/postgresql/pljava/internal/TupleDesc;[Lorg/postgresql/pljava/internal/Tuple;)V");
}
