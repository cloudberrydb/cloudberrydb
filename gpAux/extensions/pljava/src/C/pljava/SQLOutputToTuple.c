/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include <postgres.h>
#include "pljava/type/TupleDesc.h"
#include "pljava/type/JavaWrapper.h"
#include "pljava/SQLOutputToTuple.h"

#include "org_postgresql_pljava_jdbc_SQLOutputToTuple.h"

static jclass    s_SQLOutputToTuple_class;
static jmethodID s_SQLOutputToTuple_init;
static jmethodID s_SQLOutputToTuple_getTuple;

jobject SQLOutputToTuple_create(TupleDesc td)
{
	jobject tupleDesc = TupleDesc_create(td);
	jobject result = JNI_newObject(s_SQLOutputToTuple_class, s_SQLOutputToTuple_init, tupleDesc);
	JNI_deleteLocalRef(tupleDesc);
	return result;
}

HeapTuple SQLOutputToTuple_getTuple(jobject sqlOutput)
{
	Ptr2Long p2l;
	if(sqlOutput == 0)
		return 0;

	p2l.longVal = JNI_callLongMethod(sqlOutput, s_SQLOutputToTuple_getTuple);
	if(p2l.longVal == 0)
		return 0;

	return (HeapTuple)p2l.ptrVal;
}

/* Make this datatype available to the postgres system.
 */
extern void SQLOutputToTuple_initialize(void);
void SQLOutputToTuple_initialize(void)
{
	s_SQLOutputToTuple_class = JNI_newGlobalRef(PgObject_getJavaClass("org/postgresql/pljava/jdbc/SQLOutputToTuple"));
	s_SQLOutputToTuple_init = PgObject_getJavaMethod(s_SQLOutputToTuple_class, "<init>", "(Lorg/postgresql/pljava/internal/TupleDesc;)V");
	s_SQLOutputToTuple_getTuple = PgObject_getJavaMethod(s_SQLOutputToTuple_class, "getTuple", "()J");
}
