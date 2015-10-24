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

#include "org_postgresql_pljava_internal_Tuple.h"
#include "pljava/Backend.h"
#include "pljava/Exception.h"
#include "pljava/type/Type_priv.h"
#include "pljava/type/Tuple.h"
#include "pljava/type/TupleDesc.h"

static jclass    s_Tuple_class;
static jmethodID s_Tuple_init;

/*
 * org.postgresql.pljava.type.Tuple type.
 */
jobject Tuple_create(HeapTuple ht)
{
	jobject jht = 0;
	if(ht != 0)
	{
		MemoryContext curr = MemoryContextSwitchTo(JavaMemoryContext);
		jht = Tuple_internalCreate(ht, true);
		MemoryContextSwitchTo(curr);
	}
	return jht;
}

jobjectArray Tuple_createArray(HeapTuple* vals, jint size, bool mustCopy)
{
	jobjectArray tuples = JNI_newObjectArray(size, s_Tuple_class, 0);
	while(--size >= 0)
	{
		jobject heapTuple = Tuple_internalCreate(vals[size], mustCopy);
		JNI_setObjectArrayElement(tuples, size, heapTuple);
		JNI_deleteLocalRef(heapTuple);
	}
	return tuples;
}

jobject Tuple_internalCreate(HeapTuple ht, bool mustCopy)
{
	jobject jht;
	Ptr2Long htH;

	if(mustCopy)
		ht = heap_copytuple(ht);

	htH.longVal = 0L; /* ensure that the rest is zeroed out */
	htH.ptrVal = ht;
	jht = JNI_newObject(s_Tuple_class, s_Tuple_init, htH.longVal);
	return jht;
}

static jvalue _Tuple_coerceDatum(Type self, Datum arg)
{
	jvalue result;
	result.l = Tuple_create((HeapTuple)DatumGetPointer(arg));
	return result;
}

/* Make this datatype available to the postgres system.
 */
extern void Tuple_initialize(void);
void Tuple_initialize(void)
{
	TypeClass cls;
	JNINativeMethod methods[] = {
		{
		"_getObject",
	  	"(JJI)Ljava/lang/Object;",
	  	Java_org_postgresql_pljava_internal_Tuple__1getObject
		},
		{
		"_free",
	  	"(J)V",
	  	Java_org_postgresql_pljava_internal_Tuple__1free
		},
		{ 0, 0, 0 }};

	s_Tuple_class = JNI_newGlobalRef(PgObject_getJavaClass("org/postgresql/pljava/internal/Tuple"));
	PgObject_registerNatives2(s_Tuple_class, methods);
	s_Tuple_init = PgObject_getJavaMethod(s_Tuple_class, "<init>", "(J)V");

	cls = JavaWrapperClass_alloc("type.Tuple");
	cls->JNISignature = "Lorg/postgresql/pljava/internal/Tuple;";
	cls->javaTypeName = "org.postgresql.pljava.internal.Tuple";
	cls->coerceDatum  = _Tuple_coerceDatum;
	Type_registerType("org.postgresql.pljava.internal.Tuple", TypeClass_allocInstance(cls, InvalidOid));
}

jobject
Tuple_getObject(TupleDesc tupleDesc, HeapTuple tuple, int index)
{
	jobject result = 0;
	PG_TRY();
	{
		Type type = TupleDesc_getColumnType(tupleDesc, index);
		if(type != 0)
		{
			bool wasNull = false;
			Datum binVal = SPI_getbinval(tuple, tupleDesc, (int)index, &wasNull);
			if(!wasNull)
				result = Type_coerceDatum(type, binVal).l;
		}
	}
	PG_CATCH();
	{
		Exception_throw_ERROR("SPI_getbinval");
	}
	PG_END_TRY();
	return result;
}

/****************************************
 * JNI methods
 ****************************************/
 
/*
 * Class:     org_postgresql_pljava_internal_Tuple
 * Method:    _getObject
 * Signature: (JJI)Ljava/lang/Object;
 */
JNIEXPORT jobject JNICALL
Java_org_postgresql_pljava_internal_Tuple__1getObject(JNIEnv* env, jclass cls, jlong _this, jlong _tupleDesc, jint index)
{
	jobject result = 0;
	Ptr2Long p2l;
	p2l.longVal = _this;

	BEGIN_NATIVE
	HeapTuple self = (HeapTuple)p2l.ptrVal;
	p2l.longVal = _tupleDesc;
	result = Tuple_getObject((TupleDesc)p2l.ptrVal, self, (int)index);
	END_NATIVE
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_Tuple
 * Method:    _free
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_postgresql_pljava_internal_Tuple__1free(JNIEnv* env, jobject _this, jlong pointer)
{
	BEGIN_NATIVE_NO_ERRCHECK
	Ptr2Long p2l;
	p2l.longVal = pointer;
	heap_freetuple(p2l.ptrVal);
	END_NATIVE
}
